use std::collections::{HashMap, BTreeMap};
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json;

use crate::error::KvError::{KeyNotFound, UnexpectedCommand};
use crate::log::Log;
use crate::logpointer::*;
use crate::datafile::*;
use crate::utils::*;

pub use crate::error::{KvError, Result};
use std::time::UNIX_EPOCH;

const RECORDS_LIMIT: u64 = 100;
const RECORDS_IN_COMPACTED: usize = 100;

#[derive(Serialize, Deserialize, Debug, Clone)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

type Index = HashMap<String, LogPointer>;

pub struct KvStore {
    index: Index,
    log: Log,
    unused_records: u64,
    backups_dir: Option<PathBuf>,
}

impl KvStore {
    pub fn open<T>(path: T) -> Result<KvStore>
    where
        T: Into<std::path::PathBuf>,
    {
        let path = path.into();
        let mut log = Log::open(&path)?;
        let index = KvStore::index(&mut log)?;

        Ok(KvStore {
            index,
            log,
            unused_records: 0,
            backups_dir: None,
        })
    }

    pub fn set_backups_dir<T>(&mut self, path: T)
        where
            T: Into<std::path::PathBuf>,
    {
        self.backups_dir = Some(path.into());
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let log = &mut self.log;
        self.index
            .get(&key)
            .map_or(
                Ok(None),
                |offset| {
                    match KvStore::read_command(log, offset)? {
                        Command::Set { value, .. } => Ok(Some(value)),
                        Command::Remove { .. } => Err(UnexpectedCommand),
            }
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let mut writer = &mut self.log.active.writer;
        let pos = writer.seek(SeekFrom::Current(0))?;

        let cmd = Command::Set { key: key.clone(), value };

        serde_json::to_writer(&mut writer, &cmd)?;
        writer.flush()?;

        if let Some(_) = self.index.insert(key, LogPointer { file: DataFile::Active, offset: pos }) {
            self.unused_records += 1;
        }

        if self.unused_records > RECORDS_LIMIT {
            self.compact()?;
            self.reindex()?;
            self.unused_records = 0;
        }

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let mut writer = &mut self.log.active.writer;

        self.index.remove(&key).ok_or(KeyNotFound)?;

        let cmd = Command::Remove { key };

        serde_json::to_writer(&mut writer, &cmd)?;
        writer.flush()?;

        self.unused_records += 1;
        Ok(())
    }

    fn reindex(&mut self) -> Result<()> {
        self.index = KvStore::index(&mut self.log)?;
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        self.log.dump()?;
        self.reindex()?;

        // Create backup if specified
        if let Some(backups_dir) = &self.backups_dir {
            let mut backup_dir = backups_dir.clone();
            let time = std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros(); // Note: Error while creating directory due to equal names if time duration is too big.
            backup_dir.push(format!("precompact_backup_{0}", time));
            self.backup(&backup_dir)?;
        }

        // Read actual commands
        let commands = self.read_actual_commands();

        // Remove current passive files
        for passive in &mut self.log.passive.values_mut() {
            std::fs::remove_file(&mut passive.path)?;
        }

        // Create new passive files and write actual commands to them,
        // then replace old passive files to new in self.log
        self.write_actual_commands(commands)?;

        Ok(())
    }

    fn backup(&mut self, mut backup_dir: &PathBuf) -> Result<()> {
        fs::create_dir(&mut backup_dir)?;

        for passive in self.log.passive.values_mut() {
            let mut new_file = backup_dir.clone();
            new_file.push(passive.path.file_name().unwrap());
            fs::copy(&passive.path, new_file)?;
        }
        Ok(())
    }

    fn read_actual_commands(&mut self) -> Vec<Result<Command>> {
        let log = &mut self.log;
        self.index
            .values()
            .map(|log_ptr| -> Result<Command> {
                match KvStore::read_command(log, log_ptr)? {
                    Command::Set { key, value } => Ok(Command::Set { key, value }),
                    _ => Err(UnexpectedCommand),
                }
            })
            .collect()
    }

    /// Write saved in memory actual commands to new passive files. Split commands to chunks
    /// of `RECORDS_IN_COMPACTED` elements and write each chunk to new passive file in log directory.
    /// Collect passive files to BTreeMap and set it to log.
    ///
    /// Note: There must be no passive files in log directory before calling this function
    fn write_actual_commands(&mut self, mut commands: Vec<Result<Command>>) -> Result<()> { //todo move to log?
        let mut passive_files: BTreeMap<u64, PassiveFile> = BTreeMap::new();

        let mut counter: u64 = 1;
        let commands = &mut commands;
        while !commands.is_empty() {
            let chunk = std::iter::from_fn(|| commands.pop())
                .take(RECORDS_IN_COMPACTED)
                .collect::<Vec<_>>();
            let mut path = self.log.dir_path.clone();
            path.push(format!("{}.{}", counter, PASSIVE_EXT));

            let passive_file = PassiveFile::from_commands(chunk, path)?;
            passive_files.insert(counter, passive_file);

            counter += 1;
        }

        self.log.set_passive(passive_files)?;
        Ok(())
    }

    fn index_datafile<T>(index: &mut Index, datafile: &mut T) -> Result<()>
    where
        T: DataFileGetter,
    {
        let (path, reader) = datafile.get_inner();
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter();
        while let Some(item) = stream.next() {
            match item? {
                Command::Set { key, .. } => {
                    index.insert(key, LogPointer::new(pos, path)?);
                }
                Command::Remove { key } => {
                    index.remove(&key).unwrap();
                }
            }
            pos = stream.byte_offset() as u64;
        }
        Ok(())
    }

    fn index(log: &mut Log) -> Result<Index> {
        let mut index = Index::new();

        for passive in &mut log.passive.values_mut() {
            KvStore::index_datafile(&mut index, passive)?;
        }

        let active = &mut log.active;
        KvStore::index_datafile(&mut index, active)?;

        Ok(index)
    }

    fn read_command(log: &mut Log, log_ptr: &LogPointer) -> Result<Command> { //todo move to log?
        let offset = log_ptr.offset;

        let reader = match log_ptr.file {
            DataFile::Active => &mut log.active.reader,
            DataFile::Passive(serial_number) => &mut log
                .passive
                .get_mut(&serial_number)
                .unwrap()
                .reader,
        };

        reader.seek(SeekFrom::Start(offset))?;

        Ok(serde_json::Deserializer::from_reader(reader)
            .into_iter()
            .next()
            .unwrap()?)
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        if let Err(e) = self.compact() {
            panic!("Error while dropping KvStore: {}", e);
        }
    }
}
