use std::collections::{HashMap, BTreeMap};
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json;

use crate::error::KvError::{KeyNotFound, UnexpectedCommand};
pub use crate::error::{KvError, Result};

const ACTIVE_FILE_NAME: &'static str = "log.active";
const ACTIVE_EXT: &'static str = "active";
const PASSIVE_EXT: &'static str = "passive";
const RECORDS_LIMIT: u64 = 100;
const RECORDS_IN_COMPACTED: usize = 100;

#[derive(Serialize, Deserialize, Debug, Clone)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

type Index = HashMap<String, LogPointer>;

trait DataFileGetter {
    fn get_inner(&mut self) -> (&PathBuf, &mut BufReader<File>);
    fn get_path(&self) -> &PathBuf;
    fn get_reader(&mut self) -> &mut BufReader<File>;
}

#[derive(Debug)]
struct PassiveFile {
    path: PathBuf,
    reader: BufReader<File>,
}

impl PassiveFile {
    fn new<T>(path: T) -> Result<PassiveFile>
    where
        T: Into<std::path::PathBuf>,
    {
        let mut path = path.into();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(&mut path)?;
        let reader = BufReader::new(file);
        Ok(PassiveFile { path, reader })
    }

    /// Create PassiveFile from commands and path
    /// Create new passive file on `path` and write commands to this file.
    ///
    /// Note: There must be no passive file with name `path` before calling this function
    fn from_commands(commands: Vec<Result<Command>>, mut path: PathBuf) -> Result<PassiveFile> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&mut path)?;
        let mut writer = BufWriter::new(file.try_clone()?);
        let mut reader = BufReader::new(file.try_clone()?);

        for cmd in commands {
            serde_json::to_writer(&mut writer, &cmd?)?;
        }
        writer.flush()?;
        Ok(PassiveFile{ path, reader })
    }
}

impl DataFileGetter for PassiveFile {
    fn get_inner(&mut self) -> (&PathBuf, &mut BufReader<File>) {
        (&self.path, &mut self.reader)
    }
    fn get_path(&self) -> &PathBuf {
        &self.path
    }
    fn get_reader(&mut self) -> &mut BufReader<File> {
        &mut self.reader
    }
}

#[derive(Debug)]
struct ActiveFile {
    path: PathBuf,
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

impl ActiveFile {
    fn new<T>(path: T) -> Result<ActiveFile>
    where
        T: Into<std::path::PathBuf>,
    {
        let mut path = path.into();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&mut path)?;
        let reader = BufReader::new(file.try_clone()?);
        let writer = BufWriter::new(file.try_clone()?);

        Ok(ActiveFile {
            path,
            reader,
            writer,
        })
    }
}

impl DataFileGetter for ActiveFile {
    fn get_inner(&mut self) -> (&PathBuf, &mut BufReader<File>) {
        (&self.path, &mut self.reader)
    }
    fn get_path(&self) -> &PathBuf {
        &self.path
    }
    fn get_reader(&mut self) -> &mut BufReader<File> {
        &mut self.reader
    }
}

/*
struct IndexFile {
    path: PathBuf,
    reader: BufReader<File>,
    passive_number: u64, // Or reference?
}*/

pub struct Log {
    active: ActiveFile,
    passive: BTreeMap<u64, PassiveFile>,
    dir_path: PathBuf,
    //indexes: Vec<IndexFile>,
}

impl Log {
    fn open<T>(dir_path: T) -> Result<Log>
    where
        T: Into<std::path::PathBuf>,
    {
        let dir_path = dir_path.into();
        let mut passive_files = dir_path
            .read_dir()?
            .filter_map(std::result::Result::ok)
            .map(|file| file.path())
            .filter(|path| path.is_file() && path.extension().map_or(false, |ext| ext == PASSIVE_EXT))
            .map(|path| -> Result<(u64, PassiveFile)>{
                Ok((get_serial_number(&path)?, PassiveFile::new(path)?))
            })
            .collect::<Result<_>>()?;

        let mut active_file_path = dir_path.clone();
        active_file_path.push(ACTIVE_FILE_NAME);
        let active_file = ActiveFile::new(active_file_path)?;

        Ok(Log {
            active: active_file,
            passive: passive_files,
            dir_path,
        })
    }

    fn set_passive(&mut self, passive: BTreeMap<u64, PassiveFile>) -> Result<()> {
        self.passive = passive;
        Ok(())
    }

    fn dump(&mut self) -> Result<()> {
        if self.active.reader.get_mut().metadata()?.len() == 0 {
            // File is already empty, nothing to do here
            return Ok(());
        }

        // Rename current ACTIVE_FILE_NAME to serial_number.passive
        let serial_number = self.passive
            .values_mut()
            .next_back() //option here
            .map_or(Ok(0), |file| get_serial_number(&file.path))?
            + 1;
        let mut new_path = self.dir_path.clone();
        new_path.push(format!("{}.{}", serial_number, PASSIVE_EXT));
        fs::rename(&self.active.path, &mut new_path)?;

        // Move old active file to passives and create new active
        self.passive.insert(serial_number, PassiveFile::new(new_path)?);
        self.active = ActiveFile::new(ACTIVE_FILE_NAME)?;
        Ok(())
    }
}

struct LogPointer {
    offset: u64,
    file: PathBuf, //todo or just Path? //or serial number?
}

pub struct KvStore {
    index: Index,
    log: Log,
    unused_records: u64,
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
        })
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

        if let Some(_) = self.index.insert(key, LogPointer { file: self.log.active.path.clone(), offset: pos}) {
            //avoid cloning?
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
        //todo needed?
        self.log.dump()?;
        self.reindex()?; //todo rly here?

        // Create backup
        // Disabled due to "compaction" test failing/
        // Todo create backup_dir not as subdir of log dir
        /*
        let mut backup_dir = self.path.clone();
        let time = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros(); // Note: Error while creating directory due to equal names if time duration is too big.
        //Todo add serial number to backup name
        backup_dir.push(format!("precompact_backup_{0}", time));
        self.backup(&backup_dir)?;
        */

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

    /// Write saved in memory actual commands to new passive files
    /// Split commands to chunks of `RECORDS_IN_COMPACTED` elements
    /// and write each chunk to new passive file in log directory.
    /// Collect passive files to BTreeMap and set it to log.
    ///
    /// Note: There must be no passive files in log directory before calling this function
    fn write_actual_commands(&mut self, mut commands: Vec<Result<Command>>) -> Result<()> {
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
                    index.insert(key, LogPointer { offset: pos, file: path.to_owned() });
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

    fn read_command(log: &mut Log, log_ptr: &LogPointer) -> Result<Command> {
        let file_path = &log_ptr.file;
        let offset = log_ptr.offset;

        let reader = if file_path.file_name().unwrap() == ACTIVE_FILE_NAME {
            &mut log.active.reader
        } else {
            &mut log
                .passive
                .get_mut(&get_serial_number(file_path)?)
                .unwrap()
                .reader
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

fn get_serial_number(path: &PathBuf) -> Result<u64> {
    path.file_stem()
        .and_then(|name| name.to_str())
        .ok_or(KvError::InvalidDatafileName)?
        .parse::<u64>()
        .or(Err(KvError::InvalidDatafileName))
}
