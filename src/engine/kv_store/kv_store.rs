use std::collections::HashMap;
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use log::debug;
use serde::{Deserialize, Serialize};
use serde_json;

use super::datafile::*;
use super::log::Log;
use super::logpointer::*;
use crate::engine::{
    KvError::KeyNotFound,
    KvError::UnexpectedCommand,
    KvsEngine,
    Result
};

/// Max number of records in one data file.
/// Compaction will be triggered after exceeding.
const RECORDS_LIMIT: u64 = 1024; //todo make configurable

/// Record in storage
#[derive(Serialize, Deserialize, Debug, Clone)]
enum Command { //todo rename to Record?
    Set { key: String, value: String },
    Remove { key: String },
}

/// A map that associates a Key with position of its Value on the disk.
/// Index is used to get values faster.
type Index = HashMap<String, LogPointer>;

/// `KvStore` is a log-based storage engine that stores a pairs Key/Value.
/// The `Log` is a persistent sequence of records on disk, that represents commands to storage like `Set` or `Remove`.
/// All records are written to the end of the log. After updating or removing value from storage,
/// related to this value records are not removed from log. Instead, new records are written to the end of the log.
///
/// # Example:
/// ```rust
/// use kvs::KvStore;
/// let mut storage = KvStore::open(std::env::current_dir().unwrap()).unwrap();
/// storage.set("Key".to_string(), "Value".to_string());
/// assert_eq!(storage.get("Key".to_string()).unwrap(), Some("Value".to_string()));
/// ```
pub struct KvStore {
    index: Index,
    log: Log,
    unused_records: u64,
    backups_dir: Option<PathBuf>,
}

impl KvStore {
    /// Open a `KvStore` with the given path.
    pub fn open<T>(path: T) -> Result<KvStore>
    where
        T: Into<std::path::PathBuf>,
    {
        let path = path.into();
        debug!("Open KvStore, path: {:?}", path);

        let mut log = Log::open(&path)?;
        let index = KvStore::index(&mut log)?;

        Ok(KvStore {
            index,
            log,
            unused_records: 0,
            backups_dir: None,
        })
    }

    /// Set path for saving backups.
    pub fn set_backups_dir<T>(&mut self, path: T)
    where
        T: Into<std::path::PathBuf>,
    {
        let path = path.into();
        debug!("Set new backup directory: {:?}", path);
        self.backups_dir = Some(path);
    }

    /// Reindex datafiles.
    fn reindex(&mut self) -> Result<()> {
        debug!("Reindex");
        self.index = KvStore::index(&mut self.log)?;
        Ok(())
    }

    /// Compact the `Log`.
    /// Compaction is the process of removing deprecated records from passive datafiles of `Log`.
    /// Old passive datafiles will be replaced by new ones with only actual records.
    /// Backup will be created if specified.
    fn compact_log(&mut self) -> Result<()> {
        debug!("Compact log");
        self.log.dump()?; //todo dumping is unnecessary here?
        self.reindex()?;

        // Create backup if specified
        if let Some(backups_dir) = &self.backups_dir {
            debug!("Backup triggered, backups directory: {:?}", backups_dir);
            let time = std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros(); // Note: Error while creating directory due to equal names if time duration is too big.
            let backup_dir = backups_dir.join(format!("pre_compact_backup_{0}", time));
            self.backup(&backup_dir)?;
        }

        // Read actual commands
        let commands = self.actual_commands();

        // Create new passive files and write actual commands to them,
        // then replace old passive files to new in self.log
        self.log.compact(commands)?;
        self.reindex()?;

        Ok(())
    }

    /// Copy passive datafiles of `Log` to specified directory.
    fn backup(&mut self, mut backup_dir: &PathBuf) -> Result<()> {
        debug!("Backup, path: {:?}", backup_dir);
        fs::create_dir(&mut backup_dir)?;

        for passive in self.log.passive.values_mut() {
            let new_file = backup_dir.join(passive.path.file_name().unwrap());
            fs::copy(&passive.path, new_file)?;
        }
        Ok(())
    }

    /// Return actual commands from `Log`.
    fn actual_commands(&mut self) -> Vec<Result<Command>> {
        //todo move to Log module?
        debug!("Get actual commands");
        let log = &mut self.log; //Seems moving this method to Log is impossible due to borrow-checkers error here
        self.index
            .values()
            .map(|log_ptr| -> Result<Command> {
                match log.get_record(log_ptr)? {
                    Command::Set { key, value } => Ok(Command::Set { key, value }),
                    _ => Err(UnexpectedCommand),
                }
            })
            .collect()
    }

    /// Index actual records from specified datafile.
    fn index_datafile(index: &mut Index, datafile: &mut impl DataFileGetter) -> Result<()> {
        let (path, reader) = datafile.get_inner();
        debug!("Index datafile: {:?}", path);
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

    /// Index active and passive datafiles from `Log`.
    fn index(log: &mut Log) -> Result<Index> {
        debug!("Index log");
        let mut index = Index::new();

        for passive in &mut log.passive.values_mut() {
            KvStore::index_datafile(&mut index, passive)?;
        }

        let active = &mut log.active;
        KvStore::index_datafile(&mut index, active)?;

        Ok(index)
    }
}

impl KvsEngine for KvStore {
    /// Get the value of a given key.
    /// Returns `None` if the given key does not exist.
    fn get(&self, key: String) -> Result<Option<String>> {
        debug!("Get key: {}", key);
        let log = &mut self.log;
        self.index
            .get(&key)
            .map_or(
                Ok(None),
                |log_ptr| {
                    match log.get_record(log_ptr)? {
                        Command::Set { value, .. } => Ok(Some(value)),
                        Command::Remove { .. } => Err(UnexpectedCommand),
                    }
                })
    }

    /// Set the key and value
    fn set(&self, key: String, value: String) -> Result<()> {
        debug!("Set key: {}, value: {}", key, value);
        let mut writer = &mut self.log.active.writer;
        let pos = writer.seek(SeekFrom::Current(0))?;

        let cmd = Command::Set { key: key.clone(), value };

        serde_json::to_writer(&mut writer, &cmd)?;
        writer.flush()?;

        if let Some(_) = self.index.insert(key, LogPointer { file: DataFile::Active, offset: pos }) {
            self.unused_records += 1;
            debug!("Increased unused records: {}", self.unused_records);
        }

        if self.unused_records > RECORDS_LIMIT {
            debug!("Unused records {} exceeds records limit {}. Compaction triggered",
                   self.unused_records,
                   RECORDS_LIMIT);
            self.compact_log()?;
            self.reindex()?;
            self.unused_records = 0;
        }

        Ok(())
    }

    /// Remove a given key.
    /// # Error
    /// It returns `KvError::KeyNotFound` if the given key is not found.
    fn remove(&self, key: String) -> Result<()> {
        debug!("Remove key: {}", key);

        let mut writer = &mut self.log.active.writer;

        self.index.remove(&key).ok_or(KeyNotFound)?;

        let cmd = Command::Remove { key };

        serde_json::to_writer(&mut writer, &cmd)?;
        writer.flush()?;

        self.unused_records += 1;
        Ok(())
    }
}

impl Drop for KvStore {
    /// Compact the log.
    fn drop(&mut self) {
        debug!("Drop KvStore");
        if let Err(e) = self.compact_log() {
            panic!("Error while dropping KvStore: {}", e);
        }
    }
}
