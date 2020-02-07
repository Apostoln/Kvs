use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use std::sync::{Arc, atomic::AtomicU64, atomic::Ordering, Mutex};

use log::debug;
use serde::{Deserialize, Serialize};

use super::log::Log;
use super::location::*;
use crate::engine::{
    KvError::KeyNotFound,
    KvError::UnexpectedCommand,
    KvsEngine,
    Result
};
use crate::engine::kv_store::utils::{PASSIVE_EXT, ACTIVE_FILE_NAME};

/// Max number of records in one data file.
/// Compaction will be triggered after exceeding.
const RECORDS_LIMIT: u64 = 1024; //todo make configurable

/// Record in storage
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Record {
    Set { key: String, value: String },
    Remove { key: String },
}

/// A map that associates a Key with position of its Value on the disk.
/// Index is used to get values faster.
pub type Index = HashMap<String, Location>;

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
    index: Arc<Mutex<Index>>, //Arc<RwLock>
    log: Arc<Log>,
    unused_records: Arc<AtomicU64>, //Arc<AtomicU64>
    backups_dir: Option<PathBuf>,
}

impl KvsEngine for KvStore {
    /// Open a `KvStore` with the given path.
    fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        debug!("Open KvStore, path: {:?}", path);

        let mut log = Log::open(&path)?;
        let index = log.index()?;

        let index = Arc::new(Mutex::new(index));
        let log = Arc::new(log);

        Ok(KvStore {
            index,
            log,
            unused_records: Arc::new(AtomicU64::new(0)),
            backups_dir: None,
        })
    }

    /// Get the value of a given key.
    /// Returns `None` if the given key does not exist.
    fn get(&self, key: String) -> Result<Option<String>> {
        debug!("Get key: {}", key);
        self.index
            .lock().unwrap()
            .get(&key)
            .map_or(
                Ok(None),
                |location| {
                    match self.log.get_record(location)? {
                        Record::Set { value, .. } => Ok(Some(value)),
                        Record::Remove { .. } => Err(UnexpectedCommand),
                    }
                })
    }

    /// Set the key and value
    fn set(&self, key: String, value: String) -> Result<()> {
        debug!("Set key: {}, value: {}", key, value);
        let cmd = Record::Set { key: key.clone(), value };
        let location = self.log.set_record(&cmd)?;

        let prev_location = self.index.lock().unwrap().insert(key, location);
        if let Some(_) = prev_location {
            self.unused_records.fetch_add(1, Ordering::SeqCst);
            debug!("Increased unused records: {}", self.unused_records.load(Ordering::SeqCst));
            //todo threads sync and atomic usage
            if self.unused_records.load(Ordering::SeqCst) > RECORDS_LIMIT {
                debug!("Unused records exceeds records limit({}). Compaction triggered", RECORDS_LIMIT);
                self.compact_log()?;
                self.unused_records.store(0, Ordering::SeqCst);
            }
        }

        Ok(())
    }

    /// Remove a given key.
    /// # Error
    /// It returns `KvError::KeyNotFound` if the given key is not found.
    fn remove(&self, key: String) -> Result<()> {
        debug!("Remove key: {}", key);
        let cmd = Record::Remove { key: key.clone() };
        self.log.set_record(&cmd)?;
        self.index
            .lock()
            .unwrap()
            .remove(&key)
            .ok_or(KeyNotFound)?;
        self.unused_records.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl KvStore {
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
    fn reindex(&self) -> Result<()> {
        debug!("Reindex");
        *self.index.lock().unwrap() = self.log.index()?;
        Ok(())
    }

    /// Compact the `Log`.
    /// Compaction is the process of removing deprecated records from passive datafiles of `Log`.
    /// Old passive datafiles will be replaced by new ones with only actual records.
    /// Backup will be created if specified.
    fn compact_log(&self) -> Result<()> {
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
    fn backup(&self, backup_dir: &PathBuf) -> Result<()> {
        debug!("Backup, path: {:?}", backup_dir);
        fs::create_dir(&backup_dir)?;

        for serial_number in 1..self.log.last_serial_number.load(Ordering::SeqCst) {
            let file_name = format!("{}.{}", serial_number, PASSIVE_EXT);
            let old_path = self.log.dir_path.join(&file_name);
            let new_path = backup_dir.join(&file_name);
            fs::copy(&old_path, &new_path)?;
        }

        Ok(())
    }

    /// Return actual commands from `Log`.
    fn actual_commands(&self) -> Vec<Result<Record>> {
        debug!("Get actual commands");
        self.index
            .lock()
            .unwrap()
            .values()
            .map(|location| -> Result<Record> {
                match self.log.get_record(location)? {
                    Record::Set { key, value } => Ok(Record::Set { key, value }),
                    _ => Err(UnexpectedCommand),
                }
            })
            .collect()
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

impl Clone for KvStore {
    fn clone(&self) -> Self {
        KvStore {
            index: Arc::clone(&self.index),
            log: Arc::clone(&self.log),
            unused_records: Arc::clone(&self.unused_records),
            backups_dir: self.backups_dir.clone(),
        }
    }

}
