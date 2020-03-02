use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use std::sync::{Arc, atomic::AtomicU64, atomic::Ordering, Mutex};

use lockfree;
use log::{debug, warn};
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

/// A lock-free hashmap that associates a Key with location (position on the disk) of its Value.
/// Index is used to get values faster.
pub type Index = lockfree::map::Map<String, Location>;


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
    index: Arc<Index>,
    log: Arc<Log>,
    unused_records: Arc<Mutex<u64>>, //todo replace to atomic and rework synchronization during compact()
    backups_dir: Option<PathBuf>,
}

impl KvsEngine for KvStore {
    /// Open a `KvStore` with the given path.
    fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        debug!("Open KvStore, path: {:?}", path);

        let log = Arc::new(Log::open(&path)?);
        let index = Arc::new(log.index()?);

        Ok(KvStore {
            index,
            log,
            unused_records: Arc::new(Mutex::new(0)),
            backups_dir: None,
        })
    }

    /// Get the value of a given key.
    /// Returns `None` if the given key does not exist.
    fn get(&self, key: String) -> Result<Option<String>> {
        debug!("Get key: {}", key);
        self.index
            .get(&key)
            .map_or(
                Ok(None),
                |pair| {
                    match self.log.get_record(pair.val())? {
                        Record::Set { value, .. } => Ok(Some(value)),
                        Record::Remove { .. } => Err(UnexpectedCommand), //todo rly?
                    }
                })
    }

    /// Set the key and value
    fn set(&self, key: String, value: String) -> Result<()> {
        debug!("Set key: {}, value: {}", key, value);
        let cmd = Record::Set { key: key.clone(), value };
        let location = self.log.set_record(&cmd)?;

        let prev_location = self.index.insert(key, location);
        if let Some(_) = prev_location {
            let mut unused_records = self.unused_records.lock().unwrap();
            *unused_records += 1;
            debug!("Increased unused records: {}", *unused_records);
            if *unused_records > RECORDS_LIMIT {
                debug!("Unused records exceeds records limit({}). Compaction triggered", RECORDS_LIMIT);
                self.compact_log()?;
                *unused_records = 0;
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
            .remove(&key)
            .ok_or(KeyNotFound)?;
        *self.unused_records.lock().unwrap() += 1;
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

    /// Dump active file to passive and update index
    fn dump_log(&self) -> Result<()> {
        self.log.dump()?;
        // Change location in index items from ActiveFile to last PassiveFile after dumping to guarantee
        // invariants of Index and avoid fully reindexing like
        // self.reindex_log()?;
        self.index
            .iter()
            .filter(|pair| pair.val().file.path == self.log.active_file_path)
            .for_each(|index_item| {
                let serial_number = self.log.last_serial_number.load(Ordering::SeqCst);
                let file_path = self.log.passive_path(serial_number);
                let location = Location::new(index_item.val().offset, &file_path);
                if let None = self.index.insert(index_item.key().clone(), location) {
                    warn!("Maybe invariant are broken during partition reindexing after dumping")
                }
            });

        Ok(())
    }

    /// Reindex datafiles.
    fn reindex_log(&self) -> Result<()> {
        debug!("Reindex log of KvStore");
        self.log.reindex(&self.index)
    }

    /// Compact the `Log`.
    /// Compaction is the process of removing deprecated records from passive datafiles of `Log`.
    /// Old passive datafiles will be replaced by new ones with only actual records.
    /// Backup will be created if specified.
    fn compact_log(&self) -> Result<()> {
        debug!("Compact log");
        self.dump_log()?; //todo dumping is unnecessary here?
        // todo bug with race condition here - other thread will read by incorrect path of active path
        // todo make some kind of global lock for compacting (with channels/condvar/barrier/wait_group/etc)

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
        self.reindex_log()?; //todo implement indexfile for faster indexing of already compacted files

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
            .iter()
            .map(|pair| -> Result<Record> {
                match self.log.get_record(pair.val())? {
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
        // We must compact the log only if we drop the last ("main") instance of KvStore.
        // Thus if self.log has only one instance then the whole KvStore has only one instance.
        // Arc::get_mut() returns Some(_) only if there are no other `Arc` or `Weak`
        // pointers to the same allocation.
        if let Some(_) = Arc::get_mut(&mut self.log) {
            if let Err(e) = self.compact_log() {
                panic!("Error of compaction while dropping KvStore: {}", e);
            }
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
