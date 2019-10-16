use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

use serde_json;

use crate::error::KvError::KeyNotFound;
pub use crate::error::{KvError, Result};

pub struct KvStore {
    storage: HashMap<String, String>,
    log: File,
}

impl KvStore {
    pub fn open<T>(path: T) -> Result<KvStore>
    where
        T: Into<std::path::PathBuf>
    {
        let mut path = path.into();
        path.push("log.log");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let mut buf = String::new();
        let inner_storage = if 0 != file.read_to_string(&mut buf)? {
            serde_json::from_str(&buf)?
        } else {
            HashMap::new()
        };

        Ok(KvStore {
            storage: inner_storage,
            log: file,
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self.storage.get(&key).cloned())
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.storage.insert(key, value);
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.storage.remove(&key).ok_or(KeyNotFound)?;
        Ok(())
    }

    fn save(&mut self) -> Result<()> {
        // Clear storage file
        self.log.set_len(0)?;
        self.log.seek(SeekFrom::Start(0))?;

        // Write new content
        let content = serde_json::to_string(&self.storage)?;
        self.log.write_all(content.as_bytes())?;
        Ok(())
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        if let Err(e) = self.save() {
            panic!("{}", e);
        }
    }
}
