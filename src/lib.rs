use std::collections::HashMap;
use std::result;
use std::fs::File;
use std::io::{Write, Read, Seek, SeekFrom};

use serde_json;
use failure::Fail;

use crate::KvError::KeyNotFound;

#[derive(Fail, Debug)]
pub enum KvError {
    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "{}", _0)]
    StorageFileError(#[cause] std::io::Error),

    #[fail(display = "{}", _0)]
    SerdeError(#[cause] serde_json::Error),
}

impl From<std::io::Error> for KvError {
    fn from(err : std::io::Error) -> KvError {
        KvError::StorageFileError(err)
    }
}

impl From<serde_json::Error> for KvError {
    fn from(err : serde_json::Error) -> KvError {
        KvError::SerdeError(err)
    }
}

pub type Result<T> = result::Result<T, KvError>;


pub struct KvStore {
    storage : HashMap<String, String>,
    log : File,
}

impl KvStore {
    pub fn open<T>(path : T) -> Result<KvStore>
    where T: Into<std::path::PathBuf> + std::convert::AsRef<std::path::Path> {
        //todo error handling
        let mut path = path.into();
        path.push("log.log");
        let mut file = std::fs::OpenOptions::new().read(true)
                                                       .write(true)
                                                       .create(true)
                                                       .open(path)?;

        let mut buf = String::new();
        file.read_to_string(&mut buf)?;

        let mut inner_storage : HashMap<String, String> = HashMap::new();
        if !buf.is_empty() { //todo crutch?
            inner_storage = serde_json::from_str(&buf)?;
        }

        Ok(KvStore{storage : inner_storage, log : file})
    }

    pub fn get(&mut self, key : String) -> Result<Option<String>> {
        Ok(self.storage.get(&key).cloned())
    }

    pub fn set(&mut self, key : String, value : String) -> Result<()> {
        self.storage.insert(key, value);
        Ok(())
    }

    pub fn remove(&mut self, key : String) -> Result<()> {
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