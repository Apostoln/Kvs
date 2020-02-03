use crate::{KvError, KvsEngine, Result};

use sled;
use sled::{Db, Tree};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub struct SledEngine {
    db: Arc<Mutex<Db>>,
}

impl KvsEngine for SledEngine {
    fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let db = Arc::new(Mutex::new(sled::open(path.into())?));
        Ok(SledEngine { db })
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let tree: &Tree = &self.db.lock().unwrap();
        Ok(tree
            .get(key)?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        let tree: &Tree = &self.db.lock().unwrap();
        tree.insert(key, value.into_bytes())?;
        tree.flush()?;
        Ok(())
    }

    fn remove(&self, key: String) -> Result<()> {
        let tree: &Tree = &self.db.lock().unwrap();
        tree.remove(key)?.ok_or(KvError::KeyNotFound)?;
        tree.flush()?;
        Ok(())
    }
}

impl Clone for SledEngine {
    fn clone(&self) -> Self {
        SledEngine{ db: Arc::clone(&self.db) }
    }
}