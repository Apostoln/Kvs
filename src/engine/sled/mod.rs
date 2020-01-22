use crate::{KvsEngine, Result, KvError};

use sled;
use sled::{Tree, Db};

pub struct SledEngine {
    db: Db,
}

impl SledEngine {
    pub fn open<T>(path: T) -> Result<SledEngine>
    where
        T: Into<std::path::PathBuf>,
    {
        let db = sled::open(path.into())?;
        Ok(SledEngine { db })
    }
}

impl KvsEngine for SledEngine {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let tree: &Tree = &self.db;
        Ok(tree
            .get(key)?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let tree: &Tree = &self.db;
        tree.insert(key, value.into_bytes())?; //todo deprecated
        tree.flush()?;
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let tree: &Tree = &self.db;
        tree.remove(key)?.ok_or(KvError::KeyNotFound)?;
        tree.flush()?;
        Ok(())
    }
}