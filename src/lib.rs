use std::collections::HashMap;

#[derive(Default)]
pub struct KvStore {
    storage : HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore{storage : HashMap::new()}
    }

    pub fn get(&mut self, key : String) -> Option<String> {
        self.storage.get(&key).cloned()
    }

    pub fn set(&mut self, key : String, value : String) {
        self.storage.insert(key, value);
    }

    pub fn remove(&mut self, key : String) {
        self.storage.remove(&key);
    }
}