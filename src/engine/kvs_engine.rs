use super::error::Result;

pub trait KvsEngine : Send {
    fn get(&self, key: String) -> Result<Option<String>>;
    fn set(&self, key: String, value: String) -> Result<()>;
    fn remove(&self, key: String) -> Result<()>;
}
