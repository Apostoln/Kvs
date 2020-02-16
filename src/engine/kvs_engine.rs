use super::error::Result;
use std::path::PathBuf;
use std::panic::UnwindSafe;

pub trait KvsEngine : Send + Clone + UnwindSafe + 'static {
    fn open(path: impl Into<PathBuf>) -> Result<Self>;
    fn get(&self, key: String) -> Result<Option<String>>;
    fn set(&self, key: String, value: String) -> Result<()>;
    fn remove(&self, key: String) -> Result<()>;
}
