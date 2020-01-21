use crate::Result;

trait KvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn rm(&mut self, key: String, value: String) -> Result<()>;
}