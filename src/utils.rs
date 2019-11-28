use std::path::PathBuf;

use crate::error::{Result, KvError};

pub const ACTIVE_FILE_NAME: &'static str = "log.active";
pub const PASSIVE_EXT: &'static str = "passive";

pub fn get_serial_number(path: &PathBuf) -> Result<u64> {
    path.file_stem()
        .and_then(|name| name.to_str())
        .ok_or(KvError::InvalidDatafileName)?
        .parse::<u64>()
        .or(Err(KvError::InvalidDatafileName))
}
