use std::path::PathBuf;

use crate::engine::{KvError, Result};

pub const ACTIVE_FILE_NAME: &'static str = "log.active";
pub const PASSIVE_EXT: &'static str = "passive";
pub const RECORDS_IN_COMPACTED: usize = 100;

/// Get serial number from name of passive file
///
/// # Examples:
///
/// ```
/// use std::path::Path;
/// assert_eq!(Ok(42), get_serial_number(Path::from("42.passive").to_path_buf()));
/// ```
pub fn get_serial_number(path: &PathBuf) -> Result<u64> {
    path.file_stem()
        .and_then(|name| name.to_str())
        .ok_or(KvError::InvalidDatafileName)?
        .parse::<u64>()
        .or(Err(KvError::InvalidDatafileName))
}
