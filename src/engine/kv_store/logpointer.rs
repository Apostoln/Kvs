use std::path::PathBuf;

use super::utils::*;
use crate::engine::Result;

pub enum DataFile {
    Active,
    Passive(u64), //serial number
}

/// Represents the position of the Value on the disk.
/// Describes the type of DataFile: Passive or Active,
/// and offset in bytes from the begin of the file.
pub struct LogPointer {
    pub offset: u64,
    pub file: DataFile,
}

impl LogPointer {
    pub fn new(offset: u64, file_path: &PathBuf) -> Result<LogPointer> {
        Ok(
            if file_path.file_name().unwrap() == ACTIVE_FILE_NAME {
                LogPointer {
                    offset,
                    file: DataFile::Active,
                }
            } else {
                LogPointer {
                    offset,
                    file: DataFile::Passive(get_serial_number(file_path)?),
                }
            }
        )
    }
}
