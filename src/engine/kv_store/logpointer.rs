use std::path::PathBuf;

use crate::engine::Result;
use super::utils::*;

pub enum DataFile {
    Active,
    Passive(u64), //serial number
}

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
            }
            else {
                LogPointer {
                    offset,
                    file: DataFile::Passive(get_serial_number(file_path)?),
                }
            }
        )
    }
}