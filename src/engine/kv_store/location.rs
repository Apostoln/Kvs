use std::path::PathBuf;

use super::utils::*;

pub enum FileType {
    ACTIVE,
    PASSIVE,
}

impl FileType {
    fn new(file_path: &PathBuf) -> FileType {
        if file_path.file_name().unwrap() == ACTIVE_FILE_NAME {
            FileType::ACTIVE
        } else {
            FileType::PASSIVE
        }
    }
}

pub struct DataFile {
    pub file_type: FileType,
    pub path: PathBuf,
}

impl DataFile {
    pub fn serial_number(&self) -> Option<u64>{
        match self.file_type {
            FileType::ACTIVE => None,
            FileType::PASSIVE => get_serial_number(&self.path).ok()
        }
    }
}

/// Represents the position of the Value on the disk.
/// Describes the type of DataFile: Passive or Active,
/// and offset in bytes from the begin of the file.
pub struct Location {
    pub offset: u64,
    pub file: DataFile,
}

impl Location {
    pub fn new(offset: u64, file_path: &PathBuf) -> Location {
        let file_type = FileType::new(file_path);
        Location {
            offset,
            file: DataFile {
                file_type,
                path: file_path.clone(),
            }
        }
    }
}

impl Into<std::path::PathBuf> for Location {
    fn into(self)-> PathBuf {
        self.file.path
    }
}