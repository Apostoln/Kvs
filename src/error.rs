use std::result;

use failure::Fail;
use serde_json;

#[derive(Fail, Debug)]
pub enum KvError {
    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "Storage File Error: {}", _0)]
    StorageFileError(#[cause] std::io::Error),

    #[fail(display = "Serde Error: {}", _0)]
    SerdeError(#[cause] serde_json::Error),

    #[fail(display = "Unexpected command")]
    UnexpectedCommand,

    #[fail(display = "Unknown Error: {}", _0)]
    UnknownError(String),
}

impl From<std::io::Error> for KvError {
    fn from(err: std::io::Error) -> KvError {
        KvError::StorageFileError(err)
    }
}

impl From<serde_json::Error> for KvError {
    fn from(err: serde_json::Error) -> KvError {
        KvError::SerdeError(err)
    }
}

impl From<String> for KvError {
    fn from(err: String) -> KvError {
        KvError::UnknownError(err)
    }
}

pub type Result<T> = result::Result<T, KvError>;
