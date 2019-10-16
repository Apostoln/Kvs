use std::result;

use failure::Fail;
use serde_json;

#[derive(Fail, Debug)]
pub enum KvError {
    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "{}", _0)]
    StorageFileError(#[cause] std::io::Error),

    #[fail(display = "{}", _0)]
    SerdeError(#[cause] serde_json::Error),
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

pub type Result<T> = result::Result<T, KvError>;
