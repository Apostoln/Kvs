use std::result;
use std::string::FromUtf8Error;

use failure::Fail;
use log::error;
use serde_json;
use sled;

#[derive(Fail, Debug)]
pub enum KvError {
    #[fail(display = "Key not found")]
    KeyNotFound, // Use in case of removing key, otherwise use Option::None

    #[fail(display = "Storage File Error: {}", _0)]
    StorageFileError(#[cause] std::io::Error),

    #[fail(display = "Serde Error: {}", _0)]
    SerdeError(#[cause] serde_json::Error),

    #[fail(display = "Unexpected command")]
    UnexpectedCommand,

    #[fail(display = "Invalid name of datafile")]
    InvalidDatafileName,

    #[fail(display = "Sled error: {}", _0)]
    SledError(#[cause] sled::Error),

    #[fail(display = "Encoding error: {}", _0)]
    EncodingError(#[cause] FromUtf8Error),

    #[fail(display = "Unknown Error: {}", _0)]
    UnknownError(String),
}

impl From<std::io::Error> for KvError {
    fn from(err: std::io::Error) -> KvError {
        let res = KvError::StorageFileError(err);
        error!("{}", res);
        res
    }
}

impl From<serde_json::Error> for KvError {
    fn from(err: serde_json::Error) -> KvError {
        let res = KvError::SerdeError(err);
        error!("{}", res);
        res
    }
}

impl From<sled::Error> for KvError {
    fn from(err: sled::Error) -> KvError {
        let res = KvError::SledError(err);
        error!("{}", res);
        res
    }
}

impl From<FromUtf8Error> for KvError {
    fn from(err: FromUtf8Error) -> KvError {
        let res = KvError::EncodingError(err);
        error!("{}", res);
        res
    }
}

impl From<String> for KvError {
    fn from(err: String) -> KvError {
        let res = KvError::UnknownError(err);
        error!("{}", res);
        res
    }
}

pub type Result<T> = result::Result<T, KvError>;
