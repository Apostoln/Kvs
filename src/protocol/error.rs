use failure::Fail;
use log::error;

#[derive(Fail, Debug)]
pub enum ProtocolError {
    #[fail(display = "IO Error: {}", _0)]
    IoError(#[cause] std::io::Error),

    #[fail(display = "Serde Error: {}", _0)]
    SerdeError(#[cause] serde_json::Error),

    #[fail(display = "Unknown Error: {}", _0)]
    UnknownError(String),
}

impl From<std::io::Error> for ProtocolError {
    fn from(err: std::io::Error) -> ProtocolError {
        let res = ProtocolError::IoError(err);
        error!("{}", res);
        res
    }
}

impl From<serde_json::Error> for ProtocolError {
    fn from(err: serde_json::Error) -> ProtocolError {
        let res = ProtocolError::SerdeError(err);
        error!("{}", res);
        res
    }
}

impl From<String> for ProtocolError {
    fn from(err: String) -> ProtocolError {
        let res = ProtocolError::UnknownError(err);
        error!("{}", res);
        res
    }
}
