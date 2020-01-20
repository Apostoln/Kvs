use std::net::{TcpStream, SocketAddr};
use std::io::{Write, Read, BufWriter, BufReader};

use log::{debug, info, warn, error};
use failure::Fail;

use crate::protocol::{Response, Request};

#[derive(Fail, Debug)]
pub enum ClientError { //todo avoid duplicate with ServerError
    #[fail(display = "IO Error: {}", _0)]
    IoError(#[cause] std::io::Error),

    #[fail(display = "Serde Error: {}", _0)]
    SerdeError(#[cause] serde_json::Error),

    #[fail(display = "Unknown Error: {}", _0)]
    UnknownError(String),
}

impl From<std::io::Error> for ClientError {
    fn from(err: std::io::Error) -> ClientError {
        let res = ClientError::IoError(err);
        error!("{}", res);
        res
    }
}

impl From<serde_json::Error> for ClientError {
    fn from(err: serde_json::Error) -> ClientError {
        let res = ClientError::SerdeError(err);
        error!("{}", res);
        res
    }
}

impl From<String> for ClientError {
    fn from(err: String) -> ClientError {
        let res = ClientError::UnknownError(err);
        error!("{}", res);
        res
    }
}

pub struct Client {
    server_addr: SocketAddr,
}

impl Client {
    pub fn new(server_addr: SocketAddr) -> Client {
        Client{server_addr}
    }

    pub fn send(&self, req: Request) -> Result<Response, ClientError> {
        debug!("Request: {:?}", req);
        debug!("Trying to connect to server at {}", self.server_addr);
        let mut stream = TcpStream::connect(self.server_addr)?;
        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        debug!("Client started at {}", stream.local_addr()?);
        debug!("Send request: {:?}", req);
        serde_json::to_writer(&mut writer, &req)?;
        writer.flush()?;
        Ok(serde_json::from_reader(reader)?)
    }

    pub fn get(&self, key: String) -> Result<Response, ClientError> {
        let req = Request::Get { key };
        self.send(req)
    }

    pub fn set(&self, key: String, value: String) -> Result<Response, ClientError> {
        let req = Request::Set { key, value };
        self.send(req)
    }

    pub fn rm(&self, key: String) -> Result<Response, ClientError> {
        let req = Request::Rm { key };
        self.send(req)
    }

}