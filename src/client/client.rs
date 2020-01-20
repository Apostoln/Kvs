use std::net::{TcpStream, SocketAddr};
use std::io::{Write, Read, BufWriter, BufReader};

use log::{debug, info, warn, error};
use failure::Fail;

use crate::protocol::{Response, Request, ProtocolError};

pub struct Client {
    server_addr: SocketAddr,
}

impl Client {
    pub fn new(server_addr: SocketAddr) -> Client {
        Client{server_addr}
    }

    pub fn send(&self, req: Request) -> Result<Response, ProtocolError> {
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

    pub fn get(&self, key: String) -> Result<Response, ProtocolError> {
        let req = Request::Get { key };
        self.send(req)
    }

    pub fn set(&self, key: String, value: String) -> Result<Response, ProtocolError> {
        let req = Request::Set { key, value };
        self.send(req)
    }

    pub fn rm(&self, key: String) -> Result<Response, ProtocolError> {
        let req = Request::Rm { key };
        self.send(req)
    }
}