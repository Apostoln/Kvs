use std::io::{BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpStream};

use log::debug;

use crate::protocol::{ProtocolError, Request, Response};

pub struct Client {
    server_addr: SocketAddr,
}

impl Client {
    pub fn new(server_addr: SocketAddr) -> Client {
        Client { server_addr }
    }

    pub fn send(&self, req: Request) -> Result<Response, ProtocolError> {
        debug!("Request: {:?}", req);
        debug!("Trying to connect to server at {}", self.server_addr);
        let stream = TcpStream::connect(self.server_addr)?;
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
