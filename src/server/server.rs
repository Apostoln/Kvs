use std::net::{SocketAddr, TcpListener};
use std::io;
use std::io::{Read, Write, BufReader, BufWriter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json;
use serde::de::Deserialize;
use log::{debug, info, warn, error};
use failure::Fail;

use crate::{KvStore, KvError};
use crate::engine::KvsEngine;
use crate::protocol::{Request, Response, ProtocolError};

fn send_error<W: Write>(writer: W, error: KvError) -> Result<(), ProtocolError> {
    let error_msg = format!("{}", error);
    warn!("KvStore error: {}", error_msg);
    let response = Response::Err(error_msg);
    debug!("Send response: {:?}", response);
    Ok(serde_json::to_writer(writer, &response)?)
}

fn send_ok<W: Write>(writer: W, value: Option<String>) -> Result<(), ProtocolError>{
    let response = Response::Ok(value);
    debug!("Send response: {:?}", response);
    Ok(serde_json::to_writer(writer, &response)?)
}

pub struct Server {
    addr: SocketAddr,
}

impl Server {
    pub fn new(addr: SocketAddr) -> Server {
        Server {addr}
    }

    pub fn run(&self, mut storage: KvStore) -> Result<(), ProtocolError> {
        let interrupt = Arc::new(AtomicBool::new(false));
        let interrupt_clone = interrupt.clone();

        ctrlc::set_handler(move || {
            debug!("SIGINT");
            interrupt_clone.store(true, Ordering::SeqCst);
        }).expect("Error setting SIGINT handler");

        info!("Server started on {}", self.addr);
        let tcp_listener = TcpListener::bind(self.addr)?;
        tcp_listener.set_nonblocking(true)?;

        for stream in tcp_listener.incoming() {
            if interrupt.load(Ordering::SeqCst) {
                debug!("Stop server");
                break;
            }

            let mut stream = match stream {
                Ok(s) => s,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(_) => stream?,
            };

            let remote_addr = stream.peer_addr()?.to_string();
            debug!("Accept client {}", remote_addr);

            let tcp_reader = BufReader::new(&stream);
            let mut tcp_writer = BufWriter::new(&stream);
            let mut deserializer = serde_json::Deserializer::from_reader(tcp_reader);
            let incoming_request = Request::deserialize(&mut deserializer)?;

            debug!("Get request");
            match incoming_request {
                Request::Get {key} => {
                    debug!("Get key: {}", key);
                    match storage.get(key) {
                        Ok(value) => {
                            if value.is_none() {
                                debug!("{}", KvError::KeyNotFound);
                            }
                            send_ok(tcp_writer, value)?;
                        }
                        Err(e) => send_error(tcp_writer, e)?,
                    }
                },
                Request::Set {key, value} => {
                    debug!("Set key: {}, value: {}", key, value);
                    match storage.set(key, value) {
                        Ok(_) => send_ok(tcp_writer, None)?,
                        Err(e) => send_error(tcp_writer, e)?,
                    }
                },
                Request::Rm {key} => {
                    debug!("Remove key: {}", key);
                    match storage.remove(key) {
                        Ok(_) => send_ok(tcp_writer, None)?,
                        Err(e) => send_error(tcp_writer, e)?,
                    }
                }
            }
        }

        Ok(())
    }
}