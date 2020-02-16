use std::io;
use std::io::{BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use log::{debug, info, warn};
use serde::de::Deserialize;
use serde_json;

use crate::engine::KvsEngine;
use crate::protocol::{ProtocolError, Request, Response};
use crate::KvError;
use crate::thread_pool::{naive_pool::NaiveThreadPool, ThreadPool};
use crate::thread_pool::queue_pool::QueueThreadPool;

fn handle_connection(stream: &TcpStream, storage: impl KvsEngine) -> Result<(), ProtocolError> {
    let remote_addr = stream.peer_addr()?.to_string();
    debug!("Accept client {}", remote_addr);

    let tcp_reader = BufReader::new(stream);
    let tcp_writer = BufWriter::new(stream);
    let mut deserializer = serde_json::Deserializer::from_reader(tcp_reader);
    let incoming_request = Request::deserialize(&mut deserializer)?;

    debug!("Get request");
    match incoming_request {
        Request::Get { key } => {
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
        }
        Request::Set { key, value } => {
            debug!("Set key: {}, value: {}", key, value);
            match storage.set(key, value) {
                Ok(_) => send_ok(tcp_writer, None)?,
                Err(e) => send_error(tcp_writer, e)?,
            }
        }
        Request::Rm { key } => {
            debug!("Remove key: {}", key);
            match storage.remove(key) {
                Ok(_) => send_ok(tcp_writer, None)?,
                Err(e) => send_error(tcp_writer, e)?,
            }
        }
    }
    Ok(())
}

fn send_error<W: Write>(writer: W, error: KvError) -> Result<(), ProtocolError> {
    let error_msg = format!("{}", error);
    warn!("KvStore error: {}", error_msg);
    let response = Response::Err(error_msg);
    debug!("Send response: {:?}", response);
    Ok(serde_json::to_writer(writer, &response)?)
}

fn send_ok<W: Write>(writer: W, value: Option<String>) -> Result<(), ProtocolError> {
    let response = Response::Ok(value);
    debug!("Send response: {:?}", response);
    Ok(serde_json::to_writer(writer, &response)?)
}

pub struct Server {
    addr: SocketAddr,
    thread_pool: QueueThreadPool,
}

impl Server {
    pub fn new(addr: SocketAddr) -> Server {
        let thread_pool = QueueThreadPool::new(8);
        Server { addr, thread_pool }
    }

    pub fn run(&self, storage: impl KvsEngine) -> Result<(), ProtocolError> {
        //flag for the interruption by SIGINT
        let interrupt = Arc::new(AtomicBool::new(false));
        let interrupt_clone = interrupt.clone();
        ctrlc::set_handler(move || {
            debug!("SIGINT");
            interrupt_clone.store(true, Ordering::SeqCst);
        })
        .expect("Error setting SIGINT handler");

        info!("Server started on {}", self.addr);
        let tcp_listener = TcpListener::bind(self.addr)?;
        tcp_listener.set_nonblocking(true)?;

        for stream in tcp_listener.incoming() {
            if interrupt.load(Ordering::SeqCst) {
                debug!("Stop server");
                break;
            }

            let stream = match stream {
                Ok(s) => s,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(_) => stream?,
            };

            let storage = storage.clone();
            self.thread_pool.spawn(move || {
                handle_connection(&stream, storage); //todo error handling
            });
        }

        Ok(())
    }
}