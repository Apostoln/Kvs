use std::net::{SocketAddr, TcpListener};
use std::io::{Read, Write, BufReader, BufWriter};
use std::env;
use structopt::StructOpt;
use simplelog::*;
use log::{debug, info, warn, error};
use serde::de::Deserialize;
use failure::Fail;

use kvs::{KvError, KvStore};
use kvs::{Request, Response};

const DEFAULT_ADDRESS: &'static str = "127.0.0.1:4000";

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-server")]
struct ServerArgs {
    #[structopt(
        short,
        long,
        default_value = DEFAULT_ADDRESS,
        parse(try_from_str))]
    addr: SocketAddr,
}

#[derive(Fail, Debug)]
enum ServerError{
    #[fail(display = "IO Error: {}", _0)]
    IoError(#[cause] std::io::Error),

    #[fail(display = "Serde Error: {}", _0)]
    SerdeError(#[cause] serde_json::Error),

    #[fail(display = "Unknown Error: {}", _0)]
    UnknownError(String),
}

impl From<std::io::Error> for ServerError {
    fn from(err: std::io::Error) -> ServerError {
        let res = ServerError::IoError(err);
        error!("{}", res);
        res
    }
}

impl From<serde_json::Error> for ServerError {
    fn from(err: serde_json::Error) -> ServerError {
        let res = ServerError::SerdeError(err);
        error!("{}", res);
        res
    }
}

impl From<String> for ServerError {
    fn from(err: String) -> ServerError {
        let res = ServerError::UnknownError(err);
        error!("{}", res);
        res
    }
}

fn send_error<W: Write>(writer: W, error: KvError) -> Result<(), ServerError> {
    let error_msg = format!("{}", error);
    warn!("KvStore error: {}", error_msg);
    let response = Response::Err(error_msg);
    debug!("Send response: {:?}", response);
    serde_json::to_writer(writer, &response)?;
    Ok(())
}

fn send_ok<W: Write>(writer: W, value: Option<String>) -> Result<(), ServerError>{
    let response = Response::Ok(value);
    debug!("Send response: {:?}", response);
    serde_json::to_writer(writer, &response)?;
    Ok(())
}

fn run(addr: SocketAddr, mut storage: KvStore) -> Result<(), ServerError> {
    info!("Server started on {}", addr);
    let tcp_listener = TcpListener::bind(addr)?;

    let mut buffer = [0; 512];
    for stream in tcp_listener.incoming() {
        let mut stream = stream?;
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

fn main() {
    TermLogger::init(LevelFilter::Debug, Config::default(), TerminalMode::Stderr)
        .expect("Error while initializing of TermLogger");

    let current_dir = env::current_dir()
        .expect("Can not get current dir");
    let mut storage = KvStore::open(current_dir)
        .expect("Error while opening KvStore");

    let addr = ServerArgs::from_args().addr;

    if let Err(e) = run(addr, storage) {
        error!("{}", e);
    }
}