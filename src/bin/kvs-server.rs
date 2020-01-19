use std::net::{SocketAddr, TcpListener};
use std::io::{Read, Write, BufReader, BufWriter};
use std::env;
use structopt::StructOpt;
use simplelog::*;
use log::{debug, info, warn, error};
use serde::de::Deserialize;

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

fn main() -> kvs::Result<()> {
    TermLogger::init(LevelFilter::Debug, Config::default(), TerminalMode::Stderr).unwrap();

    let mut storage = KvStore::open(env::current_dir()?)?;

    let addr = ServerArgs::from_args().addr;
    info!("Server started on {}", addr);
    let tcp_listener = TcpListener::bind(addr).unwrap();

    let mut buffer = [0; 512];
    for stream in tcp_listener.incoming() {
        let mut stream = stream.unwrap();
        let remote_addr = stream.peer_addr().unwrap().to_string();
        debug!("Accept client {}", remote_addr);

        let tcp_reader = BufReader::new(&stream);
        let mut tcp_writer = BufWriter::new(&stream);
        let mut deserializer = serde_json::Deserializer::from_reader(tcp_reader);
        let incoming_request = Request::deserialize(&mut deserializer)?; //todo error handling

        debug!("Get request");
        match incoming_request {
            Request::Get {key} => {
                match storage.get(key)? { //todo send error?
                    Some(value) => {
                        debug!("Get value: {}", value);
                        let response = Response::Ok(Some(value));
                        debug!("Send response: {:?}", response);
                        serde_json::to_writer(tcp_writer, &response).unwrap();
                    },
                    None => {
                        debug!("{}", KvError::KeyNotFound);
                        let response = Response::Ok(None);
                        debug!("Send response: {:?}", response);
                        serde_json::to_writer(tcp_writer, &response).unwrap();
                    },
                }
            },
            Request::Set {key, value} => {
                debug!("Set key: {}, value: {}", key, value);
                match storage.set(key, value) {
                    Ok(_) => {
                        let response = Response::Ok(None);
                        debug!("Send response: {:?}", response);
                        serde_json::to_writer(tcp_writer, &response).unwrap();
                    },
                    Err(e) => return Err(e),
                }
            },
            Request::Rm {key} => {
                debug!("Remove key: {}", key);
                match storage.remove(key) {
                    Ok(_) => {
                        let response = Response::Ok(None);
                        debug!("Send response: {:?}", response);
                        serde_json::to_writer(tcp_writer, &response).unwrap();
                    },
                    Err(e) => return Err(e),
                }
            }
        }
    }

    Ok(())
}