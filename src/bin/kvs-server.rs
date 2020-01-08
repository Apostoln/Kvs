use std::net::{SocketAddr, TcpListener};
use std::io::{Read, Write};
use std::env;
use structopt::StructOpt;
use simplelog::*;
use log::{debug, info, warn, error};

use kvs::{KvError, KvStore};

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

        let read_size = stream.read(&mut buffer).unwrap();
        let content = std::str::from_utf8(&buffer[0..read_size]).unwrap().trim_end();
        debug!("Read {} bytes: {}", read_size, content);

        let content = content
            .split_whitespace()
            .collect::<Vec<_>>();

        if content.len() == 0 {
            error!("Uncorrect request from client");
            continue;
        }
        else if content.starts_with(&["g"]) {
            if let Some(&key) = content.last() {
                match storage.get(key.to_owned())? {
                    Some(value) => debug!("Get value: {}", value),
                    None => debug!("{}", KvError::KeyNotFound),
                }
            }
        }
        else if content.starts_with(&["s"]) {
            if let (Some(&key), Some(&value)) = (content.get(1), content.get(2)) {
                storage.set(key.to_owned(),value.to_owned())?;
                debug!("Set key: {}, value: {}", key, value);
            }
        }
        else if content.starts_with(&["r"]) {
            if let Some(&key) = content.last() {
                storage.remove(key.to_owned())?;
                debug!("Remove key: {}", key);
            }
        }
        else if content.starts_with(&["c"]) {
            info!("Shutdown server by remote request");
            return Ok(());
        }
        else {
            error!("Uncorrect request from client");
            continue;
        }

        let written_size = stream.write(&mut buffer[0..read_size]).unwrap();
        debug!("Write {} bytes: {}", written_size, std::str::from_utf8(&buffer[0..written_size]).unwrap().trim_end());
        stream.flush().unwrap();
    }

    Ok(())
}