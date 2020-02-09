use std::net::SocketAddr;
use std::process::exit;

use log::{debug, error};
use simplelog::*;
use structopt::StructOpt;
use std::str::FromStr;

use kvs::protocol::{ProtocolError, Response};
use kvs::{Client, KvError};

const DEFAULT_SERVER_ADDRESS: &'static str = "127.0.0.1:4000";

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-client")]
struct ClientArgs {
    #[structopt(
    short,
    long,
    global = true,
    default_value = DEFAULT_SERVER_ADDRESS,
    parse(try_from_str))]
    addr: SocketAddr,

    #[structopt(
    short,
    long,
    global = true,
    default_value = "DEBUG",
    parse(try_from_str))]
    logging: LevelFilter,

    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    Get { key: String },
    Set { key: String, value: String },
    Rm { key: String },
}

fn get(client: Client, key: String) -> Result<(), ProtocolError> {
    let response = client.get(key)?;
    debug!("Response: {:?}", response);
    match response {
        Response::Ok(option_value) => match option_value {
            Some(value) => println!("{}", value),
            None => println!("{}", KvError::KeyNotFound),
        },
        Response::Err(e) => {
            error!("{}", e);
            exit(-1);
        }
    }
    Ok(())
}

fn set(client: &Client, key: String, value: String) -> Result<(), ProtocolError> {
    let response = client.set(key, value)?;
    debug!("Response: {:?}", response);
    if let Response::Err(e) = response {
        error!("{}", e);
        exit(-2);
    }
    Ok(())
}

fn rm(client: Client, key: String) -> Result<(), ProtocolError> {
    let response = client.rm(key)?;
    debug!("Response: {:?}", response);
    match response {
        Response::Ok(_) => Ok(()),
        Response::Err(what) => {
            if what == format!("{}", KvError::KeyNotFound) {
                error!("{}", KvError::KeyNotFound);
                eprintln!("{}", KvError::KeyNotFound);
                exit(1);
            } else {
                error!("{}", what);
                exit(-3);
            }
        }
    }
}

fn main() {
    let server_addr = SocketAddr::from_str(DEFAULT_SERVER_ADDRESS).unwrap();
    let client = Client::new(server_addr);

    for i in 0..=1024 {
        if let Err(e) = set(&client, "foo".to_string(), "bar".to_string()) {
            println!("#{}: {}", i, e);
            break;
        }
    }
}
