use std::net::SocketAddr;
use std::process::exit;

use simplelog::*;
use log::{debug, error};
use structopt::StructOpt;

use kvs::protocol::{Response, ProtocolError};
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
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
    },
    Rm {
        key: String,
    }
}

fn get(client: Client, key: String) -> Result<(), ProtocolError> {
    let response = client.get(key)?;
    debug!("Response: {:?}", response);
    match response {
        Response::Ok(option_value) => {
            match option_value {
                Some(value) => println!("{}", value),
                None => println!("{}", KvError::KeyNotFound),
            }
        },
        Response::Err(e) => {
            error!("{}", e);
            exit(-1);
        }
    }
    Ok(())
}

fn set(client: Client, key: String, value: String) -> Result<(), ProtocolError> {
    let response = client.set(key, value)?;
    debug!("Response: {:?}", response);
    if let Response::Err(e) = response {
        error!("{}", e);
        exit(-2);
    }
    Ok(())
}

fn rm(client: Client, key: String) -> Result<(), ProtocolError>{
    let response = client.rm(key)?;
    debug!("Response: {:?}", response);
    match response {
        Response::Ok(_) => Ok(()),
        Response::Err(what) => {
            if what == format!("{}", KvError::KeyNotFound) {
                error!("{}", KvError::KeyNotFound);
                eprintln!("{}", KvError::KeyNotFound); //todo use KvError instead of String
                exit(1);
            }
            else {
                error!("{}", what);
                exit(-3);
            }
        }
    }
}

fn main() {
    let log_filter = ClientArgs::from_args().logging;
    TermLogger::init(log_filter, Config::default(), TerminalMode::Stderr)
        .expect("Error while initializing of TermLogger");

    let server_addr = ClientArgs::from_args().addr;
    let client = Client::new(server_addr);

    let cmd = ClientArgs::from_args().cmd;
    let res = match cmd {
        Command::Get{key} => get(client, key),
        Command::Set{key, value} => set(client, key, value),
        Command::Rm{key} => rm(client, key),
    };

    if let Err(e) = res {
        error!("{}", e);
        exit(-4);
    }
}