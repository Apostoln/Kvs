use std::net::{TcpStream, SocketAddr};
use simplelog::*;
use log::{debug, info, warn, error};
use std::io::{Write, Read, BufWriter, BufReader};
use structopt::StructOpt;

use kvs::{Request, Response, KvError};
use std::process::exit;

const DEFAULT_SERVER_ADDRESS: &'static str = "127.0.0.1:4000";

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-client")]
struct ClientArgs {
    #[structopt(
        short,
        long,
        default_value = DEFAULT_SERVER_ADDRESS,
        parse(try_from_str))]
    addr: SocketAddr,

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

fn request(server_addr: SocketAddr, req: Request) -> Response {
    debug!("Trying to connect to server at {}", server_addr);

    let mut stream = TcpStream::connect(server_addr).unwrap();
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    debug!("Client started at {}", stream.local_addr().unwrap());
    debug!("Send request: {:?}", req);
    serde_json::to_writer(&mut writer, &req).unwrap();
    writer.flush().unwrap();
    serde_json::from_reader(reader).unwrap()
}

fn get(server_addr: SocketAddr, key: String) {
    let req = Request::Get {key};
    let response = request(server_addr, req);
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
}

fn set(server_addr: SocketAddr, key: String, value: String) {
    let req = Request::Set {key, value};
    let response = request(server_addr, req);
    debug!("Response: {:?}", response);
    if let Response::Err(e) = response {
        error!("{}", e);
        exit(-2);
    }
}

fn rm(server_addr: SocketAddr, key: String) {
    let req = Request::Rm {key};
    let response = request(server_addr, req);
    debug!("Response: {:?}", response);
    match response {
        Response::Ok(_) => {},
        Response::Err(what) => {
            if what == format!("{}", KvError::KeyNotFound) {
                eprintln!("{}", KvError::KeyNotFound); //todo use KvError instead of String
            }
            else {
                error!("{}", what);
                exit(-3);
            }
        }
    }
}

fn main() {
    TermLogger::init(LevelFilter::Debug, Config::default(), TerminalMode::Stderr)
        .expect("Error while initializing of TermLogger");;

    let server_addr = ClientArgs::from_args().addr;
    let cmd = ClientArgs::from_args().cmd;
    match cmd {
        Command::Get{key} => get(server_addr, key),
        Command::Set{key, value} => set(server_addr, key, value),
        Command::Rm{key} => rm(server_addr, key),
    }
}