use std::net::TcpStream;
use simplelog::*;
use log::{debug, info, warn, error};
use std::io::{Write, Read, BufWriter, BufReader};

use kvs::{Request, Response};

fn request(server_addr: String, req: Request) {
    info!("Trying to connect to server at {}", server_addr);

    let mut stream = TcpStream::connect(server_addr).unwrap();
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    info!("Client started at {}", stream.local_addr().unwrap());
    info!("Send request: {:?}", req);
    serde_json::to_writer(&mut writer, &req).unwrap();
    writer.flush().unwrap();
    info!("Read response");
    let response: Response = serde_json::from_reader(reader).unwrap();
    info!("Response: {:?}", response);
}

fn get(server_addr: String) {
    let req = Request::Get {key: "foo".to_owned()};
    request(server_addr, req);
}

fn set(server_addr: String) {
    let req = Request::Set {key: "newkey".to_owned(), value: "newvalue".to_owned()};
    request(server_addr, req);
}

fn rm(server_addr: String) {
    let req = Request::Rm {key: "missingkey".to_owned()};
    request(server_addr, req);
}

fn main() {
    TermLogger::init(LevelFilter::Debug, Config::default(), TerminalMode::Stderr).unwrap();

    let ip = "127.0.0.1";
    let port = 4000;
    let server_addr = format!("{}:{}", ip, port);

    get(server_addr);
}