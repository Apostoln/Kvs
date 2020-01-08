use std::net::TcpStream;
use simplelog::*;
use log::{debug, info, warn, error};
use std::io::{Write, Read};

fn request(server_addr: String, msg: &str) {
    info!("Trying to connect to server at {}", server_addr);

    let mut stream = TcpStream::connect(server_addr).unwrap();
    info!("Client started at {}", stream.local_addr().unwrap());
    stream.write(msg.as_ref()).unwrap();
    stream.flush().unwrap();

    let mut buffer = [0; 512];
    let size = stream.read(&mut buffer).unwrap();
    debug!("Answer: {}", std::str::from_utf8(&buffer[0..size]).unwrap());

    stream.shutdown(std::net::Shutdown::Both);
}

fn shutdown_server(server_addr: String) {
    info!("Trying to shutdown server at {}", server_addr);

    let mut stream = TcpStream::connect(server_addr).unwrap();
    info!("Client started at {}", stream.local_addr().unwrap());
    let msg = "c";
    stream.write(msg.as_ref()).unwrap();
    stream.flush().unwrap();
}

fn main() {
    TermLogger::init(LevelFilter::Debug, Config::default(), TerminalMode::Stderr).unwrap();

    let ip = "127.0.0.1";
    let port = 4000;
    let server_addr = format!("{}:{}", ip, port);

    let msg = "s k2 v2";
    request(server_addr.clone(), msg);
    shutdown_server(server_addr);
}