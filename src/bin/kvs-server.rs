use std::net::SocketAddr;
use std::env;
use structopt::StructOpt;
use simplelog::*;
use log::error;

use kvs::KvStore;
use kvs::Server;

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

    #[structopt(
        short,
        long,
        default_value = "DEBUG",
        parse(try_from_str))]
    logging: LevelFilter,
}

fn main() {
    let log_level = ServerArgs::from_args().logging;
    TermLogger::init(log_level, Config::default(), TerminalMode::Stderr)
        .expect("Error while initializing of TermLogger");

    let current_dir = env::current_dir()
        .expect("Can not get current dir");
    let storage = KvStore::open(current_dir)
        .expect("Error while opening KvStore");

    let addr = ServerArgs::from_args().addr;
    if let Err(e) = Server::new(addr).run(storage) {
        error!("{}", e);
    }
}