use std::net::SocketAddr;
use std::env;
use structopt::StructOpt;
use structopt::clap::arg_enum;
use simplelog::*;
use log::error;

use kvs::{KvStore, SledEngine, KvsEngine, Result};
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

    #[structopt(
        short,
        long,
        default_value = "kvs",
        possible_values = &Engine::variants(),
        case_insensitive = true)]
    engine: Engine,
}

arg_enum! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum Engine {
        Kvs,
        Sled,
    }
}

fn main() {
    let args = ServerArgs::from_args();

    let log_level = args.logging;
    TermLogger::init(log_level, Config::default(), TerminalMode::Stderr)
        .expect("Error while initializing of TermLogger");

    let current_dir = env::current_dir()
        .expect("Can not get current dir");

    let addr = args.addr;

    let engine: Result<Box<dyn KvsEngine>> = match args.engine {
        Engine::Kvs => KvStore::open(current_dir).map(|x| Box::new(x) as _),
        Engine::Sled => SledEngine::open(current_dir).map(|x| Box::new(x) as _),
    };

    let mut engine = engine.expect("Can not open KvsEngine");

    if let Err(e) = Server::new(addr).run(engine.as_mut()) {
        error!("{}", e);
    }
}