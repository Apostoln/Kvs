use std::net::SocketAddr;
use std::env;
use std::process::exit;

use structopt::StructOpt;
use structopt::clap::arg_enum;
use simplelog::*;
use log::{debug, error};

use kvs::{KvStore, SledEngine, KvsEngine, Result};
use kvs::Server;

const DEFAULT_ADDRESS: &'static str = "127.0.0.1:4000";
const ENGINE_PATH: &'static str = "engine";

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

fn current_engine<T>(path: T) -> Option<Engine>
    where
        T: Into<std::path::PathBuf>,
{
    let path = path.into();
    if !path.exists() {
        return None;
    }

    Some(std::fs::read_to_string(path)
        .expect("Error reading from engine file")
        .parse()
        .expect("The content of engine file is invalid"))
}

fn main() {
    let args = ServerArgs::from_args();

    TermLogger::init(args.logging, Config::default(), TerminalMode::Stderr)
        .expect("Error while initializing of TermLogger");
    debug!("Conf: {:?}", args);

    let current_dir = env::current_dir()
        .expect("Can not get current dir");

    let engine_file = current_dir.join(ENGINE_PATH);
    match current_engine(&engine_file) {
        Some(engine) => {
            if engine != args.engine {
                error!("Storage directory is already powered by other engine: {}, new one: {}",
                       engine,
                       args.engine);
                exit(-1);
            }
            debug!("Engine file: {}", engine);
        },
        None => {
            debug!("Set new engine: {}", args.engine);
            std::fs::write(&engine_file,
                           format!("{}", args.engine))
                .expect("Error writing to engine file");
        }
    }

    let engine: Result<Box<dyn KvsEngine>> = match args.engine {
        Engine::Kvs => KvStore::open(current_dir).map(|x| Box::new(x) as _),
        Engine::Sled => SledEngine::open(current_dir).map(|x| Box::new(x) as _),
    };
    let mut engine = engine.expect("Can not open KvsEngine");

    let server = Server::new(args.addr);
    if let Err(e) = server.run(engine.as_mut()) {
        error!("{}", e);
    }
}