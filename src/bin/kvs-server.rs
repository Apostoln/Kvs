use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::exit;

use log::{debug, error, info};
use simplelog::*;
use structopt::clap::arg_enum;
use structopt::StructOpt;

use kvs::Server;
use kvs::{KvStore, KvsEngine, SledEngine};
use kvs::thread_pool::{ThreadPool, QueueThreadPool, RayonThreadPool};

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

/// Read current engine from engine_file
fn current_engine<T>(engine_file: T) -> Option<Engine>
where
    T: Into<std::path::PathBuf>,
{
    let engine_file = engine_file.into();
    if !engine_file.exists() {
        return None;
    }

    Some(
        std::fs::read_to_string(engine_file)
            .expect("Error reading from engine file")
            .parse()
            .expect("The content of engine file is invalid"),
    )
}

/// Compare chosen engine with engine in engine_file.
/// Exit with error if they are differ.
/// Write chosen engine to engine_file if there no engine_file.
fn process_engine_file<T>(dir_path: T, chosen_engine: Engine)
where
    T: Into<std::path::PathBuf>,
{
    let engine_file = dir_path.into().join(ENGINE_PATH);
    match current_engine(&engine_file) {
        Some(engine) => {
            if engine != chosen_engine {
                error!(
                    "Storage directory is already powered by other engine: {}, new one: {}",
                    engine, chosen_engine
                );
                exit(-1);
            }
            debug!("Engine file: {}", engine);
        }
        None => {
            debug!("Set new engine: {}", chosen_engine);
            std::fs::write(&engine_file, format!("{}", chosen_engine))
                .expect("Error writing to engine file");
        }
    }
}

fn main() {
    let args = ServerArgs::from_args();

    TermLogger::init(args.logging, Config::default(), TerminalMode::Stderr)
        .expect("Error while initializing of TermLogger");

    debug!("Conf: {:?}", args);
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", args.engine);
    info!("Listening on {}", args.addr);

    let current_dir = env::current_dir()
        .expect("Can not get current directory");

    process_engine_file(&current_dir, args.engine);

    match args.engine {
        Engine::Kvs => run::<KvStore, RayonThreadPool>(args.addr, current_dir),
        Engine::Sled => run::<SledEngine, RayonThreadPool>(args.addr, current_dir),
    }
}

fn run<T: KvsEngine, P: ThreadPool>(addr: SocketAddr, dir_path: PathBuf) {
    const CORES_NUM : u32 = 8;
    let thread_pool = P::new(CORES_NUM);
    let engine = T::open(dir_path)
        .expect("Can not open chosen engine");

    let server = Server::new(addr, thread_pool, engine);
    if let Err(e) = server.run() {
        error!("{}", e);
        exit(-1);
    }
}