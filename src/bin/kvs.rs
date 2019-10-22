use std::env;
use kvs::{KvError, KvStore};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs")]
enum CliCommand {
    #[structopt(name = "set", about = "Set value and key")]
    Set {
        #[structopt(name = "KEY", required = true)]
        key: String,
        #[structopt(name = "VALUE", required = true)]
        value: String,
    },
    #[structopt(name = "get", about = "Get value by key")]
    Get {
        #[structopt(name = "KEY", required = true)]
        key: String,
    },
    #[structopt(name = "rm", about = "Remove value by key")]
    Remove {
        #[structopt(name = "KEY", required = true)]
        key: String,
    },
}

fn main() -> kvs::Result<()> {
    let mut storage = KvStore::open(env::current_dir()?)?;

    match CliCommand::from_args() {
        CliCommand::Set { key: k, value: v } => {
            storage.set(k, v)?;
        }
        CliCommand::Get { key: k } => {
            println!("{}", storage.get(k)?.unwrap_or(format!("{}", KvError::KeyNotFound)));
        }
        CliCommand::Remove { key: k } => {
            storage.remove(k).map_err(|err| {
                println!("{}", err);
                err
            })?;
        }
    }
    Ok(())
}
