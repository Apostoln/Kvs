use std::process::exit;
use structopt::StructOpt;

fn unimpl() {
    eprintln!("unimplemented");
    exit(-1);
}

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs")]
enum Command {
    #[structopt(name = "set", about = "Set value and key")]
    Set {
        #[structopt(name = "KEY", required = true)]
        key : String,
        #[structopt(name = "VALUE", required = true)]
        value : String,
    },
    #[structopt(name = "get", about = "Get value by key")]
    Get {
        #[structopt(name = "KEY", required = true)]
        key : String,
    },
    #[structopt(name = "rm", about = "Remove value by key")]
    Remove {
        #[structopt(name = "KEY", required = true)]
        key : String,
    }
}

fn main() {
    match Command::from_args() {
        Command::Set {key : _, value : _} => {
            unimpl();
        }
        Command::Get {key : _} => {
            unimpl();
        }
        Command::Remove {key : _} => {
            unimpl();
        }
    }
}
