use std::process::exit;
use clap::{App, Arg, SubCommand};

fn unimpl() {
    eprintln!("unimplemented");
    exit(-1);
}

fn main() {
    let matches = App::new("Kvs")
                                .name(env!("CARGO_PKG_NAME"))
                                .version(env!("CARGO_PKG_VERSION"))
                                .author(env!("CARGO_PKG_AUTHORS"))
                                .about(env!("CARGO_PKG_DESCRIPTION"))
                                .subcommand(SubCommand::with_name("get")
                                    .about("Get value by key")
                                    .arg(Arg::with_name("KEY")
                                        .required(true)))
                                .subcommand(SubCommand::with_name("set")
                                    .about("Set value by key")
                                    .arg(Arg::with_name("KEY")
                                        .required(true))
                                    .arg(Arg::with_name("VALUE")
                                        .required(true)))
                                .subcommand(SubCommand::with_name("rm")
                                    .about("Remove value by key")
                                    .arg(Arg::with_name("KEY")
                                        .required(true)))
                                .get_matches();

    match matches.subcommand() {
        ("get", _) => {
            unimpl();
        }
        ("set", _) => {
            unimpl();
        }
        ("rm", _) => {
            unimpl();
        }
        _ => unreachable!(),
    }
}
