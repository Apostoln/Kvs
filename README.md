# Kvs
Persistent key-value storage with client-server architecture based on [bitcast](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf) algorithm.

Implementation of project from course [Practical Networked Applications in Rust](https://github.com/pingcap/talent-plan/tree/master/rust).

Current implementation status: [Project 4: Concurrency and parallelism ](https://github.com/pingcap/talent-plan/blob/master/rust/projects/project-4/README.md) 

## Kvs-server
Server is synchronous but uses thread pool for concurrent processing of commands. Asynchronous implementation will be soon.

Two engines are supported:
- `Kvs`  - KvsEngine, custom implementation of bitcast algorithm. Implementation is mostly lock-free. An exception is compaction process which requires a global lock.
- `Sled` - Wrapper for [Sled](https://github.com/spacejam/sled) engine.

Note that data-files of different engines are not interchangeable, so you must choose which one should be used for your dataset.


 Running:
 ```bash
 cargo run --bin kvs-server 
 ```

Usage:
```Bash
USAGE:
    kvs-server [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -a, --addr <addr>           [default: 127.0.0.1:4000]
    -e, --engine <engine>       [default: kvs]  [possible values: Kvs, Sled]
    -l, --logging <logging>     [default: DEBUG]
```

## Kvs-client 
Running:
```bash
cargo run --bin kvs-server
 ```

Usage:
```bash
USAGE:
    kvs-client [OPTIONS] <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -a, --addr <addr>           [default: 127.0.0.1:4000]
    -l, --logging <logging>     [default: DEBUG]

SUBCOMMANDS:
    get     
    help    Prints this message or the help of the given subcommand(s)
    rm      
    set     
```

Commands:
```bash
kvs-client set [OPTIONS] <key> <value>
kvs-client get [OPTIONS] <key>
kvs-client rm [OPTIONS] <key>
```
