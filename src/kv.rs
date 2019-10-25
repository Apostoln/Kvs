use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write, BufReader, BufWriter};
use std::path::PathBuf;

use serde::{Serialize, Deserialize};
use serde_json;

use crate::error::KvError::{KeyNotFound, UnexpectedCommand};
pub use crate::error::{KvError, Result};

const LOG_NAME : &'static str = "log.log";
const RECORDS_LIMIT : u64 = 100;

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key : String, value : String},
    Remove { key : String},
}

type Offset = u64;
type Index = HashMap<String, Offset>;

pub struct Log {
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

impl Log {
    fn new<T>(path: T) -> Result<Log>
    where
        T: Into<std::path::PathBuf>
    {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path.into())?;
        let reader = BufReader::new(file.try_clone()?);
        let writer = BufWriter::new(file.try_clone()?);
        Ok(Log{reader, writer})
    }
}

pub struct KvStore {
    index: Index,
    log: Log,
    path: PathBuf,
    unused_records: u64,
}

impl KvStore {
    pub fn open<T>(path: T) -> Result<KvStore>
    where
        T: Into<std::path::PathBuf>
    {
        let mut path = path.into();
        path.push(LOG_NAME);
        let mut log = Log::new(&path)?;
        let index = KvStore::index(&mut log.reader)?;

        Ok(KvStore{index, log, path, unused_records: 0})
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let reader = &mut self.log.reader;
        self.index
            .get(&key)
            .map_or(
                Ok(None),
                |offset| {
                    match KvStore::read_command(reader, *offset)? {
                        Command::Set {value, ..} => Ok(Some(value)),
                        Command::Remove {..} => Err(UnexpectedCommand),
                    }
                }
            )
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let pos = self.log.writer.seek(SeekFrom::Current(0))?;

        let cmd = Command::Set{key: key.clone(), value };

        serde_json::to_writer(&mut self.log.writer, &cmd)?;
        self.log.writer.flush()?;

        if let Some(_) = self.index.insert(key, pos) {
            self.unused_records += 1;
        }

        if self.unused_records > RECORDS_LIMIT {
            self.compact()?;
            self.reindex()?;
            self.unused_records = 0;
        }

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.index.remove(&key).ok_or(KeyNotFound)?;

        let cmd = Command::Remove {key};

        serde_json::to_writer(&mut self.log.writer, &cmd)?;
        self.log.writer.flush()?;

        self.unused_records += 1;
        Ok(())
    }

    fn reindex(&mut self) -> Result<()> {
        self.index = KvStore::index(&mut self.log.reader)?;
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let commands = self.read_actual_commands();

        // Clear log file
        std::fs::remove_file(&self.path)?;
        self.log = Log::new(&self.path)?;

        self.write_actual_commands(commands)
    }

    fn read_actual_commands(&mut self) -> Vec<Result<Command>> {
        let reader = &mut self.log.reader;
        self.index
            .values()
            .map(|offset| -> Result<Command> {
                match KvStore::read_command(reader, *offset)? {
                    Command::Set {key, value} => Ok(Command::Set {key, value}),
                    _ => Err(UnexpectedCommand),
                }
            })
            .collect()
    }

    //todo make sure commands actual?
    fn write_actual_commands(&mut self, commands: Vec<Result<Command>>) -> Result<()> {
        for cmd in commands {
            serde_json::to_writer(&mut self.log.writer, &cmd?)?;
        }
        self.log.writer.flush()?;
        Ok(())
    }

    fn index(reader: &mut BufReader<File>) -> Result<Index> {
        let mut index = Index::new();
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter();
        while let Some(item) = stream.next() {
            match item? {
                Command::Set {key, ..} => {
                    index.insert(key, pos);
                }
                Command::Remove {key} => {
                    index.remove(&key).unwrap();
                }
            }
            pos = stream.byte_offset() as u64;
        }
        Ok(index)
    }

    fn read_command(mut reader: &mut BufReader<File>, offset: u64)-> Result<Command> {
        reader.seek(SeekFrom::Start(offset))?;

        Ok(serde_json::Deserializer::from_reader(&mut reader)
            .into_iter()
            .next()
            .unwrap()?)
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        if let Err(e) = self.compact() {
            panic!("{}", e);
        }
    }
}
