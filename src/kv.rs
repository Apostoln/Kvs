use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write, BufReader};

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

pub struct KvStore {
    index : HashMap<String, Offset>,
    log: File,
    unused_records: u64,
}

impl KvStore {
    pub fn open<T>(path: T) -> Result<KvStore>
    where
        T: Into<std::path::PathBuf>
    {
        let mut path = path.into();
        path.push(LOG_NAME);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        let index = KvStore::index(&mut BufReader::new(&file))?;

        Ok(KvStore{index, log: file, unused_records: 0})
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.index
            .get(&key)
            .map_or(
                Ok(None),
                |offset| {
                    match self.read_command(*offset)? {
                        Command::Set {value, ..} => Ok(Some(value)),
                        Command::Remove {..} => Err(UnexpectedCommand),
                    }
                }
            )
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let pos = self.log.seek(SeekFrom::Current(0))?;

        let cmd = Command::Set{key: key.clone(), value };
        let command_record = serde_json::to_string(&cmd)?;
        self.log.write(command_record.as_bytes())?;

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
        let command_record = serde_json::to_string(&cmd)?;
        self.log.write(command_record.as_bytes())?;

        self.unused_records += 1;
        Ok(())
    }

    fn reindex(&mut self) -> Result<()> {
        self.index = KvStore::index(&mut BufReader::new(&self.log))?;
        Ok(())
    }

    fn index(reader: &mut BufReader<&File>) -> Result<HashMap<String, u64>> {
        let mut index = HashMap::new();
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

    fn compact(&mut self) -> Result<()> {
        let commands = self.read_actual_commands();

        // Clear log file
        self.log.set_len(0)?;
        self.log.seek(SeekFrom::Start(0))?;

        self.write_actual_commands(commands)
    }

    fn read_command(&self, offset: u64)-> Result<Command> {
        let mut reader = BufReader::new(&self.log);
        reader.seek(SeekFrom::Start(offset))?;

        Ok(serde_json::Deserializer::from_reader(reader)
            .into_iter()
            .next()
            .unwrap()?)
    }

    fn read_actual_commands(&self) -> Vec<Result<Command>> {
        self.index
            .values()
            .map(|offset| -> Result<Command> {
                match self.read_command(*offset)? {
                    Command::Set {key, value} => Ok(Command::Set {key, value}),
                    _ => Err(UnexpectedCommand),
                }
            })
            .collect()
    }

    fn write_actual_commands(&mut self, commands: Vec<Result<Command>>) -> Result<()> {
        for cmd in commands {
            let command_record = serde_json::to_string(&cmd?)?;
            self.log.write(command_record.as_bytes())?;
        }
        Ok(())
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        if let Err(e) = self.compact() {
            panic!("{}", e);
        }
    }
}