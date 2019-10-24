use std::collections::HashMap;
use std::fs::{File, read};
use std::io::{Seek, SeekFrom, Write, BufReader, BufWriter};

use serde::{Serialize, Deserialize};
use serde_json;

use crate::error::KvError::{KeyNotFound, UnexpectedCommand, SerdeError};
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
    index: HashMap<String, Offset>,
    reader: BufReader<File>,
    writer: BufWriter<File>,
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

        let mut reader = BufReader::new(file.try_clone()?);
        let mut writer = BufWriter::new(file.try_clone()?);

        let index = KvStore::index(&mut reader)?;

        Ok(KvStore{index, reader, writer, log: file, unused_records: 0})
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let reader = &mut self.reader;
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
        let pos = self.writer.seek(SeekFrom::Current(0))?;

        let cmd = Command::Set{key: key.clone(), value };

        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

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

        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush();

        self.unused_records += 1;
        Ok(())
    }

    fn reindex(&mut self) -> Result<()> {
        self.index = KvStore::index(&mut self.reader)?;
        Ok(())
    }

    fn index(reader: &mut BufReader<File>) -> Result<HashMap<String, u64>> {
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
        // todo remove log-file usage
        self.log.set_len(0)?;
        self.log.seek(SeekFrom::Start(0))?;

        self.write_actual_commands(commands)
    }

    fn read_command(mut reader: &mut BufReader<File>, offset: u64)-> Result<Command> {
        reader.seek(SeekFrom::Start(offset))?;

        Ok(serde_json::Deserializer::from_reader(&mut reader)
            .into_iter()
            .next()
            .unwrap()?)
    }

    fn read_actual_commands(&mut self) -> Vec<Result<Command>> {
        let reader = &mut self.reader;
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
            serde_json::to_writer(&mut self.writer, &cmd?)?;
        }
        self.writer.flush()?;
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
