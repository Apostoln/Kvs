use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write, BufReader};

use serde::{Serialize, Deserialize};
use serde_json;

use crate::error::KvError::KeyNotFound;
pub use crate::error::{KvError, Result};

const LOG_NAME : &'static str = "log.log";

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key : String, value : String},
    Remove { key : String},
}

type Offset = u64;

pub struct KvStore {
    index : HashMap<String, Offset>,
    log: File,
}

impl KvStore {
    pub fn open<T>(path: T) -> Result<KvStore>
    where
        T: Into<std::path::PathBuf>
    {
        let mut path = path.into();
        path.push(LOG_NAME);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;

        let mut index = HashMap::new();
        let mut reader = BufReader::new(&file);
        let mut pos = reader.seek(SeekFrom::Current(0))?;
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter();
        while let Some(item) = stream.next() {
            match item? {
                Command::Set {key, ..} => {
                    index.insert(key, pos);
                }
                Command::Remove {key} => {
                    index.remove(&key);
                }
            }
            pos = stream.byte_offset() as u64;
        }

        Ok(KvStore{index, log: file})
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.index.get(&key).map_or(
            Ok(None),
            |offset| {
                //todo move to function?
                let mut reader = BufReader::new(&self.log);
                reader.seek(SeekFrom::Start(*offset))?;
                if let Command::Set {value, ..} = serde_json::Deserializer::from_reader(reader)
                    .into_iter()
                    .next()
                    .unwrap()? {
                    Ok(Some(value))
                }
                else {
                    Err(KvError::UnknownError("Expected `Set` record".to_string()))
                }
            })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let pos = self.log.seek(SeekFrom::Current(0))?;

        let cmd = Command::Set{key: key.clone(), value };
        let command_record = serde_json::to_string(&cmd)?;
        self.log.write(command_record.as_bytes())?;

        self.index.insert(key, pos);
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.index.remove(&key).ok_or(KeyNotFound)?;

        let cmd = Command::Remove {key};
        let command_record = serde_json::to_string(&cmd)?;
        self.log.write(command_record.as_bytes())?;

        Ok(())
    }

    fn save(&mut self) -> Result<()> {
        let commands : Vec<_> = self.index
            .values()
            .map(|offset| -> Result<Command> {
                let mut reader = BufReader::new(&self.log);
                reader.seek(SeekFrom::Start(*offset))?;

                if let Command::Set {key, value} = serde_json::Deserializer::from_reader(reader)
                    .into_iter()
                    .next()
                    .unwrap()? {
                    Ok(Command::Set {key, value})
                }
                else {
                    unreachable!()
                }
            })
            .collect();

        // Clear log file
        self.log.set_len(0)?;
        self.log.seek(SeekFrom::Start(0))?;

        for cmd in commands {
            let command_record = serde_json::to_string(&cmd?)?;
            self.log.write(command_record.as_bytes())?;
        }

        Ok(())
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        if let Err(e) = self.save() {
            panic!("{}", e);
        }
    }
}
