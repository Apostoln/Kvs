use std::collections::HashMap;
use std::fs::{File, DirEntry};
use std::fs;
use std::io::{Seek, SeekFrom, Write, BufReader, BufWriter, BufRead};
use std::path::{PathBuf, Path};

use serde::{Serialize, Deserialize};
use serde_json;

use crate::error::KvError::{KeyNotFound, UnexpectedCommand};
pub use crate::error::{KvError, Result};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use failure::{Fail, AsFail};
use serde_json::error::Category::Data;
use std::time::UNIX_EPOCH;
use std::os::raw::c_uint;

// todo rm const LOG_NAME : &'static str = "log.log";
const ACTIVE_FILE_NAME: &'static str = "log.active";
const ACTIVE_EXT : &'static str = "active";
const PASSIVE_EXT : &'static str = "passive";
const RECORDS_LIMIT : u64 = 100;
const RECORDS_IN_COMPACTED: usize = 100;

#[derive(Serialize, Deserialize, Debug, Clone)]
enum Command {
    Set { key : String, value : String},
    Remove { key : String},
}

type Offset = u64;
type Index = HashMap<String, LogPointer>;

//todo remove
enum DataFile {
    Active(ActiveFile),
    Passive(PassiveFile),
}

trait DataFileGetter {
    fn get_path(&self) -> &PathBuf;
    fn get_reader(&mut self) -> &mut BufReader<File>;
}

#[derive(Debug)]
struct PassiveFile {
    path: PathBuf,
    reader: BufReader<File>,
}

impl PassiveFile {
    fn new<T>(path: T) -> Result<PassiveFile>
        where
            T: Into<std::path::PathBuf>
    {
        let path= path.into();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(path.clone())?; //todo avoid cloning?
        let reader = BufReader::new(file);
        Ok(PassiveFile{path, reader})
    }
}

impl DataFileGetter for PassiveFile {
    fn get_path(&self) -> &PathBuf {
        &self.path
    }
    fn get_reader(&mut self) -> &mut BufReader<File> {
        &mut self.reader
    }
}

#[derive(Debug)]
struct ActiveFile {
    path: PathBuf,
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

impl ActiveFile {
    fn new<T>(path: T) -> Result<ActiveFile>
        where
            T: Into<std::path::PathBuf>
    {
        let path = path.into();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path.clone())?; //todo avoid cloning
        let reader = BufReader::new(file.try_clone()?);
        let writer = BufWriter::new(file.try_clone()?);

        Ok(ActiveFile{path, reader, writer})
    }
}

impl DataFileGetter for ActiveFile {
    fn get_path(& self) -> &PathBuf {
        &self.path
    }
    fn get_reader(&mut self) -> &mut BufReader<File> {
        &mut self.reader
    }
}

pub struct Log {
    active: ActiveFile,
    passive: Vec<PassiveFile>,
}

impl Log {
    fn new<T>(dir_path: T) -> Result<Log>
    where
        T: Into<std::path::PathBuf>
    {
        let dir_path = dir_path.into();
        let mut passive_files = dir_path.read_dir()?
            .filter_map(std::result::Result::ok)
            .map(|file| file.path())
            .filter(|path| path.is_file())
            .filter(|path| {
                if let Some(ext) = path.extension() {
                    ext == PASSIVE_EXT
                } else {
                    false
                }})
            .collect::<Vec<PathBuf>>();

        // Sort passive files by serial number in name
        passive_files.sort_by(|left, right| { //TODO PLEASE DO NOT HANDLE ERRORS LIKE AN ASSHOLE
            let to_int = |path: &PathBuf|->u64 {
                path.file_stem().unwrap()
                    .to_str().unwrap()
                    .parse::<u64>().unwrap()
            };
            let left_number = to_int(left);
            let right_number = to_int(right);
            left_number.cmp(&right_number)
        });

        let passive_files = passive_files
            .iter()
            .map(PassiveFile::new)
            .collect::<Result<Vec<PassiveFile>>>()?;

        let mut active_file_path = dir_path.clone();
        active_file_path.push(ACTIVE_FILE_NAME);
        let active_file = ActiveFile::new(active_file_path)?;

        Ok(Log{active: active_file, passive: passive_files})
    }


}

//todo refact LogPointer with direct reference to file instead of PathBuf?
/*
struct LogPointer<'a> {
    offset: u64,
    datafile: &'a PassiveFile,
}*/

struct LogPointer {
    offset: u64,
    file: PathBuf, //todo or just Path?
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
        let mut log = Log::new(&path)?;
        let index = KvStore::index(&mut log)?;

        Ok(KvStore{index, log, path, unused_records: 0})
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let log = &mut self.log;
        self.index
            .get(&key)
            .map_or(
                Ok(None),
                |offset| {
                    match KvStore::read_command(log, offset)? {
                        Command::Set {value, ..} => Ok(Some(value)),
                        Command::Remove {..} => Err(UnexpectedCommand),
                    }
                }
            )
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let mut writer = &mut self.log.active.writer;
        let pos = writer.seek(SeekFrom::Current(0))?;

        let cmd = Command::Set{key: key.clone(), value };

        serde_json::to_writer(&mut writer, &cmd)?;
        writer.flush()?;

        if let Some(_) = self.index.insert(key, LogPointer{file: self.log.active.path.clone(), offset: pos }) { //avoid cloning?
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
        let mut writer = &mut self.log.active.writer;

        self.index.remove(&key).ok_or(KeyNotFound)?;

        let cmd = Command::Remove {key};

        serde_json::to_writer(&mut writer, &cmd)?;
        writer.flush()?;

        self.unused_records += 1;
        Ok(())
    }

    fn reindex(&mut self) -> Result<()> {
        self.index = KvStore::index(&mut self.log)?;
        Ok(())
    }


    fn compact(&mut self) -> Result<()> {
        //todo needed?
        self.dump()?;
        self.reindex(); //todo rly here?

        // Create backup
        // Disabled due to "compaction" test failing/
        // Todo create backup_dir not as subdir of log dir
        /*
        let mut backup_dir = self.path.clone();
        let time = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros(); // Note: Error while creating directory due to equal names if time duration is too big.
        //Todo add serial number to backup name
        backup_dir.push(format!("precompact_backup_{0}", time));
        self.backup(&backup_dir)?;
        */

        // Read actual commands
        let commands = self.read_actual_commands();

        // Remove current passive files
        for passive in &mut self.log.passive {
            std::fs::remove_file(&mut passive.path)?;
        }

        // Create new passive files and write actual commands to them, then recreate struct Log
        self.write_actual_commands(commands)?;
        self.log = Log::new(&self.path)?;

        Ok(())
    }

    fn backup(&mut self, mut backup_dir: &PathBuf) -> Result<()> {
        fs::create_dir(&mut backup_dir)?;

        for passive in &self.log.passive {
            let mut new_file = backup_dir.clone();
            new_file.push(passive.path.file_name().unwrap());
            fs::copy(&passive.path, new_file)?;
        }
        Ok(())
    }

    fn read_actual_commands(&mut self) -> Vec<Result<Command>> {
        let log = &mut self.log;
        self.index
            .values()
            .map(|log_ptr| -> Result<Command> {
                match KvStore::read_command(log, log_ptr)? {
                    Command::Set {key, value} => Ok(Command::Set {key, value}),
                    _ => Err(UnexpectedCommand),
                }
            })
            .collect()
    }

    fn write_actual_commands(&mut self, mut commands: Vec<Result<Command>>) -> Result<()> {
        //todo move creating files to other function?
        //todo create PassiveFile's directly here?
        let mut counter: u64 = 1;

        // Separate commands to chunks with RECORDS_IN_COMPACTED elements and write
        // each chunk to new passive file.
        let commands = &mut commands;
        while !commands.is_empty() {
            // Create new file
            let mut path = self.path.clone();
            path.push(format!("{}.{}", counter, PASSIVE_EXT));
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(path)?;
            let mut writer = BufWriter::new(file);

            // Take chunk with RECORDS_IN_COMPACTED from commands and write its content
            for cmd in std::iter::from_fn(|| commands.pop()).take(RECORDS_IN_COMPACTED) {
                serde_json::to_writer(&mut writer, &cmd?)?;
            }
            writer.flush()?;

            counter += 1;
        }
        Ok(())
    }

    fn index_datafile<T>(index: &mut Index, datafile: &mut T) -> Result<()>
        where T: DataFileGetter
    {
        let path = datafile.get_path().clone(); //todo avoid cloning //write only one method returning both path&reader
        let reader = datafile.get_reader();

        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter();
        while let Some(item) = stream.next() {
            match item? {
                Command::Set {key, ..} => {
                    index.insert(key, LogPointer{offset: pos, file: path.clone()}); //todo avoid cloning?
                }
                Command::Remove {key} => {
                    index.remove(&key).unwrap();
                }
            }
            pos = stream.byte_offset() as u64;
        }
        Ok(())
    }

    fn index(log: &mut Log) -> Result<Index> {
        let mut index = Index::new();

        for passive in &mut log.passive {
            KvStore::index_datafile(&mut index, passive)?;
        }

        let active = &mut log.active;
        KvStore::index_datafile(&mut index, active);

        Ok(index)
    }

    fn read_command(log: &mut Log, log_ptr: &LogPointer) -> Result<Command> {
        let file_path = &log_ptr.file;
        let offset = log_ptr.offset;

        let reader = if file_path.file_name().unwrap() == ACTIVE_FILE_NAME {
            &mut log.active.reader
        } else { //todo refact file finding
            &mut log
                .passive
                .iter_mut()
                .find(|el| el.path == *file_path)
                .unwrap()
                .reader
        };

        reader.seek(SeekFrom::Start(offset))?;

        Ok(serde_json::Deserializer::from_reader(reader)
            .into_iter()
            .next()
            .unwrap()?)
    }

    fn dump(&mut self) -> Result<()> {
        // todo Move this to Log:: methods

        if self.log.active.reader.get_mut().metadata()?.len() == 0 {
            // File is already empty, nothing to do here
            return Ok(())
        }

        // Rename current ACTIVE_FILE_NAME to serial_number.passive
        let serial_number = self.log
            .passive
            .last()
            .map_or(0, |file| {
                file.path
                    .file_stem().unwrap()
                    .to_str().unwrap()
                    .parse::<i32>().unwrap() //todo error handling
            })
            + 1;
        let mut new_path = self.path.clone();
        new_path.push(format!("{}.{}", serial_number, PASSIVE_EXT));
        fs::rename(&self.log.active.path, &mut new_path)?;

        // Move old active file to passives and create new active
        self.log.passive.push(PassiveFile::new(new_path)?);
        self.log.active = ActiveFile::new(ACTIVE_FILE_NAME)?;
        Ok(())
    }

}

impl Drop for KvStore {
    fn drop(&mut self) {
        if let Err(e) = self.compact() {
            panic!("Error while dropping KvStore: {}", e);
        }
    }
}
