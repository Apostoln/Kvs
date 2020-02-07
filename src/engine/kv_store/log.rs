use std::fs;
use std::io::{Seek, SeekFrom, BufWriter, BufReader, Write};
use std::path::PathBuf;

use log::debug;
use serde::{Deserialize, Serialize}; //todo use it

use super::location::*;
use super::utils::*;
use super::kv_store::Index;
use crate::engine::Result;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use std::fs::File;

use super::kv_store::Record;
use std::ffi::OsStr;


/// The `Log` is an abstraction over the persistent sequence of records on disk.
/// It consists of datafiles with records. There are two types of datafiles: active and passive.
/// There is only one active datafile and some passives datafiles in the `Log`
/// Active datafile is opened for reading and writing while passive files only for reading.
/// New records are added in the end of active datafile.
/// Passive datafiles contain immutable sequence of records.
/// Passive datafiles are enumerated monotonically starting from 1.
/*pub struct Log {
    pub active: ActiveFile,
    pub passive: BTreeMap<u64 /*serial number*/, PassiveFile>, //<u64, Mutex<PassiveFile>
    pub dir_path: PathBuf,
}*/

#[derive(Debug)]
struct LogReader;

impl LogReader {
    pub fn get_reader(&self, location: impl Into<PathBuf>) -> BufReader<File> {
        let path = location.into();
        BufReader::new(File::open(path).unwrap())
    }
}

#[derive(Debug)]
pub struct Log {
    reader: LogReader,
    writer: Mutex<BufWriter<File>>,
    pub dir_path: PathBuf,
    pub last_serial_number: AtomicU64,
}

impl Log {
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<Log> {
        let dir_path = dir_path.into();
        debug!("Open Log, path: {:?}", dir_path);

        let last_serial_number: u64 = dir_path
            .read_dir()?
            .filter_map(std::result::Result::ok)
            .map(|file| Ok(get_serial_number(&file.path())?))
            .filter_map(Result::ok)
            .max()
            .unwrap_or(0);

        let last_serial_number = AtomicU64::new(last_serial_number);

        let active_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&mut dir_path.join(ACTIVE_FILE_NAME))?;
        let writer = Mutex::new(BufWriter::new(active_file));
        let reader = LogReader{};

        Ok(Log {
            writer,
            reader,
            last_serial_number,
            dir_path,
        })
    }

    pub fn get_record(&self, location: &Location) -> Result<Record> {
        let mut reader = self.reader.get_reader(&location.file.path);
        reader.seek(SeekFrom::Start(location.offset))?;
        Ok(serde_json::Deserializer::from_reader(reader.get_mut())
            .into_iter()
            .next()
            .unwrap()?)
    }

    pub fn set_record(&self, record: &Record) -> Result<Location> {
        let mut writer = self.writer.lock().unwrap();
        let pos = writer.seek(SeekFrom::Current(0))?;
        serde_json::to_writer(writer.get_mut(),record)?;
        writer.flush()?;
        Ok(
            Location::new(pos,
                         &self.dir_path.join(ACTIVE_FILE_NAME))
        )
    }

    //todo update docs
    pub fn dump(&self) -> Result<()> {
        debug!("Dump Log");
        let active_path = self.dir_path.join(ACTIVE_FILE_NAME); //todo move as const to log or smth like this
        let mut active_file = self.reader.get_reader(&active_path);
        if active_file.get_mut().metadata()?.len() == 0 {
            debug!("File is already empty"); // Nothing to do here
            return Ok(());
        }

        // Rename current ACTIVE_FILE_NAME to serial_number.passive
        self.last_serial_number.fetch_add(1, Ordering::SeqCst);
        let new_path = self.dir_path.join(format!("{}.{}",
                                                  self.last_serial_number.load(Ordering::SeqCst),
                                                  PASSIVE_EXT));
        fs::rename(&active_path, &new_path)?;
        debug!("Move active file to {:?}", new_path);

        self.create_active()?;
        let active_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&active_path)?;
        *self.writer.lock().unwrap() = BufWriter::new(active_file);
        debug!("Active file writer after dumping: {:?}", self.writer);
        Ok(())
    }

    pub fn compact(&self, mut records: Vec<Result<Record>>) -> Result<()> { //todo? change to Vec<Result<impl Serialize>>
        debug!("Compact Log");
        self.clear_passives()?;

        let mut counter: u64 = 0; // serial number of passive file

        // Create `counter` passive files with appropriated records on the filesystem
        let records = &mut records;
        while !records.is_empty() {
            counter += 1;
            let chunk = std::iter::from_fn(|| records.pop())
                .take(RECORDS_IN_COMPACTED)
                .collect::<Vec<_>>();

            self.create_passive(chunk, counter)?;
        }
        debug!("Created {} compacted passive files", counter);
        self.last_serial_number.store(counter, Ordering::SeqCst);

        Ok(())
    }
    fn create_active(&self) -> Result<()> {
        let active_file_path = self.dir_path.join(ACTIVE_FILE_NAME);
        debug!("Create new active file {}", active_file_path.to_str().unwrap()); //todo wtf how to display Path

        fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(active_file_path)?;
        Ok(())
    }

    fn create_passive(&self, records: Vec<Result<Record>>, serial_number: u64) -> Result<()> { //todo iter instead of vec?
        let passive_file_path = self.dir_path.join(format!("{}.{}",
                                                           serial_number,
                                                           PASSIVE_EXT));
        debug!("Create new passive file {:?} from {} records", passive_file_path, records.len());
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(passive_file_path)?;
        let mut writer = BufWriter::new(file);

        for record in records {
            serde_json::to_writer(&mut writer, &record?)?;
        }
        writer.flush()?;
        Ok(())
    }

    fn clear_passives(&self) -> Result<()> { //todo WIP
        debug!("Clear passive files");
        self.dir_path
            .read_dir()?
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().extension() == Some(OsStr::new(PASSIVE_EXT)))
            .try_for_each(|entry| fs::remove_file(entry.path()))?;
        Ok(())
    }

    pub fn index_datafile(&self, index: &mut Index, datafile_path: PathBuf) -> Result<()> {
        debug!("Index datafile: {:?}", datafile_path);
        let mut reader= self.reader.get_reader(&datafile_path);
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter();
        while let Some(item) = stream.next() {
            match item? {
                Record::Set { key, .. } => {
                    index.insert(key, Location::new(pos, &datafile_path));
                }
                Record::Remove { key } => {
                    index.remove(&key);
                }
            }
            pos = stream.byte_offset() as u64;
        }
        Ok(())
    }
}
/*
impl Log {
    /// Open a `Log` with the given path.
    pub fn open<T>(dir_path: T) -> Result<Log>
    where
        T: Into<std::path::PathBuf>,
    {
        let dir_path = dir_path.into();
        debug!("Open Log, path: {:?}", dir_path);
        let passive_files = dir_path
            .read_dir()?
            .filter_map(std::result::Result::ok)
            .map(|file| file.path())
            .filter(|path| path.is_file() && path.extension().map_or(false, |ext| ext == PASSIVE_EXT))
            .map(|path| -> Result<(u64, PassiveFile)> {
                Ok((get_serial_number(&path)?, PassiveFile::new(path)?))
            })
            .collect::<Result<_>>()?;

        let active_file = ActiveFile::new(dir_path.join(ACTIVE_FILE_NAME))?;

        Ok(Log {
            active: active_file,
            passive: passive_files,
            dir_path,
        })
    }

    /// Get record from `Log` by `LogPointer`.
    pub fn get_record<'a, T>(&self, location: &Location) -> Result<T>
    where
        T: Deserialize<'a>,
    {
        let offset = location.offset;

        //todo create new reader for appropriated path instead of getting existed
        let mut reader = match location.file {
            DataFile::Active => {
                debug!("Get record of active file, offset: {}", offset);
                (&self.active.reader).lock().unwrap()
            }
            DataFile::Passive(serial_number) => {
                debug!("Get record of passive file #{}, offset: {}", serial_number, offset);
                (&self.passive)
                    .get(&serial_number)
                    .unwrap()
                    .reader
                    .lock()
                    .unwrap()
            }
        };

        reader.seek(SeekFrom::Start(offset))?;

        Ok(serde_json::Deserializer::from_reader(reader.get_mut())
            .into_iter()
            .next()
            .unwrap()?)
    }

    /// Dump the active datafile.
    /// Dumping is the process of moving the content of active datafile to the new passive one
    /// and creating new empty active datafile.
    pub fn dump(&mut self) -> Result<()> {
        debug!("Dump Log");
        if self.active.reader.lock().unwrap().get_mut().metadata()?.len() == 0 {
            debug!("File is already empty"); //Nothing to do here
            return Ok(());
        }

        // Rename current ACTIVE_FILE_NAME to serial_number.passive
        let serial_number = self
            .passive
            .values_mut()
            .next_back() // Last PassiveFile
            .map_or(Ok(0), |file| get_serial_number(&file.path))?
            + 1;
        let mut new_path = self.dir_path.join(format!("{}.{}",
                                                      serial_number,
                                                      PASSIVE_EXT));
        fs::rename(&self.active.path, &mut new_path)?;
        debug!("Move active file to {:?}", new_path);


        //todo DO NOT CREATE NEW Passive FROM PATH, CREATE NEW FROM READER OF ACTIVE,
        // i.e. save the old FD for index correctness

        // Move old active file to passives and create new active
        self.passive.insert(serial_number, PassiveFile::new(new_path)?);

        self.active = ActiveFile::new(self.dir_path.join(ACTIVE_FILE_NAME))?;
        Ok(())
    }

    /// Compact the log.
    /// Compaction is the process of removing deprecated records from passive datafiles of Log.
    /// Old passive datafiles will be replaced by new ones with only actual(unique) records.
    /// New files are compacted and created from unique records in the next way:
    /// 1. Split commands to chunks of `RECORDS_IN_COMPACTED` elements
    /// 2. Write each chunk to new passive file in log directory.
    /// 3. Collect passive files to BTreeMap and set it to `self.passive`.
    pub fn compact(&mut self, mut records: Vec<Result<impl Serialize>>) -> Result<()> {
        debug!("Compact Log");
        self.clear_passives()?;

        let mut passive_files: BTreeMap<u64, PassiveFile> = BTreeMap::new();
        let mut counter: u64 = 1; // serial number of passive file
        let records = &mut records;
        while !records.is_empty() {
            let chunk = std::iter::from_fn(|| records.pop())
                .take(RECORDS_IN_COMPACTED)
                .collect::<Vec<_>>();

            // New file on the fs will be created here with appropriated records
            let passive_file_path = self.dir_path.join(format!("{}.{}", counter, PASSIVE_EXT));
            let passive_file = PassiveFile::from_records(chunk, passive_file_path)?;
            passive_files.insert(counter, passive_file);

            counter += 1;
        }
        debug!("Created {} compacted passive files", counter);

        self.passive = passive_files;
        Ok(())
    }

    /// Remove all passive datafiles.
    fn clear_passives(&mut self) -> Result<()> {
        debug!("Clear passive files");
        for passive in &mut self.passive.values_mut() {
            std::fs::remove_file(&mut passive.path)?;
        }
        self.passive.clear();
        Ok(())
    }
}
*/