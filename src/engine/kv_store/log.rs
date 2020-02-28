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
use std::ffi::OsStr;

use super::kv_store::Record;


#[derive(Debug)]
struct LogReader;

impl LogReader {
    pub fn get_reader(&self, location: impl Into<PathBuf>) -> BufReader<File> {
        //todo implement reusing of readers
        let path = location.into();
        BufReader::new(File::open(path).unwrap())
    }
}

/// The `Log` is an abstraction over the persistent sequence of records on disk.
/// It consists of datafiles with records. There are two types of datafiles: active and passive.
/// There is only one active datafile and some passives datafiles in the `Log`
/// Active datafile is opened for reading and writing while passive files only for reading.
/// New records are added in the end of active datafile.
/// Passive datafiles contain immutable sequence of records.
/// Passive datafiles are enumerated monotonically starting from 1.
#[derive(Debug)]
pub struct Log {
    reader: LogReader,
    writer: Mutex<BufWriter<File>>,
    pub dir_path: PathBuf,
    pub active_file_path: PathBuf,
    pub last_serial_number: AtomicU64,
}

impl Log {
    /// Open a `Log` with the given path.
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<Log> {
        let dir_path = dir_path.into();
        debug!("Open Log, path: {:?}", dir_path);

        let active_file_path = dir_path.join(ACTIVE_FILE_NAME);

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
            .open(&active_file_path)?;
        let writer = Mutex::new(BufWriter::new(active_file));
        let reader = LogReader{};

        Ok(Log {
            writer,
            reader,
            last_serial_number,
            dir_path,
            active_file_path,
        })
    }

    /// Get record from `Log` by `Location`.
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
                         &self.active_file_path)
        )
    }

    //todo update docs
    /// Dump the active datafile.
    /// Dumping is the process of moving the content of active datafile to the new passive one
    /// and creating new empty active datafile.
    pub fn dump(&self) -> Result<()> {
        debug!("Dump Log");
        let active_path = &self.active_file_path;
        let mut active_file = self.reader.get_reader(&active_path);
        if active_file.get_mut().metadata()?.len() == 0 {
            debug!("File is already empty"); // Nothing to do here
            return Ok(());
        }

        // Rename current ACTIVE_FILE_NAME to serial_number.passive
        self.last_serial_number.fetch_add(1, Ordering::SeqCst);
        let new_path = self.passive_path(self.last_serial_number.load(Ordering::SeqCst));
        fs::rename(active_path, &new_path)?;
        //todo ERROR - reader on another thread will read data from incorrect location in his path
        //todo ^seems fixed

        debug!("Move active file to {:?}", new_path);

        self.create_active()?;
        let active_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(active_path)?; //todo remove opening active file twice
        *self.writer.lock().unwrap() = BufWriter::new(active_file);
        debug!("Active file writer after dumping: {:?}", self.writer);
        Ok(())
    }

    /// Compact the log.
    /// Compaction is the process of removing deprecated records from passive datafiles of Log.
    /// Old passive datafiles will be replaced by new ones with only actual(unique) records.
    /// New files are compacted and created from unique records in the next way:
    /// 1. Split commands to chunks of `RECORDS_IN_COMPACTED` elements
    /// 2. Write each chunk to new passive file in log directory.
    /// 3. Collect passive files to BTreeMap and set it to `self.passive`.
    pub fn compact(&self, mut records: Vec<Result<Record>>) -> Result<()> {
        debug!("Compact Log");
        self.clear_passives()?; //todo ERROR if another thread would read after this

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

    /// Get path of passive datafile with specified `serial_number`
    /// Note: `serial_number` must refer to an existing file
    pub fn passive_path(&self, serial_number: u64) -> PathBuf {
        self.dir_path.join(format!("{}.{}",serial_number, PASSIVE_EXT))
    }

    pub fn index(&self) -> Result<Index> {
        let index = Index::new();
        self.reindex(&index);
        Ok(index)
    }
    
    /// Index active and passive datafiles from `Log`.
    pub fn reindex(&self, index: &Index) -> Result<()> {
        debug!("Reindex log {:?}", &self);

        // Clear old_index
        // Index::clear(&mut self) is unusable because we have only &self
        // This code is correct until there are no calls to index from other threads
        index.iter().map(|pair| index.remove(pair.key()));

        for serial_number in 1..=self.last_serial_number.load(Ordering::SeqCst) {
            self.reindex_datafile(&index, &self.passive_path(serial_number))?
        }

        self.reindex_datafile(&index, &self.active_file_path)?;

        Ok(())
    }

    fn reindex_datafile(&self, index: &Index, datafile_path: &PathBuf) -> Result<()> {
        debug!("Index datafile: {:?}", datafile_path);
        let mut reader= self.reader.get_reader(datafile_path);
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter();
        while let Some(item) = stream.next() {
            match item? {
                Record::Set { key, .. } => {
                    index.insert(key, Location::new(pos, datafile_path));
                }
                Record::Remove { key } => {
                    index.remove(&key);
                }
            }
            pos = stream.byte_offset() as u64;
        }
        Ok(())
    }

    fn create_active(&self) -> Result<()> {
        let active_file_path = &self.active_file_path;
        debug!("Create new active file {:?}", active_file_path);

        fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(active_file_path)?; //todo return it!!!
        Ok(())
    }

    fn create_passive(&self, records: Vec<Result<Record>>, serial_number: u64) -> Result<()> {
        let passive_file_path = self.passive_path(serial_number);
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

    /// Remove all passive datafiles from fs
    fn clear_passives(&self) -> Result<()> {
        debug!("Clear passive files");
        self.dir_path
            .read_dir()?
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().extension() == Some(OsStr::new(PASSIVE_EXT)))
            .try_for_each(|entry| fs::remove_file(entry.path()))?;
        Ok(())
    }
}