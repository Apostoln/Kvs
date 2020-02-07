/*
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use log::debug;
use serde::Serialize;

use crate::engine::Result;

/// Crutch for getting fields of DataFile and satisfying borrow-checker
pub trait DataFileGetter {
    fn get_inner(&mut self) -> (&PathBuf, &mut BufReader<File>);
}

/// Passive datafile is read-only.
#[derive(Debug)]
pub struct PassiveFile {
    pub path: PathBuf,
    pub reader: Mutex<BufReader<File>>,
}

impl PassiveFile {
    /// Open passive datafile.
    pub fn new<T>(path: T) -> Result<PassiveFile>
    where
        T: Into<std::path::PathBuf>,
    {
        let mut path = path.into();
        debug!("Open passive file {:?}", path);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(&mut path)?;
        let reader = Mutex::new(BufReader::new(file));
        Ok(PassiveFile { path, reader })
    }

    /// Create PassiveFile from commands and path
    /// Create new passive file on `path` and write commands to this file.
    ///
    /// Note: There must be no passive file with name `path` before calling this function
    pub fn from_records(records: Vec<Result<impl Serialize>>, mut path: PathBuf) -> Result<PassiveFile> {
        debug!("Create new passive file {:?} from {} records", path, records.len());
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&mut path)?;
        let mut writer = BufWriter::new(file.try_clone()?);
        let reader = Mutex::new(BufReader::new(file.try_clone()?));

        for record in records {
            serde_json::to_writer(&mut writer, &record?)?;
        }
        writer.flush()?;
        Ok(PassiveFile { path, reader })
    }
}

impl DataFileGetter for PassiveFile {
    fn get_inner(&mut self) -> (&PathBuf, &mut BufReader<File>) {
        (&self.path, self.reader.get_mut().unwrap())
    }
}

/// Active datafile is read-write.
#[derive(Debug)]
pub struct ActiveFile {
    pub path: PathBuf,
    pub reader: Mutex<BufReader<File>>,
    pub writer: BufWriter<File>,
}

impl ActiveFile {
    /// Open active datafile by path or create new one if not exists.
    pub fn new<T>(path: T) -> Result<ActiveFile>
    where
        T: Into<std::path::PathBuf>,
    {
        let mut path = path.into();
        debug!("Create active file: {:?}", path);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&mut path)?;
        let reader = Mutex::new(BufReader::new(file.try_clone()?));
        let writer = BufWriter::new(file.try_clone()?);

        Ok(ActiveFile {
            path,
            reader,
            writer,
        })
    }
}

impl DataFileGetter for ActiveFile {
    fn get_inner(&mut self) -> (&PathBuf, &mut BufReader<File>) {
        (&self.path, self.reader.get_mut().unwrap())
    }
}
*/