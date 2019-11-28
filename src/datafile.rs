use std::path::PathBuf;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};

use crate::error::Result;
use serde::Serialize;

pub trait DataFileGetter {
    fn get_inner(&mut self) -> (&PathBuf, &mut BufReader<File>);
}

#[derive(Debug)]
pub struct PassiveFile {
    pub path: PathBuf,
    pub reader: BufReader<File>,
}

impl PassiveFile {
    pub fn new<T>(path: T) -> Result<PassiveFile>
        where
            T: Into<std::path::PathBuf>,
    {
        let mut path = path.into();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(&mut path)?;
        let reader = BufReader::new(file);
        Ok(PassiveFile { path, reader })
    }

    /// Create PassiveFile from commands and path
    /// Create new passive file on `path` and write commands to this file.
    ///
    /// Note: There must be no passive file with name `path` before calling this function
    pub fn from_records(records: Vec<Result<impl Serialize>>, mut path: PathBuf) -> Result<PassiveFile> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&mut path)?;
        let mut writer = BufWriter::new(file.try_clone()?);
        let reader = BufReader::new(file.try_clone()?);

        for record in records {
            serde_json::to_writer(&mut writer, &record?)?;
        }
        writer.flush()?;
        Ok(PassiveFile{ path, reader })
    }
}

impl DataFileGetter for PassiveFile {
    fn get_inner(&mut self) -> (&PathBuf, &mut BufReader<File>) {
        (&self.path, &mut self.reader)
    }
}

#[derive(Debug)]
pub struct ActiveFile {
    pub path: PathBuf,
    pub reader: BufReader<File>,
    pub writer: BufWriter<File>,
}

impl ActiveFile {
    pub fn new<T>(path: T) -> Result<ActiveFile>
        where
            T: Into<std::path::PathBuf>,
    {
        let mut path = path.into();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&mut path)?;
        let reader = BufReader::new(file.try_clone()?);
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
        (&self.path, &mut self.reader)
    }
}
