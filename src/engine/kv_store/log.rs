use std::collections::BTreeMap;
use std::fs;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

use log::debug;
use serde::{Deserialize, Serialize};

use super::datafile::*;
use super::logpointer::*;
use super::utils::*;
use crate::engine::Result;

pub struct Log {
    pub active: ActiveFile,
    pub passive: BTreeMap<u64, PassiveFile>,
    pub dir_path: PathBuf,
}

impl Log {
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

        let mut active_file_path = dir_path.clone();
        active_file_path.push(ACTIVE_FILE_NAME);
        let active_file = ActiveFile::new(active_file_path)?;

        Ok(Log {
            active: active_file,
            passive: passive_files,
            dir_path,
        })
    }

    pub fn get_record<'a, T>(&mut self, log_ptr: &LogPointer) -> Result<T>
    where
        T: Deserialize<'a>,
    {
        let offset = log_ptr.offset;

        let reader = match log_ptr.file {
            DataFile::Active => {
                debug!("Get record of active file, offset: {}", offset);
                &mut self.active.reader
            }
            DataFile::Passive(serial_number) => {
                debug!("Get record of passive file #{}, offset: {}", serial_number, offset);
                &mut self
                    .passive
                    .get_mut(&serial_number)
                    .unwrap()
                    .reader
            }
        };

        reader.seek(SeekFrom::Start(offset))?;

        Ok(serde_json::Deserializer::from_reader(reader)
            .into_iter()
            .next()
            .unwrap()?)
    }

    pub fn dump(&mut self) -> Result<()> {
        debug!("Dump Log");
        if self.active.reader.get_mut().metadata()?.len() == 0 {
            debug!("File is already empty"); //Nothing to do here
            return Ok(());
        }

        // Rename current ACTIVE_FILE_NAME to serial_number.passive
        let serial_number = self
            .passive
            .values_mut()
            .next_back() //option here
            .map_or(Ok(0), |file| get_serial_number(&file.path))?
            + 1;
        let mut new_path = self.dir_path.clone();
        new_path.push(format!("{}.{}", serial_number, PASSIVE_EXT));
        fs::rename(&self.active.path, &mut new_path)?;
        debug!("Move active file to {:?}", new_path);

        // Move old active file to passives and create new active
        self.passive.insert(serial_number, PassiveFile::new(new_path)?);
        let mut active_path = self.dir_path.clone();
        active_path.push(ACTIVE_FILE_NAME);
        self.active = ActiveFile::new(active_path)?;
        Ok(())
    }

    /// Replace old passive files by new ones. Old files will be deleted.
    /// New files are compacted and created from unique records in the next way
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

            let mut path = self.dir_path.clone();
            path.push(format!("{}.{}", counter, PASSIVE_EXT));

            // New file on the fs will be created here with appropriated records
            let passive_file = PassiveFile::from_records(chunk, path)?;
            passive_files.insert(counter, passive_file);

            counter += 1;
        }
        debug!("Created {} compacted passive files", counter);

        self.passive = passive_files;
        Ok(())
    }

    fn clear_passives(&mut self) -> Result<()> {
        debug!("Clear passive files");
        for passive in &mut self.passive.values_mut() {
            std::fs::remove_file(&mut passive.path)?;
        }
        self.passive.clear();
        Ok(())
    }
}
