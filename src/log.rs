use std::collections::BTreeMap;
use std::path::PathBuf;
use std::fs;

use crate::error::Result;
use crate::datafile::*;
use crate::utils::*;

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
        let passive_files = dir_path
            .read_dir()?
            .filter_map(std::result::Result::ok)
            .map(|file| file.path())
            .filter(|path| path.is_file() && path.extension().map_or(false, |ext| ext == PASSIVE_EXT))
            .map(|path| -> Result<(u64, PassiveFile)>{
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

    pub fn set_passive(&mut self, passive: BTreeMap<u64, PassiveFile>) -> Result<()> {
        self.passive = passive;
        Ok(())
    }

    pub fn dump(&mut self) -> Result<()> {
        if self.active.reader.get_mut().metadata()?.len() == 0 {
            // File is already empty, nothing to do here
            return Ok(());
        }

        // Rename current ACTIVE_FILE_NAME to serial_number.passive
        let serial_number = self.passive
            .values_mut()
            .next_back() //option here
            .map_or(Ok(0), |file| get_serial_number(&file.path))?
            + 1;
        let mut new_path = self.dir_path.clone();
        new_path.push(format!("{}.{}", serial_number, PASSIVE_EXT));
        fs::rename(&self.active.path, &mut new_path)?;

        // Move old active file to passives and create new active
        self.passive.insert(serial_number, PassiveFile::new(new_path)?);
        self.active = ActiveFile::new(ACTIVE_FILE_NAME)?;
        Ok(())
    }
}