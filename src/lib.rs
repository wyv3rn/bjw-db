use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::{RwLock, RwLockReadGuard},
};

type Result<T> = anyhow::Result<T>;

pub trait Updateable {
    type Parameters: Serialize + DeserializeOwned;

    fn update(&mut self, parameters: &Self::Parameters);
}

pub struct Database<T> {
    data: RwLock<T>,
    path: PathBuf,
    version: u64,
}

const VERSION_FILE: &str = "version";
const NEW_VERSION_FILE: &str = "new_version";
const CHECKPOINT_PREFIX: &str = "checkpoint.";
const LOG_PREFIX: &str = "logfile.";

impl<T: Default + Serialize + DeserializeOwned + Updateable> Database<T> {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Database<T>> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
            let db = Database {
                data: RwLock::default(),
                path,
                version: 0,
            };
            db.write_checkpoint_file()?;
            db.create_logfile_if_required()?;
            db.update_version_file()?;
            Ok(db)
        } else {
            let new_version_path = path.join(NEW_VERSION_FILE);
            let version_path = path.join(VERSION_FILE);
            if new_version_path.exists() {
                std::fs::rename(&new_version_path, &version_path)?;
            }
            let version_str = std::fs::read_to_string(version_path)?;
            let version: u64 = version_str.parse()?;
            let mut db = Database {
                data: RwLock::default(),
                path,
                version,
            };
            db.read_checkpoint_file()?;
            db.replay_updates()?;
            Ok(db)
        }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.data.read().unwrap()
    }

    pub fn update(&self, parameters: &<T as Updateable>::Parameters) -> Result<()> {
        let mut locked = self.data.write().unwrap();
        self.extend_update_log(parameters)?;
        locked.update(parameters);
        Ok(())
    }

    pub fn delete(self) -> Result<()> {
        std::fs::remove_dir_all(self.path)?;
        Ok(())
    }

    fn replay_updates(&mut self) -> Result<()> {
        let log_filename = format!("{LOG_PREFIX}{}", self.version);
        let file = File::open(self.path.join(log_filename))?;
        let lines = BufReader::new(file).lines();
        let mut locked = self.data.write().unwrap();
        for line in lines {
            let parameters: <T as Updateable>::Parameters = serde_json::from_str(line?.as_ref())?;
            locked.update(&parameters);
        }
        Ok(())
    }

    fn create_logfile_if_required(&self) -> Result<PathBuf> {
        let filename = format!("{LOG_PREFIX}{}", self.version);
        let path = self.path.join(filename);
        if !path.exists() {
            let file = File::create(&path)?;
            file.sync_all()?;
        }
        Ok(path.clone())
    }

    fn extend_update_log(&self, parameters: &<T as Updateable>::Parameters) -> Result<()> {
        let path = self.create_logfile_if_required()?;
        let mut json_str = serde_json::to_string(parameters)?;
        json_str.push('\n');
        let mut file = OpenOptions::new().append(true).open(path)?;
        file.write_all(json_str.as_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    fn read_checkpoint_file(&mut self) -> Result<()> {
        let filename = format!("{CHECKPOINT_PREFIX}{}", self.version);
        let json_str = std::fs::read_to_string(self.path.join(filename))?;
        let data: T = serde_json::from_str(&json_str)?;
        self.data = RwLock::new(data);
        Ok(())
    }

    fn write_checkpoint_file(&self) -> Result<()> {
        let filename = format!("{CHECKPOINT_PREFIX}{}", self.version);
        let mut file = File::create(self.path.join(filename))?;
        let json_str = serde_json::to_string(&self.data)?;
        file.write_all(json_str.as_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    fn update_version_file(&self) -> Result<()> {
        let mut file = File::create(self.path.join(NEW_VERSION_FILE))?;
        file.write_all(self.version.to_string().as_bytes())?;
        file.sync_all()?;
        std::fs::rename(
            self.path.join(NEW_VERSION_FILE),
            self.path.join(VERSION_FILE),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    type KeyValueStore = BTreeMap<String, String>;
    #[derive(Serialize, Deserialize)]
    pub enum KVParams {
        Insert(String, String),
    }

    impl Updateable for KeyValueStore {
        type Parameters = KVParams;
        fn update(&mut self, params: &KVParams) {
            match params {
                KVParams::Insert(k, v) => self.insert(k.clone(), v.clone()),
            };
        }
    }

    #[test]
    fn test_key_value_store() {
        let tempdir = TempDir::with_prefix("bjw-").unwrap();

        // create new db
        let path = tempdir.path().join("kv-store");
        let db = Database::<KeyValueStore>::open(&path).unwrap();
        let insert = &KVParams::Insert("key".to_string(), "value".to_string());
        db.update(insert).unwrap();
        let insert = &KVParams::Insert("more".to_string(), "value".to_string());
        db.update(insert).unwrap();
        let data = db.read().clone();

        // re-open db
        let db = Database::<KeyValueStore>::open(&path).unwrap();
        assert_eq!(data, *db.read());

        // delete
        db.delete().unwrap();
    }
}
