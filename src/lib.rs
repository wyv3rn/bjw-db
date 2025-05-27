use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
};

type Result<T> = std::io::Result<T>;

pub trait Readable {
    type Args<'a>;
    type ReturnType;

    fn read(&self, args: &Self::Args<'_>) -> Self::ReturnType;
}

pub trait Updateable {
    type Args: Serialize + DeserializeOwned;
    type ReturnType;

    fn update(&mut self, args: &Self::Args) -> Self::ReturnType;
}

pub struct Database<T, F> {
    data: T,
    fmt: F,
    path: PathBuf,
    version: u64,
}

pub trait DataFormat {
    type Data: Serialize + DeserializeOwned + Readable + Updateable;

    fn new() -> Self;
    fn serialize_data(&self, data: &Self::Data) -> Result<Vec<u8>>;
    fn deserialize_data(&self, input: &[u8]) -> Result<Self::Data>;
    fn serialize_params(&self, params: &<Self::Data as Updateable>::Args) -> Result<Vec<u8>>;
    fn deserialize_params(&self, input: &[u8]) -> Result<Vec<<Self::Data as Updateable>::Args>>;
}

const VERSION_FILE: &str = "version";
const NEW_VERSION_FILE: &str = "new_version";
const CHECKPOINT_PREFIX: &str = "checkpoint";
const LOG_PREFIX: &str = "logfile";
const DELIM: char = '.';

impl<T, F> Database<T, F>
where
    T: Default + Serialize + DeserializeOwned + Readable + Updateable,
    F: DataFormat<Data = T>,
{
    pub fn open<P: AsRef<Path>>(path: P, fmt: F) -> Result<Database<T, F>> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
            let db = Database {
                data: <T as Default>::default(),
                fmt,
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
            let version: u64 = version_str.parse().map_err(|_| {
                std::io::Error::new(ErrorKind::InvalidData, "Could not parse version")
            })?;
            let mut db = Database {
                data: <T as Default>::default(),
                fmt,
                path,
                version,
            };
            db.read_checkpoint_file()?;
            db.replay_updates()?;
            Ok(db)
        }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn read(&self, parameters: &<T as Readable>::Args<'_>) -> <T as Readable>::ReturnType {
        self.data.read(parameters)
    }

    pub fn read_all(&self) -> &T {
        &self.data
    }

    pub fn update(
        &mut self,
        parameters: &<T as Updateable>::Args,
    ) -> Result<<T as Updateable>::ReturnType> {
        self.extend_update_log(parameters)?;
        Ok(self.data.update(parameters))
    }

    pub fn create_checkpoint(&mut self) -> Result<()> {
        self.version += 1;
        self.write_checkpoint_file()?;
        self.create_logfile_if_required()?;
        self.update_version_file()?;
        if let Err(e) = self.cleanup() {
            log::warn!("Failed to cleanup: {:?}", e);
        };
        Ok(())
    }

    pub fn delete(self) -> Result<()> {
        std::fs::remove_dir_all(self.path)?;
        Ok(())
    }

    fn replay_updates(&mut self) -> Result<()> {
        let log_filename = format!("{LOG_PREFIX}{DELIM}{}", self.version);
        let ser = std::fs::read(self.path.join(log_filename))?;
        let updates = self.fmt.deserialize_params(&ser)?;
        for params in updates {
            self.data.update(&params);
        }
        Ok(())
    }

    fn create_logfile_if_required(&self) -> Result<PathBuf> {
        let filename = format!("{LOG_PREFIX}{DELIM}{}", self.version);
        let path = self.path.join(filename);
        if !path.exists() {
            let file = File::create(&path)?;
            file.sync_all()?;
        }
        Ok(path.clone())
    }

    fn extend_update_log(&self, params: &<T as Updateable>::Args) -> Result<()> {
        let path = self.create_logfile_if_required()?;
        let ser = self.fmt.serialize_params(params)?;
        let mut file = OpenOptions::new().append(true).open(path)?;
        file.write_all(&ser)?;
        file.sync_all()?;
        Ok(())
    }

    fn read_checkpoint_file(&mut self) -> Result<()> {
        let filename = format!("{CHECKPOINT_PREFIX}{DELIM}{}", self.version);
        let ser = std::fs::read(self.path.join(filename))?;
        let data: T = self.fmt.deserialize_data(&ser)?;
        self.data = data;
        Ok(())
    }

    fn write_checkpoint_file(&self) -> Result<()> {
        let filename = format!("{CHECKPOINT_PREFIX}{DELIM}{}", self.version);
        let mut file = File::create(self.path.join(filename))?;
        let ser = self.fmt.serialize_data(&self.data)?;
        file.write_all(&ser)?;
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

    fn cleanup(&self) -> Result<()> {
        for entry in std::fs::read_dir(&self.path)? {
            let entry = entry?;
            if entry.metadata()?.is_file() {
                if let Ok(filename) = entry.file_name().into_string() {
                    if self.is_outdated_file(&filename) {
                        std::fs::remove_file(entry.path())?;
                    }
                }
            }
        }
        Ok(())
    }

    fn is_outdated_file(&self, filename: &str) -> bool {
        if filename == NEW_VERSION_FILE {
            return true;
        };
        if let Some((base, ext)) = filename.rsplit_once(DELIM) {
            if base == CHECKPOINT_PREFIX || base == LOG_PREFIX {
                if let Ok(version) = ext.parse::<u64>() {
                    if version < self.version {
                        return true;
                    }
                }
            }
        }
        false
    }
}

impl<T: Clone, F> Database<T, F> {
    pub fn clone_data(&self) -> T {
        self.data.clone()
    }
}

#[cfg(feature = "json")]
use std::marker::PhantomData;

#[cfg(feature = "json")]
pub struct JsonFormat<T> {
    _phantom: PhantomData<T>,
}

#[cfg(feature = "json")]
impl<T> DataFormat for JsonFormat<T>
where
    T: Serialize + DeserializeOwned + Updateable + Readable,
{
    type Data = T;

    fn new() -> Self {
        JsonFormat::<T> {
            _phantom: PhantomData,
        }
    }

    fn serialize_data(&self, data: &Self::Data) -> Result<Vec<u8>> {
        Ok(serde_json::to_string(data)?.as_bytes().to_vec())
    }

    fn deserialize_data(&self, input: &[u8]) -> Result<Self::Data> {
        let str = std::str::from_utf8(input)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(serde_json::from_str(str)?)
    }

    fn serialize_params(&self, params: &<Self::Data as Updateable>::Args) -> Result<Vec<u8>> {
        let mut string = serde_json::to_string(params)?;
        string.push('\n');
        Ok(string.as_bytes().to_vec())
    }

    fn deserialize_params(&self, input: &[u8]) -> Result<Vec<<Self::Data as Updateable>::Args>> {
        let str = std::str::from_utf8(input)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let mut updates = Vec::new();
        for line in str.split('\n') {
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str(line) {
                Ok(params) => updates.push(params),
                Err(e) => {
                    log::error!("Failed to deserialize an update (error: {e}); skipping all remaining ones!");
                    return Ok(updates);
                }
            }
        }
        Ok(updates)
    }
}

#[cfg(test)]
#[cfg(feature = "derive")]
mod tests {
    use crate as bjw_db;

    use super::*;
    use serde::Deserialize;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    #[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
    struct KeyValueStore {
        store: BTreeMap<String, String>,
    }

    #[bjw_db_derive::derive_bjw_db]
    impl KeyValueStore {
        pub fn insert(&mut self, key: String, value: String) {
            self.store.insert(key, value);
        }

        pub fn insert_with_check(&mut self, key: String, value: String) -> bool {
            if self.store.contains_key(&key) {
                false
            } else {
                self.insert(key, value);
                true
            }
        }

        pub fn get(&self, key: &str) -> Option<String> {
            self.store.get(key).cloned()
        }

        pub fn unused(&self, _key: &str) -> u64 {
            unreachable!()
        }
    }

    #[test]
    fn test_normal_operation() {
        let tempdir = TempDir::with_prefix("bjw-").unwrap();

        // create new db
        let path = tempdir.path().join("kv-store");
        let mut db = KeyValueStoreDb::open(&path).unwrap();
        db.insert("key".to_string(), "value".to_string()).unwrap();
        db.insert("more".to_string(), "value".to_string()).unwrap();
        assert!(!db
            .insert_with_check("key".to_string(), "".to_string())
            .unwrap());
        assert_eq!(db.get("key"), Some("value".to_string()));

        // create a checkpoint
        db.create_checkpoint().unwrap();
        db.insert("another".to_string(), "pair".to_string())
            .unwrap();

        // re-open db
        let data = db.clone_data();
        let mut db = KeyValueStoreDb::open(&path).unwrap();
        assert_eq!(data, db.clone_data());

        // create a checkpoint and don't update, but re-open right away (-> tests empty log)
        db.create_checkpoint().unwrap();
        let db = KeyValueStoreDb::open(&path).unwrap();

        // delete
        db.delete().unwrap();
    }
}
