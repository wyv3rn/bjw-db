use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::{RwLock, RwLockReadGuard},
};

type Result<T> = anyhow::Result<T>;

pub trait Readable {
    type Args<'a>;
    type ReturnType: Clone;

    fn read(&self, args: &Self::Args<'_>) -> Self::ReturnType;
}

pub trait Updateable {
    type Args: Serialize + DeserializeOwned;

    fn update(&mut self, args: &Self::Args);
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

impl<T: Default + Serialize + DeserializeOwned + Readable + Updateable> Database<T> {
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

    pub fn read(&self, parameters: &<T as Readable>::Args<'_>) -> <T as Readable>::ReturnType {
        let locked = self.data.read().unwrap();
        locked.read(parameters)
    }

    pub fn read_all(&self) -> RwLockReadGuard<'_, T> {
        self.data.read().unwrap()
    }

    pub fn update(&self, parameters: &<T as Updateable>::Args) -> Result<()> {
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
            let parameters: <T as Updateable>::Args = serde_json::from_str(line?.as_ref())?;
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

    fn extend_update_log(&self, parameters: &<T as Updateable>::Args) -> Result<()> {
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

impl<T: Clone> Database<T> {
    pub fn clone_data(&self) -> T {
        self.data.read().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    type KeyValueStore = BTreeMap<String, String>;

    pub enum KvReadArgs<'a> {
        Get(&'a str),
    }

    #[derive(Clone)]
    pub enum KvReadReturn {
        Get(Option<String>),
    }

    impl Readable for KeyValueStore {
        type Args<'a> = KvReadArgs<'a>;
        type ReturnType = KvReadReturn;

        fn read(&self, params: &KvReadArgs) -> KvReadReturn {
            match params {
                KvReadArgs::Get(k) => KvReadReturn::Get(self.get(*k).cloned()),
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    pub enum KvUpdateArgs {
        Insert(String, String),
    }

    impl Updateable for KeyValueStore {
        type Args = KvUpdateArgs;
        fn update(&mut self, params: &KvUpdateArgs) {
            match params {
                KvUpdateArgs::Insert(k, v) => self.insert(k.clone(), v.clone()),
            };
        }
    }

    #[test]
    fn test_key_value_store() {
        let tempdir = TempDir::with_prefix("bjw-").unwrap();

        // create new db
        let path = tempdir.path().join("kv-store");
        let db = Database::<KeyValueStore>::open(&path).unwrap();
        let insert = &KvUpdateArgs::Insert("key".to_string(), "value".to_string());
        db.update(insert).unwrap();
        let insert = &KvUpdateArgs::Insert("more".to_string(), "value".to_string());
        db.update(insert).unwrap();
        let data = db.clone_data();

        // re-open db
        let db = Database::<KeyValueStore>::open(&path).unwrap();
        assert_eq!(data, *db.read_all());

        // delete
        db.delete().unwrap();
    }

    #[test]
    #[cfg(feature = "derive")]
    fn test_derive() {
        #[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
        struct MyKeyValueStore {
            store: BTreeMap<String, String>,
        }

        #[bjw_db_derive::derive_bjw_db]
        impl MyKeyValueStore {
            pub fn insert(&mut self, key: String, value: String) {
                self.store.insert(key, value);
            }

            pub fn get(&self, key: &str) -> Option<String> {
                self.store.get(key).cloned()
            }
        }

        let tempdir = TempDir::with_prefix("bjw-").unwrap();

        // create new db
        let path = tempdir.path().join("kv-store");
        let db = MyKeyValueStoreDb::open(&path).unwrap();
        db.insert("key".to_string(), "value".to_string()).unwrap();
        db.insert("more".to_string(), "value".to_string()).unwrap();
        assert_eq!(db.get("key"), Some("value".to_string()));
        let data = db.clone_data();

        // re-open db
        let db = MyKeyValueStoreDb::open(&path).unwrap();
        assert_eq!(data, db.clone_data());

        // delete
        db.delete().unwrap();
    }
}
