use std::{collections::BTreeMap, time::Duration};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use bjw_db_derive::derive_bjw_db;

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
struct KeyValueStore {
    store: BTreeMap<u64, String>,
}

#[derive_bjw_db(thread_safe)]
impl KeyValueStore {
    pub fn insert(&mut self, key: u64, value: String) {
        self.store.insert(key, value);
    }

    pub fn get(&self, key: &u64) -> Option<String> {
        self.store.get(key).cloned()
    }
}

fn create_and_insert(n: u64) -> (KeyValueStoreDb, TempDir) {
    let tempdir = TempDir::with_prefix("bjw-bench-").unwrap();

    // create new db
    let path = tempdir.path().join("kv-store");
    let db = KeyValueStoreDb::open(&path).unwrap();

    // insert `n` key value pairs
    let value = "static value".to_string();
    for i in 0..n {
        db.insert(i, value.clone()).unwrap();
    }
    (db, tempdir)
}

fn bench_create_and_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("create-and-insert");
    for n in (2500..10001).step_by(2500) {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| create_and_insert(n))
        });
    }
    group.finish();
}

fn bench_checkpoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint");
    for n in (250_000..1_000_001).step_by(250_000) {
        let (db, _tempdir) = create_and_insert(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &db, |b, db| {
            b.iter(|| db.create_checkpoint().unwrap())
        });
    }
}

criterion_group! {
    name = key_value_store;
    config = Criterion::default().sample_size(32).warm_up_time(Duration::from_secs(1));
    targets = bench_create_and_insert, bench_checkpoint
}
criterion_main!(key_value_store);
