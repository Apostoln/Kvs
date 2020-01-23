#[macro_use]
extern crate criterion;

use criterion::{BatchSize, Criterion, ParameterizedBenchmark};
use kvs::{KvStore, KvsEngine, SledEngine};
use rand::prelude::*;

use std::iter;
use tempfile::TempDir;

fn set_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, _| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let path = temp_dir.path().to_path_buf();
                    (temp_dir, KvStore::open(path).unwrap())
                },
                |tmp_and_store| {
                    let (tmp_dir, mut store) = tmp_and_store;
                    for i in 1..120 {
                        store.set(format!("key{}", i), "value".to_string()).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
        iter::once(()),
    )
        .sample_size(10)
        .with_function("sled", |b, _| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let path = temp_dir.path().to_path_buf();
                    (temp_dir, SledEngine::open(path).unwrap())
                },
                |tmp_db| {
                    let (tmp_dr, mut db) = tmp_db;
                    for i in 1..120 {
                        db.set(format!("key{}", i), "value".to_string()).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        });
    c.bench("set_bench", bench);
}

fn get_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut store = KvStore::open(temp_dir.path()).unwrap();
            for key_i in 1..(1 << i) {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = rand::rngs::SmallRng::from_seed([0; 16]);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1, 1 << i)))
                    .unwrap();
            })
        },
        vec![8/*, 12, 16, 20*/],
    )
        .sample_size(10)
        .with_function("sled", |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut db = SledEngine::open(&temp_dir.path()).unwrap();
            for key_i in 1..(1 << i) {
                db
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                db
                    .get(format!("key{}", rng.gen_range(1, 1 << i)))
                    .unwrap();
            })
        });
    c.bench("get_bench", bench);
}

criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);