//! Deterministic instruction-count benchmarks for the Phase 1 performance gate (spec
//! §6.2). These run under Valgrind's callgrind tool via `gungraun` (the actively
//! maintained successor to `iai-callgrind`; see `.github/workflows/perf-gate.yml`), which
//! counts CPU instructions rather than measuring wall-clock time. That makes results
//! reproducible run-to-run and comparable base-SHA-vs-head-SHA in CI, unlike Criterion's
//! wall-clock numbers, which vary with runner load.
//!
//! Valgrind does not run on macOS, so these benchmarks cannot be executed locally on the
//! maintainer's machine. `cargo bench -p prkdb --bench iai_hot_paths --no-run` is the
//! local check: it proves the harness compiles. The gate itself only runs in Linux CI.
//!
//! Covers the hot paths named in spec §6.2: put, get, batch, index update, and WAL
//! append/encode.

use gungraun::{library_benchmark, library_benchmark_group, main};
use prkdb::indexed_storage::IndexedStorage;
use prkdb::storage::{InMemoryAdapter, WalStorageAdapter};
use prkdb_core::wal::log_record::{LogOperation, LogRecord};
use prkdb_core::wal::WalConfig;
use prkdb_macros::Collection;
use prkdb_types::storage::StorageAdapter;
use serde::{Deserialize, Serialize};
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn one_kib_value() -> Vec<u8> {
    vec![b'x'; 1024]
}

fn wal_adapter_in(dir: &std::path::Path) -> WalStorageAdapter {
    let config = WalConfig {
        log_dir: dir.to_path_buf(),
        ..WalConfig::test_config()
    };
    WalStorageAdapter::new(config).expect("adapter builds")
}

// `WalStorageAdapter::put` for a single 1 KiB value.
//
// (a plain `//` comment, not `///`: gungraun's `#[library_benchmark]` macro rejects any
// other attribute on the function it decorates, and a doc comment desugars to one.)
#[library_benchmark]
fn bench_wal_put() {
    let rt = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let adapter = wal_adapter_in(dir.path());
    let value = one_kib_value();

    rt.block_on(async {
        adapter
            .put(black_box(b"bench-key"), black_box(&value))
            .await
            .unwrap();
    });
}

// `WalStorageAdapter::get` against a key already present (hit path).
#[library_benchmark]
fn bench_wal_get_hit() {
    let rt = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let adapter = wal_adapter_in(dir.path());
    let value = one_kib_value();

    rt.block_on(async {
        adapter.put(b"bench-key", &value).await.unwrap();
        let got = adapter.get(black_box(b"bench-key")).await.unwrap();
        black_box(got);
    });
}

// `WalStorageAdapter::put_batch` for 100 entries.
#[library_benchmark]
fn bench_wal_put_batch_100() {
    let rt = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let adapter = wal_adapter_in(dir.path());

    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..100u32)
        .map(|i| (format!("key-{i}").into_bytes(), one_kib_value()))
        .collect();

    rt.block_on(async {
        adapter.put_batch(black_box(entries)).await.unwrap();
    });
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct IaiIndexedRecord {
    #[id]
    id: u64,
    #[index]
    name: String,
    data: Vec<u8>,
}

// `IndexedStorage::insert` for a collection with one secondary index.
#[library_benchmark]
fn bench_indexed_storage_insert() {
    let rt = Runtime::new().unwrap();
    let storage = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
    let record = IaiIndexedRecord {
        id: 1,
        name: "bench-record".to_string(),
        data: vec![b'x'; 1024],
    };

    rt.block_on(async {
        storage.insert(black_box(&record)).await.unwrap();
    });
}

// `LogRecord` encode (on-disk `serialize`).
#[library_benchmark]
fn bench_log_record_encode() {
    let record = LogRecord::new(LogOperation::Put {
        collection: String::new(),
        id: b"bench-key".to_vec(),
        data: one_kib_value(),
    });

    black_box(record.serialize());
}

// `LogRecord` decode (on-disk `deserialize`), the inverse of the encode benchmark above.
#[library_benchmark]
fn bench_log_record_decode() {
    let record = LogRecord::new(LogOperation::Put {
        collection: String::new(),
        id: b"bench-key".to_vec(),
        data: one_kib_value(),
    });
    let bytes = record.serialize();

    black_box(LogRecord::deserialize(black_box(&bytes)).unwrap());
}

library_benchmark_group!(
    name = hot_paths;
    benchmarks =
        bench_wal_put,
        bench_wal_get_hit,
        bench_wal_put_batch_100,
        bench_indexed_storage_insert,
        bench_log_record_encode,
        bench_log_record_decode,
);

main!(library_benchmark_groups = hot_paths);
