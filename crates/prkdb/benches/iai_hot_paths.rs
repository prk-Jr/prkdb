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
//!
//! Every benchmark below uses gungraun's `setup` hook (`#[library_benchmark(setup = ..)]`)
//! to build its runtime, tempdir, adapter, and input data before the timed region.
//! `#[library_benchmark]`'s default `EntryPoint` sets callgrind's `--toggle-collect` to the
//! benchmarked function itself (see the `gungraun::common::ValgrindTool::entry_point` docs
//! in gungraun 0.19.4's source, `src/common.rs`), so instruction counting only starts once
//! the benchmark function is entered — everything the setup function does runs before that
//! and is not counted. That is what makes it safe to build the WAL adapter, pre-insert
//! fixture data, etc. in `setup` without polluting the measured instruction count.
//!
//! `WalStorageAdapter::new` internally calls `tokio::task::block_in_place` +
//! `tokio::runtime::Handle::current()` (see `crates/prkdb/src/storage/wal_adapter.rs`), so
//! it must be constructed from inside an active runtime task on a multi-thread runtime —
//! `rt.block_on(async { .. })`, not merely `rt.enter()` (which sets the "current" handle
//! but does not run on a worker thread, and `block_in_place` panics off one). Every WAL
//! setup function below builds the adapter that way, on a `Builder::new_multi_thread()`
//! runtime (single worker thread is enough here; multi-thread is required only because
//! `block_in_place` panics on a current-thread runtime).

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
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

/// The number of puts `bench_wal_put` performs per run (plan: "put (1 KiB, 100
/// iterations)"). gungraun/Valgrind measures a single execution of the benchmark
/// function rather than looping it like Criterion does, so the 100 iterations are a loop
/// inside the function itself.
const PUT_ITERATIONS: usize = 100;

fn one_kib_value() -> Vec<u8> {
    vec![b'x'; 1024]
}

fn single_worker_runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime builds")
}

fn wal_adapter_in(dir: &std::path::Path) -> WalStorageAdapter {
    let config = WalConfig {
        log_dir: dir.to_path_buf(),
        ..WalConfig::test_config()
    };
    WalStorageAdapter::new(config).expect("adapter builds")
}

// Setup for `bench_wal_put`: runtime, tempdir, adapter, and the 1 KiB value are all built
// here, outside the measured region.
fn setup_wal_put() -> (Runtime, TempDir, WalStorageAdapter, Vec<u8>) {
    let rt = single_worker_runtime();
    let dir = tempfile::tempdir().unwrap();
    let adapter = rt.block_on(async { wal_adapter_in(dir.path()) });
    let value = one_kib_value();
    (rt, dir, adapter, value)
}

// `WalStorageAdapter::put` for a single 1 KiB value, called `PUT_ITERATIONS` times.
//
// (a plain `//` comment, not `///`: gungraun's `#[library_benchmark]` macro rejects any
// other attribute on the function it decorates, and a doc comment desugars to one.)
#[library_benchmark(setup = setup_wal_put)]
fn bench_wal_put((rt, _dir, adapter, value): (Runtime, TempDir, WalStorageAdapter, Vec<u8>)) {
    rt.block_on(async {
        for i in 0..PUT_ITERATIONS {
            let key = format!("bench-key-{i}").into_bytes();
            adapter
                .put(black_box(&key), black_box(&value))
                .await
                .unwrap();
        }
    });
}

// Setup for `bench_wal_get_hit`: the fixture key is pre-inserted here, outside the
// measured region, so only the `get` itself is counted.
fn setup_wal_get_hit() -> (Runtime, TempDir, WalStorageAdapter) {
    let rt = single_worker_runtime();
    let dir = tempfile::tempdir().unwrap();
    let adapter = rt.block_on(async {
        let adapter = wal_adapter_in(dir.path());
        adapter.put(b"bench-key", &one_kib_value()).await.unwrap();
        adapter
    });
    (rt, dir, adapter)
}

// `WalStorageAdapter::get` against a key already present (hit path).
#[library_benchmark(setup = setup_wal_get_hit)]
fn bench_wal_get_hit((rt, _dir, adapter): (Runtime, TempDir, WalStorageAdapter)) {
    rt.block_on(async {
        let got = adapter.get(black_box(b"bench-key")).await.unwrap();
        black_box(got);
    });
}

/// A batch of key/value entries for `bench_wal_put_batch_100`.
type WalEntries = Vec<(Vec<u8>, Vec<u8>)>;

/// Everything `bench_wal_put_batch_100` needs, built by its `setup` function.
type WalPutBatchFixture = (Runtime, TempDir, WalStorageAdapter, WalEntries);

// Setup for `bench_wal_put_batch_100`: the 100 key/value entries are built here, outside
// the measured region, so only `put_batch` itself is counted.
fn setup_wal_put_batch_100() -> WalPutBatchFixture {
    let rt = single_worker_runtime();
    let dir = tempfile::tempdir().unwrap();
    let adapter = rt.block_on(async { wal_adapter_in(dir.path()) });
    let entries: WalEntries = (0..100u32)
        .map(|i| (format!("key-{i}").into_bytes(), one_kib_value()))
        .collect();
    (rt, dir, adapter, entries)
}

// `WalStorageAdapter::put_batch` for 100 entries.
#[library_benchmark(setup = setup_wal_put_batch_100)]
fn bench_wal_put_batch_100((rt, _dir, adapter, entries): WalPutBatchFixture) {
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

// Setup for `bench_indexed_storage_insert`: the runtime, storage, and record are all
// built here, outside the measured region.
fn setup_indexed_storage_insert() -> (Runtime, IndexedStorage<InMemoryAdapter>, IaiIndexedRecord) {
    let rt = single_worker_runtime();
    let storage = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
    let record = IaiIndexedRecord {
        id: 1,
        name: "bench-record".to_string(),
        data: vec![b'x'; 1024],
    };
    (rt, storage, record)
}

// `IndexedStorage::insert` for a collection with one secondary index.
#[library_benchmark(setup = setup_indexed_storage_insert)]
fn bench_indexed_storage_insert(
    (rt, storage, record): (Runtime, IndexedStorage<InMemoryAdapter>, IaiIndexedRecord),
) {
    rt.block_on(async {
        storage.insert(black_box(&record)).await.unwrap();
    });
}

fn new_log_record() -> LogRecord {
    LogRecord::new(LogOperation::Put {
        collection: String::new(),
        id: b"bench-key".to_vec(),
        data: one_kib_value(),
    })
}

// `LogRecord` encode (on-disk `serialize`).
#[library_benchmark(setup = new_log_record)]
fn bench_log_record_encode(record: LogRecord) {
    black_box(record.serialize());
}

// Setup for `bench_log_record_decode`: the record is built and serialized here, outside
// the measured region, so only `deserialize` itself is counted.
fn setup_log_record_decode() -> Vec<u8> {
    new_log_record().serialize()
}

// `LogRecord` decode (on-disk `deserialize`), the inverse of the encode benchmark above.
#[library_benchmark(setup = setup_log_record_decode)]
fn bench_log_record_decode(bytes: Vec<u8>) {
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
