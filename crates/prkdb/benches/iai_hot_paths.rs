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
//!
//! **WAL benchmarks and the whole-process entry point (TST-09).** `#[library_benchmark]`'s
//! default `EntryPoint` sets callgrind's `--toggle-collect` to
//! `*::__gungraun_wrapper_mod::*` (`gungraun-runner-0.19.4`'s `runner::DEFAULT_TOGGLE`),
//! which toggles collection on entry to *and exit from* every symbol matching that glob —
//! including the `async { .. }` block's `poll` closure, which is itself compiled as a
//! symbol under the wrapper module. Entering that closure flips collection back **off**
//! for the WAL work the closure performs, which is why the WAL benchmarks on `main`
//! measure only ~500 instructions for 100 puts (TST-09; confirmed on Linux, see the fix
//! commit body for the probe run and per-thread counts). Toggle state is also per
//! `std::thread`, which will matter once the write path moves onto a dedicated WAL writer
//! thread (Task 2.6): the benchmark's own thread never runs that thread's code, so a
//! per-function toggle would never see it either way.
//!
//! The fix does not depend on picking apart those two effects: every WAL benchmark below
//! disables the default entry point (`EntryPoint::None` + `--collect-at-start=no`, the
//! pattern gungraun's own `Callgrind::entry_point` rustdoc documents for client-request
//! benchmarks) and instead brackets the measured call with the `client_requests` crate
//! feature's `start_instrumentation`/`stop_instrumentation`. Those client requests switch
//! Valgrind's instrumentation for the **whole process**, not a per-thread toggle, so
//! every thread's work between the two calls is counted — including a WAL writer thread
//! once one exists. The measured body also lives in a free function *outside* the
//! `#[library_benchmark]`-annotated function, so no part of it is a symbol under
//! `__gungraun_wrapper_mod` that the (now-disabled) default toggle could still affect.
//! One consequence: any background runtime threads that happen to be running (e.g. idle
//! tokio workers) are also counted for as long as instrumentation is on, since the
//! switch is process-wide, not scoped to a particular thread or call stack; Task 2.8d
//! moves these benchmarks to a current-thread runtime once the adapter no longer needs
//! `block_in_place`, which removes that source of noise.
//!
//! The two `LogRecord` benchmarks below (`bench_log_record_encode`,
//! `bench_log_record_decode`) keep the default entry point: they are the pure,
//! single-threaded reference benchmarks `scripts/perf_gate_floors.toml`'s ratios divide
//! by, so their measured region is unchanged.
//!
//! `WalStorageAdapter::new` internally calls `tokio::task::block_in_place` +
//! `tokio::runtime::Handle::current()` (see `crates/prkdb/src/storage/wal_adapter.rs`), so
//! it must be constructed from inside an active runtime task on a multi-thread runtime —
//! `rt.block_on(async { .. })`, not merely `rt.enter()` (which sets the "current" handle
//! but does not run on a worker thread, and `block_in_place` panics off one). Every WAL
//! setup function below builds the adapter that way, on a `Builder::new_multi_thread()`
//! runtime (single worker thread is enough here; multi-thread is required only because
//! `block_in_place` panics on a current-thread runtime).
//!
//! Every benchmark function below also returns its fixture instead of dropping it.
//! `gungraun-macros` 0.9.1's codegen (`src/lib_bench.rs`, `render_standalone` around
//! lines 623-660, mirrored for the `#[bench::..]` case in `render_as_code` around
//! lines 294-315) places the *body* of the annotated function inside an inner
//! `mod __gungraun_wrapper_mod`, and callgrind's default `--toggle-collect` glob
//! (`*::__gungraun_wrapper_mod::*`, `gungraun-runner-0.19.4`'s
//! `runner::DEFAULT_TOGGLE`) starts counting on entry to that module and stops when it
//! *returns* to its caller (a separate, uncounted `__gungraun_wrapper_id_mod::wrapper`
//! function). If the annotated function takes an owned fixture and does not return it,
//! the fixture's drop glue runs as part of that function's own epilogue — i.e. before
//! the `ret`, still inside the counted region — so `Runtime` shutdown, `WalStorageAdapter`
//! drop (closing WAL file handles), and `TempDir`'s directory removal would all be
//! counted as if they were part of the operation under test. Returning the fixture moves
//! it out instead: the uncounted caller (`__gungraun_wrapper_id_mod::wrapper`, whose
//! return type mirrors the annotated function's via `to_caller_signature`'s
//! `..self.0.clone()`) receives it and the top-level `__run` function drops it via
//! `let _ = ..`, entirely outside `__gungraun_wrapper_mod`. For the WAL benchmarks this
//! still holds even though the counted work itself moved to a free function: the
//! `#[library_benchmark]`-annotated function's own body — the `start_instrumentation`
//! call, the `block_on`, and `stop_instrumentation` — is still what gets wrapped, and its
//! signature still returns the fixture for the same reason.

use gungraun::client_requests::callgrind::{start_instrumentation, stop_instrumentation};
use gungraun::{library_benchmark, library_benchmark_group, main};
use gungraun::{Callgrind, EntryPoint, LibraryBenchmarkConfig};
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

/// The number of puts `bench_wal_put_100` performs per run (plan: "put (1 KiB, 100
/// iterations)"). gungraun/Valgrind measures a single execution of the benchmark
/// function rather than looping it like Criterion does, so the 100 iterations are a loop
/// inside the function itself.
const PUT_ITERATIONS: usize = 100;

/// Callgrind config for the WAL benchmarks (TST-09): whole-process, client-request-gated
/// instrumentation instead of the default per-function toggle. See the module doc
/// comment for why the default toggle undercounts the WAL work.
///
/// Two Linux probe iterations (Task 2.3 step 6) before this converged:
///
/// 1. `Callgrind::entry_point`'s rustdoc (gungraun 0.19.4's `src/common.rs`) pairs
///    `EntryPoint::None` with a `--collect-at-start=no` argument, but that spelling is
///    the rustdoc's own prose, not a real Valgrind flag: Valgrind rejected it outright
///    (`valgrind: Unknown option: --collect-at-start=no`).
/// 2. Fixing the spelling to Callgrind's real `--collect-atstart=no` flag (no hyphen
///    between `collect` and `atstart`) made the benchmark run, but every WAL bench came
///    back at exactly 0 Ir. `--collect-atstart`/`--toggle-collect` gate *collection*
///    (counting into cost centers) assuming instrumentation is already active;
///    `start_instrumentation`/`stop_instrumentation`
///    (`valgrind-requests-1.1.0`'s `src/callgrind.rs`) instead call
///    `VR_START_INSTRUMENTATION`/`VR_STOP_INSTRUMENTATION`, which gate *instrumentation*
///    itself and are documented there as paired with `--instr-atstart=no`, not
///    `--collect-atstart`. With collection left at its default (on) and instrumentation
///    off until `start_instrumentation()`, there was nothing running to collect from.
///
/// The combination below is the one the `valgrind-requests` docs actually pair with
/// these two client requests: `--instr-atstart=no` (no instrumentation, hence nothing
/// counted, until `start_instrumentation()`) with collection left enabled
/// (`--collect-atstart=yes`, the default, listed explicitly for clarity) so that once
/// instrumentation turns on, counting starts immediately rather than needing a further
/// `--collect-atstart`/`toggle_collect` dance.
fn whole_process() -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config.tool(
        Callgrind::with_args(["--instr-atstart=no", "--collect-atstart=yes"])
            .entry_point(EntryPoint::None),
    );
    config
}

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

// Setup for `bench_wal_put_100`: runtime, tempdir, adapter, and the 1 KiB value are all
// built here, outside the measured region.
fn setup_wal_put() -> (Runtime, TempDir, WalStorageAdapter, Vec<u8>) {
    let rt = single_worker_runtime();
    let dir = tempfile::tempdir().unwrap();
    let adapter = rt.block_on(async { wal_adapter_in(dir.path()) });
    let value = one_kib_value();
    (rt, dir, adapter, value)
}

// `WalStorageAdapter::put` for a single 1 KiB value, called `PUT_ITERATIONS` times. A
// free function, outside the `#[library_benchmark]`-annotated function below, so no part
// of it is a symbol under `__gungraun_wrapper_mod` (module doc comment).
async fn put_100(adapter: &WalStorageAdapter, value: &[u8]) {
    for i in 0..PUT_ITERATIONS {
        let key = format!("bench-key-{i}").into_bytes();
        adapter
            .put(black_box(&key), black_box(value))
            .await
            .unwrap();
    }
}

// (a plain `//` comment, not `///`: gungraun's `#[library_benchmark]` macro rejects any
// other attribute on the function it decorates, and a doc comment desugars to one.)
#[library_benchmark(setup = setup_wal_put, config = whole_process())]
fn bench_wal_put_100(
    (rt, dir, adapter, value): (Runtime, TempDir, WalStorageAdapter, Vec<u8>),
) -> (Runtime, TempDir, WalStorageAdapter, Vec<u8>) {
    start_instrumentation();
    rt.block_on(put_100(&adapter, &value));
    stop_instrumentation();
    // Returned (not dropped here) so the runtime/tempdir/adapter teardown happens in
    // the uncounted caller — see the module doc comment.
    (rt, dir, adapter, value)
}

// Setup for `bench_wal_get_one`: the fixture key is pre-inserted here, outside the
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

// `WalStorageAdapter::get` against a key already present (hit path). Free function, same
// reasoning as `put_100` above.
async fn get_one(adapter: &WalStorageAdapter) {
    let got = adapter.get(black_box(b"bench-key")).await.unwrap();
    black_box(got);
}

#[library_benchmark(setup = setup_wal_get_hit, config = whole_process())]
fn bench_wal_get_one(
    (rt, dir, adapter): (Runtime, TempDir, WalStorageAdapter),
) -> (Runtime, TempDir, WalStorageAdapter) {
    start_instrumentation();
    rt.block_on(get_one(&adapter));
    stop_instrumentation();
    // See the module doc comment: return the fixture so its teardown happens outside
    // the counted region.
    (rt, dir, adapter)
}

/// A batch of key/value entries for `bench_wal_batch_of_100`.
type WalEntries = Vec<(Vec<u8>, Vec<u8>)>;

/// Everything `bench_wal_batch_of_100` needs, built by its `setup` function.
type WalPutBatchFixture = (Runtime, TempDir, WalStorageAdapter, WalEntries);

// Setup for `bench_wal_batch_of_100`: the 100 key/value entries are built here, outside
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

/// What's left of the fixture after `put_batch` consumes `entries`: the runtime,
/// tempdir, and adapter, whose teardown must happen outside the counted region.
type WalPutBatchTeardown = (Runtime, TempDir, WalStorageAdapter);

// `WalStorageAdapter::put_batch` for 100 entries. Free function, same reasoning as
// `put_100` above.
async fn put_batch_100(adapter: &WalStorageAdapter, entries: WalEntries) {
    adapter.put_batch(black_box(entries)).await.unwrap();
}

#[library_benchmark(setup = setup_wal_put_batch_100, config = whole_process())]
fn bench_wal_batch_of_100((rt, dir, adapter, entries): WalPutBatchFixture) -> WalPutBatchTeardown {
    start_instrumentation();
    rt.block_on(put_batch_100(&adapter, entries));
    stop_instrumentation();
    // `entries` is consumed by `put_batch` itself (real work, not fixture teardown) so
    // it can't be returned too; the runtime/tempdir/adapter still are, per the module
    // doc comment.
    (rt, dir, adapter)
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct IaiIndexedRecord {
    #[id]
    id: u64,
    #[index]
    name: String,
    data: Vec<u8>,
}

// Setup for `bench_indexed_insert_one`: the runtime, storage, and record are all built
// here, outside the measured region.
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

// `IndexedStorage::insert` for a collection with one secondary index. Free function,
// same reasoning as `put_100` above.
async fn insert_one(storage: &IndexedStorage<InMemoryAdapter>, record: &IaiIndexedRecord) {
    storage.insert(black_box(record)).await.unwrap();
}

#[library_benchmark(setup = setup_indexed_storage_insert, config = whole_process())]
fn bench_indexed_insert_one(
    (rt, storage, record): (Runtime, IndexedStorage<InMemoryAdapter>, IaiIndexedRecord),
) -> (Runtime, IndexedStorage<InMemoryAdapter>, IaiIndexedRecord) {
    start_instrumentation();
    rt.block_on(insert_one(&storage, &record));
    stop_instrumentation();
    // `insert` only borrows `record`; return the whole fixture so the runtime and
    // storage teardown happens outside the counted region (module doc comment).
    (rt, storage, record)
}

fn new_log_record() -> LogRecord {
    LogRecord::new(LogOperation::Put {
        collection: String::new(),
        id: b"bench-key".to_vec(),
        data: one_kib_value(),
    })
}

// `LogRecord` encode (on-disk `serialize`). Kept on the default entry point: this is one
// of the pure, single-threaded reference benchmarks the floors in
// scripts/perf_gate_floors.toml divide by (module doc comment).
#[library_benchmark(setup = new_log_record)]
fn bench_log_record_encode(record: LogRecord) -> LogRecord {
    black_box(record.serialize());
    // `serialize` only borrows `record` (`&self`); return the input fixture itself so
    // its drop (the 1 KiB `data` buffer) happens outside the counted region, per the
    // module doc comment.
    record
}

// Setup for `bench_log_record_decode`: the record is built and serialized here, outside
// the measured region, so only `deserialize` itself is counted.
fn setup_log_record_decode() -> Vec<u8> {
    new_log_record().serialize()
}

// `LogRecord` decode (on-disk `deserialize`), the inverse of the encode benchmark above.
// Kept on the default entry point, same reasoning as encode above.
#[library_benchmark(setup = setup_log_record_decode)]
fn bench_log_record_decode(bytes: Vec<u8>) -> Vec<u8> {
    black_box(LogRecord::deserialize(black_box(&bytes)).unwrap());
    // `deserialize` only borrows `bytes` (`&[u8]`); return the input fixture so its
    // drop happens outside the counted region, same reasoning as encode above.
    bytes
}

library_benchmark_group!(
    name = hot_paths;
    benchmarks =
        bench_wal_put_100,
        bench_wal_get_one,
        bench_wal_batch_of_100,
        bench_indexed_insert_one,
        bench_log_record_encode,
        bench_log_record_decode,
);

main!(library_benchmark_groups = hot_paths);
