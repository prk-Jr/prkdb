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
//! disables the default entry point (`EntryPoint::None` + the Callgrind arguments
//! `--instr-atstart=no --collect-atstart=yes`; see `whole_process`'s doc comment below
//! for why those two flags, not the `--collect-at-start=no` gungraun's own rustdoc
//! example uses, which Valgrind rejects outright) and instead brackets the measured call
//! with the `client_requests` crate feature's `start_instrumentation`/
//! `stop_instrumentation`. Those client requests switch
//! Valgrind's instrumentation for the **whole process**, not a per-thread toggle, so
//! every thread's work between the two calls is counted — including a WAL writer thread
//! once one exists. The measured body also lives in a free function *outside* the
//! `#[library_benchmark]`-annotated function, so no part of it is a symbol under
//! `__gungraun_wrapper_mod` that the (now-disabled) default toggle could still affect.
//! One consequence: any other thread that does work while instrumentation is on is
//! counted too, since the switch is process-wide, not scoped to a particular thread or
//! call stack. That is what counts the `Wal`'s `prkdb-wal-writer` thread (Task 2.8a: the
//! adapter writes through the single WAL, whose writer is a dedicated thread). Since Task
//! 2.8d every benchmark runs on a `current_thread` runtime — the adapter no longer calls
//! `block_in_place`, which was the only reason for a multi-thread one — so there are no
//! idle tokio workers to count, only the benchmark's own thread and the WAL writer.
//!
//! The two `Batch` benchmarks below (`bench_batch_encode`, `bench_batch_decode`: one
//! 1 KiB put through the WAL's frame payload codec) keep the default entry point: they are
//! the pure, single-threaded reference benchmarks `scripts/perf_gate_floors.toml`'s ratios
//! divide by. They replaced the `LogRecord` encode/decode references in Task 2.8d, because
//! `LogRecord` is no longer what the adapter writes.
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
use prkdb::keys::{encode_record_key, CollectionId};
use prkdb::storage::config::StorageConfig;
use prkdb::storage::{InMemoryAdapter, WalStorageAdapter};
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::{CompressionConfig, WalConfig};
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

/// A `current_thread` runtime: no worker threads, so nothing idle runs while the
/// process-wide instrumentation is on (module doc comment).
fn current_thread_runtime() -> Runtime {
    Builder::new_current_thread()
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
    let rt = current_thread_runtime();
    let dir = tempfile::tempdir().unwrap();
    let adapter = wal_adapter_in(dir.path()); // `new` needs no runtime (Task 2.8a)
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

/// `ShardedLruCache` (`crates/prkdb/src/storage/cache.rs`) always uses 16 shards, each
/// with capacity `max(1, cache_capacity / 16)`; any `cache_capacity <= 16` gives every
/// shard capacity exactly 1, so a second `put` into the same shard evicts whatever was
/// there.
const WAL_GET_CACHE_CAPACITY: usize = 16;

/// How many decoy keys `setup_wal_get_one` inserts, after `bench-key`, to evict it from
/// its cache shard. See that function's doc comment for why this count, not a
/// hash-replicated single collision, is what forces the eviction.
const EVICTION_KEYS: u32 = 256;

/// Setup for `bench_wal_get_one`: builds an adapter whose cache cannot still be holding
/// `bench-key` by the time the benchmark runs, so the measured `get` exercises the index
/// lookup and WAL read (`WalStorageAdapter::get`'s steps 2-3), not the cache-hit
/// short-circuit (step 1). Without this, `get_one` was measuring a cache hit — an
/// in-memory hash lookup — not the WAL read its name promises.
///
/// The eviction: `WAL_GET_CACHE_CAPACITY` gives every one of `ShardedLruCache`'s 16
/// shards a capacity of 1, so `bench-key` is inserted, then `EVICTION_KEYS` further
/// distinct keys are inserted. Whichever shard `bench-key` hashed into, at least one of
/// those `EVICTION_KEYS` keys almost certainly lands there too and evicts it: with keys
/// spread roughly uniformly over 16 shards, the chance any given shard is missed by all
/// `EVICTION_KEYS` of them is `(15/16)^EVICTION_KEYS`, effectively zero at 256
/// (`≈ 4e-8`). This count-based approach, not a hand-computed single colliding key, is
/// deliberate: matching `ShardedLruCache::shard_index`'s hash would mean replicating
/// `std::collections::hash_map::DefaultHasher`'s algorithm here, which the standard
/// library explicitly does not guarantee stable across Rust versions — a replica could
/// silently stop matching on a toolchain bump, and this benchmark would quietly go back
/// to measuring a cache hit with no test failure to say so. `bench_wal_get_one` also
/// asserts, outside the counted region, that the measured `get` raised the adapter's
/// `cache_misses` metric by exactly one, so a slide back to a cache hit fails the gate
/// outright instead of relying on the instruction floor alone.
fn setup_wal_get_one() -> (Runtime, TempDir, WalStorageAdapter) {
    let rt = current_thread_runtime();
    let dir = tempfile::tempdir().unwrap();
    let adapter = rt.block_on(async {
        let wal = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let config = StorageConfig {
            wal,
            cache_capacity: WAL_GET_CACHE_CAPACITY,
            ..StorageConfig::default()
        };
        let adapter = WalStorageAdapter::new_with_config(config).expect("adapter builds");
        adapter.put(b"bench-key", &one_kib_value()).await.unwrap();
        for i in 0..EVICTION_KEYS {
            let key = format!("evict-{i}").into_bytes();
            adapter.put(&key, b"x").await.unwrap();
        }
        adapter
    });
    (rt, dir, adapter)
}

// `WalStorageAdapter::get` for a key evicted from cache (see `setup_wal_get_one`'s doc
// comment): exercises the index lookup + WAL read path, not the cache-hit short-circuit.
// Free function, same reasoning as `put_100` above.
async fn get_one(adapter: &WalStorageAdapter) {
    let got = adapter.get(black_box(b"bench-key")).await.unwrap();
    black_box(got);
}

#[library_benchmark(setup = setup_wal_get_one, config = whole_process())]
fn bench_wal_get_one(
    (rt, dir, adapter): (Runtime, TempDir, WalStorageAdapter),
) -> (Runtime, TempDir, WalStorageAdapter) {
    let misses_before = adapter.metrics().cache_misses;
    start_instrumentation();
    rt.block_on(get_one(&adapter));
    stop_instrumentation();
    assert_eq!(
        adapter.metrics().cache_misses - misses_before,
        1,
        "bench_wal_get_one must miss the cache and read the WAL"
    );
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
    let rt = current_thread_runtime();
    let dir = tempfile::tempdir().unwrap();
    let adapter = wal_adapter_in(dir.path()); // `new` needs no runtime (Task 2.8a)
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
    let rt = current_thread_runtime();
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

/// One 1 KiB put: what a single `WalStorageAdapter::put` writes as its frame payload.
fn one_put_batch() -> Batch {
    Batch {
        ops: vec![BatchOp::Put {
            key: b"bench-key".to_vec(),
            value: one_kib_value(),
        }],
    }
}

// `Batch::encode` (uncompressed, as `test_config` writes it). Kept on the default entry
// point: this is one of the pure, single-threaded reference benchmarks the floors in
// scripts/perf_gate_floors.toml divide by (module doc comment).
#[library_benchmark(setup = one_put_batch)]
fn bench_batch_encode(batch: Batch) -> Batch {
    black_box(batch.encode(black_box(&CompressionConfig::none())).unwrap());
    // `encode` only borrows `batch`; return the input fixture itself so its drop (the
    // 1 KiB value) happens outside the counted region, per the module doc comment.
    batch
}

// Setup for `bench_batch_decode`: the batch is built and encoded here, outside the
// measured region, so only `decode` itself is counted.
fn setup_batch_decode() -> Vec<u8> {
    one_put_batch().encode(&CompressionConfig::none()).unwrap()
}

// `Batch::decode`, the inverse of the encode benchmark above. Kept on the default entry
// point, same reasoning as encode above.
#[library_benchmark(setup = setup_batch_decode)]
fn bench_batch_decode(bytes: Vec<u8>) -> Vec<u8> {
    black_box(Batch::decode(black_box(&bytes)).unwrap());
    // `decode` only borrows `bytes`; return the input fixture so its drop happens
    // outside the counted region, same reasoning as encode above.
    bytes
}

// The stored key of one typed record (KEY-01): header plus the order-preserving
// (memcomparable) id, in one allocation — what every typed put and get computes. Three
// typical ids: an integer, a short string, and a UUID in its 36-character text form.
// Default entry point: pure single-threaded CPU work, like the codec benchmarks below.
fn setup_u64_id() -> u64 {
    1_234_567
}

fn setup_string_id() -> String {
    "user-000123".to_string()
}

fn setup_uuid_string_id() -> String {
    "550e8400-e29b-41d4-a716-446655440000".to_string()
}

#[library_benchmark(setup = setup_u64_id)]
fn bench_encode_record_key_u64(id: u64) -> u64 {
    black_box(encode_record_key(black_box(b""), CollectionId(7), black_box(&id)).unwrap());
    id
}

#[library_benchmark(setup = setup_string_id)]
fn bench_encode_record_key_string(id: String) -> String {
    black_box(encode_record_key(black_box(b""), CollectionId(7), black_box(&id)).unwrap());
    // Return the fixture so its drop happens outside the counted region.
    id
}

#[library_benchmark(setup = setup_uuid_string_id)]
fn bench_encode_record_key_uuid(id: String) -> String {
    black_box(encode_record_key(black_box(b""), CollectionId(7), black_box(&id)).unwrap());
    id
}

library_benchmark_group!(
    name = hot_paths;
    benchmarks =
        bench_wal_put_100,
        bench_wal_get_one,
        bench_wal_batch_of_100,
        bench_indexed_insert_one,
        bench_batch_encode,
        bench_batch_decode,
        bench_encode_record_key_u64,
        bench_encode_record_key_string,
        bench_encode_record_key_uuid,
);

main!(library_benchmark_groups = hot_paths);
