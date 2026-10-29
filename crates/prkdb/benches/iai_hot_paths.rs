//! Deterministic instruction-count benchmarks for the Phase 1 performance gate.
//!
//! Async operations use process-wide Callgrind client requests with instrumentation
//! initially disabled and collection enabled. Their free operation helpers live outside
//! gungraun's generated wrapper, so polling a future cannot toggle collection around
//! nested wrapper symbols. Setup, controls parsing, exact postcondition checks and
//! fixture teardown all happen outside the instrumented interval. Fixtures are returned
//! so runtime shutdown and temporary-directory removal remain outside measurement.
//!
//! The pure LogRecord encode/decode reference benchmarks keep their original bodies,
//! IDs and gungraun default entry-point collection.

use gungraun::{
    library_benchmark, library_benchmark_group, main, Callgrind, EntryPoint, LibraryBenchmarkConfig,
};
use prkdb_core::wal::log_record::{LogOperation, LogRecord};
use std::hint::black_box;

#[path = "support/perf_gate_measurement.rs"]
mod measurement;
use measurement::{one_kib_value, Controls, IndexedFixture, WalFixture};

fn measured_config() -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    // Gungraun clears the child's environment by default. Forward only the two
    // explicit measurement controls so the selected canary reaches benchmark setup.
    config.pass_through_envs(["PRKDB_PERF_CANARY", "PRKDB_PERF_SKIP_OPERATION"]);
    config.tool(
        Callgrind::with_args(["--instr-atstart=no", "--collect-atstart=yes"])
            .entry_point(EntryPoint::None),
    );
    config
}

fn benchmark_controls() -> Controls {
    let controls = Controls::from_env().expect("valid performance controls");
    assert!(
        controls.instrumentation_enabled(),
        "performance benchmarks require Callgrind instrumentation"
    );
    controls
}

fn setup_wal_put() -> WalFixture {
    measurement::setup_wal_put(benchmark_controls())
}

#[library_benchmark(setup = setup_wal_put, config = measured_config())]
fn bench_wal_put_100_measured(fixture: WalFixture) -> WalFixture {
    let fixture = measurement::run_wal_put(fixture);
    measurement::validate_wal_put(&fixture).expect("100 exact 1 KiB puts and canary");
    fixture
}

fn setup_wal_get_hit() -> WalFixture {
    measurement::setup_wal_get_hit(benchmark_controls())
}

#[library_benchmark(setup = setup_wal_get_hit, config = measured_config())]
fn bench_wal_get_hit_measured(fixture: WalFixture) -> WalFixture {
    let fixture = measurement::run_wal_get_hit(fixture);
    measurement::validate_wal_get_hit(&fixture).expect("one exact 1 KiB get and canary");
    fixture
}

fn setup_wal_put_batch_100() -> WalFixture {
    measurement::setup_wal_put_batch_100(benchmark_controls())
}

#[library_benchmark(setup = setup_wal_put_batch_100, config = measured_config())]
fn bench_wal_put_batch_100_measured(fixture: WalFixture) -> WalFixture {
    let fixture = measurement::run_wal_put_batch_100(fixture);
    measurement::validate_wal_put_batch_100(&fixture)
        .expect("100 exact 1 KiB batch entries and canary");
    fixture
}

fn setup_indexed_insert_one() -> IndexedFixture {
    measurement::setup_indexed_insert_one(benchmark_controls())
}

#[library_benchmark(setup = setup_indexed_insert_one, config = measured_config())]
fn bench_indexed_insert_one_measured(fixture: IndexedFixture) -> IndexedFixture {
    let fixture = measurement::run_indexed_insert_one(fixture);
    measurement::validate_indexed_insert_one(&fixture)
        .expect("one exact 1 KiB indexed record, secondary index and canary");
    fixture
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
        bench_wal_put_100_measured,
        bench_wal_get_hit_measured,
        bench_wal_put_batch_100_measured,
        bench_indexed_insert_one_measured,
        bench_log_record_encode,
        bench_log_record_decode,
);

main!(library_benchmark_groups = hot_paths);
