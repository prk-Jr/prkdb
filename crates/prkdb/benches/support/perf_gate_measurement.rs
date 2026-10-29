//! Fixtures, operation bodies and postconditions shared by the native contract and
//! the Callgrind benchmarks. Native controls exercise the operation bodies and
//! postconditions without issuing Valgrind client requests. Benchmark controls always
//! instrument the process; only Linux execution can verify the measurement itself.

use prkdb::indexed_storage::IndexedStorage;
use prkdb::storage::{InMemoryAdapter, WalStorageAdapter};
use prkdb_core::wal::WalConfig;
use prkdb_macros::Collection;
use prkdb_types::storage::StorageAdapter;
use serde::{Deserialize, Serialize};
use std::hint::black_box;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

pub const PUT_ITERATIONS: usize = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Instrumentation {
    Native,
    Callgrind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Controls {
    pub canary_iterations: usize,
    pub skip_operation: bool,
    instrumentation: Instrumentation,
}

impl Controls {
    pub fn new(canary_iterations: usize, skip_operation: bool) -> Result<Self, String> {
        if !matches!(canary_iterations, 0 | 4096) {
            return Err("PRKDB_PERF_CANARY must be exactly 0 or 4096".into());
        }
        Ok(Self {
            canary_iterations,
            skip_operation,
            instrumentation: Instrumentation::Native,
        })
    }

    pub fn parse(canary: Option<&str>, skip: Option<&str>) -> Result<Self, String> {
        let canary_iterations = match canary {
            None | Some("0") => 0,
            Some("4096") => 4096,
            Some(_) => return Err("PRKDB_PERF_CANARY must be exactly 0 or 4096".into()),
        };
        let skip_operation = match skip {
            None | Some("0") => false,
            Some("1") => true,
            Some(_) => return Err("PRKDB_PERF_SKIP_OPERATION must be exactly 0 or 1".into()),
        };
        Self::new(canary_iterations, skip_operation)
    }

    pub fn from_env() -> Result<Self, String> {
        fn read(name: &str) -> Result<Option<String>, String> {
            match std::env::var(name) {
                Ok(value) => Ok(Some(value)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(error) => Err(format!("invalid {name}: {error}")),
            }
        }
        let canary = read("PRKDB_PERF_CANARY")?;
        let skip = read("PRKDB_PERF_SKIP_OPERATION")?;
        let mut controls = Self::parse(canary.as_deref(), skip.as_deref())?;
        controls.instrumentation = Instrumentation::Callgrind;
        Ok(controls)
    }

    pub fn instrumentation_enabled(&self) -> bool {
        self.instrumentation == Instrumentation::Callgrind
    }
}

#[inline(never)]
pub fn canary(iterations: usize) -> usize {
    let mut result = black_box(0usize);
    for _ in 0..black_box(iterations) {
        // SAFETY: both volatile accesses use the valid, aligned local result slot.
        // The black-box-dependent update and volatile store must occur every turn.
        unsafe {
            let previous = std::ptr::read_volatile(&result);
            std::ptr::write_volatile(&mut result, previous.wrapping_add(black_box(1)));
        }
    }
    black_box(result)
}

/// Instrument the entire process, including runtime workers, without a symbol toggle.
/// The caller keeps setup, validation and fixture destruction outside this interval.
fn measured<T>(controls: Controls, operation: impl FnOnce() -> T) -> T {
    if controls.instrumentation_enabled() {
        gungraun::client_requests::callgrind::start_instrumentation();
    }
    let result = operation();
    if controls.instrumentation_enabled() {
        gungraun::client_requests::callgrind::stop_instrumentation();
    }
    result
}

pub fn one_kib_value() -> Vec<u8> {
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
    WalStorageAdapter::new(WalConfig {
        log_dir: dir.to_path_buf(),
        ..WalConfig::test_config()
    })
    .expect("adapter builds")
}

pub struct WalFixture {
    pub rt: Runtime,
    pub _dir: TempDir,
    pub adapter: WalStorageAdapter,
    pub value: Vec<u8>,
    pub entries: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    pub controls: Controls,
    pub canary_result: usize,
    /// Outer None means no read was executed; inner None means a measured miss.
    pub observed: Option<Option<Vec<u8>>>,
}

pub fn setup_wal_put(controls: Controls) -> WalFixture {
    let rt = single_worker_runtime();
    let dir = tempfile::tempdir().unwrap();
    let adapter = rt.block_on(async { wal_adapter_in(dir.path()) });
    WalFixture {
        rt,
        _dir: dir,
        adapter,
        value: one_kib_value(),
        entries: None,
        controls,
        canary_result: 0,
        observed: None,
    }
}

pub fn setup_wal_get_hit(controls: Controls) -> WalFixture {
    let fixture = setup_wal_put(controls);
    fixture.rt.block_on(async {
        fixture
            .adapter
            .put(b"bench-key", &fixture.value)
            .await
            .unwrap();
    });
    fixture
}

pub fn setup_wal_put_batch_100(controls: Controls) -> WalFixture {
    let mut fixture = setup_wal_put(controls);
    fixture.entries = Some(
        (0..100u32)
            .map(|i| (format!("key-{i}").into_bytes(), one_kib_value()))
            .collect(),
    );
    fixture
}

async fn wal_put_operation(adapter: &WalStorageAdapter, value: &[u8], controls: Controls) -> usize {
    let result = canary(controls.canary_iterations);
    if !controls.skip_operation {
        for i in 0..PUT_ITERATIONS {
            let key = format!("bench-key-{i}").into_bytes();
            adapter
                .put(black_box(&key), black_box(value))
                .await
                .unwrap();
        }
    }
    result
}

pub fn run_wal_put(mut fixture: WalFixture) -> WalFixture {
    fixture.canary_result = measured(fixture.controls, || {
        fixture.rt.block_on(wal_put_operation(
            &fixture.adapter,
            &fixture.value,
            fixture.controls,
        ))
    });
    fixture
}

async fn wal_get_hit_operation(
    adapter: &WalStorageAdapter,
    controls: Controls,
) -> (usize, Option<Option<Vec<u8>>>) {
    let result = canary(controls.canary_iterations);
    let observed = if controls.skip_operation {
        None
    } else {
        let got = adapter.get(black_box(b"bench-key")).await.unwrap();
        Some(black_box(got))
    };
    (result, observed)
}

pub fn run_wal_get_hit(mut fixture: WalFixture) -> WalFixture {
    (fixture.canary_result, fixture.observed) = measured(fixture.controls, || {
        fixture
            .rt
            .block_on(wal_get_hit_operation(&fixture.adapter, fixture.controls))
    });
    fixture
}

async fn wal_put_batch_operation(
    adapter: &WalStorageAdapter,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    controls: Controls,
) -> usize {
    let result = canary(controls.canary_iterations);
    if !controls.skip_operation {
        adapter.put_batch(black_box(entries)).await.unwrap();
    }
    result
}

pub fn run_wal_put_batch_100(mut fixture: WalFixture) -> WalFixture {
    let entries = fixture
        .entries
        .take()
        .expect("batch entries prepared in setup");
    fixture.canary_result = measured(fixture.controls, || {
        fixture.rt.block_on(wal_put_batch_operation(
            &fixture.adapter,
            entries,
            fixture.controls,
        ))
    });
    fixture
}

fn validate_canary(controls: Controls, result: usize) -> Result<(), String> {
    if result == controls.canary_iterations {
        Ok(())
    } else {
        Err(format!(
            "canary returned {result}, expected {}",
            controls.canary_iterations
        ))
    }
}

fn validate_entries(fixture: &WalFixture, prefix: &str) -> Result<(), String> {
    validate_canary(fixture.controls, fixture.canary_result)?;
    fixture.rt.block_on(async {
        for i in 0..PUT_ITERATIONS {
            let key = format!("{prefix}{i}");
            let got = fixture
                .adapter
                .get(key.as_bytes())
                .await
                .map_err(|e| e.to_string())?;
            if got.as_deref() != Some(one_kib_value().as_slice()) {
                return Err(format!("stored value mismatch for {key}"));
            }
        }
        Ok(())
    })
}

pub fn validate_wal_put(fixture: &WalFixture) -> Result<(), String> {
    validate_entries(fixture, "bench-key-")
}

pub fn validate_wal_get_hit(fixture: &WalFixture) -> Result<(), String> {
    validate_canary(fixture.controls, fixture.canary_result)?;
    if fixture.observed == Some(Some(one_kib_value())) {
        Ok(())
    } else {
        Err("measured get did not return the exact 1 KiB fixture value".into())
    }
}

pub fn validate_wal_put_batch_100(fixture: &WalFixture) -> Result<(), String> {
    validate_entries(fixture, "key-")
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct IaiIndexedRecord {
    #[id]
    pub id: u64,
    #[index]
    pub name: String,
    pub data: Vec<u8>,
}

pub struct IndexedFixture {
    pub rt: Runtime,
    pub storage: IndexedStorage<InMemoryAdapter>,
    pub record: IaiIndexedRecord,
    pub controls: Controls,
    pub canary_result: usize,
}

pub fn setup_indexed_insert_one(controls: Controls) -> IndexedFixture {
    IndexedFixture {
        // In-memory indexed storage does not need WAL's block_in_place runtime.
        rt: Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime builds"),
        storage: IndexedStorage::new(Arc::new(InMemoryAdapter::new())),
        record: IaiIndexedRecord {
            id: 1,
            name: "bench-record".to_string(),
            data: one_kib_value(),
        },
        controls,
        canary_result: 0,
    }
}

async fn indexed_insert_operation(
    storage: &IndexedStorage<InMemoryAdapter>,
    record: &IaiIndexedRecord,
    controls: Controls,
) -> usize {
    let result = canary(controls.canary_iterations);
    if !controls.skip_operation {
        storage.insert(black_box(record)).await.unwrap();
    }
    result
}

pub fn run_indexed_insert_one(mut fixture: IndexedFixture) -> IndexedFixture {
    fixture.canary_result = measured(fixture.controls, || {
        fixture.rt.block_on(indexed_insert_operation(
            &fixture.storage,
            &fixture.record,
            fixture.controls,
        ))
    });
    fixture
}

pub fn validate_indexed_insert_one(fixture: &IndexedFixture) -> Result<(), String> {
    validate_canary(fixture.controls, fixture.canary_result)?;
    fixture.rt.block_on(async {
        let got = fixture
            .storage
            .get::<IaiIndexedRecord>(&1)
            .await
            .map_err(|e| e.to_string())?;
        if got.as_ref() != Some(&fixture.record) {
            return Err("indexed insert primary record mismatch".into());
        }
        let indexed = fixture
            .storage
            .query_by::<IaiIndexedRecord>("name", &"bench-record")
            .await
            .map_err(|e| e.to_string())?;
        if indexed != vec![fixture.record.clone()] {
            return Err("indexed insert secondary index mismatch".into());
        }
        if fixture
            .storage
            .count::<IaiIndexedRecord>()
            .await
            .map_err(|e| e.to_string())?
            != 1
        {
            return Err("indexed insert must store exactly one record".into());
        }
        Ok(())
    })
}
