#[path = "../benches/support/stream_durable_diagnostic.rs"]
mod diagnostic;

use diagnostic::{
    ClockWitness, Diagnostic, Event, MonotonicClock, ObservedFile, ObservedVfs, RECORD_BYTES,
};
use prkdb_core::vfs::{StdVfs, Vfs, VfsFile};
use std::io;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

// Test readers inspect the actual bounded exported files rather than exposing
// test-only clone/decoder APIs in the benchmark observer.
fn decode_event(raw: &[u8]) -> io::Result<Event> {
    if raw.len() != RECORD_BYTES {
        return Err(io::Error::other("incomplete diagnostic record"));
    }
    let mut fields = [0_u64; 11];
    for (field, bytes) in fields.iter_mut().zip(raw.as_chunks::<8>().0.iter()) {
        let mut value = [0; 8];
        value.copy_from_slice(bytes);
        *field = u64::from_le_bytes(value);
    }
    Ok(Event {
        kind: fields[0],
        phase: fields[1],
        operation_id: fields[2],
        start_ns: fields[3],
        append_start_ns: fields[4],
        end_ns: fields[5],
        tid: fields[6],
        arg: fields[7],
        amount: fields[8],
        outcome: fields[9] as i64,
        path_id: fields[10],
    })
}
fn snapshot(d: &Diagnostic) -> (Vec<Event>, serde_json::Value) {
    let dir = tempfile::tempdir().unwrap();
    d.export(dir.path(), "A", 0, 1000, 3000).unwrap();
    let raw = std::fs::read(dir.path().join("events.bin")).unwrap();
    assert_eq!(raw.len() % RECORD_BYTES, 0);
    let events = raw
        .as_chunks::<RECORD_BYTES>()
        .0
        .iter()
        .map(|bytes| decode_event(bytes).unwrap())
        .collect();
    let metadata =
        serde_json::from_slice(&std::fs::read(dir.path().join("metadata.json")).unwrap()).unwrap();
    (events, metadata)
}
trait TestSnapshot {
    fn events(&self) -> Vec<Event>;
    fn phase_totals(&self) -> (u64, u64);
    fn finish_operation(
        &self,
        op: diagnostic::Operation,
        append_start_ns: u64,
        lsn: u64,
        count: u64,
        outcome: i64,
    );
}
impl TestSnapshot for Diagnostic {
    fn events(&self) -> Vec<Event> {
        snapshot(self).0
    }
    fn phase_totals(&self) -> (u64, u64) {
        let metadata = snapshot(self).1;
        (
            metadata["phase_totals"]["warmup"].as_u64().unwrap(),
            metadata["phase_totals"]["measurement"].as_u64().unwrap(),
        )
    }
    fn finish_operation(
        &self,
        op: diagnostic::Operation,
        append_start_ns: u64,
        lsn: u64,
        count: u64,
        outcome: i64,
    ) {
        let end_ns = self.timestamp();
        self.finish_operation_at(op, append_start_ns, end_ns, lsn, count, outcome);
    }
}

struct StepClock(AtomicU64);
impl MonotonicClock for StepClock {
    fn now_ns(&self) -> io::Result<u64> {
        Ok(self.0.fetch_add(10, Ordering::SeqCst))
    }
    fn tid(&self) -> u64 {
        42
    }
}
fn observer(cap: usize) -> Arc<Diagnostic> {
    Diagnostic::new(Arc::new(StepClock(AtomicU64::new(100))), cap).unwrap()
}

#[test]
fn clock_mapping_is_checked_and_has_a_conversion_witness() {
    let anchor = Instant::now();
    let witness = ClockWitness::checked(anchor, 100, 110).unwrap();
    assert_eq!(witness.map(anchor).unwrap(), 105);
    assert_eq!(witness.map(anchor + Duration::from_nanos(7)).unwrap(), 112);
    assert_eq!(witness.uncertainty_ns(), 10);
    assert!(ClockWitness::checked(anchor, 110, 100).is_err());
    assert!(ClockWitness::checked(anchor, 0, 0)
        .unwrap()
        .map(anchor - Duration::from_nanos(1))
        .is_err());
}

#[test]
fn measured_markers_enclose_t0_selected_operations_and_acknowledgements() {
    let d = observer(32 * RECORD_BYTES);
    d.begin_measurement(105);
    let op = d.start_operation(1, 110).unwrap();
    let append = d.timestamp();
    d.finish_operation(op, append, 7, 1, 0);
    d.end_measurement();
    let events = d.events();
    assert_eq!(
        events.iter().map(|e| e.kind).collect::<Vec<_>>(),
        [1, 3, 7, 2]
    );
    assert_eq!(events[1].start_ns, 110);
    assert!(events[0].start_ns <= events[1].start_ns);
    assert!(events[1].start_ns <= events[1].append_start_ns);
    assert!(events[1].append_start_ns <= events[1].end_ns);
    assert!(events[1].end_ns <= events[3].end_ns);
    assert_eq!(
        (events[1].operation_id, events[1].phase, events[1].tid),
        (1, 1, 42)
    );
    assert_eq!((events[1].arg, events[1].amount), (7, 1));
    assert_eq!(events[2].operation_id, events[1].operation_id);
    assert_eq!(events[2].start_ns, events[1].end_ns);
    assert_eq!(events[2].end_ns, events[1].end_ns);
    assert_eq!(events[2].tid, 42);
    assert_eq!(d.phase_totals(), (0, 1));
}

#[derive(Default)]
struct Delegate {
    calls: Mutex<Vec<String>>,
    fail: bool,
}
impl VfsFile for Delegate {
    fn write_at(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("write:{offset}:{bytes:?}"));
        if self.fail {
            Err(io::Error::from_raw_os_error(5))
        } else {
            Ok(())
        }
    }
    fn sync_data(&self) -> io::Result<()> {
        self.calls.lock().unwrap().push("sync".into());
        if self.fail {
            Err(io::Error::from_raw_os_error(28))
        } else {
            Ok(())
        }
    }
    fn read_at(&self, _: u64, _: &mut [u8]) -> io::Result<usize> {
        Ok(0)
    }
    fn set_len(&self, _: u64) -> io::Result<()> {
        Ok(())
    }
    fn len(&self) -> io::Result<u64> {
        Ok(0)
    }
}

#[test]
fn vfs_preserves_bytes_offsets_errors_and_sync_once() {
    let d = observer(32 * RECORD_BYTES);
    let op = d.start_operation(0, 105).unwrap();
    let delegate = Arc::new(Delegate {
        fail: true,
        ..Delegate::default()
    });
    let f = ObservedFile::new(delegate.clone(), d.clone(), 1);
    assert_eq!(f.write_at(17, b"abc").unwrap_err().raw_os_error(), Some(5));
    assert_eq!(f.sync_data().unwrap_err().raw_os_error(), Some(28));
    d.finish_operation(op, 110, 0, 0, -1);
    assert_eq!(
        *delegate.calls.lock().unwrap(),
        ["write:17:[97, 98, 99]", "sync"]
    );
    let e = d.events();
    assert_eq!(
        (e[0].kind, e[0].arg, e[0].amount, e[0].outcome),
        (4, 17, 3, -5)
    );
    assert_eq!((e[1].kind, e[1].outcome), (5, -28));
}

#[test]
fn bounded_breach_finishes_delegate_preserves_sync_and_exports_prefix() {
    let d = observer(4 * RECORD_BYTES);
    let op = d.start_operation(0, 105).unwrap();
    let delegate = Arc::new(Delegate::default());
    let f = ObservedFile::new(delegate.clone(), d.clone(), 1);
    f.write_at(0, b"a").unwrap();
    f.sync_data().unwrap();
    f.write_at(1, b"b").unwrap();
    f.sync_data().unwrap();
    d.finish_operation(op, 110, 1, 1, 0);
    assert!(d.failed());
    assert!(d.start_operation(1, 200).is_none());
    assert_eq!(
        *delegate.calls.lock().unwrap(),
        ["write:0:[97]", "sync", "write:1:[98]", "sync"]
    );
    let dir = tempfile::tempdir().unwrap();
    d.export(dir.path(), "A", 0, 1000, 3000).unwrap();
    let raw = std::fs::read(dir.path().join("events.bin")).unwrap();
    assert!(raw.len() <= 4 * RECORD_BYTES);
    assert_eq!(decode_event(&raw[0..RECORD_BYTES]).unwrap().kind, 4);
    assert_eq!(
        decode_event(&raw[raw.len() - RECORD_BYTES..]).unwrap().kind,
        6
    );
    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("metadata.json")).unwrap()).unwrap();
    assert_eq!(meta["failed"], true);
    assert_eq!(meta["complete"], false);
}

#[test]
fn transparent_vfs_conformance_includes_exclusive_lock_and_read_only_errors() {
    let dir = tempfile::tempdir().unwrap();
    let d = observer(256 * RECORD_BYTES);
    let vfs = ObservedVfs::new(Arc::new(StdVfs), d);
    prkdb_core::vfs::conformance::run(&vfs, dir.path());
    assert!(vfs.exists(dir.path()).unwrap());
}

#[test]
fn binary_record_encoding_is_fixed_little_endian_and_rejects_partial_records() {
    let e = Event {
        kind: 3,
        phase: 1,
        operation_id: 7,
        start_ns: 10,
        append_start_ns: 11,
        end_ns: 12,
        tid: 42,
        arg: 17,
        amount: 1,
        outcome: -28,
        path_id: 0,
    };
    let raw = e.encode();
    assert_eq!(raw.len(), 88);
    assert_eq!(&raw[16..24], &7_u64.to_le_bytes());
    assert_eq!(&raw[72..80], &(-28_i64).to_le_bytes());
    assert_eq!(decode_event(&raw).unwrap(), e);
    assert!(decode_event(&raw[..87]).is_err());
}

#[tokio::test]
async fn real_durable_append_preserves_ack_bytes_and_record_clock() {
    use prkdb::stream_log::{Clock, ReadLimits, Record, StartAt, StreamConfig, StreamLog};
    use prkdb_core::wal::SyncMode;
    struct FixedRecordClock;
    impl Clock for FixedRecordClock {
        fn now_ms(&self) -> i64 {
            123456789
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let d = observer(256 * RECORD_BYTES);
    let mut cfg = StreamConfig::new(dir.path());
    cfg.clock = Arc::new(FixedRecordClock);
    cfg.wal.sync_mode = SyncMode::Durable;
    cfg.retention_interval = Duration::ZERO;
    let log =
        StreamLog::open_with_vfs(Arc::new(ObservedVfs::new(Arc::new(StdVfs), d.clone())), cfg)
            .await
            .unwrap();
    d.begin_measurement(105);
    let op = d.start_operation(1, 110).unwrap();
    let append = d.timestamp();
    let ack = log
        .append(vec![Record {
            key: Some(b"key".to_vec()),
            value: b"value".to_vec(),
            headers: vec![],
        }])
        .await
        .unwrap();
    d.finish_operation(op, append, ack.lsn, ack.count as u64, 0);
    d.end_measurement();
    assert_eq!(ack.count, 1);
    assert!(log.durable_end() > ack.last());
    let records = log
        .read_from(
            StartAt::Earliest,
            ReadLimits {
                max_records: 1,
                max_bytes: 1024,
            },
        )
        .await
        .unwrap();
    assert_eq!(records.records[0].append_time_ms, 123456789);
    assert_eq!(records.records[0].key.as_deref(), Some(b"key".as_slice()));
    assert_eq!(records.records[0].value, b"value");
    let events = d.events();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == 5 && e.operation_id == 1 && e.outcome == 0)
            .count(),
        1
    );
    let event = events.iter().find(|e| e.kind == 3).unwrap();
    assert_eq!((event.arg, event.amount), (ack.lsn, ack.count as u64));
    log.close().unwrap();
}

#[test]
fn clock_failure_does_not_replace_delegated_engine_result() {
    struct FailingClock(AtomicU64);
    impl MonotonicClock for FailingClock {
        fn now_ns(&self) -> io::Result<u64> {
            if self.0.fetch_add(1, Ordering::SeqCst) < 2 {
                Ok(100)
            } else {
                Err(io::Error::other("observer clock failure"))
            }
        }
        fn tid(&self) -> u64 {
            42
        }
    }
    let d = Diagnostic::new(Arc::new(FailingClock(AtomicU64::new(0))), 16 * RECORD_BYTES).unwrap();
    let op = d.start_operation(0, 100).unwrap();
    let delegate = Arc::new(Delegate::default());
    let f = ObservedFile::new(delegate.clone(), d.clone(), 1);
    f.write_at(7, b"x").unwrap();
    f.sync_data().unwrap();
    d.finish_operation(op, 100, 1, 1, 0);
    assert_eq!(*delegate.calls.lock().unwrap(), ["write:7:[120]", "sync"]);
    assert!(d.failed());
}

#[test]
fn invalid_marker_and_operation_correspondence_fails_closed() {
    let d = observer(16 * RECORD_BYTES);
    assert!(d.start_operation(1, 110).is_none());
    assert!(d.failed());
    let d = observer(16 * RECORD_BYTES);
    d.begin_measurement(200);
    assert!(d.start_operation(1, 199).is_none());
    let d = observer(16 * RECORD_BYTES);
    let op = d.start_operation(0, 130).unwrap();
    d.finish_operation(op, 120, 1, 1, 0);
    assert!(d.failed());
    let d = observer(16 * RECORD_BYTES);
    let _op = d.start_operation(0, 100).unwrap();
    assert!(d.start_operation(0, 110).is_none());
    assert!(d.failed());
}

#[test]
fn event_budget_does_not_reset_between_repetitions() {
    let d = observer(5 * RECORD_BYTES);
    let delegate = Arc::new(Delegate::default());
    let f = ObservedFile::new(delegate, d.clone(), 1);
    let op = d.start_operation(0, 100).unwrap();
    f.write_at(0, b"a").unwrap();
    d.finish_operation(op, 120, 1, 1, 0);
    assert!(!d.failed());
    d.reset_cell();
    let op = d.start_operation(0, 140).unwrap();
    f.sync_data().unwrap();
    d.finish_operation(op, 150, 2, 1, 0);
    assert!(d.failed());
    assert_eq!(d.events().last().unwrap().kind, 6);
    assert!(d.events().len() * RECORD_BYTES <= 5 * RECORD_BYTES);
}

#[test]
fn native_clock_is_monotonic_and_identifies_current_native_thread() {
    let clock = diagnostic::NativeClock;
    let before = clock.now_ns().unwrap();
    let after = clock.now_ns().unwrap();
    assert!(before > 0);
    assert!(after >= before);
    assert!(clock.tid() > 0);
    let d = Diagnostic::new(Arc::new(clock), 16 * RECORD_BYTES).unwrap();
    assert!(d.mapped_timestamp(Instant::now()) > 0);
    assert!(!d.failed());
}

#[test]
fn session_enabling_is_explicit_and_source_identity_is_required() {
    use std::process::Command;
    let dir = tempfile::tempdir().unwrap();
    for scenario in [
        "default-off",
        "missing-source",
        "invalid-source",
        "empty-directory",
        "enabled",
    ] {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "diagnostic_session_probe", "--nocapture"])
            .env("PRKDB_DURABLE_DIAGNOSTIC_TEST_CASE", scenario)
            .env_remove("PRKDB_STREAM_DURABLE_DIAGNOSTIC_DIR")
            .env_remove("PRKDB_STREAM_DURABLE_DIAGNOSTIC_SOURCE");
        if scenario != "default-off" {
            command.env(
                "PRKDB_STREAM_DURABLE_DIAGNOSTIC_DIR",
                if scenario == "empty-directory" {
                    "".as_ref()
                } else {
                    dir.path().as_os_str()
                },
            );
        }
        if matches!(scenario, "enabled" | "empty-directory") {
            command.env(
                "PRKDB_STREAM_DURABLE_DIAGNOSTIC_SOURCE",
                "f2a798413b0575abd6416d23a16f417a45aab70b",
            );
        } else if scenario == "invalid-source" {
            command.env("PRKDB_STREAM_DURABLE_DIAGNOSTIC_SOURCE", "not-a-git-sha");
        }
        let result = command.output().unwrap();
        assert!(
            result.status.success(),
            "scenario {scenario}: {} {}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
    }
}

// Spawned by the configuration test with isolated environment variables, so
// configuration coverage does not mutate the parallel test runner's environment.
#[test]
fn diagnostic_session_probe() {
    let Ok(scenario) = std::env::var("PRKDB_DURABLE_DIAGNOSTIC_TEST_CASE") else {
        return;
    };
    let result = diagnostic::Session::from_env();
    match scenario.as_str() {
        "default-off" => assert!(result.unwrap().is_none()),
        "missing-source" | "invalid-source" | "empty-directory" => assert!(result.is_err()),
        "enabled" => {
            let session = result.unwrap().unwrap();
            assert_eq!(session.source, "f2a798413b0575abd6416d23a16f417a45aab70b");
            assert_eq!(
                session.directory,
                std::path::PathBuf::from(
                    std::env::var_os("PRKDB_STREAM_DURABLE_DIAGNOSTIC_DIR").unwrap()
                )
            );
            assert!(!session.diagnostic.failed());
        }
        _ => panic!("unknown diagnostic test scenario"),
    }
}
