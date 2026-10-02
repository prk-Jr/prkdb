//! Whole sealed-segment stream retention (streaming design §8).
use prkdb::stream_log::{
    Clock, EventSeq, ReadLimits, Record, RetentionPolicy, StartAt, StreamConfig, StreamLog,
};
use prkdb_core::wal::{CompressionConfig, WalHealth};
use prkdb_types::error::StorageError;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct TestClock(AtomicI64, AtomicUsize, tokio::sync::Notify);
impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.1.fetch_add(1, Ordering::SeqCst);
        self.2.notify_one();
        self.0.load(Ordering::SeqCst)
    }
}
impl TestClock {
    fn set(&self, t: i64) {
        self.0.store(t, Ordering::SeqCst);
    }
}
fn config(path: &Path, clock: Arc<TestClock>) -> StreamConfig {
    let mut cfg = StreamConfig::new(path);
    cfg.wal.segment_bytes = 4096;
    cfg.wal.compression = CompressionConfig::none();
    cfg.clock = clock;
    cfg.segment_max_age = Some(Duration::MAX);
    cfg.retention_interval = Duration::ZERO;
    cfg
}
fn rec(n: u8, len: usize) -> Record {
    Record {
        key: None,
        value: vec![n; len],
        headers: Vec::new(),
    }
}
async fn append(log: &StreamLog, clock: &TestClock, t: i64, n: u8) -> EventSeq {
    clock.set(t);
    log.append(vec![rec(n, 3000)]).await.unwrap().first()
}
fn limits() -> ReadLimits {
    ReadLimits {
        max_records: usize::MAX,
        max_bytes: usize::MAX,
    }
}
async fn retained(log: &StreamLog) -> Vec<(EventSeq, u8)> {
    log.read_from(StartAt::Earliest, limits())
        .await
        .unwrap()
        .records
        .into_iter()
        .map(|r| (r.offset, r.value[0]))
        .collect()
}

#[tokio::test]
async fn default_policy_never_removes_records() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(TestClock::default());
    let cfg = config(dir.path(), clock.clone());
    assert_eq!(cfg.retention, RetentionPolicy::default());
    let log = StreamLog::open(cfg).await.unwrap();
    let a = append(&log, &clock, 0, 1).await;
    let b = append(&log, &clock, 1, 2).await;
    clock.set(i64::MAX);
    let report = log.apply_retention().unwrap();
    assert_eq!(report.segments_removed, 0);
    assert!(!report.rolled);
    assert_eq!(retained(&log).await, vec![(a, 1), (b, 2)]);
    log.close().unwrap();
}

#[tokio::test]
async fn age_only_raises_durable_floor_and_preserves_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(TestClock::default());
    let mut cfg = config(dir.path(), clock.clone());
    cfg.retention.max_age = Some(Duration::from_millis(1500));
    let log = StreamLog::open(cfg.clone()).await.unwrap();
    let a = append(&log, &clock, 1000, 1).await;
    let b = append(&log, &clock, 2000, 2).await;
    let c = append(&log, &clock, 3000, 3).await;
    let bytes = log.log_bytes().unwrap();
    let report = log.apply_retention().unwrap();
    assert_eq!(report.segments_removed, 1);
    assert_eq!(report.earliest_before, a);
    assert_eq!(report.earliest_after, b);
    assert_eq!(report.bytes_removed, bytes - log.log_bytes().unwrap());
    assert_eq!(retained(&log).await, vec![(b, 2), (c, 3)]);
    assert!(matches!(
        log.read_from(StartAt::Offset(a), limits()).await,
        Err(StorageError::OffsetOutOfRange { .. })
    ));
    log.close().unwrap();
    let log = StreamLog::open(cfg).await.unwrap();
    assert_eq!(log.earliest(), b);
    assert_eq!(retained(&log).await, vec![(b, 2), (c, 3)]);
    log.close().unwrap();
}

#[tokio::test]
async fn size_only_keeps_at_least_max_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(TestClock::default());
    let mut cfg = config(dir.path(), clock.clone());
    cfg.retention.max_bytes = Some(5000);
    let log = StreamLog::open(cfg).await.unwrap();
    append(&log, &clock, 0, 1).await;
    let b = append(&log, &clock, 0, 2).await;
    let c = append(&log, &clock, 0, 3).await;
    let report = log.apply_retention().unwrap();
    assert_eq!(report.segments_removed, 1);
    assert!(log.log_bytes().unwrap() >= 5000);
    assert_eq!(retained(&log).await, vec![(b, 2), (c, 3)]);
    assert_eq!(log.apply_retention().unwrap().segments_removed, 0);
    log.close().unwrap();
}

#[tokio::test]
async fn age_or_size_removes_only_the_eligible_oldest_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(TestClock::default());
    let mut cfg = config(dir.path(), clock.clone());
    cfg.retention = RetentionPolicy {
        max_age: Some(Duration::from_millis(1000)),
        max_bytes: Some(5000),
    };
    let log = StreamLog::open(cfg).await.unwrap();
    append(&log, &clock, 9000, 1).await; // young but size-eligible
    append(&log, &clock, 0, 2).await; // age-eligible even after size is satisfied
    let c = append(&log, &clock, 9000, 3).await;
    clock.set(9000);
    assert_eq!(log.apply_retention().unwrap().segments_removed, 2);
    assert_eq!(retained(&log).await, vec![(c, 3)]);
    log.close().unwrap();
}

#[tokio::test]
async fn age_uses_maximum_timestamp_and_stops_at_first_ineligible_segment() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(TestClock::default());
    let mut cfg = config(dir.path(), clock.clone());
    cfg.retention.max_age = Some(Duration::from_millis(1000));
    let log = StreamLog::open(cfg).await.unwrap();
    clock.set(0);
    log.append(vec![rec(1, 1200)]).await.unwrap();
    clock.set(9000);
    log.append(vec![rec(2, 1200)]).await.unwrap();
    clock.set(1);
    log.append(vec![rec(3, 1200)]).await.unwrap();
    log.append(vec![rec(4, 3000)]).await.unwrap();
    log.append(vec![rec(5, 3000)]).await.unwrap();
    clock.set(9000);
    assert_eq!(log.apply_retention().unwrap().segments_removed, 0);
    assert_eq!(retained(&log).await.len(), 5);
    log.close().unwrap();
}

#[tokio::test]
async fn active_segment_is_never_removed_even_when_eligible() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(TestClock::default());
    let mut cfg = config(dir.path(), clock.clone());
    cfg.retention = RetentionPolicy {
        max_age: Some(Duration::ZERO),
        max_bytes: Some(0),
    };
    let log = StreamLog::open(cfg).await.unwrap();
    let a = append(&log, &clock, 0, 1).await;
    clock.set(1);
    assert_eq!(log.apply_retention().unwrap().segments_removed, 0);
    assert_eq!(retained(&log).await, vec![(a, 1)]);
    log.close().unwrap();
}

#[tokio::test]
async fn quiet_nonempty_segment_rolls_at_default_quarter_age_then_expires() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(TestClock::default());
    let mut cfg = config(dir.path(), clock.clone());
    cfg.segment_max_age = None;
    cfg.retention.max_age = Some(Duration::from_millis(1000));
    let log = StreamLog::open(cfg).await.unwrap();
    append(&log, &clock, 0, 1).await;
    clock.set(250);
    assert!(!log.apply_retention().unwrap().rolled); // strictly older
    clock.set(251);
    let report = log.apply_retention().unwrap();
    assert!(report.rolled);
    assert_eq!(report.segments_removed, 0);
    clock.set(1001);
    let report = log.apply_retention().unwrap();
    assert!(!report.rolled); // empty active segment must not roll
    assert_eq!(report.segments_removed, 1);
    assert_eq!(log.earliest(), log.next_offset());
    assert!(retained(&log).await.is_empty());
    assert!(matches!(log.health(), WalHealth::Healthy));
    log.close().unwrap();
}

#[tokio::test]
async fn quiet_roll_uses_oldest_append_even_after_backward_clock_jump() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(TestClock::default());
    let mut cfg = config(dir.path(), clock.clone());
    cfg.segment_max_age = Some(Duration::from_millis(100));
    let log = StreamLog::open(cfg).await.unwrap();
    clock.set(1000);
    log.append(vec![rec(1, 100)]).await.unwrap();
    clock.set(0);
    log.append(vec![rec(2, 100)]).await.unwrap();
    clock.set(101);
    assert!(log.apply_retention().unwrap().rolled);
    assert_eq!(retained(&log).await.len(), 2);
    log.close().unwrap();
}

#[tokio::test]
async fn background_task_releases_inner_when_last_handle_drops() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(TestClock::default());
    let mut cfg = config(dir.path(), clock.clone());
    cfg.retention.max_bytes = Some(0);
    cfg.retention_interval = Duration::from_secs(60);
    let log = StreamLog::open(cfg.clone()).await.unwrap();
    append(&log, &clock, 0, 1).await;
    append(&log, &clock, 0, 2).await;
    drop(log);
    assert_eq!(Arc::strong_count(&clock), 2); // caller and saved config, no task ownership
    let reopened = StreamLog::open(cfg).await.unwrap();
    assert_eq!(retained(&reopened).await.len(), 2);
    reopened.close().unwrap();
}

use parking_lot::Mutex;
use prkdb_core::vfs::{LockGuard, OpenMode, Vfs, VfsFile};
#[path = "../../prkdb-verify/src/faultfs.rs"]
mod faultfs;
use faultfs::{FaultFs, Tear};
use rand::SeedableRng;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize};

#[derive(Clone, Copy, Debug)]
enum CrashPoint {
    AfterFloor,
    AfterRemove(usize),
    BeforeSyncDir(usize),
}
#[derive(Default)]
struct RemovalFs {
    fs: FaultFs,
    failures: AtomicUsize,
    attempts: AtomicUsize,
    removes: AtomicUsize,
    pending_remove: AtomicBool,
    crash: Mutex<Option<CrashPoint>>,
    crashed: AtomicBool,
    attempted: tokio::sync::Notify,
    removal_gate: Mutex<Option<Arc<RemovalGate>>>,
    lock_released: Arc<tokio::sync::Notify>,
    read_gate: Arc<Mutex<Option<Arc<RemovalGate>>>>,
    fail_data_sync: Arc<AtomicBool>,
}
impl RemovalFs {
    fn power_loss(&self) -> io::Result<()> {
        self.crash.lock().take();
        self.crashed.store(true, Ordering::SeqCst);
        self.fs
            .power_loss(&mut rand::rngs::StdRng::seed_from_u64(4), Tear::None);
        Err(io::Error::other("injected retention crash"))
    }
}
impl Vfs for RemovalFs {
    fn open(&self, p: &Path, m: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        if p.extension().is_none_or(|e| e != "wal") {
            return self.fs.open(p, m);
        }
        Ok(Arc::new(GatedFile {
            file: self.fs.open(p, m)?,
            gate: self.read_gate.clone(),
            fail_data_sync: self.fail_data_sync.clone(),
        }))
    }
    fn create(&self, p: &Path) -> io::Result<Arc<dyn VfsFile>> {
        if p.extension().is_none_or(|e| e != "wal") {
            return self.fs.create(p);
        }
        Ok(Arc::new(GatedFile {
            file: self.fs.create(p)?,
            gate: self.read_gate.clone(),
            fail_data_sync: self.fail_data_sync.clone(),
        }))
    }
    fn rename(&self, a: &Path, b: &Path) -> io::Result<()> {
        self.fs.rename(a, b)
    }
    fn create_dir_all(&self, p: &Path) -> io::Result<()> {
        self.fs.create_dir_all(p)
    }
    fn read_dir(&self, p: &Path) -> io::Result<Vec<PathBuf>> {
        self.fs.read_dir(p)
    }
    fn exists(&self, p: &Path) -> io::Result<bool> {
        self.fs.exists(p)
    }
    fn lock_exclusive(&self, p: &Path) -> io::Result<Box<dyn LockGuard>> {
        Ok(Box::new(NotifyingLock {
            held: Some(self.fs.lock_exclusive(p)?),
            released: self.lock_released.clone(),
        }))
    }
    fn remove(&self, p: &Path) -> io::Result<()> {
        if p.extension().is_none_or(|e| e != "wal") {
            return self.fs.remove(p);
        }
        if let Some(gate) = self.removal_gate.lock().clone() {
            gate.entered.notify_one();
            let mut released = gate.released.lock();
            while !*released {
                gate.ready.wait(&mut released);
            }
        }
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.attempted.notify_one();
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(io::Error::other("injected remove failure"));
        }
        let point = *self.crash.lock();
        if matches!(point, Some(CrashPoint::AfterFloor)) {
            return self.power_loss();
        }
        self.fs.remove(p)?;
        let removed = self.removes.fetch_add(1, Ordering::SeqCst) + 1;
        self.pending_remove.store(true, Ordering::SeqCst);
        if matches!(point, Some(CrashPoint::AfterRemove(n)) if n == removed) {
            return self.power_loss();
        }
        Ok(())
    }
    fn sync_dir(&self, p: &Path) -> io::Result<()> {
        let point = *self.crash.lock();
        if self.pending_remove.swap(false, Ordering::SeqCst)
            && matches!(point, Some(CrashPoint::BeforeSyncDir(n)) if n == self.removes.load(Ordering::SeqCst))
        {
            return self.power_loss();
        }
        self.fs.sync_dir(p)
    }
}
fn fault_config(fs: &RemovalFs, clock: Arc<TestClock>) -> StreamConfig {
    let path = Path::new("/retention");
    fs.fs.mkdir_durable(path).unwrap();
    config(path, clock)
}

#[tokio::test]
async fn removal_failure_keeps_durable_floor_and_retries_without_poisoning() {
    let fs = Arc::new(RemovalFs::default());
    let clock = Arc::new(TestClock::default());
    let mut cfg = fault_config(&fs, clock.clone());
    cfg.retention.max_age = Some(Duration::from_millis(100));
    let log = StreamLog::open_with_vfs(fs.clone(), cfg.clone())
        .await
        .unwrap();
    append(&log, &clock, 0, 1).await;
    let b = append(&log, &clock, 1000, 2).await;
    fs.failures.store(1, Ordering::SeqCst);
    let err = log.apply_retention().unwrap_err();
    assert!(err.to_string().contains("injected remove failure"));
    assert_eq!(log.earliest(), b);
    assert!(matches!(log.health(), WalHealth::Healthy));
    clock.set(0); // no age-eligible segment now; released prefix must still be retried
    let report = log.apply_retention().unwrap();
    assert_eq!(report.segments_removed, 1);
    assert_eq!(report.earliest_before, b);
    assert_eq!(report.earliest_after, b);
    let c = append(&log, &clock, 0, 3).await;
    assert_eq!(retained(&log).await, vec![(b, 2), (c, 3)]);
    log.close().unwrap();
    let log = StreamLog::open_with_vfs(fs.clone(), cfg).await.unwrap();
    assert_eq!(log.earliest(), b);
    log.close().unwrap();
}

#[tokio::test]
async fn background_retries_removal_failure_at_the_next_interval() {
    let fs = Arc::new(RemovalFs::default());
    let clock = Arc::new(TestClock::default());
    let mut cfg = fault_config(&fs, clock.clone());
    cfg.retention.max_bytes = Some(0);
    cfg.retention_interval = Duration::from_millis(10);
    let log = StreamLog::open_with_vfs(fs.clone(), cfg).await.unwrap();
    append(&log, &clock, 0, 1).await;
    let b = append(&log, &clock, 0, 2).await;
    fs.failures.store(1, Ordering::SeqCst);
    assert!(log.apply_retention().is_err());
    tokio::time::timeout(Duration::from_secs(2), async {
        while fs.attempts.load(Ordering::SeqCst) < 2 {
            fs.attempted.notified().await;
        }
    })
    .await
    .unwrap();
    // Join the in-flight pass via the retention mutex before observing its result.
    log.apply_retention().unwrap();
    assert_eq!(log.earliest(), b);
    assert_eq!(fs.removes.load(Ordering::SeqCst), 1);
    assert!(matches!(log.health(), WalHealth::Healthy));
    log.close().unwrap();
}

async fn crash_at(point: CrashPoint) {
    let fs = Arc::new(RemovalFs::default());
    let clock = Arc::new(TestClock::default());
    let mut cfg = fault_config(&fs, clock.clone());
    cfg.retention.max_bytes = Some(0);
    let log = StreamLog::open_with_vfs(fs.clone(), cfg.clone())
        .await
        .unwrap();
    append(&log, &clock, 0, 1).await;
    append(&log, &clock, 0, 2).await;
    let c = append(&log, &clock, 0, 3).await;
    *fs.crash.lock() = Some(point);
    assert!(log.apply_retention().is_err(), "{point:?}");
    assert!(fs.crashed.load(Ordering::SeqCst), "{point:?}");
    drop(log);
    let log = StreamLog::open_with_vfs(fs.clone(), cfg).await.unwrap();
    assert_eq!(log.earliest(), c, "{point:?}");
    assert_eq!(retained(&log).await, vec![(c, 3)], "{point:?}");
    let segments: Vec<_> = fs
        .read_dir(Path::new("/retention"))
        .unwrap()
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .collect();
    assert_eq!(segments.len(), 1, "leftovers after {point:?}: {segments:?}");
    log.close().unwrap();
}
#[tokio::test]
async fn crash_after_floor_finishes_removal_on_reopen() {
    crash_at(CrashPoint::AfterFloor).await;
}
#[tokio::test]
async fn crash_after_each_remove_finishes_removal_on_reopen() {
    for n in 1..=2 {
        crash_at(CrashPoint::AfterRemove(n)).await;
    }
}
#[tokio::test]
async fn crash_before_each_sync_dir_finishes_removal_on_reopen() {
    for n in 1..=2 {
        crash_at(CrashPoint::BeforeSyncDir(n)).await;
    }
}

#[derive(Default)]
struct RemovalGate {
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    ready: parking_lot::Condvar,
}
impl RemovalGate {
    fn release(&self) {
        *self.released.lock() = true;
        self.ready.notify_all();
    }
}
struct NotifyingLock {
    held: Option<Box<dyn LockGuard>>,
    released: Arc<tokio::sync::Notify>,
}
impl LockGuard for NotifyingLock {}
impl Drop for NotifyingLock {
    fn drop(&mut self) {
        drop(self.held.take());
        self.released.notify_one();
    }
}

#[tokio::test]
async fn dropping_during_background_pass_does_not_wait_on_executor_and_keeps_lock_safe() {
    let fs = Arc::new(RemovalFs::default());
    let clock = Arc::new(TestClock::default());
    let mut cfg = fault_config(&fs, clock.clone());
    cfg.retention.max_bytes = Some(0);
    cfg.retention_interval = Duration::from_millis(10);
    let log = StreamLog::open_with_vfs(fs.clone(), cfg.clone())
        .await
        .unwrap();
    append(&log, &clock, 0, 1).await;
    let b = append(&log, &clock, 0, 2).await;
    let gate = Arc::new(RemovalGate::default());
    let _release_on_panic = ReleaseGate(gate.clone());
    *fs.removal_gate.lock() = Some(gate.clone());
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    drop(log); // must return on this single-thread executor while the pass is blocked
    let refused = StreamLog::open_with_vfs(fs.clone(), cfg.clone())
        .await
        .err()
        .expect("in-flight pass must retain the lock");
    assert!(matches!(refused, StorageError::Locked { .. }));
    gate.release();
    tokio::time::timeout(Duration::from_secs(2), fs.lock_released.notified())
        .await
        .unwrap();
    fs.removal_gate.lock().take();
    let log = StreamLog::open_with_vfs(fs.clone(), cfg).await.unwrap();
    assert_eq!(log.earliest(), b);
    assert_eq!(retained(&log).await, vec![(b, 2)]);
    log.close().unwrap();
}

struct GatedFile {
    file: Arc<dyn VfsFile>,
    gate: Arc<Mutex<Option<Arc<RemovalGate>>>>,
    fail_data_sync: Arc<AtomicBool>,
}
impl VfsFile for GatedFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if let Some(gate) = self.gate.lock().clone() {
            gate.entered.notify_one();
            let mut released = gate.released.lock();
            while !*released {
                gate.ready.wait(&mut released);
            }
        }
        self.file.read_at(offset, buf)
    }
    fn write_at(&self, o: u64, b: &[u8]) -> io::Result<()> {
        self.file.write_at(o, b)
    }
    fn len(&self) -> io::Result<u64> {
        self.file.len()
    }
    fn set_len(&self, n: u64) -> io::Result<()> {
        self.file.set_len(n)
    }
    fn sync_data(&self) -> io::Result<()> {
        if self.fail_data_sync.swap(false, Ordering::SeqCst) {
            return Err(io::Error::other("injected WAL sync failure"));
        }
        self.file.sync_data()
    }
}

#[tokio::test]
async fn cancelled_read_keeps_inner_alive_but_public_drop_stops_background() {
    let fs = Arc::new(RemovalFs::default());
    let clock = Arc::new(TestClock::default());
    let mut cfg = fault_config(&fs, clock.clone());
    cfg.retention.max_bytes = Some(u64::MAX);
    cfg.retention_interval = Duration::from_millis(10);
    let log = StreamLog::open_with_vfs(fs.clone(), cfg.clone())
        .await
        .unwrap();
    let a = append(&log, &clock, 0, 1).await;
    let gate = Arc::new(RemovalGate::default());
    let _release_on_panic = ReleaseGate(gate.clone());
    *fs.read_gate.lock() = Some(gate.clone());
    let mut read = Box::pin(log.read_from(StartAt::Earliest, limits()));
    tokio::select! {
        _ = gate.entered.notified() => {},
        result = &mut read => panic!("read bypassed gate: {result:?}"),
    }
    drop(read);
    let calls = clock.1.load(Ordering::SeqCst);
    drop(log);
    // Observe several configured intervals while the cancelled read retains Inner.
    // Any cleaner driven only by Weak<Inner> would still call the clock here.
    let cleaner_ran = tokio::time::timeout(Duration::from_millis(40), async {
        while clock.1.load(Ordering::SeqCst) == calls {
            clock.2.notified().await;
        }
    })
    .await;
    assert!(cleaner_ran.is_err(), "cleaner survived its public handle");
    let refused = StreamLog::open_with_vfs(fs.clone(), cfg.clone())
        .await
        .err()
        .expect("cancelled read must retain lock");
    assert!(matches!(refused, StorageError::Locked { .. }));
    gate.release();
    tokio::time::timeout(Duration::from_secs(2), fs.lock_released.notified())
        .await
        .unwrap();
    fs.read_gate.lock().take();
    let log = StreamLog::open_with_vfs(fs.clone(), cfg).await.unwrap();
    assert_eq!(retained(&log).await, vec![(a, 1)]);
    log.close().unwrap();
}

// A failed assertion must release blocked I/O before Tokio joins its blocking pool.
struct ReleaseGate(Arc<RemovalGate>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[test]
fn retention_between_read_bounds_and_handle_capture_is_explicitly_out_of_range() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let fs = Arc::new(RemovalFs::default());
        let clock = Arc::new(TestClock::default());
        let mut cfg = fault_config(&fs, clock.clone());
        cfg.retention.max_bytes = Some(0);
        let log = StreamLog::open_with_vfs(fs, cfg).await.unwrap();
        let a = append(&log, &clock, 0, 1).await;
        let b = append(&log, &clock, 0, 2).await;
        let (ready, entered) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let worker = tokio::task::spawn_blocking(move || {
            ready.send(()).unwrap();
            blocked.recv().unwrap();
        });
        entered.await.unwrap();
        let mut read = Box::pin(log.read_from(StartAt::Offset(a), limits()));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(read.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        }).await; // bounds have been sampled; scan is queued behind the blocked worker
        assert_eq!(log.apply_retention().unwrap().earliest_after, b);
        release.send(()).unwrap();
        worker.await.unwrap();
        let err = read.await.expect_err("retention must not silently skip a requested segment");
        assert!(matches!(err, StorageError::OffsetOutOfRange { requested, floor, .. } if requested == a.raw() && floor == b.raw()), "{err:?}");
        log.close().unwrap();
    });
}

async fn refuse_poisoned(pending: bool) {
    let fs = Arc::new(RemovalFs::default());
    let clock = Arc::new(TestClock::default());
    let mut cfg = fault_config(&fs, clock.clone());
    cfg.retention.max_bytes = Some(0);
    let log = StreamLog::open_with_vfs(fs.clone(), cfg).await.unwrap();
    append(&log, &clock, 0, 1).await;
    append(&log, &clock, 0, 2).await;
    if pending {
        fs.failures.store(1, Ordering::SeqCst);
        assert!(log.apply_retention().is_err());
    }
    fs.fail_data_sync.store(true, Ordering::SeqCst);
    assert!(log.append(vec![rec(3, 3000)]).await.is_err());
    assert!(matches!(log.health(), WalHealth::Poisoned(_)));
    let floor = log.earliest();
    let files = fs.read_dir(Path::new("/retention")).unwrap();
    let bytes = log.log_bytes().unwrap();
    let attempts = fs.attempts.load(Ordering::SeqCst);
    let err = log
        .apply_retention()
        .expect_err("retention must refuse a poisoned writer");
    assert!(err.to_string().contains("poisoned"), "{err}");
    assert_eq!(log.earliest(), floor);
    assert_eq!(fs.read_dir(Path::new("/retention")).unwrap(), files);
    assert_eq!(log.log_bytes().unwrap(), bytes);
    assert_eq!(fs.attempts.load(Ordering::SeqCst), attempts);
    drop(log);
}
#[tokio::test]
async fn poisoned_writer_refuses_retention_before_advancing_floor_or_removing_files() {
    refuse_poisoned(false).await;
}
#[tokio::test]
async fn poisoned_writer_refuses_pending_retention_cleanup() {
    refuse_poisoned(true).await;
}
