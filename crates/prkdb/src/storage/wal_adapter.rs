use super::cache::ShardedLruCache;
use super::config::{CompactionConfig, StorageConfig, SyncMode};
use super::recovery::RecoveryManager;
use super::snapshot::SnapshotWriter;
use super::writer_liveness::{unix_millis, LivenessBounds};
use prkdb_types::snapshot::{CompressionType, SnapshotHeader};

use papaya::HashMap as LockFreeHashMap;
use prkdb_core::batching::adaptive::AdaptiveBatchConfig;
use prkdb_core::replication::{Change, ReplicationManager};
use prkdb_core::vfs::{StdVfs, Vfs};
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::frame::FrameKind;
use prkdb_core::wal::{
    CommitHook, LogOperation, LogRecord, Lsn, RecordLoc, Wal, WalConfig, WalError, WalHealth,
    WalOptions,
};
use prkdb_metrics::storage::StorageMetrics;
use prkdb_types::error::StorageError;
use prkdb_types::storage::{StorageAdapter, WritePathHealth};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};
use tracing::{info, instrument};

/// Builder for WalStorageAdapter
pub struct WalStorageAdapterBuilder {
    config: StorageConfig,
}

impl WalStorageAdapterBuilder {
    pub fn new(log_dir: PathBuf) -> Self {
        Self {
            config: StorageConfig::new(log_dir),
        }
    }

    pub fn with_cache_capacity(mut self, capacity: usize) -> Self {
        self.config.cache_capacity = capacity;
        self
    }

    pub fn with_compaction_config(mut self, config: CompactionConfig) -> Self {
        self.config.compaction = config;
        self
    }

    pub fn with_batching_config(mut self, config: AdaptiveBatchConfig) -> Self {
        self.config.batching = config;
        self
    }

    /// Sets `WalConfig::sync_mode`, the one acknowledgement knob.
    pub fn with_sync_mode(mut self, mode: SyncMode) -> Self {
        self.config.wal.sync_mode = mode;
        self
    }

    pub fn build(self) -> Result<WalStorageAdapter, StorageError> {
        WalStorageAdapter::new_with_config(self.config)
    }
}

/// Test-only fault injection for the storage layer.
///
/// # Why this exists
///
/// `CollectionPartitionedAdapter::flush` forwards to each collection's adapter, and
/// mutation testing replaced its whole body with `Ok(())` — a flush that flushes nothing
/// and reports success — without a single test noticing (run 31358158012, shard 7). It
/// was unkillable through the public surface: this adapter's `put` path writes through
/// rather than accumulating, so a value survives a reopen whether or not `flush` ran.
///
/// The only observable difference is whether the wrapper *forwards* — which needs an
/// inner adapter that can fail. That is what this provides.
///
/// # Why `cfg(test)` and not a Cargo feature
///
/// Both adapters live in this crate, so a unit test compiles with `cfg(test)` active and
/// can reach this directly. A feature would put fault injection in the public API and
/// risk it being enabled in a release build — the mistake the `chaos` feature already
/// made once, where anyone able to write `CHAOS_CONFIG_PATH` could partition a live
/// cluster. Integration tests in `tests/` compile the crate without `cfg(test)`, so this
/// is invisible there, which is correct: it is a unit-level seam.
///
/// Keyed by WAL directory rather than by a flag on the adapter, so no constructor
/// changes. Tests use a unique `tempdir`, so parallel tests cannot collide.
///
/// # Writer faults go through `Vfs` (Task 2.8b)
///
/// `fail_flush_at` is checked at the top of `flush`, before the WAL: it tests that
/// wrappers forward errors and must not poison the log. The writer faults
/// (`fail_append_at`, `stall_writer_at`, `panic_writer_at`) are injected where a real disk
/// fault reaches the log: `open_inner` wraps the adapter's `Vfs` in [`FaultInjectingVfs`],
/// whose files consult this registry on every `write_at`.
///
/// [`FaultInjectingVfs`]: fault_injection::FaultInjectingVfs
#[cfg(test)]
pub(crate) mod fault_injection {
    use prkdb_core::vfs::{OpenMode, Vfs, VfsFile};
    use std::collections::HashSet;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Condvar, Mutex, OnceLock};

    fn registry(kind: Fault) -> &'static Mutex<HashSet<PathBuf>> {
        static FLUSH_FAILURE: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
        static WRITER_STALL: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
        static WRITER_PANIC: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
        static APPEND_FAILURE: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();

        let slot = match kind {
            Fault::FlushFailure => &FLUSH_FAILURE,
            Fault::WriterStall => &WRITER_STALL,
            Fault::WriterPanic => &WRITER_PANIC,
            Fault::AppendFailure => &APPEND_FAILURE,
        };
        slot.get_or_init(|| Mutex::new(HashSet::new()))
    }

    /// Notified whenever a stall is cleared; a stalled `write_at` waits on it with the
    /// `WriterStall` registry's mutex.
    fn stall_cleared() -> &'static Condvar {
        static CLEARED: OnceLock<Condvar> = OnceLock::new();
        CLEARED.get_or_init(Condvar::new)
    }

    #[derive(Clone, Copy)]
    enum Fault {
        FlushFailure,
        WriterStall,
        WriterPanic,
        AppendFailure,
    }

    fn arm(kind: Fault, dir: impl Into<PathBuf>) {
        registry(kind)
            .lock()
            .expect("fault registry lock")
            .insert(dir.into());
    }

    fn disarm(kind: Fault, dir: &Path) {
        registry(kind)
            .lock()
            .expect("fault registry lock")
            .remove(dir);
        if matches!(kind, Fault::WriterStall) {
            stall_cleared().notify_all();
        }
    }

    fn armed(kind: Fault, dir: &Path) -> bool {
        registry(kind)
            .lock()
            .expect("fault registry lock")
            .contains(dir)
    }

    /// Make `flush` fail for the adapter whose WAL lives at `dir`.
    pub fn fail_flush_at(dir: impl Into<PathBuf>) {
        arm(Fault::FlushFailure, dir);
    }

    /// Stop failing `flush` for `dir`. Call it when the assertion is done, so a later
    /// flush in the same test — including one during teardown — is not affected.
    pub fn clear_flush_failure(dir: &Path) {
        disarm(Fault::FlushFailure, dir);
    }

    pub(super) fn flush_should_fail(dir: &Path) -> bool {
        armed(Fault::FlushFailure, dir)
    }

    /// Make the WAL writer's `write_at` fail for the adapter whose WAL lives at `dir`, as a
    /// full disk or an I/O error would: the log is poisoned until reopen.
    ///
    /// Distinct from `fail_flush_at`, which fails the caller-facing `flush` and never
    /// reaches the writer.
    pub fn fail_append_at(dir: impl Into<PathBuf>) {
        arm(Fault::AppendFailure, dir);
    }

    pub fn clear_append_failure(dir: &Path) {
        disarm(Fault::AppendFailure, dir);
    }

    fn append_should_fail(dir: &Path) -> bool {
        armed(Fault::AppendFailure, dir)
    }

    /// Make the writer's `write_at` block until [`clear_writer_stall`], with the writer
    /// thread alive.
    ///
    /// This is the failure the liveness spec exists for: the writer runs, and the only
    /// evidence anything is wrong is that the queue stops moving. Tests use [`StallGuard`],
    /// which clears the stall even when an assertion fails: a stall left armed blocks the
    /// writer, and with it the adapter's drop (which joins the writer), for good.
    pub fn stall_writer_at(dir: impl Into<PathBuf>) {
        arm(Fault::WriterStall, dir);
    }

    pub fn clear_writer_stall(dir: &Path) {
        disarm(Fault::WriterStall, dir);
    }

    /// Blocks while a stall is armed for `dir`.
    fn wait_while_stalled(dir: &Path) {
        let stalled = registry(Fault::WriterStall)
            .lock()
            .expect("fault registry lock");
        let _released = stall_cleared()
            .wait_while(stalled, |set| set.contains(dir))
            .expect("fault registry lock");
    }

    /// Stalls the writer for `dir` while alive and clears the stall when dropped. Declare
    /// it after the adapter, so it drops (and releases the writer) first.
    pub struct StallGuard(PathBuf);

    impl StallGuard {
        pub fn new(dir: impl Into<PathBuf>) -> Self {
            let dir = dir.into();
            stall_writer_at(dir.clone());
            Self(dir)
        }
    }

    impl Drop for StallGuard {
        fn drop(&mut self) {
            clear_writer_stall(&self.0);
        }
    }

    /// Make the writer's `write_at` panic.
    pub fn panic_writer_at(dir: impl Into<PathBuf>) {
        arm(Fault::WriterPanic, dir);
    }

    pub fn clear_writer_panic(dir: &Path) {
        disarm(Fault::WriterPanic, dir);
    }

    fn writer_should_panic(dir: &Path) -> bool {
        armed(Fault::WriterPanic, dir)
    }

    /// A `Vfs` whose files consult the fault registry for `dir` on every `write_at`;
    /// everything else is forwarded to `inner` unchanged.
    pub(crate) struct FaultInjectingVfs {
        inner: Arc<dyn Vfs>,
        dir: PathBuf,
    }

    impl FaultInjectingVfs {
        pub(crate) fn new(inner: Arc<dyn Vfs>, dir: PathBuf) -> Self {
            Self { inner, dir }
        }

        fn wrap(&self, file: Arc<dyn VfsFile>) -> Arc<dyn VfsFile> {
            Arc::new(FaultInjectingFile {
                inner: file,
                dir: self.dir.clone(),
            })
        }
    }

    impl Vfs for FaultInjectingVfs {
        fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
            Ok(self.wrap(self.inner.open(path, mode)?))
        }
        fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
            Ok(self.wrap(self.inner.create(path)?))
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.inner.rename(from, to)
        }
        fn remove(&self, path: &Path) -> io::Result<()> {
            self.inner.remove(path)
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            self.inner.create_dir_all(path)
        }
        fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
            self.inner.read_dir(path)
        }
        fn exists(&self, path: &Path) -> io::Result<bool> {
            self.inner.exists(path)
        }
        fn sync_dir(&self, dir: &Path) -> io::Result<()> {
            self.inner.sync_dir(dir)
        }
    }

    struct FaultInjectingFile {
        inner: Arc<dyn VfsFile>,
        dir: PathBuf,
    }

    impl VfsFile for FaultInjectingFile {
        fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
            if writer_should_panic(&self.dir) {
                panic!("injected writer panic at {}", self.dir.display());
            }
            if append_should_fail(&self.dir) {
                return Err(io::Error::other("injected append failure"));
            }
            wait_while_stalled(&self.dir);
            self.inner.write_at(offset, buf)
        }
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
            self.inner.read_at(offset, buf)
        }
        fn set_len(&self, len: u64) -> io::Result<()> {
            self.inner.set_len(len)
        }
        fn len(&self) -> io::Result<u64> {
            self.inner.len()
        }
        fn sync_data(&self) -> io::Result<()> {
            self.inner.sync_data()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use prkdb_core::vfs::StdVfs;
        use std::time::Duration;

        /// A file under `root`, created through a wrapper registered for `dir`.
        fn file_for(root: &Path, name: &str, dir: &Path) -> Arc<dyn VfsFile> {
            FaultInjectingVfs::new(Arc::new(StdVfs), dir.to_path_buf())
                .create(&root.join(name))
                .expect("create")
        }

        #[test]
        fn a_registered_directory_fails_its_writes_and_an_unregistered_one_does_not() {
            let root = tempfile::tempdir().unwrap();
            let armed_dir = root.path().join("armed");
            let armed_file = file_for(root.path(), "a", &armed_dir);
            let other_file = file_for(root.path(), "b", &root.path().join("other"));

            fail_append_at(&armed_dir);
            let failed = armed_file.write_at(0, b"x");
            let unaffected = other_file.write_at(0, b"y");
            clear_append_failure(&armed_dir);

            let error = failed.expect_err("an armed directory's write must fail");
            assert!(error.to_string().contains("injected append failure"));
            unaffected.expect("another directory's writes are untouched");
            armed_file
                .write_at(0, b"z")
                .expect("once cleared, writes pass again");
        }

        #[test]
        fn a_registered_directory_stalls_its_writes_until_cleared() {
            let root = tempfile::tempdir().unwrap();
            let dir = root.path().join("stalled");
            let file = file_for(root.path(), "a", &dir);
            let other_file = file_for(root.path(), "b", &root.path().join("free"));

            let guard = StallGuard::new(&dir);
            let (tx, rx) = std::sync::mpsc::channel();
            let writer = std::thread::spawn(move || {
                tx.send(file.write_at(0, b"x").is_ok()).unwrap();
            });
            assert!(
                rx.recv_timeout(Duration::from_millis(200)).is_err(),
                "the write must block while the stall is armed"
            );
            other_file
                .write_at(0, b"y")
                .expect("an unregistered directory does not stall");

            drop(guard);
            assert_eq!(
                rx.recv_timeout(Duration::from_secs(5)),
                Ok(true),
                "clearing the stall must release the blocked write"
            );
            writer.join().unwrap();
        }

        #[test]
        fn a_registered_directory_panics_on_write_and_an_unregistered_one_does_not() {
            let root = tempfile::tempdir().unwrap();
            let dir = root.path().join("panics");
            let file = file_for(root.path(), "a", &dir);
            let other_file = file_for(root.path(), "b", &root.path().join("calm"));

            panic_writer_at(&dir);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = file.write_at(0, b"x");
            }));
            let unaffected = other_file.write_at(0, b"y");
            clear_writer_panic(&dir);

            assert!(outcome.is_err(), "an armed directory's write must panic");
            unaffected.expect("another directory's writes are untouched");
            file.write_at(0, b"x")
                .expect("once cleared, writes pass again");
        }
    }
}

/// Storage adapter backed by the single, globally ordered write-ahead log
/// ([`prkdb_core::wal::Wal`]).
///
/// Every mutating call encodes one [`Batch`] and appends it as one frame: one LSN, one
/// sync in `SyncMode::Durable`. The frame's keys are published into the in-memory index
/// by a commit hook that the WAL's writer thread runs in LSN order, after the frame is
/// durable (`Durable`) or written (`Fast`), so the live index always equals what recovery
/// would rebuild from the same log (STO-03). Recovery is a full replay of the log.
#[derive(Clone)]
pub struct WalStorageAdapter {
    inner: Arc<WalStorageInner>,
}

/// Write-path accounting for [`WalStorageAdapter::write_path_health`], updated by the
/// commit hooks on the WAL writer thread (atomics only: a hook must not block).
#[derive(Debug, Default)]
struct PublishProgress {
    /// Appends handed to the writer and not yet disposed of by it (answered or dropped).
    in_flight: AtomicU64,
    /// Frames whose commit hook has run.
    frames: AtomicU64,
    /// Wall-clock millis of the last completed frame; 0 = never.
    last_publish_ms: AtomicU64,
}

/// Counts one append as in flight from its admission until the writer is done with it. Moved into the commit hook, so it is released when the hook runs or, if the
/// writer answers the request with an error instead, when the request is dropped.
struct InFlight(Arc<PublishProgress>);

impl InFlight {
    fn new(progress: Arc<PublishProgress>) -> Self {
        progress.in_flight.fetch_add(1, Ordering::AcqRel);
        Self(progress)
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// key -> (LSN of the frame the value was read from, value).
type ValueCache = ShardedLruCache<Vec<u8>, (Lsn, Vec<u8>)>;

struct WalStorageInner {
    config: StorageConfig,
    wal: Wal,
    /// key -> location of the frame holding its latest put. Written only by commit hooks,
    /// which the WAL writer runs in LSN order (STO-03).
    index: Arc<LockFreeHashMap<Vec<u8>, RecordLoc>>,
    /// Held for write by a commit hook while it publishes one batch; for read while
    /// `snapshot_get_many` resolves its keys. Never held across an await.
    publish: Arc<parking_lot::RwLock<()>>,
    /// Highest LSN whose hook has run.
    applied_lsn: Arc<AtomicU64>,
    /// A validated memo: an entry is used only when its LSN equals the index's.
    cache: Arc<ValueCache>,
    outbox: Arc<LockFreeHashMap<String, Vec<u8>>>, // memory-only until Task 2.19 (EVT-02)
    /// Kept until Task 2.9 decides the fate of `new_with_replication`.
    replication: Option<tokio::sync::Mutex<ReplicationManager>>,
    metrics: Arc<StorageMetrics>,
    transaction_barrier: Arc<RwLock<()>>,
    bounds: LivenessBounds,
    recovery: Arc<RecoveryManager>,
    progress: Arc<PublishProgress>,
}

/// Maps a WAL error onto the storage error a caller can act on (D12).
fn wal_err(e: WalError) -> StorageError {
    match e {
        WalError::Poisoned(reason) => StorageError::Internal(format!(
            "WAL poisoned: {reason}; reopen the database. The outcome of the write that \
                 failed is unknown: its frame may have reached the disk and reappear after \
                 the reopen"
        )),
        WalError::Closed => {
            StorageError::WriteAbandoned("the WAL is closed and accepts no more writes".to_string())
        }
        e @ WalError::RecordTooLarge { .. } => StorageError::Validation(e.to_string()),
        e @ (WalError::CorruptSegment { .. }
        | WalError::ReplayFailed { .. }
        | WalError::UnsupportedFormat { .. }
        | WalError::Corruption(_)
        | WalError::ChecksumMismatch { .. }) => StorageError::Corruption(e.to_string()),
        e => StorageError::Internal(e.to_string()),
    }
}

/// Maps an error from awaiting an append the writer already owns (after
/// `append_reserved` returned `Ok`).
///
/// `Closed` here does not mean "nothing was written": the request's reply is dropped
/// unanswered when the writer thread dies outside the hook's `catch_unwind`, possibly
/// after `write_at` succeeded, and that frame replays on reopen. So it is
/// `WriteNotConfirmed`, never the definite `WriteAbandoned` that `wal_err` gives a
/// refusal before queueing.
fn queued_wal_err(e: WalError) -> StorageError {
    match e {
        WalError::Closed => StorageError::WriteNotConfirmed(
            "the WAL writer stopped before answering; the write may have landed".to_string(),
        ),
        e => wal_err(e),
    }
}

/// The value a batch holds for `key`: the last op for it wins; `None` if the last op is a
/// delete or the batch never mentions the key (a stale location).
fn value_in_batch(ops: Vec<BatchOp>, key: &[u8]) -> Option<Vec<u8>> {
    ops.into_iter()
        .rev()
        .find_map(|op| match op {
            BatchOp::Put { key: k, value } if k == key => Some(Some(value)),
            BatchOp::Delete { key: k } if k == key => Some(None),
            _ => None,
        })
        .flatten()
}

/// `LogRecord`s for `ReplicationManager::replicate_batch`, which predates the batch
/// codec. Temporary: goes with `new_with_replication` at Task 2.9's STOP.
fn to_log_records(ops: &[BatchOp]) -> Vec<LogRecord> {
    ops.iter()
        .map(|op| match op {
            BatchOp::Put { key, value } => LogRecord::new(LogOperation::Put {
                collection: String::new(),
                id: key.clone(),
                data: value.clone(),
            }),
            BatchOp::Delete { key } => LogRecord::new(LogOperation::Delete {
                collection: String::new(),
                id: key.clone(),
            }),
        })
        .collect()
}

fn raft_key() -> Vec<u8> {
    let mut key = b"__raft_log/".to_vec();
    key.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    key
}

impl WalStorageAdapter {
    pub(crate) async fn acquire_transaction_write_guard(&self) -> OwnedRwLockWriteGuard<()> {
        self.inner.transaction_barrier.clone().write_owned().await
    }

    /// Create a builder for WalStorageAdapter
    pub fn builder(log_dir: PathBuf) -> WalStorageAdapterBuilder {
        WalStorageAdapterBuilder::new(log_dir)
    }

    /// Create a new WAL storage adapter with default configuration. Creates the directory
    /// if it is missing and recovers whatever log it holds.
    #[instrument(skip(config), fields(log_dir = %config.log_dir.display()))]
    pub fn new(config: WalConfig) -> Result<Self, StorageError> {
        Self::new_with_config(StorageConfig {
            wal: config,
            ..StorageConfig::default()
        })
    }

    /// Create a new WAL storage adapter with custom configuration. Creates the directory
    /// if it is missing and recovers whatever log it holds.
    #[instrument(skip(config), fields(log_dir = %config.wal.log_dir.display()))]
    pub fn new_with_config(config: StorageConfig) -> Result<Self, StorageError> {
        Self::open_inner(config, Arc::new(StdVfs), None)
    }

    /// Create a new WAL storage adapter that also forwards every committed batch to
    /// `replication_manager`. Kept until Task 2.9 decides the fate of core replication.
    #[instrument(skip(config, replication_manager), fields(log_dir = %config.log_dir.display()))]
    pub async fn new_with_replication(
        config: WalConfig,
        replication_manager: ReplicationManager,
    ) -> Result<Self, StorageError> {
        let config = StorageConfig {
            wal: config,
            ..StorageConfig::default()
        };
        tokio::task::spawn_blocking(move || {
            Self::open_inner(config, Arc::new(StdVfs), Some(replication_manager))
        })
        .await
        .map_err(|e| StorageError::Internal(format!("WAL open task failed: {e}")))?
    }

    /// Open a WAL storage adapter and rebuild its index by replaying the log.
    ///
    /// Creates the directory if it is missing, like every other constructor (it used to
    /// fail instead; nothing depended on that).
    #[instrument(skip(config), fields(log_dir = %config.log_dir.display()))]
    pub fn open(config: WalConfig) -> Result<Self, StorageError> {
        Self::new(config)
    }

    /// [`Self::open`], run on the blocking pool so a long replay does not stall a runtime
    /// worker.
    #[instrument(skip(config), fields(log_dir = %config.log_dir.display()))]
    pub async fn open_async(config: WalConfig) -> Result<Self, StorageError> {
        tokio::task::spawn_blocking(move || Self::open(config))
            .await
            .map_err(|e| StorageError::Internal(format!("WAL open task failed: {e}")))?
    }

    /// Opens on any `Vfs`: the harness passes `FaultFs`.
    pub fn open_with_vfs(config: StorageConfig, vfs: Arc<dyn Vfs>) -> Result<Self, StorageError> {
        Self::open_inner(config, vfs, None)
    }

    /// The one open path. Synchronous: `Wal::open` needs no runtime.
    fn open_inner(
        config: StorageConfig,
        vfs: Arc<dyn Vfs>,
        replication: Option<ReplicationManager>,
    ) -> Result<Self, StorageError> {
        let log_dir = config.wal.log_dir.clone();
        info!("Opening WalStorageAdapter at {}", log_dir.display());
        #[cfg(test)]
        let vfs: Arc<dyn Vfs> = Arc::new(fault_injection::FaultInjectingVfs::new(
            vfs,
            log_dir.clone(),
        ));

        // The open rules (spec 2b, D3): before `Wal::open`, so `FORMAT` exists before the
        // first segment, and a format-1 directory (no `FORMAT`, old files) is refused
        // before an empty log could be opened next to it and make the database look wiped.
        super::format::ensure_format(vfs.as_ref(), &log_dir)?;

        let start = std::time::Instant::now();
        let index: LockFreeHashMap<Vec<u8>, RecordLoc> = LockFreeHashMap::new();
        let (wal, report) = {
            let pinned = index.pin();
            let mut replay = |loc: RecordLoc, kind: FrameKind, payload: &[u8]| {
                if kind != FrameKind::Batch {
                    return Ok(());
                }
                for op in Batch::decode(payload)?.ops {
                    match op {
                        BatchOp::Put { key, .. } => {
                            pinned.insert(key, loc);
                        }
                        BatchOp::Delete { key } => {
                            pinned.remove(&key);
                        }
                    }
                }
                Ok(())
            };
            Wal::open(
                vfs.clone(),
                &log_dir,
                WalOptions::from_config(&config.wal),
                1,
                &mut replay,
            )
            .map_err(wal_err)?
        };
        info!(
            "Replayed {} frame(s) from {} segment(s) in {:?}",
            report.frames,
            report.segments,
            start.elapsed()
        );

        let metrics = Arc::new(StorageMetrics::new());
        let applied_lsn = wal.next_lsn().saturating_sub(1);
        let inner = WalStorageInner {
            wal,
            index: Arc::new(index),
            publish: Arc::new(parking_lot::RwLock::new(())),
            applied_lsn: Arc::new(AtomicU64::new(applied_lsn)),
            cache: Arc::new(ShardedLruCache::with_metrics(
                config.cache_capacity,
                metrics.clone(),
            )),
            outbox: Arc::new(LockFreeHashMap::new()),
            replication: replication.map(tokio::sync::Mutex::new),
            metrics,
            transaction_barrier: Arc::new(RwLock::new(())),
            bounds: LivenessBounds::from_max_flush_ms(config.batching.max_flush_ms),
            recovery: Arc::new(RecoveryManager::new(vfs, log_dir)),
            progress: Arc::new(PublishProgress::default()),
            config,
        };
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Encodes `batch`, appends it as one frame, and publishes it into the index from
    /// the WAL writer thread (LSN order) before returning its location. `outbox`, if set,
    /// is published in the same step (memory-only until Task 2.19).
    ///
    /// Admission and completion are timed separately, which is what makes the two error
    /// kinds honest: waiting for admission past the client bound is `WriteBackpressure`
    /// (nothing was queued), no answer after the request was queued is
    /// `WriteNotConfirmed` (the write is with the writer and may still land).
    async fn commit(
        &self,
        batch: &Batch,
        outbox: Option<(String, Vec<u8>)>,
    ) -> Result<RecordLoc, StorageError> {
        let inner = &self.inner;
        let payload = batch
            .encode(&inner.config.wal.compression)
            .map_err(wal_err)?;
        let keys: Vec<(Vec<u8>, bool)> = batch
            .ops
            .iter()
            .map(|op| match op {
                BatchOp::Put { key, .. } => (key.clone(), true),
                BatchOp::Delete { key } => (key.clone(), false),
            })
            .collect();
        let replicated = inner
            .replication
            .as_ref()
            .map(|_| to_log_records(&batch.ops));

        let (index, publish, applied) = (
            inner.index.clone(),
            inner.publish.clone(),
            inner.applied_lsn.clone(),
        );
        let (outbox_map, progress, metrics) = (
            inner.outbox.clone(),
            inner.progress.clone(),
            inner.metrics.clone(),
        );
        let bound = inner.bounds.client_bound;
        let reservation = match tokio::time::timeout(bound, inner.wal.reserve(payload.len())).await
        {
            Ok(r) => r.map_err(wal_err)?,
            Err(_) => {
                return Err(StorageError::WriteBackpressure(format!(
                    "WAL admission queue full for {}ms; nothing was written",
                    bound.as_millis()
                )))
            }
        };

        // Counted from here, not before `reserve`: a write still waiting for admission
        // is not queued, and must not show in `queue_depth`.
        let in_flight = InFlight::new(inner.progress.clone());
        let hook: CommitHook = Box::new(move |loc| {
            let _in_flight = in_flight;
            {
                let _visible = publish.write();
                let pinned = index.pin();
                for (key, is_put) in keys {
                    if is_put {
                        pinned.insert(key, loc);
                    } else {
                        pinned.remove(&key);
                    }
                }
                if let Some((id, event)) = outbox {
                    outbox_map.pin().insert(id, event);
                }
                applied.store(loc.lsn, Ordering::Release);
            }
            let now = unix_millis();
            progress.frames.fetch_add(1, Ordering::AcqRel);
            progress.last_publish_ms.store(now, Ordering::Release);
            metrics.record_writer_publish(1, now);
        });

        // From here the writer owns the request: a timeout means "not confirmed", not
        // "not written".
        let pending = inner
            .wal
            .append_reserved(reservation, payload, Some(hook))
            .map_err(wal_err)?;
        let loc = match tokio::time::timeout(bound, pending).await {
            Ok(r) => r.map_err(queued_wal_err)?,
            Err(_) => {
                return Err(StorageError::WriteNotConfirmed(format!(
                    "no result from the WAL writer within {}ms",
                    bound.as_millis()
                )))
            }
        };

        if let (Some(records), Some(replication)) = (replicated, &inner.replication) {
            let mut manager = replication.lock().await;
            if let Err(e) = manager.replicate_batch(records, loc.lsn).await {
                tracing::error!("Replication failed: {}", e);
            }
        }
        Ok(loc)
    }

    /// Commits `ops` as one frame, then refreshes the cache from them. The cache is only
    /// a memo keyed by LSN, so this is never needed for correctness, only for the next
    /// read of a key just written.
    async fn write(
        &self,
        ops: Vec<BatchOp>,
        outbox: Option<(String, Vec<u8>)>,
    ) -> Result<RecordLoc, StorageError> {
        let batch = Batch { ops };
        let loc = self.commit(&batch, outbox).await?;

        let mut deleted = Vec::new();
        let mut written = Vec::new();
        for op in batch.ops {
            match op {
                BatchOp::Put { key, value } => written.push((key, (loc.lsn, value))),
                BatchOp::Delete { key } => deleted.push(key),
            }
        }
        if !deleted.is_empty() {
            self.inner.cache.remove_batch(deleted).await;
        }
        if !written.is_empty() {
            // In op order, so the last put of a key in this batch is what stays cached.
            self.inner.cache.put_batch(written).await;
        }
        Ok(loc)
    }

    async fn put_batch_impl(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), StorageError> {
        if entries.is_empty() {
            return Ok(());
        }
        let total_bytes: u64 = entries
            .iter()
            .map(|(key, value)| (key.len() + value.len()) as u64)
            .sum();
        self.inner.metrics.record_write(total_bytes);

        let ops = entries
            .into_iter()
            .map(|(key, value)| BatchOp::Put { key, value })
            .collect();
        self.write(ops, None).await.map(|_| ())
    }

    async fn delete_many_impl(&self, keys: Vec<Vec<u8>>) -> Result<(), StorageError> {
        if keys.is_empty() {
            return Ok(());
        }
        let total_bytes: u64 = keys.iter().map(|key| key.len() as u64).sum();
        self.inner
            .metrics
            .record_write_batch(keys.len() as u64, total_bytes);

        let ops = keys
            .into_iter()
            .map(|key| BatchOp::Delete { key })
            .collect();
        self.write(ops, None).await.map(|_| ())
    }

    pub(crate) async fn put_batch_unlocked(
        &self,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), StorageError> {
        self.put_batch_impl(entries).await
    }

    pub(crate) async fn delete_many_unlocked(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<(), StorageError> {
        self.delete_many_impl(keys).await
    }

    /// Makes every write acknowledged so far durable (`Wal::sync`). In `SyncMode::Durable`
    /// they already are; in `Fast` this closes the window a power cut could take.
    pub async fn flush(&self) -> Result<(), StorageError> {
        #[cfg(test)]
        if fault_injection::flush_should_fail(&self.inner.config.wal.log_dir) {
            return Err(StorageError::Internal(format!(
                "injected flush failure at {}",
                self.inner.config.wal.log_dir.display()
            )));
        }

        self.inner.wal.sync().await.map_err(wal_err)?;
        Ok(())
    }

    /// Append a single Raft entry to the WAL
    ///
    /// Returns the LSN of the frame holding the entry.
    pub async fn append_raft_entry(&self, data: &[u8]) -> Result<u64, StorageError> {
        let ops = vec![BatchOp::Put {
            key: raft_key(),
            value: data.to_vec(),
        }];
        Ok(self.commit(&Batch { ops }, None).await?.lsn)
    }

    /// Append multiple Raft entries as one frame: one write and one sync for the batch.
    ///
    /// # Returns
    /// One LSN per entry, in input order. The whole batch is one frame, so every entry
    /// carries that frame's LSN.
    pub async fn append_raft_entries_batch(
        &self,
        entries: &[Vec<u8>],
    ) -> Result<Vec<u64>, StorageError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let ops = entries
            .iter()
            .map(|data| BatchOp::Put {
                key: raft_key(),
                value: data.clone(),
            })
            .collect();
        let loc = self.commit(&Batch { ops }, None).await?;
        Ok(vec![loc.lsn; entries.len()])
    }

    /// Get a snapshot of current metrics
    pub fn metrics(&self) -> prkdb_metrics::storage::MetricsSnapshot {
        self.inner.metrics.snapshot()
    }

    /// Get the recovery manager
    pub fn recovery(&self) -> Arc<RecoveryManager> {
        self.inner.recovery.clone()
    }

    /// Get all keys in the storage (for snapshotting)
    pub fn get_all_keys(&self) -> Vec<Vec<u8>> {
        let pinned = self.inner.index.pin();
        pinned.iter().map(|(key, _)| key.to_vec()).collect()
    }

    /// The value stored for `key` in the frame at `loc`: from the cache if it holds that
    /// exact LSN, otherwise read from the WAL (CRC and LSN verified) and cached.
    async fn read_value(
        &self,
        key: &[u8],
        loc: RecordLoc,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let metrics = &self.inner.metrics;
        if let Some((lsn, value)) = self.inner.cache.get(&key.to_vec()).await {
            if lsn == loc.lsn {
                metrics.record_cache_hit();
                metrics.record_read((key.len() + value.len()) as u64);
                return Ok(Some(value));
            }
        }
        metrics.record_cache_miss();

        let payload = self.inner.wal.read(loc).map_err(wal_err)?;
        let batch = Batch::decode(&payload).map_err(wal_err)?;
        let value = value_in_batch(batch.ops, key);
        if let Some(value) = &value {
            metrics.record_read((key.len() + value.len()) as u64);
            self.inner
                .cache
                .put(key.to_vec(), (loc.lsn, value.clone()))
                .await;
        }
        Ok(value)
    }

    /// The index entries whose key satisfies `keep`, sorted by key. The index guard is
    /// released before this returns, so callers can await while reading the values.
    fn locations_where(&self, keep: impl Fn(&[u8]) -> bool) -> Vec<(Vec<u8>, RecordLoc)> {
        let mut hits: Vec<(Vec<u8>, RecordLoc)> = {
            let pinned = self.inner.index.pin();
            pinned
                .iter()
                .filter(|(key, _)| keep(key))
                .map(|(key, loc)| (key.clone(), *loc))
                .collect()
        };
        hits.sort_by(|a, b| a.0.cmp(&b.0));
        hits
    }

    async fn read_all(
        &self,
        hits: Vec<(Vec<u8>, RecordLoc)>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let mut rows = Vec::with_capacity(hits.len());
        for (key, loc) in hits {
            if let Some(value) = self.read_value(&key, loc).await? {
                rows.push((key, value));
            }
        }
        Ok(rows)
    }

    /// Read several keys as of a single instant.
    ///
    /// # Why `get_many` is not this
    ///
    /// `get_many` takes no barrier, so a batch write landing midway through is visible to
    /// some of its keys and not others. Reading `a` and `b` while another task commits
    /// `{a, b}` as one batch could therefore observe the old `a` with the new `b` — the
    /// money-disappears symptom that spec S-03 was filed for.
    ///
    /// This resolves every key to a location while holding the publication lock for read,
    /// which excludes the commit hooks that publish batches, so every key is observed on
    /// the same side of every batch commit. The values are then read from those fixed
    /// locations, with no lock held across an await.
    ///
    /// # What this is not
    ///
    /// Not a transaction and not MVCC. It gives an atomic read, not a repeatable one. For
    /// read-modify-write, use a `Serializable` transaction.
    pub async fn snapshot_get_many(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
        let locations: Vec<Option<RecordLoc>> = {
            let _visible = self.inner.publish.read();
            let pinned = self.inner.index.pin();
            keys.iter().map(|key| pinned.get(key).copied()).collect()
        };
        let mut values = Vec::with_capacity(keys.len());
        for (key, loc) in keys.iter().zip(locations) {
            values.push(match loc {
                Some(loc) => self.read_value(key, loc).await?,
                None => None,
            });
        }
        Ok(values)
    }

    /// Highest LSN that is both published into the index and visible to
    /// `get_changes_since` (`min(applied, acked)`).
    ///
    /// A hook publishes a frame just before the writer answers its caller, so the index
    /// can briefly be ahead of what `get_changes_since` (capped at the WAL's acked
    /// watermark) exposes. Taking the minimum means a consumer that sets its cursor to
    /// this value never skips a change it has not been shown yet.
    ///
    /// Exposed so a wrapper holding several adapters can record the maximum across all of
    /// them in a merged snapshot header.
    pub fn max_offset(&self) -> u64 {
        let applied = self.inner.applied_lsn.load(Ordering::Acquire);
        applied.min(self.inner.wal.acked_lsn())
    }

    /// Highest LSN that is durable on disk: everything at or below it survives a power
    /// cut. Equal to [`Self::max_offset`] in `SyncMode::Durable`; may trail it in `Fast`
    /// until the next periodic sync or [`Self::flush`].
    pub fn durable_lsn(&self) -> u64 {
        self.inner.wal.durable_lsn()
    }

    /// Get the log directory path
    pub fn get_log_dir(&self) -> PathBuf {
        self.inner.config.wal.log_dir.clone()
    }

    /// Makes everything acknowledged so far durable, and nothing else yet.
    ///
    /// Recovery replays the whole log, which is always correct; the old JSON checkpoint
    /// let recovery skip keys written before it (STO-01) and is no longer written or read.
    /// Task 2.14 adds a checkpoint that is an index snapshot.
    pub fn save_checkpoint(&self) -> Result<(), StorageError> {
        self.inner.wal.sync_blocking().map_err(wal_err)?;
        Ok(())
    }

    /// Take a full snapshot of the database
    ///
    /// This captures the current state of the database (all key-value pairs)
    /// and writes it to the specified path.
    ///
    /// Returns the max_offset that this snapshot corresponds to.
    pub async fn take_snapshot(
        &self,
        path: &Path,
        compression: CompressionType,
    ) -> Result<u64, StorageError> {
        let max_offset = self.max_offset();
        let keys = self.get_all_keys();
        let count = keys.len() as u64;

        info!(
            "Starting snapshot: {} keys, max_offset={}",
            count, max_offset
        );

        // Producer-Consumer pattern: Use a blocking task for file I/O
        // to avoid blocking the async runtime.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(Vec<u8>, Vec<u8>)>(1024);
        let writer_path = path.to_path_buf();

        let write_task = tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            let header = SnapshotHeader::new(max_offset, count, compression);
            let mut writer = SnapshotWriter::new(&writer_path, header)?;

            while let Some((key, val)) = rx.blocking_recv() {
                writer.write_entry(&key, &val)?;
            }
            writer.finish()?;
            Ok(())
        });

        for key in keys {
            // May see updates newer than `max_offset`; replay applies them idempotently.
            if let Some(val) = self.get(&key).await? {
                if tx.send((key, val)).await.is_err() {
                    return Err(StorageError::Internal(
                        "Snapshot writer task failed".to_string(),
                    ));
                }
            }
        }
        drop(tx); // Signal completion

        match write_task.await {
            Ok(res) => res?,
            Err(e) => {
                return Err(StorageError::Internal(format!(
                    "Snapshot task join error: {}",
                    e
                )))
            }
        }

        info!("Snapshot completed successfully");
        Ok(max_offset)
    }

    /// State of the write path, for health and readiness probes.
    ///
    /// Computed on demand from the WAL's own state and a few atomics: there is no
    /// watchdog task, so an idle adapter performs no wakeups at all (liveness spec
    /// acceptance 1 holds by construction). Synchronous and non-blocking apart from the
    /// WAL's uncontended health read lock: a probe that can block turns a stalled writer
    /// into a stalled health check.
    pub fn write_path_health(&self) -> WritePathHealth {
        let progress = &self.inner.progress;
        let (healthy, reason, oldest_unpublished_age_ms) = match self.inner.wal.health() {
            WalHealth::Healthy => (true, None, 0),
            WalHealth::Stalled {
                queued_bytes,
                oldest_ms,
            } => (
                false,
                Some(format!(
                    "WAL writer stalled: {queued_bytes} byte(s) queued, oldest waiting \
                     {oldest_ms}ms with no batch completed"
                )),
                oldest_ms,
            ),
            WalHealth::Poisoned(reason) => (false, Some(reason), 0),
            WalHealth::Closed => (false, Some("WAL closed".to_string()), 0),
        };
        let last = progress.last_publish_ms.load(Ordering::Acquire);
        WritePathHealth {
            healthy,
            reason,
            queue_depth: progress.in_flight.load(Ordering::Acquire),
            oldest_unpublished_age_ms,
            last_publish_age_ms: (last != 0).then(|| unix_millis().saturating_sub(last)),
            publishes_total: progress.frames.load(Ordering::Acquire),
            direct_appends_total: 0,
        }
    }
}

#[async_trait::async_trait]
impl StorageAdapter for WalStorageAdapter {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let loc = self.inner.index.pin().get(key).copied();
        match loc {
            Some(loc) => self.read_value(key, loc).await,
            None => {
                // Not served from the cache either: count it as a miss, as before.
                self.inner.metrics.record_cache_miss();
                Ok(None)
            }
        }
    }

    /// Put a key-value pair: one frame.
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let _guard = self.inner.transaction_barrier.read().await;
        self.put_batch_impl(vec![(key.to_vec(), value.to_vec())])
            .await
    }

    /// Put several key-value pairs as one frame: one write and one sync, and visible
    /// together (decision record risk 4).
    async fn put_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), StorageError> {
        let _guard = self.inner.transaction_barrier.read().await;
        self.put_batch_impl(entries).await
    }

    /// Put multiple key-value pairs as one frame.
    async fn put_many(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), StorageError> {
        let _guard = self.inner.transaction_barrier.read().await;
        if items.is_empty() {
            return Ok(());
        }
        let total_bytes: u64 = items.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();
        self.inner
            .metrics
            .record_write_batch(items.len() as u64, total_bytes);

        let ops = items
            .into_iter()
            .map(|(key, value)| BatchOp::Put { key, value })
            .collect();
        self.write(ops, None).await.map(|_| ())
    }

    /// Delete a key
    async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        let _guard = self.inner.transaction_barrier.read().await;
        self.delete_many_impl(vec![key.to_vec()]).await
    }

    async fn flush(&self) -> Result<(), StorageError> {
        WalStorageAdapter::flush(self).await
    }

    /// Retrieve multiple records by key: `get` per key.
    ///
    /// # Example
    ///
    /// ```rust
    /// use prkdb::storage::WalStorageAdapter;
    /// use prkdb_core::wal::WalConfig;
    /// use prkdb::prelude::*;
    /// use std::sync::Arc;
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let dir = tempfile::tempdir().unwrap();
    /// let config = WalConfig {
    ///     log_dir: dir.path().to_path_buf(),
    ///     ..WalConfig::test_config()
    /// };
    ///
    /// let adapter = WalStorageAdapter::new(config).unwrap();
    ///
    /// // Put some data
    /// adapter.put(b"key1", b"value1").await.unwrap();
    /// adapter.put(b"key2", b"value2").await.unwrap();
    ///
    /// // Get many
    /// let ids = vec![b"key1".to_vec(), b"key2".to_vec()];
    /// let results = adapter.get_many(ids).await.unwrap();
    ///
    /// assert_eq!(results[0], Some(b"value1".to_vec()));
    /// assert_eq!(results[1], Some(b"value2".to_vec()));
    /// # });
    /// ```
    async fn get_many(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
        let mut values = Vec::with_capacity(keys.len());
        for key in &keys {
            values.push(self.get(key).await?);
        }
        Ok(values)
    }

    /// Delete several keys as one frame.
    async fn delete_many(&self, keys: Vec<Vec<u8>>) -> Result<(), StorageError> {
        let _guard = self.inner.transaction_barrier.read().await;
        self.delete_many_impl(keys).await
    }

    async fn outbox_save(&self, id: &str, payload: &[u8]) -> Result<(), StorageError> {
        self.inner
            .outbox
            .pin()
            .insert(id.to_string(), payload.to_vec());
        Ok(())
    }

    async fn outbox_list(&self) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        Ok(self
            .inner
            .outbox
            .pin()
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_vec()))
            .collect())
    }

    async fn outbox_remove(&self, id: &str) -> Result<(), StorageError> {
        self.inner.outbox.pin().remove(id);
        Ok(())
    }

    /// The put and its event are one frame and one publish step: both visible, or
    /// neither. The event itself is memory-only until Task 2.19 persists it in the frame.
    async fn put_with_outbox(
        &self,
        key: &[u8],
        value: &[u8],
        outbox_id: &str,
        outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        let _guard = self.inner.transaction_barrier.read().await;
        self.inner
            .metrics
            .record_write((key.len() + value.len()) as u64);
        let ops = vec![BatchOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        }];
        let event = (outbox_id.to_string(), outbox_payload.to_vec());
        self.write(ops, Some(event)).await.map(|_| ())
    }

    /// The delete and its event are one frame; see `put_with_outbox`.
    async fn delete_with_outbox(
        &self,
        key: &[u8],
        outbox_id: &str,
        outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        let _guard = self.inner.transaction_barrier.read().await;
        self.inner.metrics.record_write_batch(1, key.len() as u64);
        let ops = vec![BatchOp::Delete { key: key.to_vec() }];
        let event = (outbox_id.to_string(), outbox_payload.to_vec());
        self.write(ops, Some(event)).await.map(|_| ())
    }

    /// Every live key beginning with `prefix`, sorted by key, from the index.
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let hits = self.locations_where(|key| key.starts_with(prefix));
        self.read_all(hits).await
    }

    /// Every change in the log after `offset` (an LSN), one per op, in LSN order. Reads
    /// acknowledged frames (`Wal::scan_from`), so a Fast-mode write is visible to a
    /// consumer as soon as it is acknowledged.
    async fn get_changes_since(&self, offset: u64) -> Result<Vec<Change>, StorageError> {
        let mut changes = Vec::new();
        self.inner
            .wal
            .scan_from(offset.saturating_add(1), &mut |loc, kind, payload| {
                if kind != FrameKind::Batch {
                    return Ok(());
                }
                for op in Batch::decode(payload)?.ops {
                    changes.push(match op {
                        BatchOp::Put { key, value } => Change::Put {
                            key,
                            value,
                            version: loc.lsn,
                        },
                        BatchOp::Delete { key } => Change::Delete {
                            key,
                            version: loc.lsn,
                        },
                    });
                }
                Ok(())
            })
            .map_err(wal_err)?;
        Ok(changes)
    }

    /// Every live key in `[start, end)`, sorted by key, from the index.
    async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let hits = self.locations_where(|key| key >= start && key < end);
        self.read_all(hits).await
    }

    async fn take_snapshot(
        &self,
        path: PathBuf,
        compression: CompressionType,
    ) -> Result<u64, StorageError> {
        self.take_snapshot(&path, compression).await
    }

    fn write_path_health(&self) -> WritePathHealth {
        WalStorageAdapter::write_path_health(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prkdb_types::replication::ReplicationConfig;
    use std::env;
    use std::fs;
    use std::time::{Duration, Instant};

    /// STO-09: `builder(..).with_cache_capacity(n)` reaches `new_with_config`, which used
    /// to hard-code 100,000 entries and ignore the knob.
    #[tokio::test(flavor = "multi_thread")]
    async fn new_with_config_honors_cache_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = WalStorageAdapter::builder(dir.path().to_path_buf())
            .with_cache_capacity(1_600)
            .build()
            .unwrap();
        assert_eq!(adapter.inner.cache.capacity(), 1_600);
    }

    /// `with_compaction_config` takes `prkdb::storage::CompactionConfig` (Task 2.8c moved
    /// it out of `prkdb-core`'s compaction module, which Task 2.9 deletes) and keeps it.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_builder_keeps_the_compaction_config() {
        let dir = tempfile::tempdir().unwrap();
        let wanted = crate::storage::CompactionConfig {
            min_wal_size_bytes: 1,
            min_interval: Duration::from_secs(1),
            keep_segments: 7,
        };
        let adapter = WalStorageAdapter::builder(dir.path().to_path_buf())
            .with_compaction_config(wanted.clone())
            .build()
            .unwrap();
        assert_eq!(adapter.inner.config.compaction, wanted);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replication_constructor_uses_the_supplied_wal_config() {
        let dir = tempfile::tempdir().expect("temporary root");
        let log_dir = dir.path().join("replicated-wal");
        let config = WalConfig {
            log_dir: log_dir.clone(),
            segment_bytes: 64 * 1024,
            ..WalConfig::test_config()
        };
        let replication = ReplicationManager::new(ReplicationConfig::test_config())
            .await
            .expect("empty replica list needs no network");

        let adapter = WalStorageAdapter::new_with_replication(config, replication)
            .await
            .expect("replicated adapter opens");

        assert_eq!(adapter.inner.config.wal.log_dir, log_dir);
        assert_eq!(adapter.inner.config.wal.segment_bytes, 64 * 1024);
        assert!(
            log_dir
                .join(prkdb_core::wal::segment::segment_file_name(1))
                .is_file(),
            "the log's first segment must exist in the supplied directory"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wal_adapter_put_get() {
        let dir = env::temp_dir().join("test_wal_adapter_async");
        let _ = fs::remove_dir_all(&dir);

        let config = WalConfig {
            log_dir: dir.clone(),
            ..WalConfig::test_config()
        };

        let adapter = WalStorageAdapter::new(config).unwrap();

        // Put
        adapter.put(b"key1", b"value1").await.unwrap();

        // Get
        let value = adapter.get(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Clean up
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wal_adapter_delete() {
        let dir = env::temp_dir().join("test_wal_adapter_delete_async");
        let _ = fs::remove_dir_all(&dir);

        let config = WalConfig {
            log_dir: dir.clone(),
            ..WalConfig::test_config()
        };

        let adapter = WalStorageAdapter::new(config).unwrap();

        // Put
        adapter.put(b"key1", b"value1").await.unwrap();

        // Delete
        adapter.delete(b"key1").await.unwrap();

        // Should not exist
        let value = adapter.get(b"key1").await.unwrap();
        assert_eq!(value, None);

        // Clean up
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wal_adapter_bulk_operations() {
        let dir = env::temp_dir().join("test_wal_adapter_bulk");
        let _ = fs::remove_dir_all(&dir);

        let config = WalConfig {
            log_dir: dir.clone(),
            ..WalConfig::test_config()
        };

        let adapter = WalStorageAdapter::new(config).unwrap();

        // Bulk put
        let items = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];
        adapter.put_many(items).await.unwrap();

        // Bulk get
        let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
        let values = adapter.get_many(keys).await.unwrap();

        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Some(b"value1".to_vec()));
        assert_eq!(values[1], Some(b"value2".to_vec()));
        assert_eq!(values[2], Some(b"value3".to_vec()));

        // Clean up
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wal_adapter_recovery() {
        let dir = env::temp_dir().join("test_wal_adapter_recovery_async");
        let _ = fs::remove_dir_all(&dir);

        let config = WalConfig {
            log_dir: dir.clone(),
            ..WalConfig::test_config()
        };

        // 1. Write some data
        {
            let adapter = WalStorageAdapter::new(config.clone()).unwrap();
            adapter.put(b"key1", b"value1").await.unwrap();
            adapter.put(b"key2", b"value2").await.unwrap();
            adapter.flush().await.unwrap();
        }

        // 2. Reopen and verify
        {
            let adapter = WalStorageAdapter::open(config).unwrap();
            let value1 = adapter.get(b"key1").await.unwrap();
            let value2 = adapter.get(b"key2").await.unwrap();

            assert_eq!(value1, Some(b"value1".to_vec()));
            assert_eq!(value2, Some(b"value2".to_vec()));
        }

        // Clean up
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wal_adapter_recovery_with_multiple_records() {
        let dir = env::temp_dir().join("test_wal_adapter_recovery_multiple_records");
        let _ = fs::remove_dir_all(&dir);

        let config = WalConfig {
            log_dir: dir.clone(),
            ..WalConfig::test_config()
        };

        {
            let adapter = WalStorageAdapter::new(config.clone()).unwrap();
            for i in 0..10 {
                let key = format!("cycle_key_{}", i);
                let value = format!("cycle_value_{}", i);
                adapter.put(key.as_bytes(), value.as_bytes()).await.unwrap();
            }
            adapter.flush().await.unwrap();
        }

        let adapter = WalStorageAdapter::open(config).unwrap();
        assert_eq!(
            adapter.get_changes_since(0).await.unwrap().len(),
            10,
            "reopened WAL should expose all records"
        );
        assert_eq!(adapter.max_offset(), 10, "one frame per put, LSNs from 1");

        for i in 0..10 {
            let key = format!("cycle_key_{}", i);
            let expected = format!("cycle_value_{}", i).into_bytes();
            assert_eq!(adapter.get(key.as_bytes()).await.unwrap(), Some(expected));
        }

        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wal_adapter_recovery_with_tempdir() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        {
            let adapter = WalStorageAdapter::new(config.clone()).unwrap();
            for i in 0..10 {
                let key = format!("cycle_0_key_{}", i);
                let value = format!("cycle_0_value_{}", i);
                adapter.put(key.as_bytes(), value.as_bytes()).await.unwrap();
            }
            adapter.flush().await.unwrap();
        }

        let adapter = WalStorageAdapter::open(config).unwrap();
        assert_eq!(
            adapter.get_changes_since(0).await.unwrap().len(),
            10,
            "reopened WAL should expose all records"
        );

        for i in 0..10 {
            let key = format!("cycle_0_key_{}", i);
            let expected = format!("cycle_0_value_{}", i).into_bytes();
            assert_eq!(adapter.get(key.as_bytes()).await.unwrap(), Some(expected));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wal_adapter_replication() {
        let dir = env::temp_dir().join("test_wal_adapter_replication");
        let _ = fs::remove_dir_all(&dir);
        let config = WalConfig {
            log_dir: dir.clone(),
            ..WalConfig::test_config()
        };

        let adapter = WalStorageAdapter::new(config).unwrap();

        // 1. Initial write
        adapter.put(b"key1", b"value1").await.unwrap();

        // 2. Get changes from beginning (offset 0)
        let changes = adapter.get_changes_since(0).await.unwrap();
        assert_eq!(changes.len(), 1);

        match &changes[0] {
            Change::Put {
                key,
                value,
                version,
            } => {
                assert_eq!(key, b"key1");
                assert_eq!(value, b"value1");
                assert!(*version > 0);
            }
            _ => panic!("Expected Put change"),
        }

        let first_offset = match &changes[0] {
            Change::Put { version, .. } => *version,
            _ => 0,
        };

        // 3. Write more data
        adapter.put(b"key2", b"value2").await.unwrap();
        adapter.delete(b"key1").await.unwrap();

        // 4. Get changes since first offset
        let new_changes = adapter.get_changes_since(first_offset).await.unwrap();
        assert_eq!(new_changes.len(), 2);

        match &new_changes[0] {
            Change::Put { key, value, .. } => {
                assert_eq!(key, b"key2");
                assert_eq!(value, b"value2");
            }
            _ => panic!("Expected Put change"),
        }

        match &new_changes[1] {
            Change::Delete { key, .. } => {
                assert_eq!(key, b"key1");
            }
            _ => panic!("Expected Delete change"),
        }

        // Clean up
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wal_adapter_cache() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let adapter = WalStorageAdapter::new(config).unwrap();

        // Put a key
        adapter.put(b"cached_key", b"cached_value").await.unwrap();

        // First get - served from the cache the put populated
        let val1 = adapter.get(b"cached_key").await.unwrap();
        assert_eq!(val1, Some(b"cached_value".to_vec()));

        // Second get - should hit cache (faster)
        let val2 = adapter.get(b"cached_key").await.unwrap();
        assert_eq!(val2, Some(b"cached_value".to_vec()));

        // Delete - the index no longer has the key, whatever the cache holds
        adapter.delete(b"cached_key").await.unwrap();

        // Get after delete - should return None
        let val3 = adapter.get(b"cached_key").await.unwrap();
        assert_eq!(val3, None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wal_adapter_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let adapter = WalStorageAdapter::new(config).unwrap();

        // Initial metrics should be zero
        let metrics = adapter.metrics();
        assert_eq!(metrics.writes_total, 0);
        assert_eq!(metrics.reads_total, 0);
        assert_eq!(metrics.cache_hits, 0);
        assert_eq!(metrics.cache_misses, 0);

        // Test write metrics
        adapter.put(b"key1", b"value1").await.unwrap();

        let metrics = adapter.metrics();
        assert!(
            metrics.writes_total >= 1,
            "Expected at least 1 write, got {}",
            metrics.writes_total
        );
        assert_eq!(metrics.write_bytes_total, 10); // "key1" (4) + "value1" (6) = 10

        // Write-through caching: the put populated the cache, so the first get is a hit.
        let _ = adapter.get(b"key1").await.unwrap();
        let metrics = adapter.metrics();
        assert_eq!(
            metrics.cache_hits, 1,
            "Expected cache hit after put. Metrics: {:?}",
            metrics
        );
        assert_eq!(
            metrics.cache_misses, 0,
            "Expected no cache misses. Metrics: {:?}",
            metrics
        );
        assert_eq!(metrics.reads_total, 1);

        // Test cache hit on second read
        let _ = adapter.get(b"key1").await.unwrap();
        let metrics = adapter.metrics();
        assert_eq!(metrics.cache_hits, 2);
        assert_eq!(metrics.cache_misses, 0);
        assert_eq!(metrics.reads_total, 2);

        // Test batch write metrics
        let items = vec![
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];
        adapter.put_many(items).await.unwrap();

        let metrics = adapter.metrics();
        assert_eq!(metrics.write_batches_total, 1);
        assert!(metrics.writes_total >= 3); // At least 3 writes total

        // Test delete metrics
        adapter.delete(b"key1").await.unwrap();

        let metrics = adapter.metrics();
        assert!(metrics.writes_total >= 4); // Delete is also a write

        // Test that getting a non-existent key records a cache miss
        let _ = adapter.get(b"nonexistent").await.unwrap();
        let metrics = adapter.metrics();
        assert_eq!(metrics.cache_misses, 1); // One miss for non-existent key
    }

    /// The first segment file of `dir`'s log.
    fn first_segment(dir: &Path) -> PathBuf {
        dir.join(prkdb_core::wal::segment::segment_file_name(1))
    }

    /// A torn tail (a crash mid-write) is truncated at open: the database opens, the key
    /// before the tear is readable, the torn record is gone, and writes continue.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wal_adapter_auto_recovery_on_startup() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        {
            let adapter = WalStorageAdapter::new(config.clone()).unwrap();
            adapter.put(b"key1", b"value1").await.unwrap();
            adapter.put(b"key2", b"value2").await.unwrap();
        }

        // Tear the last frame: cut the file a few bytes short of its end.
        let segment = first_segment(dir.path());
        let len = fs::metadata(&segment).unwrap().len();
        fs::OpenOptions::new()
            .write(true)
            .open(&segment)
            .unwrap()
            .set_len(len - 3)
            .unwrap();

        let adapter =
            WalStorageAdapter::open(config).expect("a torn tail must be truncated, not refused");
        assert_eq!(
            adapter.get(b"key1").await.unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            adapter.get(b"key2").await.unwrap(),
            None,
            "the torn record must be truncated, not half-read"
        );
        adapter.put(b"key3", b"value3").await.unwrap();
        assert_eq!(
            adapter.get(b"key3").await.unwrap(),
            Some(b"value3".to_vec())
        );
    }

    /// Corruption in a sealed segment is found by the health check on a running database,
    /// and refuses the next open by name rather than silently truncating the log there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wal_adapter_runtime_corruption_detection() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            segment_bytes: 4096,
            ..WalConfig::test_config()
        };

        let adapter = WalStorageAdapter::new(config.clone()).unwrap();
        for i in 0..100u32 {
            adapter
                .put(format!("key{i}").as_bytes(), &[7u8; 64])
                .await
                .unwrap();
        }
        assert!(
            adapter.inner.wal.segments().len() >= 2,
            "the first segment must be sealed for this test to mean anything"
        );
        adapter.recovery().check_health().await.expect("healthy");

        // Corrupt a frame body in the first (sealed) segment WHILE OPEN.
        let segment = first_segment(dir.path());
        {
            use std::io::{Seek, Write};
            let mut file = fs::OpenOptions::new().write(true).open(&segment).unwrap();
            file.seek(std::io::SeekFrom::Start(
                prkdb_core::wal::segment::SEGMENT_HEADER_LEN + 40,
            ))
            .unwrap();
            file.write_all(&[0xAB; 8]).unwrap();
            file.sync_all().unwrap();
        }

        let error = adapter
            .recovery()
            .check_health()
            .await
            .expect_err("the health check must find the corruption");
        assert!(
            matches!(error, StorageError::Corruption(ref m) if m.contains("00000000000000000001.wal")),
            "the health check must name the file: {error}"
        );
        drop(adapter);

        let error = WalStorageAdapter::open(config)
            .err()
            .expect("a corrupt sealed segment must refuse the open");
        assert!(
            error.to_string().contains("00000000000000000001.wal"),
            "the open error must name the file: {error}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wal_adapter_builder() {
        let dir = tempfile::tempdir().unwrap();

        // Create adapter with small cache capacity using builder
        let adapter = WalStorageAdapter::builder(dir.path().to_path_buf())
            .with_cache_capacity(5) // Very small cache
            .build()
            .expect("Failed to build adapter");

        // Insert more items than cache capacity
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            adapter.put(&key, &value).await.unwrap();
        }

        let _metrics = adapter.metrics();

        // Verify we can read back
        let val = adapter.get(b"key9").await.unwrap();
        assert_eq!(val, Some(b"value9".to_vec()));
        let val = adapter.get(b"key0").await.unwrap();
        assert_eq!(
            val,
            Some(b"value0".to_vec()),
            "an evicted key reads from the WAL"
        );
    }

    /// put_with_outbox / delete_with_outbox are one frame: the key and its event are
    /// published together, and neither call falls back to the trait's "not supported".
    #[tokio::test(flavor = "multi_thread")]
    async fn outbox_writes_are_one_frame_with_their_data() {
        let dir = tempfile::tempdir().unwrap();
        let db = WalStorageAdapter::new(WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        })
        .unwrap();
        let before = db.inner.wal.next_lsn();
        db.put_with_outbox(b"k", b"v", "users:0:1", b"put-event")
            .await
            .expect("implemented, not the trait default");
        assert_eq!(
            db.inner.wal.next_lsn(),
            before + 1,
            "data and event must be one frame"
        );
        assert_eq!(db.get(b"k").await.unwrap().as_deref(), Some(&b"v"[..]));
        db.delete_with_outbox(b"k", "users:0:2", b"delete-event")
            .await
            .unwrap();
        assert_eq!(db.inner.wal.next_lsn(), before + 2);
        assert_eq!(db.get(b"k").await.unwrap(), None);
        let mut outbox = db.outbox_list().await.unwrap();
        outbox.sort();
        assert_eq!(
            outbox,
            vec![
                ("users:0:1".to_string(), b"put-event".to_vec()),
                ("users:0:2".to_string(), b"delete-event".to_vec()),
            ]
        );
    }

    // ------------------------------------------------------------------------------
    // WAL writer liveness — docs/superpowers/specs/2026-08-11-wal-writer-liveness.md
    //
    // Every test below observes the write path from the *caller's* side, because that is
    // where the defect lived: a queued write is a promise, and the failure mode was that
    // nothing kept it and nothing said so. Writer faults (panic, stall, failed append) are
    // injected where a real disk fault reaches the log: `fault_injection::FaultInjectingVfs`.
    // ------------------------------------------------------------------------------

    /// Dropping the last handle closes the log durably: the `Wal` drains its queue,
    /// syncs, and joins its writer, so everything written before the drop is there on
    /// reopen, including in `Fast` mode, where nothing else would have synced it.
    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_the_last_handle_closes_the_log_durably() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            sync_mode: SyncMode::Fast,
            sync_interval_ms: 3_600_000, // only the close can sync
            ..WalConfig::test_config()
        };
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
            .map(|i| (format!("k{i}").into_bytes(), format!("v{i}").into_bytes()))
            .collect();

        {
            let adapter = WalStorageAdapter::new(config.clone()).expect("adapter opens");
            adapter.put_many(pairs.clone()).await.expect("written");
            assert!(
                adapter.durable_lsn() < adapter.max_offset(),
                "nothing may have synced the write yet, or the close proves nothing"
            );
            // Dropped here: the last handle, so the log closes.
        }

        let adapter = WalStorageAdapter::open(config).expect("reopen");
        for (k, v) in &pairs {
            assert_eq!(adapter.get(k).await.unwrap().as_ref(), Some(v));
        }
    }

    /// A batch write reports the bytes it wrote, and `flush` syncs what was acknowledged.
    ///
    /// `put_many` sums `key.len() + value.len()` across the batch for the write-bytes
    /// counter; `*` there turns a sum into a product and inflates the figure an operator
    /// sizes disks from. The lengths below are chosen so the two cannot coincide.
    ///
    /// `flush` returning a bare `Ok(())` is the more serious of the pair: in `Fast` mode it
    /// is the only thing that makes an acknowledged write survive a power cut before the
    /// periodic sync. Asserted through the durable watermark, with the periodic sync pushed
    /// out of reach so that nothing but the flush can move it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_batch_reports_its_bytes_and_flush_syncs_what_is_acknowledged() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = WalStorageAdapter::new(WalConfig {
            log_dir: dir.path().to_path_buf(),
            sync_mode: SyncMode::Fast,
            sync_interval_ms: 3_600_000,
            ..WalConfig::test_config()
        })
        .expect("adapter opens");

        // 3 + 5 and 4 + 6: sums 8 and 10 for 18 total, products 15 and 24 for 39.
        let items = vec![
            (b"abc".to_vec(), b"12345".to_vec()),
            (b"defg".to_vec(), b"123456".to_vec()),
        ];
        let expected: u64 = items.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();

        let before = adapter.metrics().write_bytes_total;
        adapter.put_many(items).await.expect("batch written");
        assert_eq!(
            adapter.metrics().write_bytes_total - before,
            expected,
            "a batch must report the bytes it wrote, not the product of the lengths"
        );

        let applied = adapter.inner.applied_lsn.load(Ordering::Acquire);
        assert!(
            adapter.inner.wal.durable_lsn() < applied,
            "the write must not be durable yet, or flush has nothing to do"
        );

        // Through the trait, deliberately: a caller holding a `dyn StorageAdapter` goes
        // through the impl, which is a separate function that delegates.
        StorageAdapter::flush(&adapter)
            .await
            .expect("flush succeeds");

        assert_eq!(
            adapter.inner.wal.durable_lsn(),
            applied,
            "flush reported success without syncing what was acknowledged"
        );
    }

    /// A batch of interleaved puts and deletes is one frame, applied in op order by the
    /// commit hook: the last op for a key decides it, both live and after a reopen.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_mixed_batch_applies_its_puts_and_deletes_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = WalStorageAdapter::new(config.clone()).expect("adapter opens");

        let put = |key: &[u8], value: &[u8]| BatchOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        let delete = |key: &[u8]| BatchOp::Delete { key: key.to_vec() };

        // `survivor` ends on a put, `casualty` ends on a delete: between them they cover
        // switching in both directions and a trailing delete.
        adapter
            .write(
                vec![
                    put(b"survivor", b"first"),
                    put(b"casualty", b"doomed"),
                    delete(b"survivor"),
                    put(b"survivor", b"second"),
                    delete(b"casualty"),
                ],
                None,
            )
            .await
            .expect("the batch commits");
        assert_eq!(adapter.max_offset(), 1, "the whole batch is one frame");

        for adapter in [adapter, WalStorageAdapter::open(config).unwrap()] {
            assert_eq!(
                adapter.get(b"survivor").await.expect("read survivor"),
                Some(b"second".to_vec()),
                "the last write for this key was a put; a delete applied after it means \
                 the batch was reordered"
            );
            assert_eq!(
                adapter.get(b"casualty").await.expect("read casualty"),
                None,
                "the last write for this key was a delete, the final op in the batch"
            );
        }
    }

    /// Once the log is closed, a write is refused definitely: `WalError::Closed` means
    /// nothing was queued, so the caller must be told to retry (`WriteAbandoned`), not
    /// that the write may still land.
    ///
    /// The adapter has no `close` of its own before Task 2.9 (dropping the last handle is
    /// the close), so this pins the mapping every write path goes through.
    #[test]
    fn a_write_after_close_is_refused() {
        let error = wal_err(WalError::Closed);
        assert!(
            error.is_write_abandoned(),
            "a closed log queued nothing, so the refusal must be definite; got: {error}"
        );

        let queued = queued_wal_err(WalError::Closed);
        assert!(
            queued.is_write_unconfirmed(),
            "a request the writer already owned may have been written before the writer \
             stopped, so its outcome is unknown, not abandoned; got: {queued}"
        );
        assert!(
            queued_wal_err(WalError::Poisoned("x".to_string()))
                .to_string()
                .contains("reopen the database"),
            "other errors after queueing map as before"
        );

        let poisoned = wal_err(WalError::Poisoned("injected".to_string()));
        assert!(
            poisoned.to_string().contains("reopen the database"),
            "a poisoned log must tell the operator what clears it; got: {poisoned}"
        );
    }

    /// A stale index entry must not hand back another key's value.
    ///
    /// `get` resolves a key to a frame through the index, decodes that frame, and returns
    /// only the op for the key it was asked for. An index entry pointing at the wrong
    /// frame — stale after compaction, or corrupted — must read as absent, not as the
    /// other key's value.
    ///
    /// Poking the index directly is the point rather than a shortcut. The guard's whole
    /// job is to be right when the index is wrong, and no amount of ordinary writing makes
    /// the index wrong.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_index_entry_does_not_return_another_keys_value() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = WalStorageAdapter::new(WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        })
        .expect("adapter opens");

        adapter
            .put(b"alpha", b"value-for-alpha")
            .await
            .expect("write alpha");
        adapter
            .put(b"beta", b"value-for-beta")
            .await
            .expect("write beta");

        assert_eq!(
            adapter.get(b"alpha").await.expect("read alpha"),
            Some(b"value-for-alpha".to_vec()),
            "the honest read must work before the index is poisoned, or the assertion \
             below would pass for the wrong reason"
        );

        // Point `alpha` at `beta`'s frame. This is what a stale entry looks like.
        let beta_loc = {
            let pinned = adapter.inner.index.pin();
            *pinned.get(&b"beta".to_vec()).expect("beta is indexed")
        };
        adapter
            .inner
            .index
            .pin()
            .insert(b"alpha".to_vec(), beta_loc);

        // The cache would answer from the earlier read and never reach the WAL.
        adapter.inner.cache.clear().await;

        assert_eq!(
            adapter
                .get(b"alpha")
                .await
                .expect("read alpha through the stale entry"),
            None,
            "a frame without an op for the requested key must not answer for that key"
        );
    }

    /// A config whose stall bound is short enough to observe inside a test.
    ///
    /// `max_flush_ms` is the knob the bounds derive from, so setting it here is the same
    /// lever an operator has — the test is not reaching past the mechanism to a private
    /// constant.
    ///
    /// The WAL declares a stall after `max(1s, 100 × sync_interval_ms)` with requests
    /// queued and no batch completed; `test_config`'s 10 ms interval makes that 1 s. The
    /// client bound is `128 × max_flush_ms` (`LivenessBounds`).
    fn liveness_config(dir: &Path, max_flush_ms: u64, max_pending: usize) -> StorageConfig {
        StorageConfig {
            wal: WalConfig {
                log_dir: dir.to_path_buf(),
                ..WalConfig::test_config()
            },
            batching: AdaptiveBatchConfig {
                max_flush_ms,
                max_pending,
                min_batch_size: 1,
                max_batch_size: max_pending,
                ..AdaptiveBatchConfig::default()
            },
            ..StorageConfig::default()
        }
    }

    /// A healthy adapter under tight bounds must not be reported as stalled. A detector
    /// that fires on everything would make the database unusable.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_working_writer_is_never_reported_as_stalled() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = WalStorageAdapter::new_with_config(liveness_config(dir.path(), 25, 65_536))
            .expect("adapter opens");

        // Several stall bounds' worth of ordinary writes.
        for round in 0..12 {
            adapter
                .put_many(vec![(format!("k{round}").into_bytes(), b"v".to_vec())])
                .await
                .expect("an ordinary write must succeed");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let health = adapter.write_path_health();
        assert!(health.healthy, "healthy writer reported as {health:?}");
        assert_eq!(health.queue_depth, 0);
        assert!(health.publishes_total >= 12);
        assert!(health.last_publish_age_ms.is_some());
        assert_eq!(adapter.metrics().writer_stalls_total, 0);
        assert!(adapter.metrics().writer_publishes_total >= 12);
        assert!(adapter.metrics().writer_last_publish_unix_ms.is_some());
    }

    /// Dropping an adapter joins the WAL's writer thread before the drop returns.
    ///
    /// The old adapter aborted two tokio tasks and let them notice on their next tick;
    /// the `Wal` instead drains, syncs and joins its `prkdb-wal-writer` thread in its own
    /// `Drop`, which runs when the last `Arc<WalStorageInner>` goes. So: the drop returns
    /// promptly, the inner state (and with it the `Wal`) is gone once it has, and the
    /// directory reopens with everything written before it.
    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_an_adapter_joins_the_writer_thread() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = WalStorageAdapter::new(config.clone()).expect("adapter opens");
        adapter.put(b"k", b"v").await.unwrap();
        let inner = Arc::downgrade(&adapter.inner);

        let started = Instant::now();
        drop(adapter);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "dropping the adapter took {:?}",
            started.elapsed()
        );
        assert!(
            inner.upgrade().is_none(),
            "the adapter's state, and the Wal whose Drop joins the writer, must be gone"
        );

        let reopened = WalStorageAdapter::open(config).expect("reopen");
        assert_eq!(reopened.get(b"k").await.unwrap(), Some(b"v".to_vec()));
    }

    /// Poll until `check` holds, so a test observes a transient window without racing it.
    async fn wait_until(what: &str, limit: Duration, mut check: impl FnMut() -> bool) {
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for {what}");
    }

    /// Acceptance 1: a writer that panics answers every waiter, and no caller blocks.
    ///
    /// The panic is injected in `write_at`, on the writer thread. Waiters whose request
    /// was in the batch being written get `WriteNotConfirmed`: their reply is dropped
    /// unanswered by the unwinding, and a panic after the bytes reached the file would
    /// leave the frame on disk. Every request after that, and every later write, gets the
    /// poisoned error, which names the panic, as does the health reason.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_panicking_write_poisons_and_answers_every_waiter() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = Arc::new(
            WalStorageAdapter::new_with_config(liveness_config(dir.path(), 25, 65_536))
                .expect("adapter opens"),
        );
        adapter
            .put(b"before", b"v")
            .await
            .expect("healthy before the fault");

        fault_injection::panic_writer_at(dir.path());
        let waiters: Vec<_> = (0..8u32)
            .map(|i| {
                let adapter = adapter.clone();
                tokio::spawn(async move { adapter.put(format!("k{i}").as_bytes(), b"v").await })
            })
            .collect();
        let mut outcomes = Vec::new();
        for waiter in waiters {
            outcomes.push(tokio::time::timeout(Duration::from_secs(10), waiter).await);
        }
        let later = adapter.put(b"later", b"v").await;
        fault_injection::clear_writer_panic(dir.path());

        for outcome in outcomes {
            let error = outcome
                .expect("every waiter must be answered, not left waiting")
                .expect("the waiting task must not panic")
                .expect_err("a panicking writer cannot confirm a write");
            assert!(
                error.is_write_unconfirmed() || error.to_string().contains("injected writer panic"),
                "a waiter is either not confirmed (in the batch that panicked) or refused \
                 with the panic named; got: {error}"
            );
        }
        let later = later.expect_err("a poisoned log refuses later writes");
        assert!(
            later.to_string().contains("injected writer panic")
                && later.to_string().contains("reopen the database"),
            "a later write must name the panic and what clears it; got: {later}"
        );

        let health = adapter.write_path_health();
        assert!(!health.healthy);
        assert!(
            health
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("panicked")),
            "the health reason must name the panic: {health:?}"
        );
        assert_eq!(health.queue_depth, 0, "no waiter may be left behind");
        for i in 0..8u32 {
            assert_eq!(adapter.get(format!("k{i}").as_bytes()).await.unwrap(), None);
        }
        assert_eq!(
            adapter.get(b"before").await.unwrap(),
            Some(b"v".to_vec()),
            "what was published before the panic stays readable"
        );
    }

    /// Acceptance 2: a writer that is alive but completes nothing is detected and reported
    /// unhealthy; the waiting caller is answered `WriteNotConfirmed` at its client bound,
    /// and (acceptance 3) that write may still land: once the stall clears, it does.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stalled_writer_is_reported_unhealthy() {
        let dir = tempfile::tempdir().unwrap();
        // max_flush_ms 25 => client bound 3.2s, beyond the WAL's 1s stall bound.
        let adapter = WalStorageAdapter::new_with_config(liveness_config(dir.path(), 25, 65_536))
            .expect("adapter opens");
        let bound = LivenessBounds::from_max_flush_ms(25).client_bound;

        let stall = fault_injection::StallGuard::new(dir.path());
        let started = Instant::now();
        let outcome = tokio::time::timeout(Duration::from_secs(20), adapter.put(b"k", b"v")).await;
        let elapsed = started.elapsed();
        let health = adapter.write_path_health();
        drop(stall);

        let error = outcome
            .expect("the caller must be answered, not left waiting")
            .expect_err("a stalled writer confirmed nothing");
        assert!(
            error.is_write_unconfirmed(),
            "the request was queued with the writer, so its outcome is unknown; got: {error}"
        );
        assert!(
            elapsed >= bound && elapsed < bound + Duration::from_secs(5),
            "the caller is answered at its client bound ({bound:?}), took {elapsed:?}"
        );
        assert!(!health.healthy, "the health probe must report the stall");
        assert!(
            health
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("stalled")),
            "{health:?}"
        );
        assert_eq!(health.queue_depth, 1);
        assert!(health.oldest_unpublished_age_ms >= 1_000, "{health:?}");

        wait_until("the writer to recover", Duration::from_secs(10), || {
            let health = adapter.write_path_health();
            health.healthy && health.queue_depth == 0
        })
        .await;
        assert_eq!(
            adapter.get(b"k").await.unwrap(),
            Some(b"v".to_vec()),
            "a not-confirmed write may still land, and this one did"
        );
    }

    /// Acceptance 3, at the boundary that matters: the variant survives the trait object
    /// every caller in the codebase actually holds. `PrkDb` stores an
    /// `Arc<dyn StorageAdapter>`, so a variant flattened on the way through it would make
    /// the distinction unobservable however carefully it is defined.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_not_confirmed_variant_survives_the_storage_adapter_boundary() {
        let dir = tempfile::tempdir().unwrap();
        // max_flush_ms 5 => client bound 640ms.
        let storage: Arc<dyn StorageAdapter> = Arc::new(
            WalStorageAdapter::new_with_config(liveness_config(dir.path(), 5, 65_536))
                .expect("adapter opens"),
        );

        let _stall = fault_injection::StallGuard::new(dir.path());
        let result = tokio::time::timeout(
            Duration::from_secs(20),
            storage.put_many(vec![(b"k".to_vec(), b"v".to_vec())]),
        )
        .await
        .expect("the caller must be answered");

        let error = result.expect_err("a stalled writer confirmed nothing");
        assert!(
            matches!(error, StorageError::WriteNotConfirmed(_)),
            "the variant must arrive intact rather than as Internal; got: {error:?}"
        );
        wait_until(
            "the stall to show through the trait object",
            Duration::from_secs(10),
            || !storage.write_path_health().healthy,
        )
        .await;
    }

    /// Acceptance 4: with admission exhausted and the writer stalled, a new write waits up
    /// to its client bound and is then refused with backpressure, never queued; memory
    /// stays bounded by `max_queued_bytes`. A write still waiting when the writer resumes
    /// gets in.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_full_queue_makes_writers_wait_then_refuses() {
        let dir = tempfile::tempdir().unwrap();
        let max_flush_ms = 10; // client bound 1.28s
        let bound = LivenessBounds::from_max_flush_ms(max_flush_ms).client_bound;
        let mut config = liveness_config(dir.path(), max_flush_ms, 65_536);
        config.wal.max_queued_bytes = 256;
        let adapter = Arc::new(WalStorageAdapter::new_with_config(config).expect("adapter opens"));

        let stall = fault_injection::StallGuard::new(dir.path());
        // A payload at least as large as the admission budget holds all of it until the
        // writer answers it.
        let filler = {
            let adapter = adapter.clone();
            tokio::spawn(async move { adapter.put(b"filler", &[7u8; 512]).await })
        };
        wait_until("the filler to be queued", Duration::from_secs(10), || {
            adapter.write_path_health().queue_depth == 1
        })
        .await;

        for attempt in 0..3 {
            let key = format!("overflow{attempt}");
            let started = Instant::now();
            let error = adapter
                .put(key.as_bytes(), b"v")
                .await
                .expect_err("admission is exhausted");
            assert!(
                matches!(error, StorageError::WriteBackpressure(_)),
                "a refused write must say so definitely, so retrying is safe; got: {error:?}"
            );
            assert!(
                started.elapsed() >= bound,
                "the writer must wait for admission up to its bound ({bound:?}) before being \
                 refused; waited {:?}",
                started.elapsed()
            );
            assert_eq!(
                adapter.write_path_health().queue_depth,
                1,
                "a refused write must not be queued"
            );
            assert_eq!(adapter.get(key.as_bytes()).await.unwrap(), None);
        }

        // Backpressure, not a wall: a write waiting for admission gets in once the writer
        // resumes.
        let waiting = {
            let adapter = adapter.clone();
            tokio::spawn(async move { adapter.put(b"waiting", b"v").await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(stall);
        tokio::time::timeout(Duration::from_secs(20), waiting)
            .await
            .expect("the waiting write must resolve once the writer resumes")
            .expect("the task must not panic")
            .expect("the waiting write must be admitted and land");
        assert_eq!(adapter.get(b"waiting").await.unwrap(), Some(b"v".to_vec()));

        // The filler was queued for longer than its own client bound, so it was answered
        // not-confirmed; it was with the writer all along, and landed once released.
        let filled = tokio::time::timeout(Duration::from_secs(20), filler)
            .await
            .expect("the filler resolves")
            .expect("the task must not panic");
        assert!(
            filled.as_ref().is_err_and(|e| e.is_write_unconfirmed()),
            "{filled:?}"
        );
        assert_eq!(adapter.get(b"filler").await.unwrap(), Some(vec![7u8; 512]));
    }

    /// The observability the spec asks for, read where an operator reads it: queue depth,
    /// the age of the oldest unpublished write, the publish count and the time of the last
    /// publish, while a stall forms and after it clears.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_write_path_publishes_the_numbers_that_show_a_stall_forming() {
        let dir = tempfile::tempdir().unwrap();
        // max_flush_ms 5000 => client bound 640s: the write waits out the stall.
        let adapter = Arc::new(
            WalStorageAdapter::new_with_config(liveness_config(dir.path(), 5_000, 65_536))
                .expect("adapter opens"),
        );
        adapter.put(b"seed", b"v").await.expect("seed write");
        let idle = adapter.write_path_health();
        assert!(idle.healthy && idle.queue_depth == 0, "{idle:?}");
        assert_eq!(idle.publishes_total, 1);

        let stall = fault_injection::StallGuard::new(dir.path());
        let filler = {
            let adapter = adapter.clone();
            tokio::spawn(async move { adapter.put(b"k", b"v").await })
        };
        wait_until(
            "the queued write to show in the depth gauge",
            Duration::from_secs(10),
            || adapter.write_path_health().queue_depth == 1,
        )
        .await;
        wait_until(
            "the stall to be reported with the oldest write's age",
            Duration::from_secs(10),
            || {
                let health = adapter.write_path_health();
                !health.healthy && health.oldest_unpublished_age_ms >= 1_000
            },
        )
        .await;
        let stalled = adapter.write_path_health();
        assert_eq!(
            stalled.publishes_total, 1,
            "nothing publishes while stalled"
        );
        assert!(stalled.last_publish_age_ms.is_some_and(|age| age >= 1_000));

        drop(stall);
        tokio::time::timeout(Duration::from_secs(20), filler)
            .await
            .expect("the write resolves once the writer resumes")
            .expect("the filler task must not panic")
            .expect("the write publishes");

        let after = adapter.write_path_health();
        assert!(after.healthy, "{after:?}");
        assert_eq!(after.queue_depth, 0);
        assert_eq!(after.oldest_unpublished_age_ms, 0);
        assert_eq!(after.publishes_total, 2);
        assert!(after.last_publish_age_ms.is_some_and(|age| age < 1_000));
        assert!(adapter.metrics().writer_publishes_total >= 2);
        assert!(adapter.metrics().writer_last_publish_unix_ms.is_some());
    }

    /// A write whose append failed is never visible, does not count as a publish, and
    /// poisons the log until reopen, as a real disk error would (fsyncgate: no retry).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_append_is_never_visible() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = WalStorageAdapter::new(config.clone()).expect("adapter opens");
        assert!(adapter.write_path_health().last_publish_age_ms.is_none());

        fault_injection::fail_append_at(dir.path());
        let refused = adapter.put(b"k", b"v").await;
        let later = adapter.put(b"k2", b"v2").await;
        fault_injection::clear_append_failure(dir.path());

        let error = refused.expect_err("a failed append must be reported to its caller");
        assert!(
            error.to_string().contains("injected append failure")
                && error.to_string().contains("reopen the database"),
            "the error must carry the cause and what clears it; got: {error}"
        );
        assert_eq!(adapter.get(b"k").await.unwrap(), None);
        let later = later.expect_err("the log stays poisoned after a failed write");
        assert!(later.to_string().contains("WAL poisoned"), "{later}");

        let health = adapter.write_path_health();
        assert!(!health.healthy);
        assert!(
            health
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("injected append failure")),
            "{health:?}"
        );
        assert_eq!(health.publishes_total, 0, "nothing reached the log");
        assert!(
            health.last_publish_age_ms.is_none(),
            "a failed append must not advance the last-publish clock"
        );
        assert_eq!(adapter.metrics().writer_last_publish_unix_ms, None);

        drop(adapter);
        let reopened = WalStorageAdapter::open(config).expect("reopen clears the poison");
        assert_eq!(reopened.get(b"k").await.unwrap(), None);
        assert_eq!(reopened.get(b"k2").await.unwrap(), None);
        reopened.put(b"k3", b"v3").await.expect("writes again");
        assert_eq!(reopened.write_path_health().publishes_total, 1);
    }
}
