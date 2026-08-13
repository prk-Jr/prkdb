#[allow(unused_imports)]
use super::cache::LruCache;
use super::checkpoint;
use super::config::{StorageConfig, SyncMode};
use super::recovery::RecoveryManager;
use super::snapshot::SnapshotWriter;
use super::writer_liveness::{
    unix_millis, LivenessBounds, SharedProgress, WritePathProgress, WriterFailure,
};
use prkdb_types::snapshot::{CompressionType, SnapshotHeader};

use papaya::HashMap as LockFreeHashMap; // Phase 5: Lock-free index
use prkdb_core::batching::adaptive::{AdaptiveBatchAccumulator, AdaptiveBatchConfig};
use prkdb_core::replication::{Change, ReplicationManager};
use prkdb_core::wal::compaction::{CompactionConfig, Compactor};
use prkdb_core::wal::mmap_parallel_wal::MmapParallelWal;
use prkdb_core::wal::{LogOperation, LogRecord, WalConfig};
use prkdb_metrics::storage::StorageMetrics;
use prkdb_types::error::StorageError;
use prkdb_types::storage::{StorageAdapter, WritePathHealth};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;
use tokio::sync::{oneshot, Mutex, Notify, OwnedRwLockWriteGuard, RwLock};
use tokio::task::{AbortHandle, JoinHandle};
use tracing::{info, instrument, warn};

// Phase 2: Dedicated Sync Writer Thread

/// Configuration for the dedicated writer thread
#[derive(Debug, Clone)]
// The dedicated writer thread was removed here.
//
// `spawn_dedicated_writer`, `run_writer_loop` and `write_batch` implemented a batching
// writer fed by a `crossbeam_channel`. Nothing ever sent on that channel: the sender was
// held as `_write_tx` — underscore-prefixed, and commented "Phase 2: Writer queue
// (disabled for now)" — and no call site anywhere constructed a `WriteRequest` for it.
// `write_queue.rs` has its own separate channel and is unrelated.
//
// So the loop blocked on `rx.recv()` for the life of every adapter and its body never ran.
// Mutation run 31411280726 reported 11 survivors inside it, which is what unreachable code
// looks like from the outside: `batch_count -= 1` on a `usize` would panic on the first
// iteration of a debug build, and it survived, because there is no first iteration.
//
// It was not free. Each constructor spawned one OS thread that then blocked forever, and
// `CollectionPartitionedAdapter` builds one `WalStorageAdapter` per collection — so a
// database with N collections carried N idle threads. That is the same thread pressure
// that made the cluster suite starve itself and produce failures wearing the costume of
// consensus bugs.
//
// Deleted rather than tested. Writing tests for code nothing calls would have turned the
// mutation report green while changing nothing about the program.

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

    pub fn with_sync_mode(mut self, mode: SyncMode) -> Self {
        self.config.sync_mode = mode;
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
#[cfg(test)]
pub(crate) mod fault_injection {
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};

    fn registry(kind: Fault) -> &'static Mutex<HashSet<PathBuf>> {
        static FLUSH_FAILURE: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
        static WRITER_STALL: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
        static WRITER_PANIC: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
        static APPEND_FAILURE: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
        static NO_WRITER: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();

        let slot = match kind {
            Fault::FlushFailure => &FLUSH_FAILURE,
            Fault::WriterStall => &WRITER_STALL,
            Fault::WriterPanic => &WRITER_PANIC,
            Fault::AppendFailure => &APPEND_FAILURE,
            Fault::WriterNeverStarted => &NO_WRITER,
        };
        slot.get_or_init(|| Mutex::new(HashSet::new()))
    }

    #[derive(Clone, Copy)]
    enum Fault {
        FlushFailure,
        WriterStall,
        WriterPanic,
        AppendFailure,
        WriterNeverStarted,
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

    /// Make the WAL append *inside the writer* fail for the adapter whose WAL lives at
    /// `dir`.
    ///
    /// Distinct from `fail_flush_at`, which fails the caller-facing `flush` and never
    /// reaches the flush loop. This one reaches the branch where a batch was taken out of
    /// the accumulator and none of it got to the log: the waiters are answered with an
    /// error, the queue accounting still has to balance, and nothing may be reported as
    /// published. Real causes are a full disk or a failing fsync, neither of which a test
    /// can arrange portably.
    pub fn fail_append_at(dir: impl Into<PathBuf>) {
        arm(Fault::AppendFailure, dir);
    }

    pub fn clear_append_failure(dir: &Path) {
        disarm(Fault::AppendFailure, dir);
    }

    pub(super) fn append_should_fail(dir: &Path) -> bool {
        armed(Fault::AppendFailure, dir)
    }

    /// Open an adapter whose writer subsystem never starts.
    ///
    /// Every constructor calls `spawn_writer`, so this state is not reachable by
    /// configuration — but cargo-mutants reaches it by deleting `spawn_writer`, and the
    /// result was every write in the suite waiting out its client bound and the mutation
    /// run timing out at 600s rather than reporting anything. The seam exists so the
    /// refusal that now covers that case can be asserted directly.
    pub fn never_start_writer_at(dir: impl Into<PathBuf>) {
        arm(Fault::WriterNeverStarted, dir);
    }

    pub fn clear_never_start_writer(dir: &Path) {
        disarm(Fault::WriterNeverStarted, dir);
    }

    pub(super) fn writer_should_not_start(dir: &Path) -> bool {
        armed(Fault::WriterNeverStarted, dir)
    }

    /// Make the flush loop stop publishing while staying alive and looping.
    ///
    /// This is the failure the liveness spec exists for, and it is the one that cannot be
    /// reached any other way: the task runs, its `JoinHandle` never resolves, and the only
    /// evidence anything is wrong is that the queue stops moving. It reproduces exactly
    /// what cargo-mutants produced by replacing `flush_accumulator_inner` with `()`.
    pub fn stall_writer_at(dir: impl Into<PathBuf>) {
        arm(Fault::WriterStall, dir);
    }

    pub fn clear_writer_stall(dir: &Path) {
        disarm(Fault::WriterStall, dir);
    }

    pub(super) fn writer_should_stall(dir: &Path) -> bool {
        armed(Fault::WriterStall, dir)
    }

    /// Make the flush loop panic on its next iteration.
    pub fn panic_writer_at(dir: impl Into<PathBuf>) {
        arm(Fault::WriterPanic, dir);
    }

    pub fn clear_writer_panic(dir: &Path) {
        disarm(Fault::WriterPanic, dir);
    }

    pub(super) fn writer_should_panic(dir: &Path) -> bool {
        armed(Fault::WriterPanic, dir)
    }
}

/// Storage adapter backed by Write-Ahead Log (Mmap Parallel)
///
/// Provides high-performance sequential write with memory-mapped I/O
/// (throughput unverified; see `docs/benchmarks/methodology.md`)
/// while maintaining an in-memory index for fast reads.
#[derive(Clone)]

pub struct WalStorageAdapter {
    inner: Arc<WalStorageInner>,
    // Phase 2: Dedicated sync writer
}

/// One client write waiting for the flush loop to publish it.
///
/// # Why both fields are `Option`
///
/// So that `Drop` can answer the caller. A `PendingWrite` destroyed without a result used
/// to close its `oneshot` silently; the call sites did handle `RecvError`, but nothing in
/// the program guaranteed a queued write was ever dropped *or* fired, so that handler was
/// unreachable — written, correct, and dead. Firing the channel from `Drop` makes the
/// obligation structural: there is no way to destroy a queued write without its caller
/// learning something.
///
/// `Drop` types cannot be moved out of or destructured, hence
/// [`into_parts`](PendingWrite::into_parts) and the `Option`s rather than a `mem::replace`
/// with a synthetic `LogRecord` — building one costs a serialization pass, and this is the
/// per-write hot path.
struct PendingWrite {
    record: Option<LogRecord>,
    tx: Option<oneshot::Sender<Result<u64, StorageError>>>,
}

impl PendingWrite {
    fn new(record: LogRecord) -> (Self, oneshot::Receiver<Result<u64, StorageError>>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                record: Some(record),
                tx: Some(tx),
            },
            rx,
        )
    }

    /// The collection this write belongs to, for the flush loop's grouping pass.
    fn operation(&self) -> &LogOperation {
        &self
            .record
            .as_ref()
            .expect("PendingWrite still holds its record until into_parts")
            .operation
    }

    /// Split into the record to write and the sender to answer, disarming the drop guard.
    ///
    /// The sender is an `Option` on the way out too: the accumulator can hold a write whose
    /// caller has already been answered — a backpressure refusal disarms one on the spot —
    /// and the publish path must not care which.
    fn into_parts(
        mut self,
    ) -> (
        LogRecord,
        Option<oneshot::Sender<Result<u64, StorageError>>>,
    ) {
        let record = self
            .record
            .take()
            .expect("PendingWrite::into_parts called once");
        (record, self.tx.take())
    }
}

impl Drop for PendingWrite {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            // Deliberately *not confirmed* rather than an error naming a failure. A write
            // can be dropped after its batch has already reached the log — a panic partway
            // through publication is exactly that — so this cannot honestly claim the
            // record did not land.
            let _ = tx.send(Err(StorageError::WriteNotConfirmed(
                "the queued write was discarded before its outcome was known".to_string(),
            )));
        }
    }
}

/// Handles for the flush loop and the task that supervises it.
///
/// Retained rather than discarded. All four constructors used to end with
/// `tokio::spawn(async move { Self::run_flush_loop(weak).await; });` and throw the
/// `JoinHandle` away — the struct below even carried the comment
/// "Phase 2: Writer thread handle (stored here for Drop)" with no field under it. That
/// hole is why a writer that panicked was indistinguishable from one that was idle:
/// nothing observed the task, so nothing discharged the writes it was holding.
///
/// Both tasks hold a `Weak<WalStorageInner>`, so keeping their handles on the strong side
/// creates no cycle.
struct WriterTasks {
    /// Stops the flush loop when the adapter goes away.
    ///
    /// An `AbortHandle` and not the `JoinHandle`, because the supervisor owns that one:
    /// learning *how* the writer exited means `await`ing it, and a handle cannot be both
    /// awaited there and kept here.
    flush_loop: AbortHandle,
    supervisor: AbortHandle,
}

struct WalStorageInner {
    _config: StorageConfig,
    wal: Arc<MmapParallelWal>,
    index: Arc<LockFreeHashMap<Vec<u8>, u64>>, // Phase 5: Lock-free index (papaya)
    cache: Arc<super::cache::ShardedLruCache<Vec<u8>, Vec<u8>>>, // Sharded for concurrent access!
    outbox: Arc<LockFreeHashMap<String, Vec<u8>>>,
    replication: Option<tokio::sync::Mutex<ReplicationManager>>,
    compactor: Option<Arc<Compactor>>,
    recovery: Arc<RecoveryManager>,
    metrics: Arc<StorageMetrics>,
    accumulator: Mutex<AdaptiveBatchAccumulator<PendingWrite>>,
    flush_notify: Arc<Notify>,
    /// Wakes the stall watchdog when a queue that was empty stops being empty.
    ///
    /// Separate from `flush_notify` because `notify_one` wakes exactly one waiter, and the
    /// flush loop and the watchdog both need the signal — sharing one would have them
    /// stealing wakeups from each other.
    writer_notify: Arc<Notify>,
    /// How many times the watchdog has looked at the write path.
    ///
    /// Test-only. Acceptance 1 of the liveness spec is that an idle adapter performs *no*
    /// periodic wakeups, and "none" has no observable unless something counts them —
    /// asserting it beats eyeballing a log.
    #[cfg(test)]
    supervisor_checks: AtomicU64,
    transaction_barrier: Arc<RwLock<()>>,
    /// Guards the moment a batch becomes *visible*, as opposed to the moment it is durable.
    ///
    /// A batch is appended to the WAL as a unit but published into the index one key at a
    /// time, so without this a reader could observe half of a multi-key commit (spec
    /// S-03). Writers take it exclusively while publishing; `snapshot_get_many` takes it
    /// shared for the whole read.
    ///
    /// Deliberately separate from `transaction_barrier`: writers already hold that lock
    /// for reading when they publish, and re-entering it for writing would deadlock.
    /// Nothing acquires this lock and then `transaction_barrier`, so the order is acyclic.
    publish_barrier: Arc<RwLock<()>>,
    // Phase 8: Track max offset for compaction and change detection
    max_offset: AtomicU64,
    // Phase 9: Checkpoint path for fast recovery
    checkpoint_path: PathBuf,
    /// Monotonic accounting for writes between the accumulator and the log.
    ///
    /// The only thing in the process that can tell a writer which is alive and publishing
    /// from one which is alive and publishing nothing. See `storage::writer_liveness`.
    progress: SharedProgress,
    /// Stall and client-wait bounds, derived once from the configured flush interval.
    bounds: LivenessBounds,
    /// Phase 2: writer task handles, stored here for `Drop` — the field the original
    /// comment promised and never had.
    ///
    /// `OnceLock` because both tasks need a `Weak<WalStorageInner>` and so cannot be
    /// spawned until this struct exists. Set once, immediately after construction.
    writer: OnceLock<WriterTasks>,
}

impl Drop for WalStorageInner {
    fn drop(&mut self) {
        self.flush_notify.notify_one();

        // Stop the background tasks rather than leaving them to notice on their next tick
        // that `Weak::upgrade` fails. Both already exit on their own once this struct is
        // gone; aborting makes it immediate, which matters most in tests, where hundreds of
        // adapters are opened and dropped inside one runtime.
        if let Some(tasks) = self.writer.get() {
            tasks.flush_loop.abort();
            tasks.supervisor.abort();
        }
    }
}

impl Drop for WalStorageAdapter {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) != 1 {
            return;
        }

        Self::flush_on_last_handle_drop(self.inner.clone());
    }
}

impl WalStorageAdapter {
    pub(crate) async fn acquire_transaction_write_guard(&self) -> OwnedRwLockWriteGuard<()> {
        self.inner.transaction_barrier.clone().write_owned().await
    }

    /// Create a builder for WalStorageAdapter
    pub fn builder(log_dir: PathBuf) -> WalStorageAdapterBuilder {
        WalStorageAdapterBuilder::new(log_dir)
    }

    async fn put_batch_impl(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), StorageError> {
        if entries.is_empty() {
            return Ok(());
        }

        let batch_size = entries.len();

        let total_bytes: u64 = entries
            .iter()
            .map(|(key, value)| (key.len() + value.len()) as u64)
            .sum();
        self.inner.metrics.record_write(total_bytes);

        let records: Vec<LogRecord> = entries
            .iter()
            .map(|(key, value)| {
                LogRecord::new(LogOperation::Put {
                    collection: String::new(),
                    id: key.clone(),
                    data: value.clone(),
                })
            })
            .collect();

        let offsets = self
            .inner
            .wal
            .append_batch(records)
            .await
            .map_err(|e| StorageError::Internal(format!("WAL batch write failed: {}", e)))?;

        if offsets.len() != batch_size {
            return Err(StorageError::Internal(format!(
                "WAL returned {} offsets for {} records",
                offsets.len(),
                batch_size
            )));
        }

        if let Some((_, max_off)) = offsets.last() {
            self.inner.max_offset.fetch_max(*max_off, Ordering::Relaxed);
        }

        // Publish the whole batch as one visible step. The WAL append above already made
        // it durable as a unit; this makes it *observable* as a unit, so a concurrent
        // reader cannot see one key of a two-key commit (S-03).
        let publish = self.inner.publish_barrier.write().await;

        let cache_entries: Vec<_> = entries
            .into_iter()
            .enumerate()
            .map(|(i, (key, value))| {
                let (_segment_id, offset) = offsets[i];
                self.inner.index.pin().insert(key.clone(), offset);
                (key, value)
            })
            .collect();

        self.inner.cache.put_batch(cache_entries).await;
        drop(publish);

        Ok(())
    }

    async fn delete_many_impl(&self, keys: Vec<Vec<u8>>) -> Result<(), StorageError> {
        if keys.is_empty() {
            return Ok(());
        }

        let batch_size = keys.len();

        let total_bytes: u64 = keys.iter().map(|key| key.len() as u64).sum();
        self.inner
            .metrics
            .record_write_batch(batch_size as u64, total_bytes);

        let records: Vec<LogRecord> = keys
            .iter()
            .map(|key| {
                LogRecord::new(LogOperation::Delete {
                    collection: String::new(),
                    id: key.clone(),
                })
            })
            .collect();

        self.inner
            .wal
            .append_batch(records)
            .await
            .map_err(|e| StorageError::Internal(format!("WAL batch delete failed: {}", e)))?;

        // Same publication barrier as put_batch_impl: a batched delete must not be
        // observable half-applied either.
        let publish = self.inner.publish_barrier.write().await;
        {
            let index_pin = self.inner.index.pin();
            for key in &keys {
                index_pin.remove(key);
            }
        }

        self.inner.cache.remove_batch(keys).await;
        drop(publish);

        Ok(())
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

    /// Create a new WAL storage adapter with default configuration
    #[instrument(skip(config), fields(log_dir = %config.log_dir.display()))]
    pub fn new(config: WalConfig) -> Result<Self, StorageError> {
        let storage_config = StorageConfig {
            wal: config,
            ..StorageConfig::default()
        };
        Self::new_with_config(storage_config)
    }

    /// Create a new WAL storage adapter with custom configuration
    #[instrument(skip(config), fields(log_dir = %config.wal.log_dir.display()))]
    pub fn new_with_config(config: StorageConfig) -> Result<Self, StorageError> {
        info!("Initializing WalStorageAdapter");
        // Create Mmap parallel WAL with 4 segments
        // open_or_create, never create: `create` truncates every segment, so opening an
        // existing data directory with it destroys the database. This is the constructor
        // `PrkDb::builder().with_data_dir()` reaches.
        let wal = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                MmapParallelWal::open_or_create(config.wal.clone(), config.wal.segment_count).await
            })
        })
        .map_err(|e| StorageError::Internal(format!("Failed to open Mmap WAL: {}", e)))?;

        let wal = Arc::new(wal);

        // Create compactor
        let compactor = Arc::new(Compactor::new(wal.clone(), config.compaction.clone()));

        // Create recovery manager
        let recovery = Arc::new(RecoveryManager::new(
            wal.clone(),
            config.wal.log_dir.clone(),
        ));

        let metrics = Arc::new(StorageMetrics::new());

        let inner = Arc::new(WalStorageInner {
            _config: config.clone(),
            wal,
            index: Arc::new(LockFreeHashMap::new()), // Phase 5: Lock-free HashMap
            cache: Arc::new(super::cache::ShardedLruCache::with_metrics(
                100_000, // Match default config cache capacity
                metrics.clone(),
            )),
            outbox: Arc::new(LockFreeHashMap::new()),
            replication: None,
            compactor: Some(compactor),
            recovery,
            metrics,
            accumulator: Mutex::new(AdaptiveBatchAccumulator::new(config.batching.clone())),
            flush_notify: Arc::new(Notify::new()),
            writer_notify: Arc::new(Notify::new()),
            #[cfg(test)]
            supervisor_checks: AtomicU64::new(0),
            transaction_barrier: Arc::new(RwLock::new(())),
            publish_barrier: Arc::new(RwLock::new(())),
            max_offset: AtomicU64::new(0),
            checkpoint_path: config.wal.log_dir.join("checkpoint.json"),
            progress: Arc::new(WritePathProgress::new()),
            bounds: LivenessBounds::from_max_flush_ms(config.batching.max_flush_ms),
            writer: OnceLock::new(),
        });

        let adapter = Self {
            inner: inner.clone(),
        };

        // Rebuild the index from the WAL. Without this the log is recovered but every key
        // in it stays invisible, so reopening a populated data directory reported an empty
        // database. `open`/`open_async` always did this; this constructor did not, and this
        // is the one `PrkDb::builder().with_data_dir()` reaches.
        info!("Rebuilding index from WAL...");
        let start = std::time::Instant::now();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(adapter.rebuild_index_async())
        })?;
        info!("Index rebuild complete in {:?}", start.elapsed());

        // Spawn the background flush task and its supervisor.
        Self::spawn_writer(&inner);

        info!("WalStorageAdapter initialized successfully");
        Ok(adapter)
    }

    /// Create a new WAL storage adapter with replication and Mmap parallel writes
    #[instrument(skip(config, replication_manager), fields(log_dir = %config.log_dir.display()))]
    pub async fn new_with_replication(
        config: WalConfig,
        replication_manager: ReplicationManager,
    ) -> Result<Self, StorageError> {
        info!("Initializing WalStorageAdapter with replication");
        let storage_config = StorageConfig {
            wal: config,
            ..StorageConfig::default()
        };

        let wal = MmapParallelWal::open_or_create(storage_config.wal.clone(), 4)
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to create Mmap WAL: {}", e)))?;

        let wal = Arc::new(wal);

        // Create compactor
        let compactor = Arc::new(Compactor::new(
            wal.clone(),
            storage_config.compaction.clone(),
        ));

        // Create recovery manager
        let recovery = Arc::new(RecoveryManager::new(
            wal.clone(),
            storage_config.wal.log_dir.clone(),
        ));

        let metrics = Arc::new(StorageMetrics::new());

        let inner = Arc::new(WalStorageInner {
            _config: storage_config.clone(),
            wal,
            index: Arc::new(LockFreeHashMap::new()),
            cache: Arc::new(super::cache::ShardedLruCache::with_metrics(
                storage_config.cache_capacity,
                metrics.clone(),
            )),
            outbox: Arc::new(LockFreeHashMap::new()),
            replication: Some(tokio::sync::Mutex::new(replication_manager)),
            compactor: Some(compactor),
            recovery,
            metrics,
            accumulator: Mutex::new(AdaptiveBatchAccumulator::new(
                storage_config.batching.clone(),
            )),
            flush_notify: Arc::new(Notify::new()),
            writer_notify: Arc::new(Notify::new()),
            #[cfg(test)]
            supervisor_checks: AtomicU64::new(0),
            transaction_barrier: Arc::new(RwLock::new(())),
            publish_barrier: Arc::new(RwLock::new(())),
            max_offset: AtomicU64::new(0),
            checkpoint_path: storage_config.wal.log_dir.join("checkpoint.json"),
            progress: Arc::new(WritePathProgress::new()),
            bounds: LivenessBounds::from_max_flush_ms(storage_config.batching.max_flush_ms),
            writer: OnceLock::new(),
        });

        let adapter = Self {
            inner: inner.clone(),
        };

        // Same index rebuild as every other open path.
        info!("Rebuilding index from WAL...");
        adapter.rebuild_index_async().await?;

        // Spawn the background flush task and its supervisor.
        Self::spawn_writer(&inner);

        info!("WalStorageAdapter with replication initialized successfully");
        Ok(adapter)
    }

    /// Open an existing WAL storage adapter and rebuild index
    #[instrument(skip(config), fields(log_dir = %config.log_dir.display()))]
    pub fn open(config: WalConfig) -> Result<Self, StorageError> {
        info!("Opening existing WalStorageAdapter");
        let storage_config = StorageConfig {
            wal: config,
            ..StorageConfig::default()
        };

        // Open Mmap parallel WAL with configured segment count
        let wal = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                MmapParallelWal::open(storage_config.wal.clone(), storage_config.wal.segment_count)
                    .await
            })
        })
        .map_err(|e| StorageError::Internal(format!("Failed to open Mmap WAL: {}", e)))?;

        let wal = Arc::new(wal);

        // Create compactor with default config
        let compactor = Arc::new(Compactor::new(
            wal.clone(),
            storage_config.compaction.clone(),
        ));

        // Create recovery manager
        let recovery = Arc::new(RecoveryManager::new(
            wal.clone(),
            storage_config.wal.log_dir.clone(),
        ));

        let metrics = Arc::new(StorageMetrics::new());

        let inner = Arc::new(WalStorageInner {
            _config: storage_config.clone(),
            wal: wal.clone(),
            index: Arc::new(LockFreeHashMap::new()),
            cache: Arc::new(super::cache::ShardedLruCache::with_metrics(
                storage_config.cache_capacity,
                metrics.clone(),
            )),
            outbox: Arc::new(LockFreeHashMap::new()),
            replication: None,
            compactor: Some(compactor),
            recovery,
            metrics,
            accumulator: Mutex::new(AdaptiveBatchAccumulator::new(
                storage_config.batching.clone(),
            )),
            flush_notify: Arc::new(Notify::new()),
            writer_notify: Arc::new(Notify::new()),
            #[cfg(test)]
            supervisor_checks: AtomicU64::new(0),
            transaction_barrier: Arc::new(RwLock::new(())),
            publish_barrier: Arc::new(RwLock::new(())),
            max_offset: AtomicU64::new(0),
            checkpoint_path: storage_config.wal.log_dir.join("checkpoint.json"),
            progress: Arc::new(WritePathProgress::new()),
            bounds: LivenessBounds::from_max_flush_ms(storage_config.batching.max_flush_ms),
            writer: OnceLock::new(),
        });

        let adapter = Self {
            inner: inner.clone(),
        };

        // Rebuild index from WAL
        info!("Rebuilding index from WAL...");
        let start = std::time::Instant::now();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(adapter.rebuild_index_async())
        })?;
        info!("Index rebuild complete in {:?}", start.elapsed());

        // Spawn the background flush task and its supervisor.
        Self::spawn_writer(&inner);

        Ok(adapter)
    }

    /// Open an existing WAL storage adapter and rebuild index asynchronously
    #[instrument(skip(config), fields(log_dir = %config.log_dir.display()))]
    pub async fn open_async(config: WalConfig) -> Result<Self, StorageError> {
        info!("Opening existing WalStorageAdapter asynchronously");
        let storage_config = StorageConfig {
            wal: config,
            ..StorageConfig::default()
        };

        // Open Mmap parallel WAL with configured segment count
        let wal =
            MmapParallelWal::open(storage_config.wal.clone(), storage_config.wal.segment_count)
                .await
                .map_err(|e| StorageError::Internal(format!("Failed to open Mmap WAL: {}", e)))?;

        let wal = Arc::new(wal);

        // Create compactor with default config
        let compactor = Arc::new(Compactor::new(
            wal.clone(),
            storage_config.compaction.clone(),
        ));

        // Create recovery manager
        let recovery = Arc::new(RecoveryManager::new(
            wal.clone(),
            storage_config.wal.log_dir.clone(),
        ));

        let metrics = Arc::new(StorageMetrics::new());

        let inner = Arc::new(WalStorageInner {
            _config: storage_config.clone(),
            wal: wal.clone(),
            index: Arc::new(LockFreeHashMap::new()),
            cache: Arc::new(super::cache::ShardedLruCache::with_metrics(
                storage_config.cache_capacity,
                metrics.clone(),
            )),
            outbox: Arc::new(LockFreeHashMap::new()),
            replication: None,
            compactor: Some(compactor),
            recovery,
            metrics,
            accumulator: Mutex::new(AdaptiveBatchAccumulator::new(
                storage_config.batching.clone(),
            )),
            flush_notify: Arc::new(Notify::new()),
            writer_notify: Arc::new(Notify::new()),
            #[cfg(test)]
            supervisor_checks: AtomicU64::new(0),
            transaction_barrier: Arc::new(RwLock::new(())),
            publish_barrier: Arc::new(RwLock::new(())),
            max_offset: AtomicU64::new(0),
            checkpoint_path: storage_config.wal.log_dir.join("checkpoint.json"),
            progress: Arc::new(WritePathProgress::new()),
            bounds: LivenessBounds::from_max_flush_ms(storage_config.batching.max_flush_ms),
            writer: OnceLock::new(),
        });

        let adapter = Self {
            inner: inner.clone(),
        };

        // Rebuild index from WAL
        info!("Rebuilding index from WAL...");
        let start = std::time::Instant::now();
        adapter.rebuild_index_async().await?;
        info!("Index rebuild complete in {:?}", start.elapsed());

        // Spawn the background flush task and its supervisor.
        Self::spawn_writer(&inner);

        Ok(adapter)
    }

    /// Flush all data to disk
    pub async fn flush(&self) -> Result<(), StorageError> {
        #[cfg(test)]
        if fault_injection::flush_should_fail(&self.inner._config.wal.log_dir) {
            return Err(StorageError::Internal(format!(
                "injected flush failure at {}",
                self.inner._config.wal.log_dir.display()
            )));
        }

        // Flush accumulator
        Self::flush_accumulator_inner(&self.inner).await;

        // Flush WAL
        self.inner
            .wal
            .flush()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    fn flush_on_last_handle_drop(inner: Arc<WalStorageInner>) {
        let flush_future = async move {
            Self::flush_accumulator_inner(&inner).await;

            if let Err(error) = inner.wal.flush().await {
                warn!("Failed to flush WAL during adapter drop: {}", error);
            }

            if let Err(error) = inner.wal.sync().await {
                warn!("Failed to sync WAL during adapter drop: {}", error);
            }
        };

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        match runtime {
            Ok(runtime) => {
                if std::thread::Builder::new()
                    .name("prkdb-wal-drop-flush".to_string())
                    .spawn(move || runtime.block_on(flush_future))
                    .and_then(|handle| handle.join().map_err(|_| std::io::ErrorKind::Other.into()))
                    .is_err()
                {
                    warn!("Failed to complete WAL drop flush thread");
                }
            }
            Err(error) => warn!("Failed to create runtime for WAL drop flush: {}", error),
        }
    }

    /// Append a single Raft entry  to the WAL
    ///
    /// Returns the offset of the appended entry
    pub async fn append_raft_entry(&self, data: &[u8]) -> Result<u64, StorageError> {
        // We use a special collection name for Raft entries to avoid conflict
        let record = LogRecord::new(LogOperation::Put {
            collection: "__raft_log".to_string(),
            id: uuid::Uuid::new_v4().as_bytes().to_vec(), // Unique ID for each entry
            data: data.to_vec(),
        });

        let rx = self.enqueue_write(record).await?;
        Self::await_write(rx, self.inner.bounds.client_bound).await
    }

    /// Append multiple Raft entries in a single batch (PERFORMANCE OPTIMIZED)
    ///
    /// This is **much more efficient** than calling `append_raft_entry` multiple times
    /// as it batches all entries into a single WAL write operation.
    ///
    /// # Performance
    /// - Reduces WAL syscalls by 100-1000x
    /// - Expected 5-10x improvement in Raft proposal throughput
    /// - Critical for cluster write performance
    ///
    /// # Returns
    /// Vector of offsets for each entry (in same order as input)
    pub async fn append_raft_entries_batch(
        &self,
        entries: &[Vec<u8>],
    ) -> Result<Vec<u64>, StorageError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let records: Vec<LogRecord> = entries
            .iter()
            .map(|data| {
                LogRecord::new(LogOperation::Put {
                    collection: "__raft_log".to_string(),
                    id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                    data: data.clone(),
                })
            })
            .collect();

        // One lock acquisition and one notification for the whole batch, and all-or-nothing
        // against the queue ceiling: a partial accept would hand this caller an error with
        // some of its entries already on their way to the log.
        let receivers = self.enqueue_writes(records).await?;

        let bound = self.inner.bounds.client_bound;
        let mut offsets = Vec::with_capacity(entries.len());
        for rx in receivers {
            offsets.push(Self::await_write(rx, bound).await?);
        }

        Ok(offsets)
    }

    /// Rebuild the in-memory index from WAL (recovery)
    ///
    /// Note: Currently disabled for MmapParallelWal as it doesn't expose offset iteration.
    /// Index will be rebuilt from application put/delete operations after restart.
    /// Index will be rebuilt from application put/delete operations after restart.
    async fn rebuild_index_async(&self) -> Result<(), StorageError> {
        // Phase 9: Load checkpoint for incremental recovery
        let checkpoint = match checkpoint::load_checkpoint(&self.inner.checkpoint_path) {
            Ok(Some(cp)) => {
                info!(
                    "Loaded checkpoint: {} segments, max_offset={}, using scan_from for incremental recovery",
                    cp.segment_offsets.len(),
                    cp.max_offset
                );
                Some(cp)
            }
            Ok(None) => {
                info!("No checkpoint found, performing full WAL scan");
                None
            }
            Err(e) => {
                warn!(
                    "Failed to load checkpoint ({}), falling back to full scan",
                    e
                );
                None
            }
        };

        // Use scan_from if checkpoint exists, otherwise full scan
        let records = if let Some(ref cp) = checkpoint {
            let segment_count = self.inner.wal.segment_count();
            let start_offsets = cp.to_start_offsets(segment_count);
            self.inner
                .wal
                .scan_from(&start_offsets)
                .await
                .map_err(|e| StorageError::Internal(format!("scan_from error: {}", e)))?
        } else {
            self.inner
                .wal
                .scan()
                .await
                .map_err(|e| StorageError::Internal(format!("Scan error: {}", e)))?
        };

        let pinned = self.inner.index.pin();

        for (_segment_id, record) in records {
            // Recovered records advance the high-water mark; without this a snapshot taken
            // after a reopen would claim max_offset=0.
            self.inner
                .max_offset
                .fetch_max(record.offset, Ordering::SeqCst);

            // record.offset is already the global offset (including segment_id)
            match record.operation {
                LogOperation::Put { id, .. } => {
                    pinned.insert(id, record.offset);
                }
                LogOperation::PutBatch { items, .. } => {
                    for (id, _) in items {
                        pinned.insert(id, record.offset);
                    }
                }
                LogOperation::CompressedPutBatch { data, .. } => {
                    // Decompress to get IDs for index
                    if let Ok(decompressed) =
                        prkdb_core::wal::compression::decompress(&data, record.compression)
                    {
                        let config = bincode::config::standard();
                        if let Ok((items, _)) = bincode::decode_from_slice::<
                            Vec<(Vec<u8>, Vec<u8>)>,
                            _,
                        >(&decompressed, config)
                        {
                            for (id, _) in items {
                                pinned.insert(id, record.offset);
                            }
                        }
                    }
                }
                LogOperation::Delete { id, .. } => {
                    pinned.remove(&id);
                }
                LogOperation::DeleteBatch { ids, .. } => {
                    for id in ids {
                        pinned.remove(&id);
                    }
                }
                LogOperation::CompressedDeleteBatch { data, .. } => {
                    // Decompress to get IDs
                    if let Ok(decompressed) =
                        prkdb_core::wal::compression::decompress(&data, record.compression)
                    {
                        let config = bincode::config::standard();
                        if let Ok((ids, _)) =
                            bincode::decode_from_slice::<Vec<Vec<u8>>, _>(&decompressed, config)
                        {
                            for id in ids {
                                pinned.remove(&id);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
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
        // Optimization: Collect keys without unnecessary intermediate clones
        pinned.iter().map(|(key, _)| key.to_vec()).collect()
    }

    /// Read several keys as of a single instant.
    ///
    /// # Why `get_many` is not this
    ///
    /// `get_many` is a throughput optimisation: it batches cache and WAL lookups but takes
    /// no barrier, so a batch write landing midway through is visible to some of its keys
    /// and not others. Calling `get` twice has the same problem. Reading `a` and `b` while
    /// another task commits `{a, b}` as one batch could therefore observe the old `a` with
    /// the new `b` — the money-disappears symptom that spec S-03 was filed for.
    ///
    /// This holds the publication barrier for the whole read, so every key is observed on
    /// the same side of every batch commit.
    ///
    /// # What this is not
    ///
    /// Not a transaction and not MVCC. It excludes *publication*, not writing: a batch may
    /// be appended to the WAL during the read and become visible immediately after. It
    /// gives an atomic read, not a repeatable one. For read-modify-write, use a
    /// `Serializable` transaction.
    pub async fn snapshot_get_many(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
        let _publish = self.inner.publish_barrier.read().await;
        self.get_many(keys).await
    }

    /// Highest WAL offset written by this adapter.
    ///
    /// Exposed so a wrapper holding several adapters can record the maximum across all of
    /// them in a merged snapshot header.
    pub fn max_offset(&self) -> u64 {
        self.inner.max_offset.load(Ordering::SeqCst)
    }

    /// Get the log directory path
    pub fn get_log_dir(&self) -> PathBuf {
        self.inner._config.wal.log_dir.clone()
    }

    /// Save checkpoint for faster recovery on next startup
    ///
    /// Call this before graceful shutdown to ensure the next startup
    /// can use incremental recovery via `scan_from`.
    pub fn save_checkpoint(&self) -> Result<(), StorageError> {
        let current_offset = self.inner.max_offset.load(Ordering::Relaxed);
        let segment_count = self.inner.wal.segment_count();
        let cp = checkpoint::Checkpoint::from_segment_count(segment_count, current_offset);
        checkpoint::save_checkpoint(&self.inner.checkpoint_path, &cp)
            .map_err(|e| StorageError::Internal(format!("Failed to save checkpoint: {}", e)))?;
        info!("Checkpoint saved with max_offset={}", current_offset);
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
        let max_offset = self.inner.max_offset.load(Ordering::SeqCst);
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

        // Iterate keys and send to writer
        for key in keys {
            // We use get() which reads from WAL/Cache
            // Note: This might see updates > max_offset if they happened after we loaded max_offset
            // This is acceptable as replay will handle them idempotently.
            if let Some(val) = self.get(&key).await? {
                if tx.send((key, val)).await.is_err() {
                    return Err(StorageError::Internal(
                        "Snapshot writer task failed".to_string(),
                    ));
                }
            }
        }
        drop(tx); // Signal completion

        // Wait for writer to finish
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

    /// Start the flush loop and the task that supervises it, retaining both handles.
    ///
    /// Replaces four identical copies of a `tokio::spawn` whose `JoinHandle` was dropped on
    /// the spot. Keeping it is Part 1 of the liveness spec: the writer's exit — clean,
    /// panicking, or cancelled — is now something the process can observe rather than
    /// something callers infer from never being answered.
    fn spawn_writer(inner: &Arc<WalStorageInner>) {
        #[cfg(test)]
        if fault_injection::writer_should_not_start(&inner._config.wal.log_dir) {
            return;
        }

        let weak_for_loop = Arc::downgrade(inner);
        let flush_loop = tokio::spawn(async move {
            Self::run_flush_loop(weak_for_loop).await;
        });
        let flush_abort = flush_loop.abort_handle();

        let weak_for_supervisor = Arc::downgrade(inner);
        let supervisor = tokio::spawn(async move {
            Self::run_writer_supervisor(weak_for_supervisor, flush_loop).await;
        });

        // `set` fails only if called twice, which would mean two writers for one adapter.
        // Nothing does that; the handles from a second one would simply be dropped, which
        // is the pre-existing behaviour rather than a new failure.
        let _ = inner.writer.set(WriterTasks {
            flush_loop: flush_abort,
            supervisor: supervisor.abort_handle(),
        });
    }

    /// Watch the flush loop, and watch the queue it is supposed to be draining.
    ///
    /// Two failure modes, and the second is why the first is not sufficient:
    ///
    /// - **The task ends** — panic, early return, cancellation, runtime shutdown. Observed
    ///   by `await`ing its `JoinHandle` (Part 1).
    /// - **The task is alive and looping but publishes nothing.** Its `JoinHandle` never
    ///   resolves, so nothing about the task reveals this. Only the queue does — hence the
    ///   progress accounting (Part 2). This is the failure cargo-mutants reproduced by
    ///   replacing `flush_accumulator_inner` with `()`, and the one that made the whole
    ///   workspace suite hang for 300s instead of failing.
    ///
    /// Holds only a `Weak`, and upgrades for the duration of a single check. Holding a
    /// strong reference would keep the adapter alive forever and suppress the flush its
    /// `Drop` performs.
    async fn run_writer_supervisor(
        weak_inner: Weak<WalStorageInner>,
        mut flush_loop: JoinHandle<()>,
    ) {
        let bounds = match weak_inner.upgrade() {
            Some(inner) => inner.bounds,
            None => return,
        };

        let writer_notify = match weak_inner.upgrade() {
            Some(inner) => inner.writer_notify.clone(),
            None => return,
        };

        let mut last_published = 0u64;
        // How long to wait before looking again, or `None` to wait for a write instead.
        //
        // There is nothing to watch while the queue is empty: a stall is a queue that
        // stops draining, and an empty queue is not draining because there is nothing in
        // it. Waking on a timer to observe that costs one wakeup per interval per
        // collection adapter, forever, to learn nothing — fifty collections is fifty
        // wakeups a second against an idle database. So the watchdog sleeps until a write
        // makes the queue non-empty, and only then starts checking on a timer.
        let mut wait = None;

        loop {
            tokio::select! {
                joined = &mut flush_loop => {
                    let cause = match joined {
                        Ok(()) => "returned".to_string(),
                        Err(error) if error.is_cancelled() => "cancelled".to_string(),
                        Err(error) if error.is_panic() => {
                            format!("panicked: {}", Self::panic_message(error.into_panic()))
                        }
                        Err(error) => format!("join failed: {error}"),
                    };

                    // No upgrade means the adapter is already gone, so the writer exiting
                    // is the ordinary end of its life and there is nobody left to tell.
                    if let Some(inner) = weak_inner.upgrade() {
                        Self::fail_write_path(&inner, WriterFailure::Exited(cause)).await;
                    }
                    return;
                }
                // `Notify` holds a permit when nobody is waiting, so a write that lands
                // between the check below and this await is not lost — it returns
                // immediately rather than sleeping until the next one.
                _ = async {
                    match wait {
                        Some(interval) => tokio::time::sleep(interval).await,
                        None => writer_notify.notified().await,
                    }
                } => {}
            }

            let Some(inner) = weak_inner.upgrade() else {
                return;
            };
            Self::observe_write_path(&inner, &mut last_published).await;

            // Watch on a timer only while there is something queued to watch.
            wait = (inner.progress.queue_depth() > 0).then(|| bounds.active_tick());
        }
    }

    /// Render whatever a panicking task carried into something loggable.
    ///
    /// `JoinError::into_panic` hands back the `Box<dyn Any>` from `panic!`, which is a
    /// `&str` for a literal message and a `String` for a formatted one. Anything else is a
    /// custom payload; naming it as unknown beats dropping the fact that a panic happened.
    fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
        if let Some(message) = payload.downcast_ref::<&'static str>() {
            return (*message).to_string();
        }
        if let Some(message) = payload.downcast_ref::<String>() {
            return message.clone();
        }
        "unknown panic payload".to_string()
    }

    /// One watchdog check: publish the write-path gauges, then decide whether the writer is
    /// stalled.
    ///
    /// Metrics are updated on every check, healthy or not. A dashboard that only receives
    /// numbers during an incident cannot show what normal looked like, which is most of
    /// what makes the numbers worth having. The watchdog no longer runs while the queue is
    /// empty, so the gauges stop being rewritten then — they hold their last values, which
    /// for an idle write path are the correct ones and are not changing.
    ///
    /// No longer returns a poll interval. It used to choose between an active and an idle
    /// tick, and the caller now waits on a write instead of a timer whenever the queue is
    /// empty, so there is no idle interval left to pick. Mutation run 31573318483 found
    /// three survivors on that comparison; the branch they lived on is gone rather than
    /// tested, which is the stronger outcome.
    async fn observe_write_path(inner: &Arc<WalStorageInner>, last_published: &mut u64) {
        #[cfg(test)]
        inner.supervisor_checks.fetch_add(1, Ordering::Relaxed);

        let progress = &inner.progress;
        let published = progress.published_total();
        let depth = progress.queue_depth();
        let oldest = progress.oldest_unpublished_age();

        inner.metrics.set_write_queue(
            depth,
            oldest
                .map(|age| age.as_millis().min(u128::from(u64::MAX)) as u64)
                .unwrap_or(0),
        );

        // Both conditions from the spec: no progress, and something old waiting. Stating
        // the "no progress" half explicitly is what makes this a progress check rather than
        // a latency check, and the difference matters for a queue that is deep because it
        // is busy.
        //
        // A third clause, `depth > 0`, used to lead this and has been removed. It could
        // never change the answer: `resolve` stores 0 into the oldest-unpublished clock
        // whenever the queue empties, so `oldest` is already `None` for an empty queue and
        // `is_some_and` is already false. Mutation run 31566656408 found it — `> with >=`
        // survived (on a u64 that comparison is always true) while `> with ==` and
        // `> with <` were caught, which is the signature of a guard that matters in one
        // direction only.
        //
        // Deleted rather than excluded as an equivalent mutant. The invariant it restated
        // is enforced in `resolve`, and one place enforcing it is better than two that can
        // drift apart.
        let stalled = published == *last_published
            && oldest.is_some_and(|age| age >= inner.bounds.stall_threshold);

        *last_published = published;

        if !stalled {
            return;
        }

        let failure = WriterFailure::Stalled {
            queue_depth: depth,
            oldest_age_ms: oldest
                .map(|age| age.as_millis().min(u128::from(u64::MAX)) as u64)
                .unwrap_or(0),
            threshold_ms: inner
                .bounds
                .stall_threshold
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
        };
        Self::fail_write_path(inner, failure).await;
    }

    /// Move the write path to a failed state and hand every waiter behind it an answer.
    ///
    /// The two halves are separate on purpose. Marking the state is idempotent and happens
    /// once; discharging runs every time, because a stalled writer keeps accepting new
    /// writes (see `WritePathProgress::refuse_if_failed` for why refusing them would be
    /// worse) and each watchdog tick must clear whatever has accumulated since the last.
    async fn fail_write_path(inner: &WalStorageInner, failure: WriterFailure) {
        if inner.progress.fail(failure.clone()) {
            warn!("{failure}");
            inner.metrics.set_writer_healthy(false);
            inner.metrics.record_error();
        }
        if matches!(failure, WriterFailure::Stalled { .. }) {
            inner.metrics.record_writer_stall();
        }

        let discharged = Self::discharge_pending(inner, failure.to_error()).await;
        if let Some(report) = Self::discharge_report(discharged, &failure) {
            warn!("{report}");
        }
    }

    /// What to log about a discharge, or `None` when there is nothing worth saying.
    ///
    /// Returning the line rather than logging it inline is what makes the condition
    /// testable: `fail_write_path` runs on every watchdog tick for as long as a stall
    /// lasts, so a stall that has already handed back everything it had would otherwise
    /// repeat "Discharged 0" at the tick rate and bury the one line that named the cause.
    /// That is a real requirement with no observable other than the log itself, and a
    /// condition whose only effect is a side effect is a condition no test can pin —
    /// mutation run 31575909551 missed `> with ==` here for exactly that reason.
    fn discharge_report(discharged: u64, failure: &WriterFailure) -> Option<String> {
        (discharged > 0).then(|| {
            format!(
                "Discharged {} unpublished write(s) with a not-confirmed result: {}",
                discharged, failure
            )
        })
    }

    /// Hand every write still sitting in the accumulator its result, and report how many.
    ///
    /// This is the step that makes the whole thing observable from outside. The senders
    /// live inside the accumulator until someone takes the batch out; if nothing ever does,
    /// they sit there alive and unfired and the caller waits forever.
    async fn discharge_pending(inner: &WalStorageInner, error: StorageError) -> u64 {
        let pending = {
            let mut acc = inner.accumulator.lock().await;
            acc.flush()
        };

        let count = pending.len() as u64;
        for write in pending {
            let (_record, tx) = write.into_parts();
            if let Some(tx) = tx {
                let _ = tx.send(Err(error.clone()));
            }
        }

        // Resolved, but explicitly not *published*: giving up on a write is not evidence
        // the writer recovered, and must not clear the unhealthy state.
        inner.progress.record_discharged(count);
        count
    }

    /// Queue one write for the flush loop, or decline to.
    ///
    /// The single place the adapter takes on a write obligation, and therefore the only
    /// place that can decline one. Two reasons to decline:
    ///
    /// - the writer has **exited**, so nothing will ever drain the queue,
    /// - the queue is **at capacity**, so buffering more would turn a stall into an OOM
    ///   (Part 4). Without this, Parts 1–3 report the stall accurately right up until the
    ///   process is killed for memory.
    async fn enqueue_write(
        &self,
        record: LogRecord,
    ) -> Result<oneshot::Receiver<Result<u64, StorageError>>, StorageError> {
        let mut rxs = self.enqueue_writes(vec![record]).await?;
        Ok(rxs.pop().expect("one record in, one receiver out"))
    }

    /// Queue a whole batch, all or nothing.
    ///
    /// All-or-nothing because the alternative — accept what fits, refuse the rest — returns
    /// an error to a caller some of whose writes are on their way to the log. There is no
    /// honest thing such a caller can do with that.
    async fn enqueue_writes(
        &self,
        records: Vec<LogRecord>,
    ) -> Result<Vec<oneshot::Receiver<Result<u64, StorageError>>>, StorageError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        if let Some(error) = self.inner.progress.refuse_if_failed() {
            return Err(error);
        }

        // No writer was ever started, so nothing will ever drain this queue. Queueing the
        // write would be making a promise in the knowledge that it cannot be kept — the
        // same reasoning `refuse_if_failed` uses for a writer that has exited, applied to
        // one that never began.
        //
        // Unreachable by configuration: every constructor calls `spawn_writer` before it
        // hands the adapter back. It is reachable by *defect*, and the cost of not
        // checking is severe rather than merely wrong — each caller waits out its full
        // client bound instead of being told, so a workload of a few hundred writes takes
        // hours to fail. cargo-mutants demonstrated exactly that by deleting
        // `spawn_writer`: the mutation run timed out at 600s having reported nothing.
        if self.inner.writer.get().is_none() {
            return Err(StorageError::WriteAbandoned(
                "the WAL writer was never started, so nothing can drain the write queue"
                    .to_string(),
            ));
        }

        let wanted = records.len();
        let mut receivers = Vec::with_capacity(wanted);

        let started_the_queue = {
            let mut acc = self.inner.accumulator.lock().await;

            if acc.len() + wanted > acc.max_pending() {
                return Err(StorageError::WriteBackpressure(format!(
                    "WAL write queue is at capacity ({} of {} slots used, {} requested); \
                     the writer is not draining fast enough",
                    acc.len(),
                    acc.max_pending(),
                    wanted
                )));
            }

            for record in records {
                let (pending, rx) = PendingWrite::new(record);
                // Cannot fail: the capacity check above reserved room for the whole batch,
                // and the accumulator lock has not been released since.
                if acc.try_add(pending).is_err() {
                    unreachable!("capacity was checked while holding the accumulator lock");
                }
                receivers.push(rx);
            }

            // Counted while the lock is held, so the flush loop — which needs the same lock
            // to take the batch — cannot publish a write before it has been counted as
            // enqueued and make the queue momentarily look empty.
            self.inner.progress.record_enqueued(wanted as u64)
        };

        self.inner.flush_notify.notify_one();

        // Only when this batch is what made the queue non-empty, and only *here* — after
        // the accumulator lock has been released and the writes are counted and visible.
        //
        // The order is the whole correctness argument. Waking the watchdog before the
        // write is visible lets it look, find an empty queue, and go back to sleep with a
        // write sitting behind it and nothing scheduled to look again: the original
        // hang this work exists to fix, wearing a different hat, and invisible to every
        // test that is not watching for exactly it.
        if started_the_queue {
            self.inner.writer_notify.notify_one();
        }

        Ok(receivers)
    }

    /// Wait for a queued write's result, bounded (Part 3).
    ///
    /// # Why there is a bound at all, and why it is not the fix
    ///
    /// Parts 1 and 2 discharge these receivers with a named cause, and under the failure
    /// this spec is about they do so long before this expires. What the bound covers is the
    /// gap the supervisor cannot reach: a batch already taken out of the accumulator, whose
    /// senders the writer alone holds. A blanket timeout here *as* the fix was considered
    /// and rejected — it changes the durability contract to make a CI signal pass.
    ///
    /// # Why the error is never "failed"
    ///
    /// The write is with the writer and may be published a moment after this returns. A
    /// caller told "failed" will retry, and a retry of a write that later commits is a
    /// double write — silent data corruption, which is strictly worse than the hang it
    /// replaces. So the answer is [`StorageError::WriteNotConfirmed`], whose documentation
    /// says exactly that.
    async fn await_write(
        rx: oneshot::Receiver<Result<u64, StorageError>>,
        bound: Duration,
    ) -> Result<u64, StorageError> {
        match tokio::time::timeout(bound, rx).await {
            Ok(Ok(result)) => result,
            // The sender was dropped without sending. Unreachable while `PendingWrite`
            // carries its drop guard — which is the point of the guard — and kept as the
            // belt to that braces.
            Ok(Err(_)) => Err(StorageError::WriteNotConfirmed(
                "the queued write was dropped before its outcome was known".to_string(),
            )),
            Err(_) => Err(StorageError::WriteNotConfirmed(format!(
                "no result from the WAL writer within {}ms",
                bound.as_millis()
            ))),
        }
    }

    /// State of the write path, for health and readiness probes.
    ///
    /// Synchronous and lock-free apart from one uncontended read lock. A probe that can
    /// block turns a stalled writer into a stalled health check, and an orchestrator that
    /// times out on a probe restarts the node — the one action guaranteed to lose the
    /// queued writes this is reporting on.
    pub fn write_path_health(&self) -> WritePathHealth {
        self.inner.progress.health()
    }

    /// Background task to flush accumulator
    async fn run_flush_loop(weak_inner: Weak<WalStorageInner>) {
        // We need the notify to wait on. We can get it from the inner if it's alive.

        let flush_notify = if let Some(inner) = weak_inner.upgrade() {
            inner.flush_notify.clone()
        } else {
            return;
        };

        loop {
            // 1. Check if we should stop and determine wait time
            let flush_interval = {
                if let Some(inner) = weak_inner.upgrade() {
                    let acc = inner.accumulator.lock().await;
                    if acc.is_empty() {
                        Duration::from_secs(1) // Wake up occasionally to check compaction
                    } else {
                        Duration::from_millis(2) // Optimized from 10ms for +48% throughput
                    }
                } else {
                    break;
                }
            };

            tokio::select! {
                _ = flush_notify.notified() => {
                    // Flush triggered
                }
                _ = tokio::time::sleep(flush_interval) => {
                    // Timeout triggered
                }
            }

            if let Some(inner) = weak_inner.upgrade() {
                #[cfg(test)]
                if fault_injection::writer_should_panic(&inner._config.wal.log_dir) {
                    panic!("injected writer panic");
                }

                Self::flush_accumulator_inner(&inner).await;

                // Update cache size metrics periodically
                let cache_size = inner.cache.estimate_size_bytes().await;
                inner.metrics.set_cache_size_bytes(cache_size);

                // Try compaction
                if let Some(compactor) = &inner.compactor {
                    // Use tracked max_offset for compaction decisions
                    let current_offset = inner.max_offset.load(Ordering::Relaxed);
                    match compactor.run_if_needed(current_offset).await {
                        Ok(true) => {
                            // Compaction ran, record metrics
                            inner.metrics.record_compaction_cycle();

                            // Phase 9: Save checkpoint after successful compaction
                            let segment_count = inner.wal.segment_count();
                            let cp = checkpoint::Checkpoint::from_segment_count(
                                segment_count,
                                current_offset,
                            );
                            if let Err(e) = checkpoint::save_checkpoint(&inner.checkpoint_path, &cp)
                            {
                                warn!("Failed to save checkpoint: {}", e);
                            } else {
                                tracing::debug!(
                                    "Checkpoint saved with max_offset={}",
                                    current_offset
                                );
                            }
                        }
                        Ok(false) => {
                            // No compaction needed
                        }
                        Err(_) => {
                            // Compaction failed, record error
                            inner.metrics.record_error();
                        }
                    }
                }
            } else {
                break;
            }
        }
    }

    /// Flush the accumulator to WAL (static helper for inner)
    ///
    /// # Why the accounting lives here and not inside `publish_batch`
    ///
    /// Everything this takes out of the accumulator is now the writer's obligation, and it
    /// is discharged one way or another before this returns: sent a result, sent an error,
    /// or dropped — and dropping a `PendingWrite` sends a result too. Recording the count
    /// here, on every path out, is what keeps `queue_depth` honest. Leaving any of it
    /// uncounted would park the depth above zero forever and have the watchdog report a
    /// stall on a writer that is working perfectly.
    async fn flush_accumulator_inner(inner: &WalStorageInner) {
        // Simulates a flush loop that is alive but no longer publishing — the failure this
        // whole mechanism exists for, and the one no other seam can produce.
        #[cfg(test)]
        if fault_injection::writer_should_stall(&inner._config.wal.log_dir) {
            return;
        }

        let batch = {
            let mut acc = inner.accumulator.lock().await;
            acc.flush()
        };

        if batch.is_empty() {
            return;
        }
        let taken = batch.len() as u64;

        let published = Self::publish_batch(inner, batch).await;

        inner.progress.record_published(taken);
        if published > 0 {
            inner
                .metrics
                .record_writer_publish(published, unix_millis());
        }
    }

    /// Write one drained batch to the WAL, update the index, and answer its waiters.
    ///
    /// Returns how many writes actually reached the log, which is zero if the append
    /// failed. The caller distinguishes that from "how many were taken", because for
    /// liveness they are both progress and for the publish-rate gauge only the first is.
    async fn publish_batch(inner: &WalStorageInner, batch: Vec<PendingWrite>) -> u64 {
        let _transaction_barrier = inner.transaction_barrier.read().await;

        // Group by collection to create batches
        // We use a preserved order map or just iterate and group?
        // Since we want to batch per collection, let's group by collection first.
        // Order between collections doesn't matter strictly for WAL append (they are concurrent).
        // Order WITHIN collection MATTERS.

        // (Record, Waiters, Optional IDs for index update)
        type BatchGroup = (
            LogRecord,
            Vec<Option<oneshot::Sender<Result<u64, StorageError>>>>,
            Option<Vec<Vec<u8>>>,
        );
        let mut batched_writes: Vec<BatchGroup> = Vec::new();

        // Simple grouping strategy:
        // 1. Separate by collection
        // 2. For each collection, coalesce consecutive Puts/Deletes

        let mut collection_map: std::collections::HashMap<String, Vec<PendingWrite>> =
            std::collections::HashMap::new();

        for write in batch {
            let collection = match write.operation() {
                LogOperation::Put { collection, .. } => collection.clone(),
                LogOperation::Delete { collection, .. } => collection.clone(),
                _ => String::new(), // Should not happen with current put/delete impl
            };

            collection_map.entry(collection).or_default().push(write);
        }

        let compression_config = &inner._config.wal.compression;

        for (collection, writes) in collection_map {
            // Pre-allocate with estimated capacity for better performance
            let mut current_puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(1024); // Hot path: batch accumulation
            let mut current_put_ids: Vec<Vec<u8>> = Vec::with_capacity(1024);
            let mut current_put_txs = Vec::with_capacity(1024);

            let mut current_deletes: Vec<Vec<u8>> = Vec::with_capacity(256);
            let mut current_delete_txs = Vec::with_capacity(256);

            for write in writes {
                // Takes the record and the sender out together, disarming the drop guard:
                // from here on this batch's answers are `publish_batch`'s responsibility.
                let (record, write_tx) = write.into_parts();
                match record.operation {
                    LogOperation::Put { id, data, .. } => {
                        // Flush pending deletes if any
                        if !current_deletes.is_empty() {
                            let op = LogOperation::DeleteBatch {
                                collection: collection.clone(),
                                ids: std::mem::take(&mut current_deletes),
                            };
                            let ids_for_index = if let LogOperation::DeleteBatch { ids, .. } = &op {
                                Some(ids.clone())
                            } else {
                                None
                            };

                            if let Ok(record) =
                                LogRecord::new_with_compression(op, compression_config)
                            {
                                batched_writes.push((
                                    record,
                                    std::mem::take(&mut current_delete_txs),
                                    ids_for_index,
                                ));
                            }
                        }
                        current_put_ids.push(id.clone());
                        current_puts.push((id, data));
                        current_put_txs.push(write_tx);
                    }
                    LogOperation::Delete { id, .. } => {
                        // Flush pending puts if any
                        if !current_puts.is_empty() {
                            let op = LogOperation::PutBatch {
                                collection: collection.clone(),
                                items: std::mem::take(&mut current_puts),
                            };

                            let ids_for_index = Some(std::mem::take(&mut current_put_ids));

                            if let Ok(record) =
                                LogRecord::new_with_compression(op, compression_config)
                            {
                                batched_writes.push((
                                    record,
                                    std::mem::take(&mut current_put_txs),
                                    ids_for_index,
                                ));
                            }
                        }
                        current_deletes.push(id);
                        current_delete_txs.push(write_tx);
                    }
                    // Listed rather than wildcarded. The accumulator only ever holds
                    // single Put and Delete records — `put_many` and the raft appends build
                    // them inline, and compression is applied later when the batch record is
                    // assembled — so these four cannot occur here today.
                    //
                    // Spelling them out costs nothing and means a seventh variant stops the
                    // build instead of being dropped. `_ => {}` in a sibling match is what
                    // let `scan_prefix` skip every compressed record silently; the wildcard,
                    // not the missing arm, is what made it invisible.
                    LogOperation::PutBatch { .. }
                    | LogOperation::CompressedPutBatch { .. }
                    | LogOperation::DeleteBatch { .. }
                    | LogOperation::CompressedDeleteBatch { .. } => {}
                }
            }

            // Flush remaining
            if !current_puts.is_empty() {
                let op = LogOperation::PutBatch {
                    collection: collection.clone(),
                    items: current_puts,
                };
                let ids_for_index = Some(current_put_ids);
                if let Ok(record) = LogRecord::new_with_compression(op, compression_config) {
                    batched_writes.push((record, current_put_txs, ids_for_index));
                }
            }
            if !current_deletes.is_empty() {
                // Clone IDs for index update before consuming in op
                let ids_for_index = Some(current_deletes.clone());
                let op = LogOperation::DeleteBatch {
                    collection: collection.clone(),
                    ids: current_deletes,
                };
                if let Ok(record) = LogRecord::new_with_compression(op, compression_config) {
                    batched_writes.push((record, current_delete_txs, ids_for_index));
                }
            }
        }

        if batched_writes.is_empty() {
            // Every sender was dropped with the groups above, so every caller has already
            // been answered by the drop guard. Reachable when `new_with_compression` fails
            // for a record, which is why the guard exists rather than an `expect`.
            return 0;
        }

        let (records, rest): (Vec<_>, Vec<_>) = batched_writes
            .into_iter()
            .map(|(r, t, i)| (r, (t, i)))
            .unzip();
        let (tx_groups, id_groups): (Vec<_>, Vec<_>) = rest.into_iter().unzip();

        // Write to WAL (async, returns Vec<(segment_id, offset)>)
        //
        // Simulates the append failing under the writer — a full disk, a failing fsync.
        // The batch has already left the accumulator at this point, so this is the only
        // seam that reaches "taken but not published".
        #[cfg(test)]
        let appended = if fault_injection::append_should_fail(&inner._config.wal.log_dir) {
            Err(prkdb_core::wal::WalError::Io(std::io::Error::other(
                "fault injection: WAL append failed",
            )))
        } else {
            inner.wal.append_batch(records.clone()).await
        };
        #[cfg(not(test))]
        let appended = inner.wal.append_batch(records.clone()).await;

        match appended {
            Ok(locations) => {
                // Pin the index for the duration of the batch update
                // Note: Guard is not Send, so we must drop it before any await point
                {
                    let pinned = inner.index.pin();
                    for (i, record) in records.iter().enumerate() {
                        let (_segment_id, offset) = locations[i];
                        let _ = &tx_groups[i];
                        let ids_opt = &id_groups[i];

                        // Update index based on the BATCH operation
                        match &record.operation {
                            LogOperation::PutBatch { items, .. } => {
                                for (id, _) in items {
                                    pinned.insert(id.clone(), offset);
                                }
                            }
                            LogOperation::CompressedPutBatch { .. } => {
                                // Use preserved IDs
                                if let Some(ids) = ids_opt {
                                    for id in ids {
                                        pinned.insert(id.clone(), offset);
                                    }
                                }
                            }
                            LogOperation::DeleteBatch { ids, .. } => {
                                for id in ids {
                                    tracing::debug!(
                                        "Removing key from index: {:?}",
                                        String::from_utf8_lossy(id)
                                    );
                                    pinned.remove(id);
                                }
                            }
                            LogOperation::CompressedDeleteBatch { .. } => {
                                // Use preserved IDs
                                if let Some(ids) = ids_opt {
                                    for id in ids {
                                        pinned.remove(id);
                                    }
                                }
                            }
                            // Single Put and Delete are published by the batch record that
                            // carries them, so there is nothing to do for them here. Named
                            // rather than wildcarded, for the reason above.
                            LogOperation::Put { .. } | LogOperation::Delete { .. } => {}
                        }
                    }
                }

                // Get last location for replication
                let last_location = if let Some(&(_seg_id, off)) = locations.last() {
                    off
                } else {
                    0
                };

                // Replicate
                if let Some(replication) = &inner.replication {
                    let mut mgr = replication.lock().await;
                    if let Err(e) = mgr.replicate_batch(records, last_location).await {
                        tracing::error!("Replication failed: {}", e);
                    }
                }

                // Notify waiters with encoded location
                let mut published = 0u64;
                for (tx, (_seg_id, off)) in tx_groups.into_iter().zip(locations.iter()) {
                    for sender in tx.into_iter().flatten() {
                        let _ = sender.send(Ok(*off));
                        published += 1;
                    }
                }
                published
            }
            Err(e) => {
                for tx_group in tx_groups {
                    for tx in tx_group.into_iter().flatten() {
                        let _ = tx.send(Err(StorageError::Internal(e.to_string())));
                    }
                }
                // Nothing reached the log, so nothing counts towards the publish rate. The
                // callers were still answered, which is what `flush_accumulator_inner`
                // records separately — a writer that returns errors is broken, but it is
                // not stalled, and reporting it as stalled would name the wrong problem.
                0
            }
        }
    }
}

#[async_trait::async_trait]
impl StorageAdapter for WalStorageAdapter {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        // 1. Check cache first
        {
            // Check sharded cache directly (async)
            if let Some(value) = self.inner.cache.get(&key.to_vec()).await {
                // Record cache hit
                self.inner.metrics.record_cache_hit();
                // Record read metrics (app level)
                self.inner
                    .metrics
                    .record_read((key.len() + value.len()) as u64);

                return Ok(Some(value));
            }
            // Record cache miss
            self.inner.metrics.record_cache_miss();
        }

        // 2. Lookup in index
        let offset = {
            let pinned = self.inner.index.pin();
            match pinned.get(key) {
                Some(offset) => {
                    tracing::debug!(
                        "Index hit for key {:?}: offset {}",
                        String::from_utf8_lossy(key),
                        offset
                    );
                    *offset
                }
                None => {
                    tracing::debug!("Index miss for key {:?}", String::from_utf8_lossy(key));
                    return Ok(None);
                }
            }
        };

        // 3. Read from WAL (async)
        // offset is global, contains segment_id
        let record = self
            .inner
            .wal
            .read(offset)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // 4. Extract value
        let value_opt = match record.operation {
            LogOperation::Put { id, data, .. } if id == key => Some(data),
            LogOperation::PutBatch { items, .. } => {
                // Find matching item in batch
                for (item_id, data) in items {
                    if item_id == key {
                        // Record read metrics
                        self.inner
                            .metrics
                            .record_read((key.len() + data.len()) as u64);

                        // 5. Populate cache on successful read
                        // Update sharded cache (async)
                        self.inner.cache.put(key.to_vec(), data.clone()).await;
                        return Ok(Some(data));
                    }
                }
                None
            }
            LogOperation::CompressedPutBatch { data, .. } => {
                // Decompress and find
                if let Ok(decompressed) =
                    prkdb_core::wal::compression::decompress(&data, record.compression)
                {
                    let config = bincode::config::standard();
                    if let Ok((items, _)) = bincode::decode_from_slice::<Vec<(Vec<u8>, Vec<u8>)>, _>(
                        &decompressed,
                        config,
                    ) {
                        for (item_id, item_data) in items {
                            if item_id == key {
                                self.inner
                                    .metrics
                                    .record_read((key.len() + item_data.len()) as u64);
                                // Update sharded cache (async)
                                self.inner.cache.put(key.to_vec(), item_data.clone()).await;
                                return Ok(Some(item_data));
                            }
                        }
                    }
                }
                None
            }
            _ => None,
        };

        // 6. Populate cache and return
        if let Some(ref value) = value_opt {
            // Record read metrics
            self.inner
                .metrics
                .record_read((key.len() + value.len()) as u64);

            // Optimization: Already have value, cache it efficiently
            self.inner.cache.put(key.to_vec(), value.clone()).await;
        }

        Ok(value_opt)
    }

    /// Put a key-value pair
    ///
    /// FIX: Direct WAL write path - bypasses accumulator flush loop deadlock
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let _guard = self.inner.transaction_barrier.read().await;
        self.put_batch_impl(vec![(key.to_vec(), value.to_vec())])
            .await
    }

    /// Put multiple key-value pairs in a single batch operation
    ///
    /// Direct WAL batch path, bypassing the accumulator.
    ///
    /// This bypasses the accumulator completely:
    /// - Single WAL batch write (not N individual writes)
    /// - Direct offset tracking (not via channels)
    /// - Bulk index update (not N sequential)
    /// - Bulk cache update (single lock)
    ///
    /// The "62K → 3M+ ops/sec" this used to advertise is unverified: no benchmark in the
    /// repository measures the two paths against each other. See
    /// `docs/benchmarks/methodology.md`.
    async fn put_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), StorageError> {
        let _guard = self.inner.transaction_barrier.read().await;
        self.put_batch_impl(entries).await
    }

    /// Put multiple key-value pairs
    async fn put_many(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), StorageError> {
        let _guard = self.inner.transaction_barrier.read().await;
        // Calculate total bytes for metrics
        let total_bytes: u64 = items.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();

        // Record batch write metrics
        self.inner
            .metrics
            .record_write_batch(items.len() as u64, total_bytes);

        let records: Vec<LogRecord> = items
            .into_iter()
            .map(|(key, value)| {
                LogRecord::new(LogOperation::Put {
                    collection: String::new(),
                    id: key,
                    data: value,
                })
            })
            .collect();

        let rxs = self.enqueue_writes(records).await?;

        let bound = self.inner.bounds.client_bound;
        for rx in rxs {
            Self::await_write(rx, bound).await?;
        }
        Ok(())
    }

    /// Delete a key
    async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        let _guard = self.inner.transaction_barrier.read().await;
        self.delete_many_impl(vec![key.to_vec()]).await
    }

    async fn flush(&self) -> Result<(), StorageError> {
        WalStorageAdapter::flush(self).await
    }

    /// Retrieve multiple records by key
    ///
    /// Uses a **hybrid strategy** for optimal performance:
    /// - **< 100 keys**: Uses index lookups + parallel WAL reads (efficient for small batches)
    /// - **≥ 100 keys**: Uses a single full WAL scan + filter (efficient for large batches)
    ///
    /// # Performance
    ///
    /// - **Small batches**: ~2,000 ops/sec (latency optimized)
    /// - **Large batches**: ~28,000 ops/sec (throughput optimized)
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
        use std::collections::HashMap;

        // OPTIMIZATION: Parallel cache lookup for all keys
        // This is ~10x faster than sequential await per key
        let cache = self.inner.cache.clone();
        let cache_futures: Vec<_> = keys
            .iter()
            .enumerate()
            .map(|(idx, key)| {
                let cache = cache.clone();
                let key = key.clone();
                async move { (idx, cache.get(&key).await) }
            })
            .collect();

        let cache_results = futures::future::join_all(cache_futures).await;

        let mut results: Vec<Option<Vec<u8>>> = vec![None; keys.len()];
        let mut cache_misses: Vec<(usize, Vec<u8>)> = Vec::new();

        for (idx, value_opt) in cache_results {
            if let Some(value) = value_opt {
                self.inner.metrics.record_cache_hit();
                results[idx] = Some(value);
            } else {
                self.inner.metrics.record_cache_miss();
                cache_misses.push((idx, keys[idx].clone()));
            }
        }

        // If all hits in cache, return immediately
        if cache_misses.is_empty() {
            return Ok(results);
        }

        // Only fetch cache misses from WAL
        let miss_keys: Vec<Vec<u8>> = cache_misses.iter().map(|(_, k)| k.clone()).collect();

        // Use optimized WAL lookup for remaining keys
        if miss_keys.len() > 100 {
            // Scan-based approach for large batches
            let records = self
                .inner
                .wal
                .scan()
                .await
                .map_err(|e| StorageError::Internal(format!("Scan error: {}", e)))?;

            // Build map of latest values for all records
            let mut latest_values: HashMap<Vec<u8>, Option<Vec<u8>>> =
                HashMap::with_capacity(miss_keys.len());

            for (_segment_id, record) in records {
                match record.operation {
                    LogOperation::Put { id, data, .. } => {
                        latest_values.insert(id, Some(data));
                    }
                    LogOperation::PutBatch { items, .. } => {
                        for (id, data) in items {
                            latest_values.insert(id, Some(data));
                        }
                    }
                    LogOperation::CompressedPutBatch { data, .. } => {
                        if let Ok(decompressed) =
                            prkdb_core::wal::compression::decompress(&data, record.compression)
                        {
                            let config = bincode::config::standard();
                            if let Ok((items, _)) =
                                bincode::decode_from_slice::<Vec<(Vec<u8>, Vec<u8>)>, _>(
                                    &decompressed,
                                    config,
                                )
                            {
                                for (id, item_data) in items {
                                    latest_values.insert(id, Some(item_data));
                                }
                            }
                        }
                    }
                    LogOperation::Delete { id, .. } => {
                        latest_values.insert(id, None);
                    }
                    LogOperation::DeleteBatch { ids, .. } => {
                        for id in ids {
                            latest_values.insert(id, None);
                        }
                    }
                    LogOperation::CompressedDeleteBatch { data, .. } => {
                        if let Ok(decompressed) =
                            prkdb_core::wal::compression::decompress(&data, record.compression)
                        {
                            let config = bincode::config::standard();
                            if let Ok((ids, _)) =
                                bincode::decode_from_slice::<Vec<Vec<u8>>, _>(&decompressed, config)
                            {
                                for id in ids {
                                    latest_values.insert(id, None);
                                }
                            }
                        }
                    }
                }
            }

            // Merge WAL results back into main results using cache_misses indices
            for (idx, key) in &cache_misses {
                if let Some(value_opt) = latest_values.get(key) {
                    results[*idx] = value_opt.clone();
                    // Populate cache for future reads
                    if let Some(value) = value_opt {
                        self.inner.cache.put(key.clone(), value.clone()).await;
                    }
                }
            }

            Ok(results)
        } else {
            // Index-based approach for small batches (more efficient for < 100 keys)
            let mut read_futures = Vec::new();

            for key in &keys {
                let pinned = self.inner.index.pin();
                if let Some(offset) = pinned.get(key) {
                    let wal = self.inner.wal.clone();
                    let offset = *offset;
                    let key_clone = key.clone();

                    read_futures.push(async move {
                        match wal.read(offset).await {
                            Ok(record) => {
                                let value = match record.operation {
                                    LogOperation::Put { id, data, .. } if id == key_clone => {
                                        Some(data)
                                    }
                                    LogOperation::PutBatch { items, .. } => items
                                        .into_iter()
                                        .find(|(item_id, _)| item_id == &key_clone)
                                        .map(|(_, data)| data),
                                    LogOperation::CompressedPutBatch { data, .. } => {
                                        if let Ok(decompressed) =
                                            prkdb_core::wal::compression::decompress(
                                                &data,
                                                record.compression,
                                            )
                                        {
                                            let config = bincode::config::standard();
                                            if let Ok((items, _)) = bincode::decode_from_slice::<
                                                Vec<(Vec<u8>, Vec<u8>)>,
                                                _,
                                            >(
                                                &decompressed, config
                                            ) {
                                                items
                                                    .into_iter()
                                                    .find(|(item_id, _)| item_id == &key_clone)
                                                    .map(|(_, data)| data)
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    }
                                    _ => None,
                                };
                                (key_clone, value)
                            }
                            Err(_) => (key_clone, None),
                        }
                    });
                }
            }

            let all_results = futures::future::join_all(read_futures).await;
            let mut key_to_value: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
            for (key, value_opt) in all_results {
                if let Some(value) = value_opt {
                    key_to_value.insert(key, value);
                }
            }

            Ok(keys
                .iter()
                .map(|key| key_to_value.get(key).cloned())
                .collect())
        }
    }

    /// Optimized bulk delete using direct WAL batch write
    ///
    /// Performance improvement similar to put_batch:
    /// - Old (accumulator): Individual delete records - slow
    /// - New (direct WAL): Single batch operation - fast!
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
            // Optimization: Convert to owned types efficiently
            .map(|(key, value)| (key.to_string(), value.to_vec()))
            .collect())
    }

    async fn outbox_remove(&self, id: &str) -> Result<(), StorageError> {
        self.inner.outbox.pin().remove(id);
        Ok(())
    }

    /// Scan for records with a specific prefix
    ///
    /// Uses **WAL scan** instead of index iteration for 10x performance improvement.
    ///
    /// # Performance
    ///
    /// - **Throughput**: ~28,000 ops/sec
    /// - **Efficiency**: Sequential I/O, no random index lookups
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        use std::collections::HashMap;

        // Use WAL scan for direct, sequential access - much faster than index + individual reads
        let records = self
            .inner
            .wal
            .scan()
            .await
            .map_err(|e| StorageError::Internal(format!("Scan error: {}", e)))?;

        // Build map of latest values (later records override earlier ones)
        // Pre-allocate with reasonable estimate for prefix scan results
        let mut latest_values: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::with_capacity(256);

        for (_segment_id, record) in records {
            // Exhaustive on purpose: no `_` arm. The compressed variants were missing here
            // while `get` handled them, and a wildcard is what let that pass review — a
            // record type nobody wrote an arm for was silently skipped. With every variant
            // named, adding a seventh stops compiling until this scan decides what it means.
            match record.operation {
                LogOperation::Put { id, data, .. } => {
                    if id.starts_with(prefix) {
                        latest_values.insert(id, Some(data));
                    }
                }
                LogOperation::PutBatch { items, .. } => {
                    for (id, data) in items {
                        if id.starts_with(prefix) {
                            latest_values.insert(id, Some(data));
                        }
                    }
                }
                LogOperation::CompressedPutBatch { data, .. } => {
                    // Same decompress-then-decode as `get`. `WalConfig::default()` enables
                    // LZ4, so on a default database *every* batched put lands here.
                    if let Ok(decompressed) =
                        prkdb_core::wal::compression::decompress(&data, record.compression)
                    {
                        let config = bincode::config::standard();
                        if let Ok((items, _)) = bincode::decode_from_slice::<
                            Vec<(Vec<u8>, Vec<u8>)>,
                            _,
                        >(&decompressed, config)
                        {
                            for (id, item_data) in items {
                                if id.starts_with(prefix) {
                                    latest_values.insert(id, Some(item_data));
                                }
                            }
                        }
                    }
                }
                LogOperation::Delete { id, .. } => {
                    if id.starts_with(prefix) {
                        latest_values.insert(id, None); // Mark as deleted
                    }
                }
                LogOperation::DeleteBatch { ids, .. } => {
                    for id in ids {
                        if id.starts_with(prefix) {
                            latest_values.insert(id, None);
                        }
                    }
                }
                LogOperation::CompressedDeleteBatch { data, .. } => {
                    if let Ok(decompressed) =
                        prkdb_core::wal::compression::decompress(&data, record.compression)
                    {
                        let config = bincode::config::standard();
                        if let Ok((ids, _)) =
                            bincode::decode_from_slice::<Vec<Vec<u8>>, _>(&decompressed, config)
                        {
                            for id in ids {
                                if id.starts_with(prefix) {
                                    latest_values.insert(id, None);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Filter out deleted items and collect results
        let mut results: Vec<_> = latest_values
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect();

        results.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(results)
    }

    async fn get_changes_since(&self, offset: u64) -> Result<Vec<Change>, StorageError> {
        // Use optimized scan_from to avoid scanning entire WAL
        // We pass offset+1 to start offsets array since scan_from returns >= start_offset
        let start_offsets: Vec<u64> = vec![offset + 1; self.inner.wal.segment_count()];
        let records = self
            .inner
            .wal
            .scan_from(&start_offsets)
            .await
            .map_err(|e| StorageError::Internal(format!("Scan error: {}", e)))?;

        let mut changes = Vec::new();

        for (_segment_id, record) in records {
            // scan_from already filters to records >= offset+1, so all are valid
            match record.operation {
                LogOperation::Put { id, data, .. } => {
                    changes.push(Change::Put {
                        key: id,
                        value: data,
                        version: record.offset,
                    });
                }
                LogOperation::PutBatch { items, .. } => {
                    // Expand batch into individual changes
                    // Note: All items in batch share the same record offset/version
                    // This is acceptable for replication as long as order is preserved
                    for (id, data) in items {
                        changes.push(Change::Put {
                            key: id,
                            value: data,
                            version: record.offset,
                        });
                    }
                }
                LogOperation::CompressedPutBatch { data, .. } => {
                    if let Ok(decompressed) =
                        prkdb_core::wal::compression::decompress(&data, record.compression)
                    {
                        let config = bincode::config::standard();
                        if let Ok((items, _)) = bincode::decode_from_slice::<
                            Vec<(Vec<u8>, Vec<u8>)>,
                            _,
                        >(&decompressed, config)
                        {
                            for (id, data) in items {
                                changes.push(Change::Put {
                                    key: id,
                                    value: data,
                                    version: record.offset,
                                });
                            }
                        }
                    }
                }
                LogOperation::Delete { id, .. } => {
                    changes.push(Change::Delete {
                        key: id,
                        version: record.offset,
                    });
                }
                LogOperation::DeleteBatch { ids, .. } => {
                    for id in ids {
                        changes.push(Change::Delete {
                            key: id,
                            version: record.offset,
                        });
                    }
                }
                LogOperation::CompressedDeleteBatch { data, .. } => {
                    if let Ok(decompressed) =
                        prkdb_core::wal::compression::decompress(&data, record.compression)
                    {
                        let config = bincode::config::standard();
                        if let Ok((ids, _)) =
                            bincode::decode_from_slice::<Vec<Vec<u8>>, _>(&decompressed, config)
                        {
                            for id in ids {
                                changes.push(Change::Delete {
                                    key: id,
                                    version: record.offset,
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(changes)
    }

    async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let mut results = Vec::new();

        // Use pin_owned() to get a Send guard for async iteration
        let pinned = self.inner.index.pin_owned();
        for (key, _offset) in pinned.iter() {
            if key.as_slice() >= start && key.as_slice() < end {
                let key_clone = key.clone();
                if let Some(value) = self.get(&key_clone).await? {
                    results.push((key_clone, value));
                }
            }
        }

        results.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(results)
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
    use std::env;
    use std::fs;
    use std::time::Instant;

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
            // Explicitly flush to ensure persistence
            adapter.flush().await.unwrap();
            // Wait for background flush (just in case)
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
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
        let scanned = adapter.inner.wal.scan().await.unwrap();
        assert_eq!(scanned.len(), 10, "reopened WAL should expose all records");

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
        let scanned = adapter.inner.wal.scan().await.unwrap();
        assert_eq!(scanned.len(), 10, "reopened WAL should expose all records");

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

        // Wait for background flush
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

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

        // Wait for background flush
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

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
    async fn test_wal_adapter_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let adapter = WalStorageAdapter::new(config).unwrap();

        // Write enough data to trigger potential compaction logic
        // (Note: In this test environment, we rely on the stub implementation,
        // so we are mainly testing that the integration doesn't panic and the loop runs)
        for i in 0..100 {
            adapter
                .put(
                    format!("key{}", i).as_bytes(),
                    format!("value{}", i).as_bytes(),
                )
                .await
                .unwrap();
        }

        // Wait for background flush and compaction check
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify data is still readable
        let val = adapter.get(b"key0").await.unwrap();
        assert_eq!(val, Some(b"value0".to_vec()));
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

        // Wait for background flush
        tokio::time::sleep(Duration::from_millis(100)).await;

        // First get - should populate cache
        let val1 = adapter.get(b"cached_key").await.unwrap();
        assert_eq!(val1, Some(b"cached_value".to_vec()));

        // Second get - should hit cache (faster)
        let val2 = adapter.get(b"cached_key").await.unwrap();
        assert_eq!(val2, Some(b"cached_value".to_vec()));

        // Delete - should invalidate cache
        adapter.delete(b"cached_key").await.unwrap();

        // Wait for background flush
        tokio::time::sleep(Duration::from_millis(100)).await;

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
        tokio::time::sleep(Duration::from_millis(100)).await;

        let metrics = adapter.metrics();
        // The adapter tracks its own metrics - check that write was recorded
        assert!(
            metrics.writes_total >= 1,
            "Expected at least 1 write, got {}",
            metrics.writes_total
        );
        assert_eq!(metrics.write_bytes_total, 10); // "key1" (4) + "value1" (6) = 10

        // Test cache HIT on first read (Write-Through Caching)
        // Since put() populates the cache, the first get() should be a hit
        let _ = adapter.get(b"key1").await.unwrap();
        let metrics = adapter.metrics();
        println!("DEBUG Metrics: {:?}", metrics);
        // Writes populate cache, so this should be a HIT, not a MISS
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
        // ...
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
        tokio::time::sleep(Duration::from_millis(100)).await;

        let metrics = adapter.metrics();
        assert_eq!(metrics.write_batches_total, 1);
        assert!(metrics.writes_total >= 3); // At least 3 writes total

        // Test delete metrics
        adapter.delete(b"key1").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let metrics = adapter.metrics();
        assert!(metrics.writes_total >= 4); // Delete is also a write

        // Test cache size tracking
        // Cache size should be updated periodically by the flush loop
        tokio::time::sleep(Duration::from_millis(200)).await;
        let metrics = adapter.metrics();
        // Cache size should be tracked (u64 is always >= 0, so we just verify it's accessible)
        let _ = metrics.cache_size_bytes;

        // Test that getting a non-existent key records a cache miss
        let _ = adapter.get(b"nonexistent").await.unwrap();
        let metrics = adapter.metrics();
        assert_eq!(metrics.cache_misses, 1); // One miss for non-existent key
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wal_adapter_auto_recovery_on_startup() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        // 1. Create and write data
        {
            let adapter = WalStorageAdapter::new(config.clone()).unwrap();
            adapter.put(b"key1", b"value1").await.unwrap();
            adapter.put(b"key2", b"value2").await.unwrap(); // Write more data
                                                            // Wait for flush
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // 2. Corrupt the file manually (corrupt key2's record)
        let mut corrupted = false;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir()
                && path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("mmap_segment_")
            {
                for seg_entry in std::fs::read_dir(path).unwrap() {
                    let seg_entry = seg_entry.unwrap();
                    let seg_path = seg_entry.path();
                    if seg_path.extension().is_some_and(|ext| ext == "log") {
                        use std::io::{Read, Seek, Write};
                        let mut file = std::fs::OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(seg_path)
                            .unwrap();
                        let mut buf = Vec::new();
                        file.read_to_end(&mut buf).unwrap();

                        // Find "value2" and corrupt it
                        if let Some(pos) = buf.windows(6).position(|w| w == b"value2") {
                            file.seek(std::io::SeekFrom::Start(pos as u64)).unwrap();
                            file.write_all(b"corrup").unwrap();
                            corrupted = true;
                        }
                    }
                }
            }
        }

        assert!(corrupted, "Failed to find and corrupt data");

        // 3. Open - should succeed but truncate corrupted record
        let adapter =
            WalStorageAdapter::open(config).expect("Open should succeed with auto-recovery");

        // 4. Verify key1 exists, key2 is gone
        assert_eq!(
            adapter.get(b"key1").await.unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            adapter.get(b"key2").await.unwrap(),
            None,
            "Corrupted record should be truncated"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wal_adapter_runtime_corruption_detection() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let adapter = WalStorageAdapter::new(config.clone()).unwrap();
        adapter.put(b"key1", b"value1").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 2. Corrupt the file manually WHILE OPEN
        let mut corrupted = false;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir()
                && path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("mmap_segment_")
            {
                for seg_entry in std::fs::read_dir(path).unwrap() {
                    let seg_entry = seg_entry.unwrap();
                    let seg_path = seg_entry.path();
                    if seg_path.extension().is_some_and(|ext| ext == "log") {
                        use std::io::{Read, Seek, Write};
                        let mut file = std::fs::OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(seg_path)
                            .unwrap();
                        let mut buf = Vec::new();
                        file.read_to_end(&mut buf).unwrap();

                        if let Some(pos) = buf.windows(6).position(|w| w == b"value1") {
                            file.seek(std::io::SeekFrom::Start(pos as u64)).unwrap();
                            file.write_all(b"corrup").unwrap();
                            file.sync_all().unwrap(); // Ensure written to disk
                            corrupted = true;
                        }
                    }
                }
            }
        }
        assert!(corrupted, "Failed to corrupt data");

        // 3. Check health - should detect corruption
        // Note: mmap updates might take a moment or require OS sync, but usually immediate for local files
        let result = adapter.recovery().check_health().await;

        match result {
            Ok(_) => panic!("Health check should fail due to corruption"),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("Checksum mismatch") || msg.contains("Corruption"),
                    "Unexpected error: {}",
                    msg
                );
            }
        }
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

        // Wait for background tasks (if any)
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check metrics - cache size should be limited (approx 5)
        // Note: LruCache might not be strictly hard-limited immediately depending on implementation,
        // but let's verify we can use the adapter and config is set.
        let _metrics = adapter.metrics();

        // Verify we can read back
        let val = adapter.get(b"key9").await.unwrap();
        assert_eq!(val, Some(b"value9".to_vec()));

        // Verify earlier keys might be evicted (optional, depends on LRU behavior)
        // For now, just ensuring builder works and adapter is functional is enough.
    }

    // ------------------------------------------------------------------------------
    // WAL writer liveness — docs/superpowers/specs/2026-08-11-wal-writer-liveness.md
    //
    // Every test below observes the write path from the *caller's* side, because that is
    // where the defect lived: a queued write is a promise, and the failure mode was that
    // nothing kept it and nothing said so. The property is liveness — "the write is
    // eventually published" — which has no non-temporal observation, so each of these
    // bounds the wait and asserts the caller was answered rather than left hanging.
    //
    // Every one of them hung indefinitely before this work.
    // ------------------------------------------------------------------------------

    /// With no writer, a write is refused rather than queued forever.
    ///
    /// `refuse_if_failed` already declines writes once the writer has *exited*, on the
    /// grounds that there is provably nobody left to drain the queue. A writer that never
    /// *started* is the same fact arriving earlier, and it was not covered.
    ///
    /// Not reachable by configuration — every constructor calls `spawn_writer` before
    /// handing the adapter back — but reachable by defect, and cargo-mutants showed what
    /// it costs: deleting `spawn_writer` left every write in the suite waiting out its
    /// full client bound, so the mutation run hit its 600s ceiling having reported
    /// nothing. A hang that expensive is not detection, it is a lost run.
    ///
    /// The bound below is the assertion. Six seconds is comfortably under the client bound
    /// this adapter would otherwise wait, so a refusal that is merely *slower* fails here
    /// too.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_write_is_refused_when_no_writer_was_started() {
        let dir = liveness_dir("no_writer");
        fault_injection::never_start_writer_at(&dir);

        let adapter = WalStorageAdapter::new(WalConfig {
            log_dir: dir.clone(),
            ..WalConfig::test_config()
        })
        .expect("the adapter still opens; only its writer is missing");

        let refused = tokio::time::timeout(
            Duration::from_secs(6),
            adapter.put_many(vec![(b"k".to_vec(), b"v".to_vec())]),
        )
        .await
        .expect("a write with no writer must be refused promptly, not left to wait out its bound");

        let error = refused.expect_err("a write nothing can publish must not report success");
        assert!(
            error.is_write_abandoned(),
            "the refusal must be definite: nothing was appended and nothing ever will be, \
             so a caller told 'not confirmed' would wrongly believe it might still land; \
             got: {error}"
        );

        fault_injection::clear_never_start_writer(&dir);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A stale index entry must not hand back another key's value.
    ///
    /// `get` resolves a key to an offset through the index, reads whatever record is at
    /// that offset, and only then checks that the record's own id matches the key it was
    /// asked for. The nightly sweep replaced that check with `true` and nothing noticed:
    /// in every other test the index is correct, so the check is redundant and its removal
    /// changes no answer.
    ///
    /// It is not redundant in the case it exists for. An index entry pointing at the wrong
    /// record — stale after compaction, or corrupted — turns `get(alpha)` into beta's
    /// value returned as alpha's. That is the silent wrong answer this file's whole
    /// premise is about, and without the id check the key is decoration: the read returns
    /// whatever the index happened to point at.
    ///
    /// Poking the index directly is the point rather than a shortcut. The guard's whole
    /// job is to be right when the index is wrong, and no amount of ordinary writing makes
    /// the index wrong.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_index_entry_does_not_return_another_keys_value() {
        let dir = liveness_dir("stale_index");
        let adapter = WalStorageAdapter::new(WalConfig {
            log_dir: dir.clone(),
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

        // Point `alpha` at `beta`'s record. This is what a stale entry looks like.
        let beta_offset = {
            let pinned = adapter.inner.index.pin();
            *pinned.get(&b"beta".to_vec()).expect("beta is indexed")
        };
        adapter
            .inner
            .index
            .pin()
            .insert(b"alpha".to_vec(), beta_offset);

        // The cache would answer from the earlier read and never reach the WAL.
        adapter.inner.cache.clear().await;

        assert_eq!(
            adapter
                .get(b"alpha")
                .await
                .expect("read alpha through the stale entry"),
            None,
            "a record whose id is not the requested key must not be returned as that \
             key's value; without the id check this is beta's value answering a read for \
             alpha"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// A config whose stall bound is short enough to observe inside a test.
    ///
    /// `max_flush_ms` is the knob the bounds derive from, so setting it here is the same
    /// lever an operator has — the test is not reaching past the mechanism to a private
    /// constant.
    fn liveness_config(dir: &Path, max_flush_ms: u64, max_pending: usize) -> StorageConfig {
        StorageConfig {
            wal: WalConfig {
                log_dir: dir.to_path_buf(),
                ..WalConfig::test_config()
            },
            batching: AdaptiveBatchConfig {
                max_flush_ms,
                max_pending,
                // The accumulator clamps `max_pending` up to `max_batch_size`, since a
                // ceiling below the batch size would make a full batch unreachable. Tests
                // that want a small queue must therefore say so on both, or the requested
                // ceiling is silently replaced by the 10K default.
                min_batch_size: 1,
                max_batch_size: max_pending,
                ..AdaptiveBatchConfig::default()
            },
            ..StorageConfig::default()
        }
    }

    fn liveness_dir(name: &str) -> PathBuf {
        let dir = env::temp_dir().join(format!("prkdb_liveness_{name}"));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    /// Poll until `check` holds, or fail naming what was being waited for. Used instead of
    /// a fixed sleep so a slow machine takes longer rather than failing.
    async fn wait_until(what: &str, limit: Duration, mut check: impl FnMut() -> bool) {
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("timed out after {limit:?} waiting for {what}");
    }

    /// The drop guard, in isolation. This is what makes the `oneshot canceled` handler
    /// reachable for the first time: before it, a queued write destroyed without a result
    /// closed its channel silently and no path in the program guaranteed even that.
    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_a_queued_write_answers_its_caller() {
        let (pending, rx) = PendingWrite::new(LogRecord::new(LogOperation::Delete {
            collection: String::new(),
            id: b"k".to_vec(),
        }));

        drop(pending);

        let result = rx
            .await
            .expect("the drop guard must send a result, not close the channel");
        let error = result.expect_err("a dropped write cannot report an offset");
        assert!(
            error.is_write_unconfirmed(),
            "a dropped write may already have reached the log, got: {error}"
        );
    }

    /// The other direction: a write the publisher has taken responsibility for must not
    /// also be answered by the guard, or every successful write would race its own
    /// destructor.
    #[tokio::test(flavor = "multi_thread")]
    async fn taking_a_write_apart_disarms_the_drop_guard() {
        let (pending, rx) = PendingWrite::new(LogRecord::new(LogOperation::Delete {
            collection: String::new(),
            id: b"k".to_vec(),
        }));

        let (_record, tx) = pending.into_parts();
        let tx = tx.expect("into_parts hands the sender to the publisher");
        drop(tx);

        assert!(
            rx.await.is_err(),
            "into_parts must transfer the obligation, not duplicate it"
        );
    }

    /// Acceptance 1: a writer task that panics discharges every pending waiter with an
    /// error naming the panic, and no caller blocks.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_panicking_writer_discharges_its_waiters_with_the_panic() {
        let dir = liveness_dir("writer_panic");
        let adapter = WalStorageAdapter::new_with_config(liveness_config(&dir, 25, 65_536))
            .expect("adapter opens");

        fault_injection::panic_writer_at(&dir);

        let started = Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            adapter.put_many(vec![(b"k".to_vec(), b"v".to_vec())]),
        )
        .await;
        fault_injection::clear_writer_panic(&dir);

        let result = outcome.expect("the caller must be answered, not left waiting");
        let error = result.expect_err("a panicking writer cannot have published the write");

        assert!(
            error.is_write_abandoned(),
            "this write was still in the accumulator when the panic was noticed, so it was \
             never handed to the writer and provably did not land — abandoned, not merely \
             unconfirmed. A write the writer had already taken goes out through \
             PendingWrite's drop guard instead, which does say not-confirmed because a \
             panic partway through publication may well have appended it; got: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("panicked") && message.contains("injected writer panic"),
            "the error must name the panic that caused it, got: {message}"
        );

        let health = adapter.write_path_health();
        assert!(!health.healthy);
        assert!(health
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("panicked")));

        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the waiter was discharged by supervision, not by a timeout"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Acceptance 2, and the part that catches the actual defect: a writer that is alive
    /// and looping but publishes nothing.
    ///
    /// Part 1 cannot substitute for this. Under this failure the task is running normally
    /// and its `JoinHandle` never resolves — there is nothing wrong with the *task*. Only
    /// the queue shows it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_writer_that_publishes_nothing_is_detected_and_reported_unhealthy() {
        let dir = liveness_dir("writer_stall");
        // max_flush_ms 25 => stall threshold 400ms, watchdog tick 100ms, client bound 3.2s.
        let adapter = WalStorageAdapter::new_with_config(liveness_config(&dir, 25, 65_536))
            .expect("adapter opens");

        fault_injection::stall_writer_at(&dir);

        let started = Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(20),
            adapter.put_many(vec![(b"k".to_vec(), b"v".to_vec())]),
        )
        .await;
        let elapsed = started.elapsed();
        fault_injection::clear_writer_stall(&dir);

        let result = outcome.expect("the caller must be answered, not left waiting");
        let error = result.expect_err("a stalled writer published nothing");

        assert!(
            error.is_write_abandoned(),
            "the watchdog discharges what is still queued, and queued means never \
             appended; got: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("stalled") && message.contains("no publication progress"),
            "the watchdog, not the client's own bound, must be what answered this caller; \
             got: {message}"
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "detection must happen within a small multiple of the flush interval \
             (threshold 400ms), took {elapsed:?}"
        );

        let health = adapter.write_path_health();
        assert!(!health.healthy, "the health endpoint must report the stall");
        assert!(health
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("stalled")));
        assert_eq!(adapter.metrics().writer_stalls_total, 1);
        assert!(!adapter.metrics().writer_healthy);

        let _ = fs::remove_dir_all(&dir);
    }

    /// The other direction for the watchdog: a healthy adapter under the same bounds must
    /// not be reported as stalled. A detector that fires on everything would pass the test
    /// above while making the database unusable.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_working_writer_is_never_reported_as_stalled() {
        let dir = liveness_dir("writer_healthy");
        let adapter = WalStorageAdapter::new_with_config(liveness_config(&dir, 25, 65_536))
            .expect("adapter opens");

        // Several watchdog intervals' worth of ordinary writes.
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
        assert_eq!(adapter.metrics().writer_stalls_total, 0);
        assert!(adapter.metrics().writer_publishes_total >= 12);
        assert!(adapter.metrics().writer_last_publish_unix_ms.is_some());

        let _ = fs::remove_dir_all(&dir);
    }

    /// Acceptance 3, at the boundary that matters: the variant survives the trait object
    /// every caller in the codebase actually holds. `PrkDb` stores an
    /// `Arc<dyn StorageAdapter>`, so a variant that were flattened on the way through it
    /// would make the distinction unobservable however carefully it is defined.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_not_confirmed_variant_survives_the_storage_adapter_boundary() {
        let dir = liveness_dir("not_confirmed_boundary");
        let adapter = WalStorageAdapter::new_with_config(liveness_config(&dir, 25, 65_536))
            .expect("adapter opens");
        let storage: Arc<dyn StorageAdapter> = Arc::new(adapter);

        fault_injection::stall_writer_at(&dir);
        let result = tokio::time::timeout(
            Duration::from_secs(20),
            storage.put_many(vec![(b"k".to_vec(), b"v".to_vec())]),
        )
        .await
        .expect("the caller must be answered");
        fault_injection::clear_writer_stall(&dir);

        let error = result.expect_err("a stalled writer published nothing");
        assert!(
            matches!(error, StorageError::WriteAbandoned(_)),
            "the variant must arrive intact rather than as Internal; got: {error:?}"
        );
        assert!(!storage.write_path_health().healthy);

        let _ = fs::remove_dir_all(&dir);
    }

    /// Acceptance 4: with the queue at capacity and the writer stalled, new writes are
    /// refused rather than buffered, and memory stays bounded.
    ///
    /// The stall threshold is set far out of reach here on purpose. The watchdog would
    /// otherwise discharge the queue and empty it, and the thing under test is what happens
    /// while it is still full.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_full_queue_refuses_new_writes_instead_of_growing() {
        let dir = liveness_dir("backpressure");
        let capacity = 8;
        // max_flush_ms 5000 => stall threshold 80s, well beyond this test.
        let adapter = Arc::new(
            WalStorageAdapter::new_with_config(liveness_config(&dir, 5_000, capacity))
                .expect("adapter opens"),
        );

        fault_injection::stall_writer_at(&dir);

        let filler = {
            let adapter = adapter.clone();
            tokio::spawn(async move {
                let items: Vec<_> = (0..capacity)
                    .map(|i| (format!("k{i}").into_bytes(), b"v".to_vec()))
                    .collect();
                adapter.put_many(items).await
            })
        };

        let queue = adapter.clone();
        wait_until("the queue to fill", Duration::from_secs(10), move || {
            queue.write_path_health().queue_depth == capacity as u64
        })
        .await;

        // Repeated attempts must all be refused, and none of them may grow the queue.
        for attempt in 0..200 {
            let error = adapter
                .put_many(vec![(
                    format!("overflow{attempt}").into_bytes(),
                    b"v".to_vec(),
                )])
                .await
                .expect_err("the queue is full");
            assert!(
                matches!(error, StorageError::WriteBackpressure(_)),
                "a refused write must say so definitely, so retrying is safe; got: {error:?}"
            );
            assert_eq!(
                adapter.write_path_health().queue_depth,
                capacity as u64,
                "a refused write must not be buffered"
            );
        }

        // Both directions: the bulkhead lifts once the writer drains, so this is
        // backpressure and not a permanent wall.
        fault_injection::clear_writer_stall(&dir);
        let filled = tokio::time::timeout(Duration::from_secs(20), filler)
            .await
            .expect("the queued batch must resolve once the writer resumes")
            .expect("the filler task must not panic");
        assert!(filled.is_ok(), "queued writes should publish: {filled:?}");

        adapter
            .put_many(vec![(b"after".to_vec(), b"v".to_vec())])
            .await
            .expect("writes are accepted again once the queue drains");

        let _ = fs::remove_dir_all(&dir);
    }

    /// The observability the spec asks for, asserted where it is read rather than where it
    /// is written: queue depth, age of the oldest unpublished write, publish count and the
    /// timestamp of the last successful publish.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_write_path_publishes_the_numbers_that_show_a_stall_forming() {
        let dir = liveness_dir("observability");
        let adapter = Arc::new(
            WalStorageAdapter::new_with_config(liveness_config(&dir, 5_000, 65_536))
                .expect("adapter opens"),
        );

        fault_injection::stall_writer_at(&dir);

        let filler = {
            let adapter = adapter.clone();
            tokio::spawn(
                async move { adapter.put_many(vec![(b"k".to_vec(), b"v".to_vec())]).await },
            )
        };

        let probe = adapter.clone();
        wait_until(
            "the queue gauges to reflect the stalled write",
            Duration::from_secs(10),
            move || {
                let metrics = probe.metrics();
                metrics.write_queue_depth == 1 && metrics.write_queue_oldest_age_ms > 0
            },
        )
        .await;

        fault_injection::clear_writer_stall(&dir);
        tokio::time::timeout(Duration::from_secs(20), filler)
            .await
            .expect("the write resolves once the writer resumes")
            .expect("the filler task must not panic")
            .expect("the write publishes");

        let probe = adapter.clone();
        wait_until(
            "the gauges to return to idle after the publish",
            Duration::from_secs(10),
            move || probe.metrics().write_queue_depth == 0,
        )
        .await;

        let metrics = adapter.metrics();
        assert!(metrics.writer_publishes_total >= 1);
        assert!(metrics.writer_last_publish_unix_ms.is_some());
        assert!(metrics.writer_healthy);

        let _ = fs::remove_dir_all(&dir);
    }

    /// An idle adapter wakes its watchdog **not at all**.
    ///
    /// Acceptance 1 of the liveness spec. The watchdog used to sleep and re-check on a
    /// timer, so an idle collection cost a wakeup per interval — up to one a second — to
    /// observe a queue that was empty and would stay empty until someone wrote. One of
    /// these tasks exists per collection adapter, so fifty idle collections were fifty
    /// wakeups a second spent learning nothing.
    ///
    /// Asserted by counting rather than eyeballed, because "no periodic wakeups" is
    /// exactly the kind of claim that quietly stops being true.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_idle_watchdog_does_not_wake_at_all() {
        let dir = liveness_dir("idle_no_wakeups");
        // max_flush_ms 25 => the old idle tick was 400ms, so the window below would have
        // produced several wakeups under the polling design.
        let adapter = WalStorageAdapter::new_with_config(liveness_config(&dir, 25, 65_536))
            .expect("adapter opens");

        // Let the supervisor reach its first wait.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let settled = adapter.inner.supervisor_checks.load(Ordering::Relaxed);

        tokio::time::sleep(Duration::from_secs(2)).await;

        assert_eq!(
            adapter.inner.supervisor_checks.load(Ordering::Relaxed),
            settled,
            "an idle write path must cost no watchdog wakeups; two seconds at the old \
             400ms idle tick would have been about five"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// And it goes back to sleep once the queue drains.
    ///
    /// The other half of acceptance 1, and the half that actually pins the comparison.
    /// `an_idle_watchdog_does_not_wake_at_all` only shows the watchdog *starts* asleep:
    /// `wait` begins as `None`, so on an adapter that never sees a write the line choosing
    /// between waiting and polling is never reached at all. Mutation run on this branch
    /// caught that — `> with >=` survived, because nothing exercised the decision.
    ///
    /// A database that served one write and then went quiet must cost nothing again. With
    /// `>=` the watchdog would poll for the rest of the adapter's life after the very
    /// first write, which is the cost this whole change exists to remove.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_watchdog_returns_to_waiting_once_the_queue_drains() {
        let dir = liveness_dir("sleeps_again");
        // max_flush_ms 25 => active tick 100ms, so two seconds of polling would be ~20
        // observations. Anything above the handful this write itself causes is a failure.
        let adapter = WalStorageAdapter::new_with_config(liveness_config(&dir, 25, 65_536))
            .expect("adapter opens");

        adapter
            .put_many(vec![(b"k".to_vec(), b"v".to_vec())])
            .await
            .expect("the write is published");

        wait_until(
            "the queue to drain after the write",
            Duration::from_secs(10),
            || adapter.inner.progress.queue_depth() == 0,
        )
        .await;

        // Let the watchdog observe the now-empty queue and decide to wait.
        tokio::time::sleep(Duration::from_millis(400)).await;
        let settled = adapter.inner.supervisor_checks.load(Ordering::Relaxed);

        tokio::time::sleep(Duration::from_secs(2)).await;

        assert_eq!(
            adapter.inner.supervisor_checks.load(Ordering::Relaxed),
            settled,
            "once the queue drained the watchdog must wait for the next write, not keep \
             polling; a database that served one write and went quiet pays nothing"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// A write into an empty queue wakes the watchdog promptly.
    ///
    /// Acceptance 2, and the other half of the property above: an idle watchdog is only
    /// safe to have if a write starts it again. This is the case the ordering in
    /// `enqueue_writes` exists for — notify after the write is visible, never before, or
    /// the watchdog looks at an empty queue and goes back to sleep with work behind it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_write_into_an_empty_queue_wakes_the_watchdog() {
        let dir = liveness_dir("wake_on_write");
        let adapter = Arc::new(
            WalStorageAdapter::new_with_config(liveness_config(&dir, 25, 65_536))
                .expect("adapter opens"),
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
        let before = adapter.inner.supervisor_checks.load(Ordering::Relaxed);

        // Stalled, so the queue stays non-empty and the watchdog has something to watch.
        fault_injection::stall_writer_at(&dir);
        let queued = {
            let adapter = adapter.clone();
            tokio::spawn(
                async move { adapter.put_many(vec![(b"k".to_vec(), b"v".to_vec())]).await },
            )
        };

        wait_until(
            "the watchdog to observe the newly queued write",
            Duration::from_secs(5),
            || adapter.inner.supervisor_checks.load(Ordering::Relaxed) > before,
        )
        .await;

        fault_injection::clear_writer_stall(&dir);
        let _ = tokio::time::timeout(Duration::from_secs(20), queued).await;
        let _ = fs::remove_dir_all(&dir);
    }

    /// A batch that never reached the log is not a publish.
    ///
    /// Mutation run 31575909551 missed `> with >=` on the `published > 0` guard around
    /// `record_writer_publish`. That guard is not cosmetic: `record_writer_publish` stores
    /// `unix_millis()` into `writer_last_publish_unix_ms` unconditionally, so calling it
    /// with a count of zero moves the "last successful publish" clock forward on a batch
    /// that failed. A dashboard would then show a writer publishing normally while every
    /// caller behind it is receiving errors — the gauge answers "when did it last *try*"
    /// instead of "when did it last succeed", which is the one question it exists for.
    ///
    /// Nothing could see this before because no test could make the append fail;
    /// `fail_append_at` is that seam.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_append_does_not_count_as_a_publish() {
        let dir = liveness_dir("append_failure");
        let adapter = WalStorageAdapter::new_with_config(liveness_config(&dir, 5000, 65_536))
            .expect("adapter opens");

        assert!(
            adapter.metrics().writer_last_publish_unix_ms.is_none(),
            "a fresh adapter has not published anything yet"
        );

        fault_injection::fail_append_at(&dir);
        let refused = adapter.put_many(vec![(b"k".to_vec(), b"v".to_vec())]).await;
        assert!(
            refused.is_err(),
            "a write whose append failed must be reported to its caller, not swallowed"
        );

        // A caller's answer is sent from inside `publish_batch`, and the metrics are
        // recorded by `flush_accumulator_inner` after it returns — so `refused` arriving
        // proves nothing about what has been recorded yet. Reading here would race the
        // writer, and race in the mutant's favour: a mutant that does stamp the clock
        // stamps it a moment later, and an assertion that got there first would call that
        // a pass.
        //
        // A second failed write closes the gap without a sleep. The flush loop is one
        // task handling one batch at a time, and this write was not enqueued until the
        // first had been answered, so it is a separate cycle; receiving its answer
        // therefore happens after every store the first cycle made.
        let also_refused = adapter
            .put_many(vec![(b"k1".to_vec(), b"v1".to_vec())])
            .await;
        assert!(also_refused.is_err(), "the fault is still armed");

        fault_injection::clear_append_failure(&dir);

        let after_failure = adapter.metrics();
        assert_eq!(
            after_failure.writer_publishes_total, 0,
            "nothing reached the log, so nothing counts towards the publish total"
        );
        assert!(
            after_failure.writer_last_publish_unix_ms.is_none(),
            "a failed append must not advance the last-successful-publish clock, or a \
             broken writer reads as a healthy one"
        );

        // And the same gauge must still move when a write genuinely lands, or the
        // assertion above would be satisfied by a writer that never records anything.
        adapter
            .put_many(vec![(b"k2".to_vec(), b"v2".to_vec())])
            .await
            .expect("the append succeeds once the fault is cleared");

        // Same ordering problem in the other direction: the caller is answered before the
        // publish is recorded, so this waits for the record rather than assuming it.
        wait_until(
            "the successful publish to reach the metrics",
            Duration::from_secs(10),
            || adapter.metrics().writer_publishes_total == 1,
        )
        .await;

        assert!(
            adapter.metrics().writer_last_publish_unix_ms.is_some(),
            "a real publish must stamp the clock"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Discharging nothing says nothing; discharging something names the count and cause.
    ///
    /// Mutation run 31575909551 missed `> with ==` on what was then an inline
    /// `if discharged > 0` around a `warn!`. It survived because the condition's only
    /// effect was the log, and no test could see a log. `discharge_report` exists to give
    /// that decision a return value; this pins both of its answers.
    ///
    /// Not a correctness property — `record_discharged` runs either way, so the count
    /// reaches the metrics regardless. It is noise control, and it matters because
    /// `fail_write_path` runs once per watchdog tick for the whole duration of a stall.
    #[test]
    fn a_discharge_of_nothing_is_silent() {
        let failure = WriterFailure::Stalled {
            queue_depth: 7,
            oldest_age_ms: 900,
            threshold_ms: 400,
        };

        assert_eq!(
            WalStorageAdapter::discharge_report(0, &failure),
            None,
            "a stall that had nothing left to hand back must log nothing, or it repeats \
             itself once per tick until the writer recovers"
        );

        let report = WalStorageAdapter::discharge_report(3, &failure)
            .expect("a discharge that answered waiters is worth a line");
        assert!(
            report.contains("Discharged 3 unpublished write(s)"),
            "the line must name how many writes were given up on; got: {report}"
        );
        assert!(
            report.contains("stalled"),
            "and why, or an operator sees a count with no cause; got: {report}"
        );
    }

    /// Dropping an adapter stops its background tasks *promptly*, not eventually.
    ///
    /// Mutation run 31539366718 missed `replace <impl Drop for WalStorageInner>::drop with
    /// ()`. Deleting that body does not break correctness — both tasks hold a
    /// `Weak<WalStorageInner>` and exit on their own once `inner` is gone — so the mutant
    /// changes only *when* they stop. That is still worth asserting: without the abort a
    /// dropped adapter leaves a task parked on the flush loop's one-second idle sleep, and
    /// a process that opens and drops collections steadily accumulates them.
    ///
    /// Observed through `Arc::strong_count` on `flush_notify`, which `run_flush_loop`
    /// clones once and holds for its whole life, so the count falls exactly when the task
    /// ends. No production code exists for this test to look at, which is the evidence that
    /// `Drop` here is an optimisation rather than a load-bearing invariant.
    ///
    /// Twenty adapters rather than one, deliberately. A single task's remaining sleep is
    /// uniform in 0..1s, so one adapter would pass under the mutant whenever its sleep
    /// happened to be nearly over. The *maximum* across twenty sits near the full second,
    /// which puts the mutant reliably outside the bound below.
    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_an_adapter_stops_its_background_tasks_promptly() {
        let mut notifies = Vec::new();
        let mut dirs = Vec::new();

        for i in 0..20 {
            let dir = liveness_dir(&format!("drop_teardown_{i}"));
            let adapter = WalStorageAdapter::new_with_config(liveness_config(&dir, 50, 65_536))
                .expect("adapter opens");
            notifies.push(adapter.inner.flush_notify.clone());
            dirs.push(dir);
            drop(adapter);
        }

        // Each `Arc` is held by the test and, until the task ends, by the flush loop.
        wait_until(
            "every dropped adapter's flush loop to end",
            Duration::from_millis(300),
            || notifies.iter().all(|notify| Arc::strong_count(notify) == 1),
        )
        .await;

        for dir in &dirs {
            let _ = fs::remove_dir_all(dir);
        }
    }
}
