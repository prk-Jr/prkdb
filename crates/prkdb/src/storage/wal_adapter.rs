#[allow(unused_imports)]
use super::cache::LruCache;
use super::checkpoint;
use super::config::{StorageConfig, SyncMode};
use super::recovery::RecoveryManager;
use super::snapshot::SnapshotWriter;
use prkdb_types::snapshot::{CompressionType, SnapshotHeader};

use papaya::HashMap as LockFreeHashMap; // Phase 5: Lock-free index
use prkdb_core::batching::adaptive::{AdaptiveBatchAccumulator, AdaptiveBatchConfig};
use prkdb_core::replication::{Change, ReplicationManager};
use prkdb_core::wal::compaction::{CompactionConfig, Compactor};
use prkdb_core::wal::mmap_parallel_wal::MmapParallelWal;
use prkdb_core::wal::{LogOperation, LogRecord, WalConfig};
use prkdb_metrics::storage::StorageMetrics;
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{oneshot, Mutex, Notify, OwnedRwLockWriteGuard, RwLock};
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

    fn registry() -> &'static Mutex<HashSet<PathBuf>> {
        static REGISTRY: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
        REGISTRY.get_or_init(|| Mutex::new(HashSet::new()))
    }

    /// Make `flush` fail for the adapter whose WAL lives at `dir`.
    pub fn fail_flush_at(dir: impl Into<PathBuf>) {
        registry()
            .lock()
            .expect("fault registry lock")
            .insert(dir.into());
    }

    /// Stop failing `flush` for `dir`. Call it when the assertion is done, so a later
    /// flush in the same test — including one during teardown — is not affected.
    pub fn clear_flush_failure(dir: &Path) {
        registry().lock().expect("fault registry lock").remove(dir);
    }

    pub(super) fn flush_should_fail(dir: &Path) -> bool {
        registry()
            .lock()
            .expect("fault registry lock")
            .contains(dir)
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

struct PendingWrite {
    record: LogRecord,
    tx: oneshot::Sender<Result<u64, StorageError>>,
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
    // Phase 2: Writer thread handle (stored here for Drop)
}

impl Drop for WalStorageInner {
    fn drop(&mut self) {
        self.flush_notify.notify_one();
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
            transaction_barrier: Arc::new(RwLock::new(())),
            publish_barrier: Arc::new(RwLock::new(())),
            max_offset: AtomicU64::new(0),
            checkpoint_path: config.wal.log_dir.join("checkpoint.json"),
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

        // Spawn background flush task
        let weak_inner = Arc::downgrade(&inner);
        tokio::spawn(async move {
            Self::run_flush_loop(weak_inner).await;
        });

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
            transaction_barrier: Arc::new(RwLock::new(())),
            publish_barrier: Arc::new(RwLock::new(())),
            max_offset: AtomicU64::new(0),
            checkpoint_path: storage_config.wal.log_dir.join("checkpoint.json"),
        });

        let adapter = Self {
            inner: inner.clone(),
        };

        // Same index rebuild as every other open path.
        info!("Rebuilding index from WAL...");
        adapter.rebuild_index_async().await?;

        // Spawn background flush task
        let weak_inner = Arc::downgrade(&inner);
        tokio::spawn(async move {
            Self::run_flush_loop(weak_inner).await;
        });

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
            transaction_barrier: Arc::new(RwLock::new(())),
            publish_barrier: Arc::new(RwLock::new(())),
            max_offset: AtomicU64::new(0),
            checkpoint_path: storage_config.wal.log_dir.join("checkpoint.json"),
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

        // Spawn background flush task
        let weak_inner = Arc::downgrade(&inner);
        tokio::spawn(async move {
            Self::run_flush_loop(weak_inner).await;
        });

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
            transaction_barrier: Arc::new(RwLock::new(())),
            publish_barrier: Arc::new(RwLock::new(())),
            max_offset: AtomicU64::new(0),
            checkpoint_path: storage_config.wal.log_dir.join("checkpoint.json"),
        });

        let adapter = Self {
            inner: inner.clone(),
        };

        // Rebuild index from WAL
        info!("Rebuilding index from WAL...");
        let start = std::time::Instant::now();
        adapter.rebuild_index_async().await?;
        info!("Index rebuild complete in {:?}", start.elapsed());

        // Spawn background flush task
        let weak_inner = Arc::downgrade(&inner);
        tokio::spawn(async move {
            Self::run_flush_loop(weak_inner).await;
        });

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

        let (tx, rx) = oneshot::channel();

        // Add to accumulator
        {
            let mut acc = self.inner.accumulator.lock().await;
            acc.add(PendingWrite { record, tx });
        }

        // Notify flush loop
        self.inner.flush_notify.notify_one();

        // Wait for persistence
        rx.await
            .map_err(|_| StorageError::Internal("oneshot canceled".into()))?
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

        // Convert all entries to LogRecords and create channels
        let mut receivers = Vec::with_capacity(entries.len());

        for data in entries {
            let record = LogRecord::new(LogOperation::Put {
                collection: "__raft_log".to_string(),
                id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                data: data.clone(),
            });

            let (tx, rx) = oneshot::channel();

            // Add to accumulator
            {
                let mut acc = self.inner.accumulator.lock().await;
                acc.add(PendingWrite {
                    record: record.clone(),
                    tx,
                });
            }

            receivers.push(rx);
        }

        // Single flush notification for all entries
        self.inner.flush_notify.notify_one();

        // Wait for all to persist
        let mut offsets = Vec::with_capacity(entries.len());
        for rx in receivers {
            let offset = rx
                .await
                .map_err(|_| StorageError::Internal("oneshot canceled".into()))??;
            offsets.push(offset);
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
    async fn flush_accumulator_inner(inner: &WalStorageInner) {
        let batch = {
            let mut acc = inner.accumulator.lock().await;
            acc.flush()
        };

        if batch.is_empty() {
            return;
        }

        let _transaction_barrier = inner.transaction_barrier.read().await;

        // Group by collection to create batches
        // We use a preserved order map or just iterate and group?
        // Since we want to batch per collection, let's group by collection first.
        // Order between collections doesn't matter strictly for WAL append (they are concurrent).
        // Order WITHIN collection MATTERS.

        // (Record, Waiters, Optional IDs for index update)
        type BatchGroup = (
            LogRecord,
            Vec<oneshot::Sender<Result<u64, StorageError>>>,
            Option<Vec<Vec<u8>>>,
        );
        let mut batched_writes: Vec<BatchGroup> = Vec::new();

        // Simple grouping strategy:
        // 1. Separate by collection
        // 2. For each collection, coalesce consecutive Puts/Deletes

        let mut collection_map: std::collections::HashMap<String, Vec<PendingWrite>> =
            std::collections::HashMap::new();

        for write in batch {
            let collection = match &write.record.operation {
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
                match write.record.operation {
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
                        current_put_txs.push(write.tx);
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
                        current_delete_txs.push(write.tx);
                    }
                    _ => {} // Ignore others for now
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
            return;
        }

        let (records, rest): (Vec<_>, Vec<_>) = batched_writes
            .into_iter()
            .map(|(r, t, i)| (r, (t, i)))
            .unzip();
        let (tx_groups, id_groups): (Vec<_>, Vec<_>) = rest.into_iter().unzip();

        // Write to WAL (async, returns Vec<(segment_id, offset)>)
        match inner.wal.append_batch(records.clone()).await {
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
                            _ => {}
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
                for (tx, (_seg_id, off)) in tx_groups.into_iter().zip(locations.iter()) {
                    for sender in tx {
                        let _ = sender.send(Ok(*off));
                    }
                }
            }
            Err(e) => {
                for tx_group in tx_groups {
                    for tx in tx_group {
                        let _ = tx.send(Err(StorageError::Internal(e.to_string())));
                    }
                }
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

        let mut rxs = Vec::with_capacity(items.len());
        let mut should_flush = false;

        {
            let mut acc = self.inner.accumulator.lock().await;
            for (key, value) in items {
                let record = LogRecord::new(LogOperation::Put {
                    collection: String::new(),
                    id: key,
                    data: value,
                });
                let (tx, rx) = oneshot::channel();
                rxs.push(rx);
                acc.add(PendingWrite { record, tx });
                should_flush = true;
            }
        }

        if should_flush {
            self.inner.flush_notify.notify_one();
        }

        // Wait for all
        for rx in rxs {
            rx.await
                .map_err(|_| StorageError::Internal("Channel closed".to_string()))??;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;

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
}
