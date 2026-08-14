use super::wal_adapter::WalStorageAdapter;
use dashmap::DashMap;
use prkdb_core::wal::WalConfig;
use prkdb_metrics::storage::StorageMetrics;
use prkdb_types::error::StorageError;
use prkdb_types::snapshot::{CompressionType, SnapshotHeader};
use prkdb_types::storage::StorageAdapter;

use super::snapshot::SnapshotWriter;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{info, instrument};

/// Collection-Level Partitioned Storage Adapter
///
/// This adapter creates independent WAL instances for each collection,
/// enabling true parallel writes across collections. This is the proven
/// approach used by Kafka (topics), Cassandra (column families), etc.
///
/// # Architecture
/// ```text
/// ┌──────────────────────────────────────────┐
/// │  CollectionPartitionedAdapter            │
/// │  ┌────────────┐  ┌────────────┐         │
/// │  │ Users WAL  │  │ Orders WAL │  ...    │
/// │  │ + Cache    │  │ + Cache    │         │
/// │  │ + Index    │  │ + Index    │         │
/// │  └────────────┘  └────────────┘         │
/// │       ↓                ↓                 │
/// │  Parallel!        Parallel!             │
/// └──────────────────────────────────────────┘
/// ```
///
/// # Performance
///
/// Writes to different collections do not contend, so throughput is expected to scale
/// with collection count. **That expectation is unmeasured** — the per-collection figures
/// previously given here were unverified, from no benchmark in this repository. See
/// `docs/benchmarks/methodology.md`.
///
/// # Key Benefits
/// - ✅ Zero cross-collection coordination overhead
/// - ✅ Linear scaling with collection count
/// - ✅ Parallel cross-collection reads
/// - ✅ Natural isolation and organization
/// - ✅ Works seamlessly with Raft transactions
pub struct CollectionPartitionedAdapter {
    /// Map: collection_name -> WalStorageAdapter
    /// Uses DashMap for lock-free concurrent access
    collections: Arc<DashMap<String, Arc<WalStorageAdapter>>>,

    /// Base directory for all collections
    /// Structure: base_dir/collections/{collection_name}/wal
    base_dir: PathBuf,

    /// Template config for creating new collections
    base_config: WalConfig,

    /// Aggregated metrics across all collections
    metrics: Arc<AggregatedMetrics>,

    /// Per-collection size tracking (approximate bytes)
    /// Tracks total bytes written to each collection
    collection_sizes: Arc<DashMap<String, AtomicU64>>,
}

/// Aggregated metrics across all collections
pub struct AggregatedMetrics {
    total_collections: AtomicU64,
    total_writes: AtomicU64,
    total_reads: AtomicU64,
    per_collection_metrics: Arc<DashMap<String, Arc<StorageMetrics>>>,
}

impl AggregatedMetrics {
    fn new() -> Self {
        Self {
            total_collections: AtomicU64::new(0),
            total_writes: AtomicU64::new(0),
            total_reads: AtomicU64::new(0),
            per_collection_metrics: Arc::new(DashMap::new()),
        }
    }

    pub fn get_total_collections(&self) -> u64 {
        self.total_collections.load(Ordering::Relaxed)
    }

    pub fn get_total_writes(&self) -> u64 {
        self.total_writes.load(Ordering::Relaxed)
    }

    pub fn get_total_reads(&self) -> u64 {
        self.total_reads.load(Ordering::Relaxed)
    }

    pub fn get_collection_names(&self) -> Vec<String> {
        self.per_collection_metrics
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }
}

/// Restore the `collection:` prefix a per-collection adapter strips.
///
/// Without this a follower would receive `alice` where the leader wrote `users:alice`, and
/// replay it into whatever collection the bare key happened to parse as.
fn prefix_change(
    collection: &str,
    change: prkdb_types::replication::Change,
) -> prkdb_types::replication::Change {
    use prkdb_types::replication::Change;

    let prefixed = |key: Vec<u8>| {
        let mut full = Vec::with_capacity(collection.len() + 1 + key.len());
        full.extend_from_slice(collection.as_bytes());
        full.push(b':');
        full.extend_from_slice(&key);
        full
    };

    match change {
        Change::Put {
            key,
            value,
            version,
        } => Change::Put {
            key: prefixed(key),
            value,
            version,
        },
        Change::Delete { key, version } => Change::Delete {
            key: prefixed(key),
            version,
        },
    }
}

/// The collection both bounds name, when they name the same one.
///
/// # Why this is a free function with its own tests
///
/// Inside `scan_range` this is a pure optimisation: every candidate row is filtered
/// against the original bounds afterwards, so a detector that wrongly returns `None` only
/// costs work — the rows come out the same. That makes it invisible from the outside, and
/// mutants on either `position` call survived the entire suite (run 31362753534, shard 8)
/// for exactly that reason. Testing `scan_range` cannot distinguish a broken detector
/// from a working one; testing the detector can.
///
/// The one direction that *is* observable is a wrong `Some`, which skips collections
/// holding matching rows — covered both here and by
/// `a_range_spanning_collections_returns_rows_from_each`.
fn single_collection_bound(start: &[u8], end: &[u8]) -> Option<Vec<u8>> {
    match (
        start.iter().position(|b| *b == b':'),
        end.iter().position(|b| *b == b':'),
    ) {
        (Some(a), Some(b)) if start[..a] == end[..b] => Some(start[..a].to_vec()),
        _ => None,
    }
}

impl CollectionPartitionedAdapter {
    /// Create a new collection-partitioned adapter
    #[instrument(skip(config), fields(base_dir = %config.log_dir.display()))]
    pub fn new(config: WalConfig) -> Result<Self, StorageError> {
        let base_dir = config.log_dir.clone();

        // Create collections directory
        let collections_dir = base_dir.join("collections");
        std::fs::create_dir_all(&collections_dir).map_err(|e| {
            StorageError::Internal(format!("Failed to create collections dir: {}", e))
        })?;

        info!("Initialized CollectionPartitionedAdapter at {:?}", base_dir);

        Ok(Self {
            collections: Arc::new(DashMap::new()),
            base_dir,
            base_config: config,
            metrics: Arc::new(AggregatedMetrics::new()),
            collection_sizes: Arc::new(DashMap::new()),
        })
    }

    /// Get or create a collection's WAL adapter
    ///
    /// This is lazy - collections are only created when first accessed.
    /// Uses DashMap for lock-free concurrent access.
    ///
    /// FIX: Uses spawn_blocking to avoid deadlock when creating adapter from async context.
    #[instrument(skip(self), fields(collection = %collection_name))]
    async fn get_or_create_collection_async(
        &self,
        collection_name: &str,
    ) -> Arc<WalStorageAdapter> {
        // Fast path: check if collection already exists
        if let Some(adapter) = self.collections.get(collection_name) {
            return adapter.clone();
        }

        // Slow path: need to create collection
        info!("Creating new collection WAL: {}", collection_name);

        // Each collection gets its own directory
        let collection_dir = self.base_dir.join("collections").join(collection_name);
        let collection_config = WalConfig {
            log_dir: collection_dir,
            ..self.base_config.clone()
        };

        // CRITICAL FIX: Use spawn_blocking to avoid deadlock when calling block_on inside WalStorageAdapter::new()
        let adapter = tokio::task::spawn_blocking(move || {
            Arc::new(
                WalStorageAdapter::new(collection_config).expect("Failed to create collection WAL"),
            )
        })
        .await
        .expect("spawn_blocking failed");

        // Insert into collections map (may race with another insert, which is fine)
        let adapter = self
            .collections
            .entry(collection_name.to_string())
            .or_insert(adapter)
            .clone();

        // Track metrics
        self.metrics
            .total_collections
            .fetch_add(1, Ordering::Relaxed);

        // Update active collections gauge for Grafana
        let active_count = self.collections.len() as f64;
        crate::prometheus_metrics::COLLECTIONS_ACTIVE
            .with_label_values(&["local"])
            .set(active_count);

        self.metrics
            .per_collection_metrics
            .insert(collection_name.to_string(), Arc::new(StorageMetrics::new()));

        adapter
    }

    /// Names of every collection that exists on disk, whether or not it has been opened
    /// in this process.
    ///
    /// Collections are created lazily on first access, so after a restart the in-memory
    /// map is empty even though `collections/` is full. Anything that operates on the
    /// whole database — a backup, most obviously — has to consult the directory rather
    /// than the map, or it silently sees nothing.
    fn collection_names_on_disk(&self) -> Vec<String> {
        let dir = self.base_dir.join("collections");
        let mut names: Vec<String> = std::fs::read_dir(&dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.path().is_dir())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();

        // A collection created in this process may not have been flushed to a directory
        // yet, so union rather than replace.
        for entry in self.collections.iter() {
            if !names.contains(entry.key()) {
                names.push(entry.key().clone());
            }
        }
        names.sort();
        names.dedup();
        names
    }

    /// Materialise an adapter for every collection on disk.
    ///
    /// Returns the full set, so callers that need to touch all data can work from it.
    pub async fn load_all_collections(&self) -> Vec<(String, Arc<WalStorageAdapter>)> {
        let mut loaded = Vec::new();
        for name in self.collection_names_on_disk() {
            let adapter = self.get_or_create_collection_async(&name).await;
            loaded.push((name, adapter));
        }
        loaded
    }

    /// Parse a key into (collection_name, actual_key)
    ///
    /// Key format: "{collection_name}:{actual_key}" (binary safe)
    /// Example: b"users:johndoe" -> ("users", b"johndoe")
    ///
    /// Note: Uses ':' (0x3A) as delimiter. First occurrence splits collection from key.
    fn parse_collection_key(&self, key: &[u8]) -> Result<(String, Vec<u8>), StorageError> {
        // Find first ':' byte
        let delimiter_pos = key.iter().position(|&b| b == b':').ok_or_else(|| {
            StorageError::Internal(format!(
                "Key must contain ':' delimiter, got {} bytes",
                key.len()
            ))
        })?;

        // Split at delimiter
        let collection_bytes = &key[..delimiter_pos];
        let actual_key = &key[delimiter_pos + 1..];

        // Collection name must be valid UTF-8, but actual key can be binary
        let collection = std::str::from_utf8(collection_bytes)
            .map_err(|e| {
                StorageError::Internal(format!("Collection name must be valid UTF-8: {}", e))
            })?
            .to_string();

        Ok((collection, actual_key.to_vec()))
    }

    /// Get from a specific collection (direct API, no key parsing)
    pub async fn get_from_collection(
        &self,
        collection: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.metrics.total_reads.fetch_add(1, Ordering::Relaxed);
        let adapter = self.get_or_create_collection_async(collection).await;
        adapter.get(key).await
    }

    /// Put to a specific collection (direct API, no key parsing)
    pub async fn put_to_collection(
        &self,
        collection: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StorageError> {
        self.metrics.total_writes.fetch_add(1, Ordering::Relaxed);
        let adapter = self.get_or_create_collection_async(collection).await;
        adapter.put(key, value).await
    }

    /// Delete from a specific collection (direct API, no key parsing)
    pub async fn delete_from_collection(
        &self,
        collection: &str,
        key: &[u8],
    ) -> Result<(), StorageError> {
        let adapter = self.get_or_create_collection_async(collection).await;
        adapter.delete(key).await
    }

    /// Batch put to a specific collection (direct API)
    pub async fn put_batch_to_collection(
        &self,
        collection: &str,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), StorageError> {
        self.metrics
            .total_writes
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        let adapter = self.get_or_create_collection_async(collection).await;
        adapter.put_batch(entries).await
    }

    /// Multi-collection parallel get
    ///
    /// Reads from multiple collections in parallel for maximum throughput.
    ///
    /// # Example
    /// ```no_run
    /// # use prkdb::storage::CollectionPartitionedAdapter;
    /// # async fn demo(adapter: &CollectionPartitionedAdapter)
    /// #     -> Result<(), Box<dyn std::error::Error>> {
    /// let queries = vec![
    ///     ("users".to_string(), b"john".to_vec()),
    ///     ("orders".to_string(), b"order_123".to_vec()),
    ///     ("products".to_string(), b"prod_456".to_vec()),
    /// ];
    ///
    /// // All 3 reads happen in PARALLEL!
    /// let results = adapter.multi_collection_get(queries).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn multi_collection_get(
        &self,
        queries: Vec<(String, Vec<u8>)>,
    ) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
        let futures: Vec<_> = queries
            .into_iter()
            .map(|(collection, key)| async move {
                let adapter = self.get_or_create_collection_async(&collection).await;
                adapter.get(&key).await
            })
            .collect();

        futures::future::try_join_all(futures).await
    }

    /// Get metrics for all collections
    pub fn get_metrics(&self) -> &AggregatedMetrics {
        &self.metrics
    }
}

/// Implement StorageAdapter trait for backwards compatibility
///
/// This allows CollectionPartitionedAdapter to be used anywhere
/// WalStorageAdapter is used, with keys in "collection:key" format.
#[async_trait::async_trait]
impl StorageAdapter for CollectionPartitionedAdapter {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let start = std::time::Instant::now();
        let (collection, actual_key) = self.parse_collection_key(key)?;
        let result = self.get_from_collection(&collection, &actual_key).await;

        // Track metrics
        let duration = start.elapsed().as_secs_f64();
        crate::prometheus_metrics::OPERATION_DURATION
            .with_label_values(&["local", "read"])
            .observe(duration);

        // Track cache hit/miss (heuristic: Some = hit, None = miss)
        if let Ok(entry) = &result {
            if entry.is_some() {
                crate::prometheus_metrics::CACHE_HITS_TOTAL
                    .with_label_values(&["local"])
                    .inc();
            } else {
                crate::prometheus_metrics::CACHE_MISSES_TOTAL
                    .with_label_values(&["local"])
                    .inc();
            }
        }

        result
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let start = std::time::Instant::now();
        let (collection, actual_key) = self.parse_collection_key(key)?;
        let result = self
            .put_to_collection(&collection, &actual_key, value)
            .await;

        // Track metrics
        let duration = start.elapsed().as_secs_f64();
        crate::prometheus_metrics::OPERATION_DURATION
            .with_label_values(&["local", "write"])
            .observe(duration);

        // Track collection size (approximate - adds value size)
        if result.is_ok() {
            let size_bytes = value.len() as u64;
            let size_counter = self
                .collection_sizes
                .entry(collection.clone())
                .or_insert_with(|| AtomicU64::new(0));
            size_counter.fetch_add(size_bytes, Ordering::Relaxed);

            // Update Prometheus metric
            let total_size = size_counter.load(Ordering::Relaxed) as f64;
            crate::prometheus_metrics::COLLECTION_SIZE_BYTES
                .with_label_values(&["local", &collection])
                .set(total_size);
        }

        result
    }

    async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        let start = std::time::Instant::now();
        let (collection, actual_key) = self.parse_collection_key(key)?;
        let result = self.delete_from_collection(&collection, &actual_key).await;

        // Track metrics
        let duration = start.elapsed().as_secs_f64();
        crate::prometheus_metrics::OPERATION_DURATION
            .with_label_values(&["local", "delete"])
            .observe(duration);

        result
    }

    async fn flush(&self) -> Result<(), StorageError> {
        let adapters: Vec<_> = self
            .collections
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        for adapter in adapters {
            adapter.flush().await?;
        }

        Ok(())
    }

    async fn put_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), StorageError> {
        // Group entries by collection for maximum parallelism
        let mut collection_batches: std::collections::HashMap<String, Vec<(Vec<u8>, Vec<u8>)>> =
            std::collections::HashMap::new();

        for (key, value) in entries {
            let (collection, actual_key) = self.parse_collection_key(&key)?;
            collection_batches
                .entry(collection)
                .or_default()
                .push((actual_key, value));
        }

        // Write to all collections in PARALLEL! (This is the magic!)
        let futures: Vec<_> = collection_batches
            .into_iter()
            .map(|(collection, batch)| async move {
                let adapter = self.get_or_create_collection_async(&collection).await;
                adapter.put_batch(batch).await
            })
            .collect();

        futures::future::try_join_all(futures).await?;
        Ok(())
    }

    // Outbox methods - provide default implementations
    async fn outbox_save(&self, _id: &str, _payload: &[u8]) -> Result<(), StorageError> {
        // Not heavily used in examples -  return OK for compatibility
        Ok(())
    }

    async fn outbox_list(&self) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        // Return empty list
        Ok(Vec::new())
    }

    async fn outbox_remove(&self, _id: &str) -> Result<(), StorageError> {
        // Not heavily used - return OK
        Ok(())
    }

    async fn put_with_outbox(
        &self,
        key: &[u8],
        value: &[u8],
        _outbox_id: &str,
        _outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        // Just do the put, ignore outbox for now
        let (collection, actual_key) = self.parse_collection_key(key)?;
        self.put_to_collection(&collection, &actual_key, value)
            .await
    }

    async fn delete_with_outbox(
        &self,
        key: &[u8],
        _outbox_id: &str,
        _outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        // Just do the delete, ignore outbox for now
        let (collection, actual_key) = self.parse_collection_key(key)?;
        self.delete_from_collection(&collection, &actual_key).await
    }

    /// Scan every key beginning with `prefix`, across collections.
    ///
    /// # Why this needs its own implementation
    ///
    /// Without it the trait default refuses with "scan_prefix not supported", and this is
    /// the adapter `PrkDb::builder().with_data_dir()` constructs. That silently broke
    /// anything built on prefix scans — `list_collections` returned an error, and
    /// persisted principals could not be loaded — in exactly the way the missing
    /// `take_snapshot` broke `prkdb backup` (S-04).
    ///
    /// # Routing
    ///
    /// Keys are `collection:id`. A prefix containing the delimiter therefore names one
    /// collection and only that collection is scanned; a prefix without it may match any
    /// collection name, so every collection is scanned and filtered. Results carry the
    /// full `collection:id` key, matching what `get` and `put` accept.
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let collections = self.load_all_collections().await;
        let split = prefix.iter().position(|b| *b == b':');

        let mut out = Vec::new();
        for (name, adapter) in collections {
            let inner_prefix: Vec<u8> = match split {
                Some(at) => {
                    // The prefix names a collection; skip the others entirely.
                    if prefix[..at] != *name.as_bytes() {
                        continue;
                    }
                    prefix[at + 1..].to_vec()
                }
                // A partial collection name matches any collection it prefixes.
                None => {
                    if !name.as_bytes().starts_with(prefix) {
                        continue;
                    }
                    Vec::new()
                }
            };

            for (key, value) in adapter.scan_prefix(&inner_prefix).await? {
                let mut full = Vec::with_capacity(name.len() + 1 + key.len());
                full.extend_from_slice(name.as_bytes());
                full.push(b':');
                full.extend_from_slice(&key);
                out.push((full, value));
            }
        }

        // Callers that page or diff results need a stable order; per-collection iteration
        // order is not one.
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// Scan the half-open key range `[start, end)`, across collections.
    ///
    /// The fourth method this wrapper was missing, after `take_snapshot` (S-04),
    /// collection discovery (S-05) and `scan_prefix` (S-07). `WalStorageAdapter` implements
    /// it; the wrapper fell through to the trait default that refuses, so
    /// `CollectionHandle::scan_range_by_id_bytes` — public API — failed on every database
    /// opened with `--database`.
    ///
    /// # Routing
    ///
    /// Bounds are full `collection:id` keys. Rather than reason about which collections a
    /// range spans — `orders:z` to `users:a` covers every collection in between, and
    /// collection names are arbitrary — each collection is scanned for its own slice of
    /// the range and the results are filtered against the original bounds. Correct by
    /// construction, at the cost of touching every collection; ranges that name one
    /// collection on both sides are the common case and are narrowed first.
    async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let collections = self.load_all_collections().await;
        let single_collection = single_collection_bound(start, end);

        let mut out = Vec::new();
        for (name, adapter) in collections {
            if let Some(only) = &single_collection {
                if name.as_bytes() != only.as_slice() {
                    continue;
                }
            }

            // Scan the whole collection and filter: the inner adapter's range is over
            // *its* keys, which have the collection prefix stripped, so translating the
            // bounds per collection would need a case for every way a bound can fall
            // inside, outside, or across the prefix.
            for (key, value) in adapter.scan_prefix(b"").await? {
                let mut full = Vec::with_capacity(name.len() + 1 + key.len());
                full.extend_from_slice(name.as_bytes());
                full.push(b':');
                full.extend_from_slice(&key);

                if full.as_slice() >= start && full.as_slice() < end {
                    out.push((full, value));
                }
            }
        }

        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// Changes after `offset`, for replication.
    ///
    /// # Why this is not simply a merge (S-09)
    ///
    /// The cursor is a `u64` WAL offset, and this adapter holds one independent WAL per
    /// collection. Two collections both number their first record 1, so an offset does not
    /// identify a position across them — and no ordering recovers one:
    ///
    /// - **By offset**: collides. Collection `a` offset 5 and collection `b` offset 5 are
    ///   unrelated events.
    /// - **By (collection, offset), cursor as an index**: a collection created later
    ///   receives low offsets and inserts into the middle of the sequence, shifting every
    ///   position after it. An outstanding cursor then skips or repeats changes — silent
    ///   data loss during replication, which is the class of bug this work exists to
    ///   remove.
    /// - **By `LogRecord::timestamp`**: wall clock, not monotonic, and collides at
    ///   millisecond granularity.
    ///
    /// A general solution needs a monotonic sequence assigned by *this* adapter at write
    /// time and persisted with each record. That is a WAL format change and would not
    /// recover history written before it, so it is not attempted here.
    ///
    /// # What is implemented
    ///
    /// A single-collection database has exactly one WAL, so the cursor is unambiguous and
    /// the call delegates. That is the common shape for a replicated collection, and it is
    /// the case `fetch_segment` is usually asked about.
    ///
    /// More than one collection is refused, naming the collections and the reason. It used
    /// to return "not supported" via the trait default, and `fetch_segment` swallowed that
    /// and streamed an empty successful response — so a follower concluded there was
    /// nothing to replicate.
    async fn get_changes_since(
        &self,
        offset: u64,
    ) -> Result<Vec<prkdb_types::replication::Change>, StorageError> {
        let collections = self.load_all_collections().await;

        match collections.len() {
            0 => Ok(Vec::new()),
            1 => collections[0].1.get_changes_since(offset).await,
            n => {
                let mut names: Vec<&str> = collections.iter().map(|(k, _)| k.as_str()).collect();
                names.sort();
                Err(StorageError::BackendError(format!(
                    "get_changes_since is not defined across {n} collections ({}): each has \
                     an independent WAL whose offsets start at 1, so a single u64 cursor \
                     cannot address a position across them. Replicate a single-collection \
                     database, or see spec S-09 for what a general cursor would require.",
                    names.join(", ")
                )))
            }
        }
    }

    /// Changes after `offset` within one collection.
    ///
    /// This is the call that makes replication of a multi-collection database possible.
    /// `get_changes_since` cannot be: it takes a bare offset, and each collection here has
    /// its own log numbering from 1, so the cursor is ambiguous. Naming the collection
    /// resolves it, and `fetch_segment` carries the name for exactly that reason.
    ///
    /// An unknown collection returns no changes rather than an error: a follower asking
    /// about a collection that has not been created yet is early, not wrong.
    async fn changes_in_collection(
        &self,
        collection: &str,
        offset: u64,
    ) -> Result<Vec<prkdb_types::replication::Change>, StorageError> {
        let Some(adapter) = self
            .load_all_collections()
            .await
            .into_iter()
            .find(|(name, _)| name == collection)
            .map(|(_, adapter)| adapter)
        else {
            return Ok(Vec::new());
        };

        // The inner adapter stores keys without the collection prefix, but a replication
        // consumer must be able to apply what it receives — and `put` at this layer takes
        // the full `collection:id` form. Re-prefix so a change can be replayed as-is.
        let changes = adapter.get_changes_since(offset).await?;
        Ok(changes
            .into_iter()
            .map(|change| prefix_change(collection, change))
            .collect())
    }

    /// Snapshot every collection into a single archive.
    ///
    /// Without this the trait default refuses with "take_snapshot not supported", which is
    /// what `prkdb backup` did for every database opened with `--database` — this adapter
    /// is what `PrkDb::builder().with_data_dir()` constructs. See S-04.
    ///
    /// # One archive, not one per collection
    ///
    /// Data is spread across one `WalStorageAdapter` per collection, so a snapshot has to
    /// merge N sources. Entries are written under their **full `collection:key` form**,
    /// which is what `get`/`put` take at this layer. Restore therefore needs no knowledge
    /// of collections at all: it replays each entry through the public `put`, and the
    /// normal routing in `parse_collection_key` puts it back where it came from.
    ///
    /// The alternative — an archive per collection — was rejected because it makes a
    /// partial restore silently possible.
    ///
    /// # Consistency
    ///
    /// The read is not atomic across collections: a write landing mid-snapshot may or may
    /// not be captured, and `max_offset` is the maximum over adapters rather than a single
    /// consistent cut. The single-adapter implementation has the same property. It is
    /// sound for `prkdb backup`, which opens the data directory offline with no other
    /// writer. Do not treat the result as a consistent cut of a live cluster.
    async fn take_snapshot(
        &self,
        path: PathBuf,
        compression: CompressionType,
    ) -> Result<u64, StorageError> {
        // Read from disk, not from the in-memory map: collections open lazily, so on a
        // freshly opened database the map is empty and a snapshot built from it would
        // succeed while containing nothing.
        let collections = self.load_all_collections().await;

        let mut max_offset = 0u64;
        let mut planned: Vec<(String, Arc<WalStorageAdapter>, Vec<Vec<u8>>)> =
            Vec::with_capacity(collections.len());
        let mut count = 0u64;
        for (name, adapter) in collections {
            max_offset = max_offset.max(adapter.max_offset());
            let keys = adapter.get_all_keys();
            count += keys.len() as u64;
            planned.push((name, adapter, keys));
        }

        info!(
            "Starting merged snapshot: {} collections, {} keys, max_offset={}",
            planned.len(),
            count,
            max_offset
        );

        // Same producer/consumer split as the single-adapter path: file I/O runs on a
        // blocking thread so compression does not stall the runtime.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(Vec<u8>, Vec<u8>)>(1024);
        let write_task = tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            let header = SnapshotHeader::new(max_offset, count, compression);
            let mut writer = SnapshotWriter::new(&path, header)?;
            while let Some((key, val)) = rx.blocking_recv() {
                writer.write_entry(&key, &val)?;
            }
            writer.finish()?;
            Ok(())
        });

        for (name, adapter, keys) in planned {
            for key in keys {
                // A key deleted between planning and reading simply drops out; the header
                // count is then an upper bound, which the reader tolerates.
                if let Some(val) = adapter.get(&key).await? {
                    let mut full_key = Vec::with_capacity(name.len() + 1 + key.len());
                    full_key.extend_from_slice(name.as_bytes());
                    full_key.push(b':');
                    full_key.extend_from_slice(&key);

                    if tx.send((full_key, val)).await.is_err() {
                        return Err(StorageError::Internal(
                            "Snapshot writer task failed".to_string(),
                        ));
                    }
                }
            }
        }
        drop(tx);

        write_task
            .await
            .map_err(|e| StorageError::Internal(format!("Snapshot writer panicked: {}", e)))??;

        Ok(max_offset)
    }

    /// Worst write-path health across every open collection.
    ///
    /// # Why the wrapper cannot inherit the default here
    ///
    /// The trait's default answers "not applicable", which is right for an adapter with no
    /// background writer and wrong for this one — it owns one `WalStorageAdapter`, and
    /// therefore one writer, per collection. Inheriting it would have a database with a
    /// stalled writer report itself perfectly healthy, which is the same silently-inherited
    /// default that produced S-04, S-05, S-07 and S-08 (see
    /// `scripts/check_wrapper_completeness.sh`).
    ///
    /// Worst-across-all, not an average: one stalled collection means writes to it are not
    /// being confirmed, and a probe that dilutes that against nine healthy collections is
    /// reporting a number nobody can act on. Depths sum, because the memory they represent
    /// does.
    ///
    /// Reads only collections already open in the map, deliberately: a probe must not open
    /// files, and a collection that has never been touched has no writer to be stalled.
    fn write_path_health(&self) -> prkdb_types::storage::WritePathHealth {
        let mut worst = prkdb_types::storage::WritePathHealth::not_applicable();
        let mut unhealthy: Vec<String> = Vec::new();

        for entry in self.collections.iter() {
            let health = entry.value().write_path_health();

            worst.queue_depth += health.queue_depth;
            worst.publishes_total += health.publishes_total;
            worst.direct_appends_total += health.direct_appends_total;
            worst.oldest_unpublished_age_ms = worst
                .oldest_unpublished_age_ms
                .max(health.oldest_unpublished_age_ms);
            // The oldest last-publish wins: the collection that has gone longest without
            // writing anything is the one worth reporting.
            worst.last_publish_age_ms =
                match (worst.last_publish_age_ms, health.last_publish_age_ms) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (a, b) => a.or(b),
                };

            if let Some(reason) = health.reason {
                unhealthy.push(format!("{}: {}", entry.key(), reason));
            }
        }

        if !unhealthy.is_empty() {
            worst.healthy = false;
            worst.reason = Some(unhealthy.join("; "));
        }

        worst
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_collection_partitioned_basic() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        // Test direct collection API
        adapter
            .put_to_collection("users", b"john", b"John Doe")
            .await
            .unwrap();

        let value = adapter.get_from_collection("users", b"john").await.unwrap();
        assert_eq!(value, Some(b"John Doe".to_vec()));

        // Test that collections are isolated
        let value = adapter
            .get_from_collection("orders", b"john")
            .await
            .unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_storage_adapter_trait() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        // Test with collection:key format
        adapter.put(b"users:john", b"John Doe").await.unwrap();

        let value = adapter.get(b"users:john").await.unwrap();
        assert_eq!(value, Some(b"John Doe".to_vec()));

        // Different collection
        adapter.put(b"orders:123", b"Order Data").await.unwrap();
        let value = adapter.get(b"orders:123").await.unwrap();
        assert_eq!(value, Some(b"Order Data".to_vec()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multi_collection_parallel_get() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        // Populate multiple collections
        adapter
            .put_to_collection("users", b"john", b"John Doe")
            .await
            .unwrap();
        adapter
            .put_to_collection("orders", b"123", b"Order 123")
            .await
            .unwrap();
        adapter
            .put_to_collection("products", b"456", b"Product 456")
            .await
            .unwrap();

        // Read from all 3 collections in parallel
        let queries = vec![
            ("users".to_string(), b"john".to_vec()),
            ("orders".to_string(), b"123".to_vec()),
            ("products".to_string(), b"456".to_vec()),
        ];

        let results = adapter.multi_collection_get(queries).await.unwrap();

        assert_eq!(results[0], Some(b"John Doe".to_vec()));
        assert_eq!(results[1], Some(b"Order 123".to_vec()));
        assert_eq!(results[2], Some(b"Product 456".to_vec()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_across_collections() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        // Batch with mixed collections
        let batch = vec![
            (b"users:john".to_vec(), b"John Doe".to_vec()),
            (b"orders:123".to_vec(), b"Order 123".to_vec()),
            (b"users:jane".to_vec(), b"Jane Doe".to_vec()),
            (b"products:456".to_vec(), b"Product 456".to_vec()),
        ];

        adapter.put_batch(batch).await.unwrap();

        // Verify all entries
        assert_eq!(
            adapter.get(b"users:john").await.unwrap(),
            Some(b"John Doe".to_vec())
        );
        assert_eq!(
            adapter.get(b"orders:123").await.unwrap(),
            Some(b"Order 123".to_vec())
        );
        assert_eq!(
            adapter.get(b"users:jane").await.unwrap(),
            Some(b"Jane Doe".to_vec())
        );
        assert_eq!(
            adapter.get(b"products:456").await.unwrap(),
            Some(b"Product 456".to_vec())
        );
    }

    /// A batch write must actually write.
    ///
    /// Replacing the whole body of `put_batch_to_collection` with `Ok(())` — a batch that
    /// stores nothing and reports success — survived the entire suite (mutation run
    /// 31358158012, shard 6). Nothing read back what a batch wrote, so the loudest
    /// possible failure, silent total data loss on the batch path, was invisible.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_batch_write_is_readable_afterwards() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..8)
            .map(|i| (format!("k{i}").into_bytes(), format!("v{i}").into_bytes()))
            .collect();

        adapter
            .put_batch_to_collection("users", entries.clone())
            .await
            .expect("the batch commits");

        for (key, value) in &entries {
            assert_eq!(
                adapter.get_from_collection("users", key).await.unwrap(),
                Some(value.clone()),
                "batch key {} did not survive the write",
                String::from_utf8_lossy(key)
            );
        }

        assert!(
            adapter.metrics.get_total_writes() >= entries.len() as u64,
            "the batch path must count its writes"
        );
    }

    /// A collection that exists only in memory must still be listed.
    ///
    /// `collection_names_on_disk` unions the in-memory map over the directory listing
    /// precisely because a freshly created collection may not have been flushed yet —
    /// that omission is spec S-05. Removing the `!` from `if !names.contains(..)` inverts
    /// the union into a no-op and drops those collections, and no test noticed (mutation
    /// run 31358158012, shard 5).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_collection_not_yet_on_disk_is_still_listed() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        adapter
            .put_to_collection("just_created", b"k", b"v")
            .await
            .unwrap();

        // Removing the directory while the adapter keeps its in-memory entry reproduces
        // the state the union exists for: known to this process, not visible on disk.
        // Writing the collection and listing it immediately does not — `put_to_collection`
        // creates the directory, so the disk listing already contains it and the union is
        // redundant. A test that skips this step passes with the `!` removed.
        std::fs::remove_dir_all(temp_dir.path().join("collections").join("just_created"))
            .expect("the collection directory exists to be removed");

        let names = adapter.collection_names_on_disk();
        assert!(
            names.contains(&"just_created".to_string()),
            "a collection held in memory must appear in the listing, found {names:?}"
        );

        // No duplicates, whether the collection reached disk or not.
        let mut sorted = names.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            names.len(),
            "listing repeats a collection: {names:?}"
        );
    }

    /// The metrics accessors report what was recorded.
    ///
    /// `get_total_reads` replaced by `1` and `get_collection_names` replaced by an empty,
    /// blank, or junk vector all survived (mutation run 31358158012, shard 5): the
    /// counters were incremented by other tests and read back by none.
    #[tokio::test(flavor = "multi_thread")]
    async fn metrics_report_recorded_activity() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        adapter
            .put_to_collection("users", b"a", b"1")
            .await
            .unwrap();
        adapter
            .put_to_collection("orders", b"b", b"2")
            .await
            .unwrap();
        for _ in 0..3 {
            adapter.get_from_collection("users", b"a").await.unwrap();
        }

        let metrics = adapter.metrics;
        assert_eq!(
            metrics.get_total_reads(),
            3,
            "three reads were issued; a constant would not track them"
        );
        assert!(metrics.get_total_writes() >= 2);
        assert_eq!(
            metrics.get_total_collections(),
            2,
            "two collections were created; a constant would not track them"
        );

        let mut names = metrics.get_collection_names();
        names.sort();
        assert_eq!(
            names,
            vec!["orders".to_string(), "users".to_string()],
            "the names must be the collections touched, not a placeholder"
        );
    }

    /// `delete_with_outbox` must delete.
    ///
    /// Replacing its body with `Ok(())` — a delete that removes nothing and reports
    /// success — survived the suite (mutation run 31358158012, shard 7).
    #[tokio::test(flavor = "multi_thread")]
    async fn delete_with_outbox_removes_the_key() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        adapter.put(b"users:gone", b"value").await.unwrap();
        assert_eq!(
            adapter.get(b"users:gone").await.unwrap(),
            Some(b"value".to_vec())
        );

        adapter
            .delete_with_outbox(b"users:gone", "outbox-1", b"event")
            .await
            .expect("the delete succeeds");

        assert_eq!(
            adapter.get(b"users:gone").await.unwrap(),
            None,
            "delete_with_outbox reported success without deleting"
        );
    }

    /// `put_with_outbox` must write, for the same reason.
    #[tokio::test(flavor = "multi_thread")]
    async fn put_with_outbox_writes_the_key() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        adapter
            .put_with_outbox(b"users:added", b"value", "outbox-1", b"event")
            .await
            .expect("the write succeeds");

        assert_eq!(
            adapter.get(b"users:added").await.unwrap(),
            Some(b"value".to_vec())
        );
    }

    /// The outbox on this adapter is a stub, and this pins that it is an *empty* stub.
    ///
    /// `outbox_list` returns `Ok(Vec::new())` — the outbox pattern is not implemented
    /// here. Mutants replacing it with a populated vector survived, which matters more
    /// than it looks: a caller draining the outbox would act on invented entries. The
    /// contract this asserts is "empty", so if the stub is ever replaced by a real
    /// implementation this test is where that shows up.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_outbox_stub_reports_nothing_rather_than_something() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        adapter.outbox_save("id-1", b"payload").await.unwrap();
        assert!(
            adapter.outbox_list().await.unwrap().is_empty(),
            "the outbox is a stub; it must report nothing, not invented entries"
        );
    }

    /// Flushing must reach disk: a value written and flushed survives a reopen.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_flushed_write_survives_reopening() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        {
            let adapter = CollectionPartitionedAdapter::new(config.clone()).unwrap();
            adapter.put(b"users:durable", b"value").await.unwrap();
            adapter.flush().await.expect("flush succeeds");
        }

        let reopened = CollectionPartitionedAdapter::new(config).unwrap();
        assert_eq!(
            reopened.get(b"users:durable").await.unwrap(),
            Some(b"value".to_vec()),
            "a flushed write did not survive reopening"
        );
    }

    /// A prefix with no colon names a *partial collection name* and must select the
    /// collections it prefixes — not the ones it does not.
    ///
    /// # What this catches
    ///
    /// That branch guards with `if !name.as_bytes().starts_with(prefix) { continue; }`.
    /// Removing the `!` inverts the selection exactly: `scan_prefix(b"use")` then skips
    /// `users` and scans every other collection, so the caller gets a confident answer
    /// made entirely of the wrong rows.
    ///
    /// The existing coverage all used `b"users:"`, which takes the *other* branch — the
    /// one where the prefix contains a colon and names a collection outright — so the
    /// partial-name path had no test at all and the mutant survived (run 31362753534,
    /// shard 8).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_partial_collection_name_selects_only_matching_collections() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        adapter.put(b"users:alice", b"a").await.unwrap();
        adapter.put(b"users:bob", b"b").await.unwrap();
        adapter.put(b"orders:1", b"o").await.unwrap();

        // "use" prefixes "users" and not "orders".
        let hits = adapter.scan_prefix(b"use").await.unwrap();
        let mut keys: Vec<String> = hits
            .iter()
            .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
            .collect();
        keys.sort();

        assert_eq!(
            keys,
            vec!["users:alice".to_string(), "users:bob".to_string()],
            "a partial collection name must select the collections it prefixes"
        );
        assert!(
            !keys.iter().any(|k| k.starts_with("orders")),
            "a collection the prefix does not match must not be scanned: {keys:?}"
        );

        // A prefix matching nothing returns nothing rather than everything.
        assert!(
            adapter.scan_prefix(b"zzz").await.unwrap().is_empty(),
            "an unmatched prefix must select no collection"
        );
    }

    /// A range spanning two collections must return rows from both.
    ///
    /// # What this catches
    ///
    /// `scan_range` narrows to a single collection only when both bounds name the *same*
    /// one, which the guard `start[..a] == end[..b]` decides. Forcing that guard to `true`
    /// makes any pair of colon-bearing bounds look single-collection, so a range from
    /// `orders:` to `users:` is answered from `orders` alone and every `users` row is
    /// silently dropped.
    ///
    /// Every existing test ranged within one collection (`users:b` to `users:d`), where
    /// the narrowing is correct either way, so the mutant survived (run 31362753534,
    /// shard 8). Spanning two collections is the only shape that exercises the guard.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_range_spanning_collections_returns_rows_from_each() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        adapter.put(b"orders:1", b"o1").await.unwrap();
        adapter.put(b"orders:2", b"o2").await.unwrap();
        adapter.put(b"users:alice", b"a").await.unwrap();
        adapter.put(b"users:bob", b"b").await.unwrap();

        // "orders:1" ..< "users:c" covers both collections entirely.
        let rows = adapter.scan_range(b"orders:1", b"users:c").await.unwrap();
        let keys: Vec<String> = rows
            .iter()
            .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
            .collect();

        assert_eq!(
            keys,
            vec![
                "orders:1".to_string(),
                "orders:2".to_string(),
                "users:alice".to_string(),
                "users:bob".to_string(),
            ],
            "a range spanning two collections must return rows from both, sorted"
        );

        // Half-open, across the boundary: excluding users:bob must not exclude users:alice.
        let rows = adapter.scan_range(b"orders:2", b"users:bob").await.unwrap();
        let keys: Vec<String> = rows
            .iter()
            .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
            .collect();
        assert_eq!(
            keys,
            vec!["orders:2".to_string(), "users:alice".to_string()],
            "scan_range is half-open [start, end) across collections too"
        );
    }

    /// The single-collection detector, tested directly.
    ///
    /// Both `position` calls and the guard are only reachable in a way that changes
    /// results when the answer is a wrong `Some`; a wrong `None` merely costs a full
    /// scan. Asserting the exact answer here makes all four mutations on this expression
    /// observable — before it was extracted, three of them survived the whole suite.
    #[test]
    fn the_single_collection_detector_answers_exactly() {
        // Both bounds name the same collection.
        assert_eq!(
            single_collection_bound(b"users:a", b"users:z"),
            Some(b"users".to_vec()),
            "both bounds name `users`, so only `users` need be scanned"
        );

        // Different collections: no narrowing is permissible, or the other collection's
        // rows are dropped.
        assert_eq!(single_collection_bound(b"orders:1", b"users:9"), None);
        assert_eq!(single_collection_bound(b"users:1", b"user:9"), None);
        assert_eq!(single_collection_bound(b"a:1", b"ab:9"), None);

        // A bound with no colon does not name a collection.
        assert_eq!(single_collection_bound(b"users", b"users:z"), None);
        assert_eq!(single_collection_bound(b"users:a", b"users"), None);
        assert_eq!(single_collection_bound(b"", b""), None);

        // A leading colon means an empty collection name on that side.
        assert_eq!(single_collection_bound(b":a", b":z"), Some(Vec::new()));
        assert_eq!(single_collection_bound(b":a", b"users:z"), None);
    }

    /// `flush` must forward to every collection, and report a failure rather than
    /// swallow it.
    ///
    /// # What this catches, and why it needed a new seam
    ///
    /// Replacing this method's whole body with `Ok(())` — a flush that flushes nothing
    /// and reports success — survived the entire suite (run 31358158012, shard 7), and
    /// stayed unkillable long enough to be recorded as an accepted exclusion in
    /// `.cargo/mutants.toml`.
    ///
    /// It was unkillable through the public surface for a specific reason: this adapter's
    /// `put` path writes through rather than accumulating, so a value survives a reopen
    /// whether or not `flush` ran. `a_flushed_write_survives_reopening` passes with the
    /// body replaced. The only observable difference is whether the wrapper *forwards* —
    /// which needs an inner adapter that can fail, and none existed.
    ///
    /// `wal_adapter::fault_injection` is that missing capability. With one collection's
    /// flush failing, a wrapper that forwards returns `Err` and a wrapper that returns
    /// `Ok(())` does not. The exclusion is deleted in the same change.
    #[tokio::test(flavor = "multi_thread")]
    async fn flush_reports_a_collection_failure_rather_than_swallowing_it() {
        use crate::storage::wal_adapter::fault_injection;

        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        adapter.put(b"users:a", b"1").await.unwrap();
        adapter.put(b"orders:b", b"2").await.unwrap();

        // Healthy to begin with, so the failure below is attributable to the injection
        // and not to flush being broken already.
        adapter
            .flush()
            .await
            .expect("flush succeeds while every collection is healthy");

        // `orders` is deliberately not the first collection created: a wrapper that
        // stopped after the first would otherwise pass.
        let failing = temp_dir.path().join("collections").join("orders");
        fault_injection::fail_flush_at(&failing);

        let outcome = adapter.flush().await;

        // Clear before asserting, so a panic does not leave the fault set for the drop
        // path — the adapter flushes as its last handle goes away.
        fault_injection::clear_flush_failure(&failing);

        let err = outcome
            .expect_err("flush must report a collection whose own flush failed, not return Ok");
        assert!(
            err.to_string().contains("injected flush failure"),
            "the error must be the inner failure, not something invented: {err}"
        );

        // And it recovers: the fault was the only reason it failed.
        adapter
            .flush()
            .await
            .expect("flush succeeds again once the injected fault is cleared");
    }

    /// Poll until `check` holds, so a test observes a transient window without racing it.
    async fn wait_until(what: &str, limit: Duration, mut check: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + limit;
        while std::time::Instant::now() < deadline {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for {what}");
    }

    /// Queue depths **sum** across collections, because the memory they represent does.
    ///
    /// Mutation run 31539366718 missed `+=` -> `-=` and `+=` -> `*=` here: nothing asserted
    /// the arithmetic, only that a number came back. `*=` reports 0 for any number of
    /// stalled collections — a probe that says "nothing queued" while two writers are stuck
    /// is worse than no probe, because it actively argues against the operator's suspicion.
    #[tokio::test(flavor = "multi_thread")]
    async fn queue_depths_sum_across_collections() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = Arc::new(CollectionPartitionedAdapter::new(config).unwrap());

        // Open both collections with a write that succeeds, so the stall below acts on a
        // live writer rather than on collection creation.
        for name in ["users", "orders"] {
            adapter
                .put_to_collection(name, b"seed", b"v")
                .await
                .unwrap();
        }

        let collections = temp_dir.path().join("collections");
        crate::storage::wal_adapter::fault_injection::stall_writer_at(collections.join("users"));
        crate::storage::wal_adapter::fault_injection::stall_writer_at(collections.join("orders"));

        // `put_to_collection` calls `put`, which routes to `put_batch_impl` and bypasses
        // the accumulator entirely — nothing to stall. `put_many` is the accumulator path,
        // so drive the per-collection adapters directly.
        let mut queued = Vec::new();
        for name in ["users", "orders"] {
            let collection = adapter
                .collections
                .get(name)
                .expect("the collection was opened above")
                .clone();
            queued.push(tokio::spawn(async move {
                collection
                    .put_many(vec![(b"queued".to_vec(), b"v".to_vec())])
                    .await
            }));
        }

        let probe = adapter.clone();
        wait_until(
            "both stalled collections to report their queued write",
            Duration::from_secs(10),
            move || probe.write_path_health().queue_depth == 2,
        )
        .await;

        // One write per collection, so the total is the sum and not either operand: 2 is
        // unreachable by `-=` (which underflows from 0) and by `*=` (which stays 0).
        assert_eq!(
            adapter.write_path_health().queue_depth,
            2,
            "two stalled collections holding one write each must report two"
        );

        crate::storage::wal_adapter::fault_injection::clear_writer_stall(
            &collections.join("users"),
        );
        crate::storage::wal_adapter::fault_injection::clear_writer_stall(
            &collections.join("orders"),
        );
        for task in queued {
            let _ = tokio::time::timeout(Duration::from_secs(20), task).await;
        }
    }

    /// One unhealthy collection makes the whole adapter unhealthy — worst-across-all, not
    /// an average.
    ///
    /// The aggregate publish total is the sum across collections, not their product.
    ///
    /// `write_path_health` folds every open collection into one report so a probe can ask
    /// a single question. `publishes_total` is a counter, so the fold is `+=`; the nightly
    /// sweep replaced it with `*=` and nothing noticed, because no test read the aggregate
    /// count at all.
    ///
    /// Two collections with one publish each is the case that separates them — 1 + 1 is 2
    /// and 1 * 1 is 1 — so the counts here are made deliberately unequal as well, since a
    /// scraper deriving a publish rate from a product would see it collapse rather than
    /// grow as collections are added.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_aggregate_publish_total_sums_across_collections() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        // `put_many`, not `put`: only the accumulator path records a publish. `put` goes
        // straight to `wal.append_batch`, so the progress accounting — and therefore
        // `publishes_total` — never sees it. Opening the collections first, then writing
        // through the accounted path.
        for name in ["users", "orders"] {
            adapter
                .put_to_collection(name, b"seed", b"v")
                .await
                .unwrap();
        }
        for (name, batches) in [("users", 1), ("orders", 2)] {
            let collection = adapter
                .collections
                .get(name)
                .expect("the collection was opened above")
                .clone();
            // Unequal batch counts on purpose: one each would let `*=` pass, since 1*1 == 1.
            for b in 0..batches {
                collection
                    .put_many(vec![(format!("{name}-{b}").into_bytes(), b"v".to_vec())])
                    .await
                    .unwrap();
            }
        }

        // A caller is answered from inside `publish_batch`, and the publish is recorded
        // by `flush_accumulator_inner` after it returns — so the writes completing above
        // says nothing about the counter yet. Reading it here without waiting sees zeros.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let per_collection = loop {
            let counts: Vec<u64> = adapter
                .collections
                .iter()
                .map(|entry| entry.value().write_path_health().publishes_total)
                .collect();
            if counts.len() >= 2 && counts.iter().all(|c| *c > 0) {
                break counts;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for both collections to record a publish; saw {counts:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let expected: u64 = per_collection.iter().sum();

        assert!(
            per_collection.len() >= 2 && expected > 0,
            "the fold needs at least two collections that have published something, or it \
             is not being exercised; saw {per_collection:?}"
        );

        assert_eq!(
            adapter.write_path_health().publishes_total,
            expected,
            "the aggregate must sum each collection's publish count; the parts were \
             {per_collection:?}"
        );
    }

    /// Mutation run 31539366718 missed `delete !` on the `unhealthy.is_empty()` guard.
    /// Without the negation the adapter reports healthy precisely when a collection has
    /// reported a reason, so `/readyz` keeps routing traffic to the node whose writes are
    /// not being confirmed. That is the exact failure the liveness work exists to end.
    #[tokio::test(flavor = "multi_thread")]
    async fn one_stalled_collection_makes_the_adapter_unhealthy() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = Arc::new(CollectionPartitionedAdapter::new(config).unwrap());

        for name in ["users", "orders"] {
            adapter
                .put_to_collection(name, b"seed", b"v")
                .await
                .unwrap();
        }
        assert!(
            adapter.write_path_health().healthy,
            "a freshly opened adapter must be healthy, or the assertion below proves nothing"
        );

        let collections = temp_dir.path().join("collections");
        crate::storage::wal_adapter::fault_injection::stall_writer_at(collections.join("orders"));

        // The accumulator path, not `put` — see the note in the sibling test.
        let stalled = {
            let collection = adapter
                .collections
                .get("orders")
                .expect("the collection was opened above")
                .clone();
            tokio::spawn(async move {
                collection
                    .put_many(vec![(b"queued".to_vec(), b"v".to_vec())])
                    .await
            })
        };

        let probe = adapter.clone();
        wait_until(
            "the stalled collection to be declared unhealthy",
            Duration::from_secs(15),
            move || !probe.write_path_health().healthy,
        )
        .await;

        let health = adapter.write_path_health();
        assert!(
            !health.healthy,
            "one stalled collection means the node is not ready"
        );
        let reason = health
            .reason
            .expect("an unhealthy adapter must name the cause");
        assert!(
            reason.contains("orders"),
            "the reason must name the collection an operator has to look at, got: {reason}"
        );
        assert!(
            !reason.contains("users"),
            "a healthy collection must not be blamed, got: {reason}"
        );

        crate::storage::wal_adapter::fault_injection::clear_writer_stall(
            &collections.join("orders"),
        );
        let _ = tokio::time::timeout(Duration::from_secs(20), stalled).await;
    }
}
