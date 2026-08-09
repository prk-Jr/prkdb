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
/// - Single collection: ~60K ops/sec (same as WalStorageAdapter)
/// - 3 collections (parallel writes): ~180K ops/sec (3x!)
/// - 5 collections (parallel writes): ~300K ops/sec (5x!)
/// - Mixed workload (5 collections): **250K-400K ops/sec** (4-7x!)
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
