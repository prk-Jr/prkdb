use super::wal_adapter::WalStorageAdapter;
use dashmap::DashMap;
use prkdb_core::wal::WalConfig;
use prkdb_metrics::storage::StorageMetrics;
use prkdb_types::error::StorageError;
use prkdb_types::replication::Change;
use prkdb_types::snapshot::CompressionType;
use prkdb_types::storage::{StorageAdapter, WritePathHealth};

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{info, instrument};

/// The storage adapter `PrkDb::builder().with_data_dir(..)` builds: one globally ordered
/// WAL for the whole data directory, with a per-collection routing API on top.
///
/// # One WAL per data directory (D11)
///
/// Every collection is stored in the one `WalStorageAdapter` opened at the data
/// directory root. There are no per-collection logs: one log gives one global commit
/// order, atomic writes across collections, and one recovery path (spec 2a). A collection
/// is part of a record's key, built by one private helper (`collection_key`); it is never
/// recovered by splitting a key at a delimiter.
///
/// # API
///
/// The [`StorageAdapter`] methods forward to the inner adapter unchanged, so keys written
/// through the trait are stored exactly as given. The routing methods
/// (`get_from_collection`, `put_to_collection`, …) take the collection explicitly and
/// build the stored key from it; they exist so callers can name a collection and get
/// per-collection metrics.
///
/// # Performance
///
/// One writer with group commit. The throughput figures previously given here were
/// unverified, from no benchmark in this repository. See
/// `docs/benchmarks/methodology.md`.
pub struct CollectionPartitionedAdapter {
    /// The one WAL of this data directory, at its root.
    inner: Arc<WalStorageAdapter>,

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
    /// Open (or create) the data directory at `config.log_dir`.
    ///
    /// The directory's format is checked by the inner adapter's open rules: a pre-D11
    /// layout (a `collections/` directory and no `FORMAT`) is refused as format 1, never
    /// opened as an empty database. Every open error is returned, none panics.
    #[instrument(skip(config), fields(base_dir = %config.log_dir.display()))]
    pub fn new(config: WalConfig) -> Result<Self, StorageError> {
        let inner = Arc::new(WalStorageAdapter::new(config.clone())?);
        info!(
            "Initialized CollectionPartitionedAdapter at {:?}",
            config.log_dir
        );

        Ok(Self {
            inner,
            metrics: Arc::new(AggregatedMetrics::new()),
            collection_sizes: Arc::new(DashMap::new()),
        })
    }

    /// The stored key of `key` in `collection`: `collection ++ b":" ++ key`, exactly the
    /// bytes a trait-path caller such as `CollectionHandle` writes today, so both paths
    /// address the same record. Task 2.12 re-implements this with the key codec.
    fn collection_key(&self, collection: &str, key: &[u8]) -> Result<Vec<u8>, StorageError> {
        let mut full = Vec::with_capacity(collection.len() + 1 + key.len());
        full.extend_from_slice(collection.as_bytes());
        full.push(b':');
        full.extend_from_slice(key);
        Ok(full)
    }

    /// The collection a trait-path key belongs to, for metrics only. Keys are never
    /// parsed, so this is `None` until Task 2.12 decodes the collection id with the key
    /// codec; such an operation counts toward the totals only.
    fn collection_of(&self, _key: &[u8]) -> Option<String> {
        None
    }

    /// Record that `collection` has been seen, for `collection_names` and the metrics.
    fn note_collection(&self, collection: &str) {
        if self.metrics.per_collection_metrics.contains_key(collection) {
            return;
        }
        let inserted = self
            .metrics
            .per_collection_metrics
            .entry(collection.to_string())
            .or_insert_with(|| {
                self.metrics
                    .total_collections
                    .fetch_add(1, Ordering::Relaxed);
                Arc::new(StorageMetrics::new())
            });
        drop(inserted);
        crate::prometheus_metrics::COLLECTIONS_ACTIVE
            .with_label_values(&["local"])
            .set(self.metrics.per_collection_metrics.len() as f64);
    }

    /// Add `bytes` to `collection`'s approximate size.
    fn track_size(&self, collection: &str, bytes: u64) {
        let size_counter = self
            .collection_sizes
            .entry(collection.to_string())
            .or_insert_with(|| AtomicU64::new(0));
        let total = size_counter.fetch_add(bytes, Ordering::Relaxed) + bytes;
        crate::prometheus_metrics::COLLECTION_SIZE_BYTES
            .with_label_values(&["local", collection])
            .set(total as f64);
    }

    /// Names of the collections this adapter has seen (through the routing API, or a
    /// trait-path key whose collection is known). Task 2.12 switches this to the
    /// collection catalog, which also knows collections written before this process
    /// started.
    pub fn collection_names(&self) -> Vec<String> {
        let mut names = self.metrics.get_collection_names();
        names.sort();
        names
    }

    /// Get from a specific collection.
    pub async fn get_from_collection(
        &self,
        collection: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.metrics.total_reads.fetch_add(1, Ordering::Relaxed);
        self.note_collection(collection);
        let full = self.collection_key(collection, key)?;
        self.inner.get(&full).await
    }

    /// Put to a specific collection.
    pub async fn put_to_collection(
        &self,
        collection: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StorageError> {
        self.metrics.total_writes.fetch_add(1, Ordering::Relaxed);
        self.note_collection(collection);
        let full = self.collection_key(collection, key)?;
        self.inner.put(&full, value).await?;
        self.track_size(collection, value.len() as u64);
        Ok(())
    }

    /// Delete from a specific collection.
    pub async fn delete_from_collection(
        &self,
        collection: &str,
        key: &[u8],
    ) -> Result<(), StorageError> {
        self.note_collection(collection);
        let full = self.collection_key(collection, key)?;
        self.inner.delete(&full).await
    }

    /// Batch put to a specific collection: one inner `put_batch`, so one frame.
    pub async fn put_batch_to_collection(
        &self,
        collection: &str,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), StorageError> {
        self.metrics
            .total_writes
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        self.note_collection(collection);
        let bytes: u64 = entries.iter().map(|(_, v)| v.len() as u64).sum();
        let entries = entries
            .into_iter()
            .map(|(key, value)| Ok((self.collection_key(collection, &key)?, value)))
            .collect::<Result<Vec<_>, StorageError>>()?;
        self.inner.put_batch(entries).await?;
        self.track_size(collection, bytes);
        Ok(())
    }

    /// Read one key from each of several collections.
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
    /// let results = adapter.multi_collection_get(queries).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn multi_collection_get(
        &self,
        queries: Vec<(String, Vec<u8>)>,
    ) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
        let mut results = Vec::with_capacity(queries.len());
        for (collection, key) in queries {
            results.push(self.get_from_collection(&collection, &key).await?);
        }
        Ok(results)
    }

    /// Get metrics for all collections
    pub fn get_metrics(&self) -> &AggregatedMetrics {
        &self.metrics
    }

    /// Attribute a trait-path write to its collection, when it is known.
    fn track_trait_write(&self, key: &[u8], bytes: u64) {
        if let Some(collection) = self.collection_of(key) {
            self.note_collection(&collection);
            self.track_size(&collection, bytes);
        }
    }
}

/// Every method forwards to the one inner WAL adapter, with no key parsing.
///
/// `scripts/check_wrapper_completeness.sh` checks that every method the inner adapter
/// implements is forwarded here: an inherited default would refuse at runtime (S-04,
/// S-05, S-07, S-08).
#[async_trait::async_trait]
impl StorageAdapter for CollectionPartitionedAdapter {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let start = std::time::Instant::now();
        self.metrics.total_reads.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.get(key).await;

        crate::prometheus_metrics::OPERATION_DURATION
            .with_label_values(&["local", "read"])
            .observe(start.elapsed().as_secs_f64());
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
        self.metrics.total_writes.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.put(key, value).await;

        crate::prometheus_metrics::OPERATION_DURATION
            .with_label_values(&["local", "write"])
            .observe(start.elapsed().as_secs_f64());
        if result.is_ok() {
            self.track_trait_write(key, value.len() as u64);
        }
        result
    }

    async fn put_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), StorageError> {
        self.metrics
            .total_writes
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        let sizes: Vec<(Vec<u8>, u64)> = entries
            .iter()
            .map(|(k, v)| (k.clone(), v.len() as u64))
            .collect();
        self.inner.put_batch(entries).await?;
        for (key, bytes) in sizes {
            self.track_trait_write(&key, bytes);
        }
        Ok(())
    }

    async fn put_many(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), StorageError> {
        self.metrics
            .total_writes
            .fetch_add(items.len() as u64, Ordering::Relaxed);
        self.inner.put_many(items).await
    }

    async fn get_many(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
        self.metrics
            .total_reads
            .fetch_add(keys.len() as u64, Ordering::Relaxed);
        self.inner.get_many(keys).await
    }

    async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        let start = std::time::Instant::now();
        let result = self.inner.delete(key).await;
        crate::prometheus_metrics::OPERATION_DURATION
            .with_label_values(&["local", "delete"])
            .observe(start.elapsed().as_secs_f64());
        result
    }

    async fn delete_many(&self, keys: Vec<Vec<u8>>) -> Result<(), StorageError> {
        self.inner.delete_many(keys).await
    }

    async fn flush(&self) -> Result<(), StorageError> {
        StorageAdapter::flush(self.inner.as_ref()).await
    }

    /// Until Task 2.19 the outbox is the inner adapter's memory-only map, as for
    /// `WalStorageAdapter`; it is no longer dropped.
    async fn outbox_save(&self, id: &str, payload: &[u8]) -> Result<(), StorageError> {
        self.inner.outbox_save(id, payload).await
    }

    async fn outbox_list(&self) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        self.inner.outbox_list().await
    }

    async fn outbox_remove(&self, id: &str) -> Result<(), StorageError> {
        self.inner.outbox_remove(id).await
    }

    /// One frame for the record and its outbox entry, so atomic.
    async fn put_with_outbox(
        &self,
        key: &[u8],
        value: &[u8],
        outbox_id: &str,
        outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        self.metrics.total_writes.fetch_add(1, Ordering::Relaxed);
        self.inner
            .put_with_outbox(key, value, outbox_id, outbox_payload)
            .await?;
        self.track_trait_write(key, value.len() as u64);
        Ok(())
    }

    async fn delete_with_outbox(
        &self,
        key: &[u8],
        outbox_id: &str,
        outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        self.inner
            .delete_with_outbox(key, outbox_id, outbox_payload)
            .await
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        self.inner.scan_prefix(prefix).await
    }

    async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        self.inner.scan_range(start, end).await
    }

    /// Every change after `offset`, across collections, in commit order: one log has one
    /// order, so the cursor is unambiguous (the old refusal, spec S-09, is gone).
    async fn get_changes_since(&self, offset: u64) -> Result<Vec<Change>, StorageError> {
        self.inner.get_changes_since(offset).await
    }

    /// The changes after `offset` (the global log cursor) whose keys belong to
    /// `collection`: those under `collection_key(collection, b"")`, the same helper that
    /// builds every routing-API key. Selected by that prefix, never by parsing a key. An
    /// unknown collection has no changes; an empty name means every collection (the bare
    /// cursor, as the trait default and `FetchSegment` define it).
    async fn changes_in_collection(
        &self,
        collection: &str,
        offset: u64,
    ) -> Result<Vec<Change>, StorageError> {
        if collection.is_empty() {
            return self.inner.get_changes_since(offset).await;
        }
        let prefix = self.collection_key(collection, b"")?;
        Ok(self
            .inner
            .get_changes_since(offset)
            .await?
            .into_iter()
            .filter(|change| match change {
                Change::Put { key, .. } | Change::Delete { key, .. } => key.starts_with(&prefix),
            })
            .collect())
    }

    async fn take_snapshot(
        &self,
        path: PathBuf,
        compression: CompressionType,
    ) -> Result<u64, StorageError> {
        StorageAdapter::take_snapshot(self.inner.as_ref(), path, compression).await
    }

    /// The one writer's health.
    fn write_path_health(&self) -> WritePathHealth {
        self.inner.write_path_health()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prkdb_types::replication::Change;

    /// D11: every collection lands in the one WAL at the directory root, in one order.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_partitioned_directory_has_one_wal() {
        let dir = tempfile::tempdir().unwrap();
        let db = CollectionPartitionedAdapter::new(WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        })
        .unwrap();
        db.put_to_collection("users", b"1", b"alice").await.unwrap();
        db.put(b"orders:1", b"book").await.unwrap();
        db.put_to_collection("invoices", b"1", b"paid")
            .await
            .unwrap();
        db.put_with_outbox(b"users:2", b"bob", "users:0:1", b"event")
            .await
            .unwrap();
        db.flush().await.unwrap();

        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert!(
            entries.iter().all(|p| !p.is_dir()),
            "no per-collection directories: {entries:?}"
        );
        assert!(
            entries
                .iter()
                .any(|p| p.extension().is_some_and(|e| e == "wal")),
            "{entries:?}"
        );

        let keys: Vec<Vec<u8>> = db
            .get_changes_since(0)
            .await
            .unwrap()
            .into_iter()
            .map(|c| match c {
                Change::Put { key, .. } | Change::Delete { key, .. } => key,
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                b"users:1".to_vec(),
                b"orders:1".to_vec(),
                b"invoices:1".to_vec(),
                b"users:2".to_vec()
            ],
            "one global commit order across collections"
        );
        assert_eq!(
            db.outbox_list().await.unwrap(),
            vec![("users:0:1".to_string(), b"event".to_vec())]
        );
    }

    /// The pre-D11 layout is refused, not opened as an empty database.
    #[test]
    fn a_per_collection_layout_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("collections/users")).unwrap();
        let err = CollectionPartitionedAdapter::new(WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        })
        .err()
        .expect("must refuse");
        assert!(err.to_string().contains("format 1"), "{err}");
    }

    /// `changes_in_collection` selects a collection's changes by the key `collection_key`
    /// builds, from the one log: not a collection whose name merely starts the same, and
    /// every collection for the empty name.
    #[tokio::test(flavor = "multi_thread")]
    async fn changes_in_collection_selects_by_the_collection_key() {
        let dir = tempfile::tempdir().unwrap();
        let db = CollectionPartitionedAdapter::new(WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        })
        .unwrap();
        db.put_to_collection("users", b"1", b"a").await.unwrap();
        db.put_to_collection("users_archive", b"1", b"b")
            .await
            .unwrap();
        db.delete_from_collection("users", b"1").await.unwrap();

        let keys = |changes: Vec<Change>| -> Vec<Vec<u8>> {
            changes
                .into_iter()
                .map(|c| match c {
                    Change::Put { key, .. } | Change::Delete { key, .. } => key,
                })
                .collect()
        };
        assert_eq!(
            keys(db.changes_in_collection("users", 0).await.unwrap()),
            vec![b"users:1".to_vec(), b"users:1".to_vec()]
        );
        assert_eq!(
            keys(db.changes_in_collection("", 0).await.unwrap()).len(),
            3,
            "the empty name is the bare cursor"
        );
        assert!(db
            .changes_in_collection("nonexistent", 0)
            .await
            .unwrap()
            .is_empty());
    }

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

    /// `collection_names` lists the collections written through the routing API, sorted
    /// and without duplicates. (It replaced `collection_names_on_disk`, which listed
    /// per-collection directories; there are none since D11.)
    #[tokio::test(flavor = "multi_thread")]
    async fn collection_names_lists_what_was_written() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = CollectionPartitionedAdapter::new(config).unwrap();
        assert!(adapter.collection_names().is_empty());

        adapter
            .put_to_collection("users", b"a", b"1")
            .await
            .unwrap();
        adapter
            .put_to_collection("orders", b"b", b"2")
            .await
            .unwrap();
        adapter
            .put_to_collection("users", b"c", b"3")
            .await
            .unwrap();

        assert_eq!(
            adapter.collection_names(),
            vec!["orders".to_string(), "users".to_string()]
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

    /// The outbox is the inner adapter's: a saved entry is listed, and removing it removes
    /// it. (Before D11 this adapter's outbox was a stub that dropped every entry.)
    #[tokio::test(flavor = "multi_thread")]
    async fn the_outbox_is_the_inner_adapters() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = CollectionPartitionedAdapter::new(config).unwrap();

        adapter.outbox_save("id-1", b"payload").await.unwrap();
        assert_eq!(
            adapter.outbox_list().await.unwrap(),
            vec![("id-1".to_string(), b"payload".to_vec())]
        );
        adapter.outbox_remove("id-1").await.unwrap();
        assert!(adapter.outbox_list().await.unwrap().is_empty());
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

    /// A prefix is a byte prefix over whole keys: a partial collection name selects the
    /// keys it prefixes and no others.
    ///
    /// Before D11 this adapter routed a colon-less prefix by matching collection names, and
    /// an inverted guard there survived the suite (run 31362753534, shard 8). The routing
    /// is gone; the behaviour it had to reproduce is still what callers rely on.
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

    /// A range spanning two collections must return rows from both, sorted and half-open.
    ///
    /// Before D11 this adapter narrowed a range to one collection when both bounds named
    /// the same one, and a guard forced to `true` dropped every row of the second
    /// collection (run 31362753534, shard 8). The narrowing is gone with the per-collection
    /// logs; this keeps the cross-collection shape covered.
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

    /// `flush` must forward to the one WAL, and report a failure rather than swallow it.
    ///
    /// Replacing this method's whole body with `Ok(())` survived the entire suite once (run
    /// 31358158012, shard 7): a value survives a reopen whether or not `flush` ran, so
    /// only an inner flush that fails can tell a wrapper that forwards from one that does
    /// not. `wal_adapter::fault_injection` provides that failure, keyed by the WAL
    /// directory, which since D11 is the data directory root.
    #[tokio::test(flavor = "multi_thread")]
    async fn flush_reports_a_wal_failure_rather_than_swallowing_it() {
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
            .expect("flush succeeds while the WAL is healthy");

        let failing = temp_dir.path().to_path_buf();
        fault_injection::fail_flush_at(&failing);

        let outcome = adapter.flush().await;

        // Clear before asserting, so a panic does not leave the fault set for the drop
        // path — the adapter flushes as its last handle goes away.
        fault_injection::clear_flush_failure(&failing);

        let err = outcome.expect_err("flush must report the WAL's own flush failure, not Ok");
        assert!(
            err.to_string().contains("injected flush failure"),
            "the error must be the inner failure, not something invented: {err}"
        );

        adapter
            .flush()
            .await
            .expect("flush succeeds again once the injected fault is cleared");
    }

    use std::time::Duration;

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

    /// One writer, one health: a stalled WAL makes the adapter unhealthy, with the queued
    /// write counted and a reason given.
    ///
    /// Before D11 the adapter folded one health report per collection into one; mutation
    /// run 31539366718 found that fold's arithmetic and its `unhealthy.is_empty()` guard
    /// untested. The fold is gone, and this pins the forwarding that replaced it: an
    /// adapter that reported healthy while its writer is stuck keeps `/readyz` routing
    /// traffic to a node whose writes are not being confirmed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stalled_wal_makes_the_partitioned_adapter_unhealthy() {
        use crate::storage::wal_adapter::fault_injection::StallGuard;

        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: temp_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let adapter = Arc::new(CollectionPartitionedAdapter::new(config).unwrap());

        adapter
            .put_to_collection("users", b"seed", b"v")
            .await
            .unwrap();
        let healthy = adapter.write_path_health();
        assert!(
            healthy.healthy && healthy.queue_depth == 0,
            "a freshly opened adapter must be healthy, or the assertions below prove \
             nothing: {healthy:?}"
        );

        let _stall = StallGuard::new(temp_dir.path());
        let _stalled = {
            let adapter = adapter.clone();
            tokio::spawn(async move { adapter.put_to_collection("orders", b"queued", b"v").await })
        };

        wait_until(
            "the stalled WAL to be declared unhealthy",
            Duration::from_secs(15),
            || !adapter.write_path_health().healthy,
        )
        .await;

        let health = adapter.write_path_health();
        assert!(
            !health.healthy,
            "a stalled writer means the node is not ready"
        );
        assert_eq!(health.queue_depth, 1, "the one queued write: {health:?}");
        assert!(
            health.reason.is_some(),
            "an unhealthy adapter must name the cause: {health:?}"
        );
    }
}
