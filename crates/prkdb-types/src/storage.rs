//! Storage adapter traits for PrkDB.
//!
//! This module defines the core storage abstraction that allows plugging in
//! different storage backends (SQLite, Postgres, RocksDB, sled, in-memory, etc.).

use crate::error::StorageError;
use crate::replication::Change;
use async_trait::async_trait;

/// Whether an adapter's write path is still keeping the promises it has accepted.
///
/// # Why this is not just a `bool`
///
/// "Unhealthy" on its own gets an operator no further than the alert did. The three
/// numbers below are what distinguishes the two states that look identical from outside —
/// a database under load with a deep-but-draining queue, and a database whose writer has
/// stopped publishing entirely. The first has a young oldest-write and a moving
/// `last_publish_age_ms`; the second has an oldest-write that only grows.
///
/// Reported by health and readiness probes, so it must be cheap and must never block:
/// every field is read from an atomic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritePathHealth {
    /// False once the writer has exited or been declared stalled.
    pub healthy: bool,
    /// Names the cause when `healthy` is false. `None` when healthy.
    pub reason: Option<String>,
    /// Writes enqueued but not yet published.
    pub queue_depth: u64,
    /// How long the oldest unpublished write has been waiting. `0` when the queue is empty.
    pub oldest_unpublished_age_ms: u64,
    /// Time since the last successful publication. `None` if nothing has ever published.
    pub last_publish_age_ms: Option<u64>,
}

impl WritePathHealth {
    /// The answer for an adapter with no background writer to supervise.
    ///
    /// Adapters that publish synchronously inside `put` have no window in which a write is
    /// accepted but unpublished, so there is nothing for a watchdog to observe and nothing
    /// that can stall. Saying so explicitly is more useful than making every such adapter
    /// invent an answer.
    pub fn not_applicable() -> Self {
        Self {
            healthy: true,
            reason: None,
            queue_depth: 0,
            oldest_unpublished_age_ms: 0,
            last_publish_age_ms: None,
        }
    }
}

/// Async storage adapter port for the hexagonal architecture.
///
/// Implement this trait to plug-in any storage backend (SQLite, Postgres, RocksDB, sled, in-memory, ...).
///
/// # Example
/// ```ignore
/// use prkdb_types::{StorageAdapter, StorageError};
/// use async_trait::async_trait;
///
/// struct InMemoryAdapter {
///     data: std::collections::HashMap<Vec<u8>, Vec<u8>>,
/// }
///
/// #[async_trait]
/// impl StorageAdapter for InMemoryAdapter {
///     async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
///         Ok(self.data.get(key).cloned())
///     }
///     
///     async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
///         // ... implementation
///         Ok(())
///     }
///     
///     async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
///         // ... implementation
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait StorageAdapter: Send + Sync + 'static {
    /// Retrieve bytes for a key.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;

    /// Put bytes under `key`.
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;

    /// Put multiple key-value pairs in a single batch operation
    ///
    /// This is significantly more efficient than calling put() in a loop,
    /// as it batches WAL writes and reduces overhead from channels, locks, etc.
    ///
    /// # Performance
    ///
    /// Measured with `cargo bench -p prkdb --bench storage_bench` on an Apple M3
    /// (2026-08-09): single put **822 ops/sec**, 100-key batch **78.4 K ops/sec** — a
    /// **95x** difference. The shape of the win is the point; the absolute numbers are
    /// hardware-specific.
    ///
    /// The earlier "800x faster" claim was unverified and wrong by roughly a factor of
    /// eight. See `docs/benchmarks/methodology.md`.
    async fn put_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), StorageError> {
        // Default implementation: fall back to individual puts
        for (key, value) in entries {
            self.put(&key, &value).await?;
        }
        Ok(())
    }

    /// Delete a key.
    async fn delete(&self, key: &[u8]) -> Result<(), StorageError>;

    /// Flush pending writes for graceful shutdown.
    async fn flush(&self) -> Result<(), StorageError> {
        Ok(())
    }

    /// Optional: persist an outbox event (id, payload) for later draining.
    async fn outbox_save(&self, _id: &str, _payload: &[u8]) -> Result<(), StorageError> {
        Err(StorageError::BackendError("outbox not supported".into()))
    }

    /// Optional: list outbox entries (id, payload)
    async fn outbox_list(&self) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        Err(StorageError::BackendError("outbox not supported".into()))
    }

    /// Optional: remove an outbox entry by id.
    async fn outbox_remove(&self, _id: &str) -> Result<(), StorageError> {
        Err(StorageError::BackendError("outbox not supported".into()))
    }

    /// Optional: atomically write to primary storage and persist outbox event.
    /// Default: not supported, callers should fall back to non-atomic sequence.
    async fn put_with_outbox(
        &self,
        _key: &[u8],
        _value: &[u8],
        _outbox_id: &str,
        _outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        Err(StorageError::BackendError(
            "atomic put_with_outbox not supported".into(),
        ))
    }

    /// Optional: atomically delete from primary storage and persist outbox event.
    /// Default: not supported, callers should fall back to non-atomic sequence.
    async fn delete_with_outbox(
        &self,
        _key: &[u8],
        _outbox_id: &str,
        _outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        Err(StorageError::BackendError(
            "atomic delete_with_outbox not supported".into(),
        ))
    }

    /// Optional: scan keys by prefix (lexicographic).
    async fn scan_prefix(&self, _prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        Err(StorageError::BackendError(
            "scan_prefix not supported".into(),
        ))
    }

    /// Optional: scan a half-open key range [start, end) (lexicographic).
    async fn scan_range(
        &self,
        _start: &[u8],
        _end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        Err(StorageError::BackendError(
            "scan_range not supported".into(),
        ))
    }

    /// Optional: run a schema migration expressed as raw SQL.
    /// Non-relational adapters can ignore by keeping the default.
    async fn migrate_table(&self, _ddl: &str) -> Result<(), StorageError> {
        Err(StorageError::BackendError(
            "schema migration not supported".into(),
        ))
    }

    /// Bulk put: write multiple key-value pairs at once.
    /// Default implementation falls back to individual puts.
    async fn put_many(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), StorageError> {
        for (key, value) in items {
            self.put(&key, &value).await?;
        }
        Ok(())
    }

    /// Bulk get: retrieve multiple values by keys.
    /// Default implementation falls back to individual gets.
    async fn get_many(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(&key).await?);
        }
        Ok(results)
    }

    /// Bulk delete: remove multiple keys at once.
    /// Default implementation falls back to individual deletes.
    async fn delete_many(&self, keys: Vec<Vec<u8>>) -> Result<(), StorageError> {
        for key in keys {
            self.delete(&key).await?;
        }
        Ok(())
    }

    /// Optional: get changes since a specific offset/version.
    /// Used for replication.
    async fn get_changes_since(&self, _offset: u64) -> Result<Vec<Change>, StorageError> {
        Err(StorageError::BackendError(
            "get_changes_since not supported".into(),
        ))
    }

    /// Changes after `offset` within one collection.
    ///
    /// # Why the collection is part of the cursor
    ///
    /// An adapter that keeps one log per collection numbers each from 1, so `offset` alone
    /// does not identify a position across them — collection `a` offset 5 and collection
    /// `b` offset 5 are unrelated events. Naming the collection makes the pair a real
    /// cursor without needing a cluster-wide sequence, which would mean a log format
    /// change and a write on the hot path.
    ///
    /// The default ignores the collection and defers to
    /// [`get_changes_since`](Self::get_changes_since), which is correct for every adapter
    /// backed by a single log: there is only one, so naming it adds nothing. This is
    /// deliberately a *benign* default rather than one that returns an error — an adapter
    /// that forgets to override it degrades to the single-log answer rather than failing
    /// at runtime.
    async fn changes_in_collection(
        &self,
        _collection: &str,
        offset: u64,
    ) -> Result<Vec<crate::replication::Change>, StorageError> {
        self.get_changes_since(offset).await
    }

    /// Take a full snapshot of the database
    async fn take_snapshot(
        &self,
        _path: std::path::PathBuf,
        _compression: crate::snapshot::CompressionType,
    ) -> Result<u64, StorageError> {
        Err(StorageError::BackendError(
            "take_snapshot not supported".into(),
        ))
    }

    /// State of the adapter's asynchronous write path, for health and readiness probes.
    ///
    /// Synchronous by design and must stay so. A probe that can block is worse than no
    /// probe: it turns a stalled writer into a stalled *health check*, and the orchestrator
    /// reading it cannot tell the difference between "the answer is bad" and "there is no
    /// answer", so it restarts on timeout — which is the one action guaranteed to lose the
    /// queued writes this is trying to report on.
    fn write_path_health(&self) -> WritePathHealth {
        WritePathHealth::not_applicable()
    }
}

/// Extension trait for transactional storage adapters
#[async_trait]
pub trait TransactionalStorageAdapter: StorageAdapter {
    /// Optional transactional execution: adapter may implement to run the closure within a DB tx.
    /// Default: not supported.
    async fn transaction<F, Fut>(&self, _f: F) -> Result<(), StorageError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Result<(), StorageError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::RwLock;

    struct MockAdapter {
        data: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
    }

    impl MockAdapter {
        fn new() -> Self {
            Self {
                data: RwLock::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl StorageAdapter for MockAdapter {
        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.data.read().unwrap().get(key).cloned())
        }

        async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
            self.data
                .write()
                .unwrap()
                .insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
            self.data.write().unwrap().remove(key);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_mock_adapter() {
        let adapter = MockAdapter::new();

        adapter.put(b"key1", b"value1").await.unwrap();
        let val = adapter.get(b"key1").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));

        adapter.delete(b"key1").await.unwrap();
        let val = adapter.get(b"key1").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_put_batch_default() {
        let adapter = MockAdapter::new();

        let entries = vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
        ];
        adapter.put_batch(entries).await.unwrap();

        assert_eq!(adapter.get(b"k1").await.unwrap(), Some(b"v1".to_vec()));
        assert_eq!(adapter.get(b"k2").await.unwrap(), Some(b"v2".to_vec()));
    }
}
