//! Storage adapter traits for PrkDB.
//!
//! This module defines the core storage abstraction that allows plugging in
//! different storage backends (SQLite, Postgres, RocksDB, sled, in-memory, etc.).

use crate::error::StorageError;
use crate::replication::Change;
use async_trait::async_trait;

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
    /// - Individual puts: ~375 ops/sec
    /// - Batched puts: ~300K+ ops/sec (800x faster!)
    async fn put_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), StorageError> {
        // Default implementation: fall back to individual puts
        for (key, value) in entries {
            self.put(&key, &value).await?;
        }
        Ok(())
    }

    /// Delete a key.
    async fn delete(&self, key: &[u8]) -> Result<(), StorageError>;

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
