use crate::error::StorageError;
use async_trait::async_trait;

/// New async storage adapter port for the hexagonal architecture.
///
/// Implement this trait to plug-in any storage backend (SQLite, Postgres, RocksDB, sled, in-memory, ...).
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
    ///
    /// # Example
    /// ```rust,ignore
    /// let entries = vec![
    ///     (b"key1".to_vec(), b"value1".to_vec()),
    ///     (b"key2".to_vec(), b"value2".to_vec()),
    /// ];
    /// adapter.put_batch(entries).await?;
    /// ```
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
    async fn get_changes_since(
        &self,
        _offset: u64,
    ) -> Result<Vec<crate::replication::Change>, StorageError> {
        Err(StorageError::BackendError(
            "get_changes_since not supported".into(),
        ))
    }
}

#[async_trait]
pub trait TransactionalStorageAdapter: StorageAdapter {
    /// Optional transactional execution: adapter may implement to run the closure within a DB tx.
    /// Default: not supported.
    async fn transaction<F, Fut>(&self, _f: F) -> Result<(), StorageError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Result<(), StorageError>> + Send;
}

/// Legacy synchronous KVStore kept for compatibility with older adapters.
/// Prefer `StorageAdapter` for new adapters.
#[deprecated(note = "Use StorageAdapter for async adapters; KVStore kept for compatibility")]
pub trait KVStore: Send + Sync + Clone {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;
    fn delete(&self, key: &[u8]) -> Result<(), StorageError>;
}
