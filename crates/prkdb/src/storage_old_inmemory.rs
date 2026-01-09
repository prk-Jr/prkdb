use dashmap::DashMap;
use prkdb_core::error::StorageError;
use prkdb_core::storage::StorageAdapter;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Simple in-memory async StorageAdapter. Great for tests and examples.
///
/// Not meant for production persistence, but supports outbox APIs for feature parity.
#[derive(Clone, Debug)]
pub struct InMemoryAdapter {
    inner: Arc<RwLock<DashMap<Vec<u8>, Vec<u8>>>>,
    outbox: Arc<RwLock<DashMap<String, Vec<u8>>>>,
}

impl InMemoryAdapter {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(DashMap::new())),
            outbox: Arc::new(RwLock::new(DashMap::new())),
        }
    }
}

impl Default for InMemoryAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl StorageAdapter for InMemoryAdapter {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let m = self.inner.read().await;
        Ok(m.get(key).map(|v| v.value().clone()))
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let m = self.inner.write().await;
        m.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        let m = self.inner.write().await;
        m.remove(key);
        Ok(())
    }

    async fn outbox_save(&self, id: &str, payload: &[u8]) -> Result<(), StorageError> {
        let o = self.outbox.write().await;
        o.insert(id.to_string(), payload.to_vec());
        Ok(())
    }

    async fn outbox_list(&self) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let o = self.outbox.read().await;
        Ok(o.iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect())
    }

    async fn outbox_remove(&self, id: &str) -> Result<(), StorageError> {
        let o = self.outbox.write().await;
        o.remove(id);
        Ok(())
    }

    async fn put_with_outbox(
        &self,
        key: &[u8],
        value: &[u8],
        outbox_id: &str,
        outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        // Best-effort atomicity under a single write lock
        let m = self.inner.write().await;
        let o = self.outbox.write().await;
        m.insert(key.to_vec(), value.to_vec());
        o.insert(outbox_id.to_string(), outbox_payload.to_vec());
        Ok(())
    }

    async fn delete_with_outbox(
        &self,
        key: &[u8],
        outbox_id: &str,
        outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        let m = self.inner.write().await;
        let o = self.outbox.write().await;
        m.remove(key);
        o.insert(outbox_id.to_string(), outbox_payload.to_vec());
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let m = self.inner.read().await;
        let mut res = Vec::new();
        for entry in m.iter() {
            if entry.key().starts_with(prefix) {
                res.push((entry.key().clone(), entry.value().clone()));
            }
        }
        res.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(res)
    }

    async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let m = self.inner.read().await;
        let mut res = Vec::new();
        for entry in m.iter() {
            if entry.key().as_slice() >= start && entry.key().as_slice() < end {
                res.push((entry.key().clone(), entry.value().clone()));
            }
        }
        res.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(res)
    }

    /// Optimized bulk put: acquire write lock once for all operations
    async fn put_many(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), StorageError> {
        let m = self.inner.write().await;
        for (key, value) in items {
            m.insert(key, value);
        }
        Ok(())
    }

    /// Optimized bulk get: acquire read lock once for all operations
    async fn get_many(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
        let m = self.inner.read().await;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(m.get(&key).map(|v| v.value().clone()));
        }
        Ok(results)
    }

    /// Optimized bulk delete: acquire write lock once for all operations
    async fn delete_many(&self, keys: Vec<Vec<u8>>) -> Result<(), StorageError> {
        let m = self.inner.write().await;
        for key in keys {
            m.remove(&key);
        }
        Ok(())
    }
}
