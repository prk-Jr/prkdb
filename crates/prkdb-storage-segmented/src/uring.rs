//! Cross-platform placeholder for an io_uring-backed adapter.
//! Currently delegates to the buffered `SegmentedLogAdapter` so the `uring` feature builds everywhere.

use crate::SegmentedLogAdapter;
use async_trait::async_trait;
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;
use std::sync::Arc;

#[derive(Clone)]
#[allow(dead_code)]
pub struct UringSegmentedAdapter {
    inner: Arc<SegmentedLogAdapter>,
}

impl UringSegmentedAdapter {
    pub async fn new<P: AsRef<std::path::Path>>(
        dir: P,
        segment_size: u64,
        max_segments: Option<usize>,
        checkpoint_every: u64,
    ) -> Result<Self, StorageError> {
        let inner =
            SegmentedLogAdapter::new(dir, segment_size, max_segments, checkpoint_every).await?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    pub async fn flush(&self) -> Result<(), StorageError> {
        self.inner.flush().await
    }
}

#[async_trait]
impl StorageAdapter for UringSegmentedAdapter {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.get(key).await
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.inner.put(key, value).await
    }

    async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        self.inner.delete(key).await
    }

    async fn outbox_save(&self, id: &str, payload: &[u8]) -> Result<(), StorageError> {
        self.inner.outbox_save(id, payload).await
    }

    async fn outbox_list(&self) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        self.inner.outbox_list().await
    }

    async fn outbox_remove(&self, id: &str) -> Result<(), StorageError> {
        self.inner.outbox_remove(id).await
    }

    async fn put_with_outbox(
        &self,
        key: &[u8],
        value: &[u8],
        outbox_id: &str,
        outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        self.inner
            .put_with_outbox(key, value, outbox_id, outbox_payload)
            .await
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

    async fn migrate_table(&self, ddl: &str) -> Result<(), StorageError> {
        self.inner.migrate_table(ddl).await
    }
}
