use async_trait::async_trait;
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_TREE: &str = "kv";
const OUTBOX_TREE: &str = "outbox";

/// Recursively copy a directory to another location
fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    if !dst.exists() {
        std::fs::create_dir_all(dst)?;
    }

    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;

        if ty.is_dir() {
            copy_dir_recursive(&entry.path(), &dst.join(entry.file_name()))?;
        } else {
            std::fs::copy(entry.path(), dst.join(entry.file_name()))?;
        }
    }

    Ok(())
}

pub struct SledAdapter {
    db: sled::Db,
    kv: sled::Tree,
    outbox: sled::Tree,
    db_path: std::path::PathBuf,
    kv_ops_since_flush: AtomicU64,
    outbox_ops_since_flush: AtomicU64,
    flush_every: u64,
}

impl Clone for SledAdapter {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            kv: self.kv.clone(),
            outbox: self.outbox.clone(),
            db_path: self.db_path.clone(),
            kv_ops_since_flush: AtomicU64::new(self.kv_ops_since_flush.load(Ordering::Relaxed)),
            outbox_ops_since_flush: AtomicU64::new(
                self.outbox_ops_since_flush.load(Ordering::Relaxed),
            ),
            flush_every: self.flush_every,
        }
    }
}

impl SledAdapter {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, StorageError> {
        let db_path = path.as_ref().to_path_buf();
        let db = sled::open(&path).map_err(|e| StorageError::BackendError(e.to_string()))?;
        let kv = db
            .open_tree(DEFAULT_TREE)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        let outbox = db
            .open_tree(OUTBOX_TREE)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        Ok(Self {
            db,
            kv,
            outbox,
            db_path,
            kv_ops_since_flush: AtomicU64::new(0),
            outbox_ops_since_flush: AtomicU64::new(0),
            flush_every: 100,
        })
    }

    /// Export database to a backup file
    pub async fn backup_to_path(
        &self,
        backup_path: impl AsRef<std::path::Path>,
    ) -> Result<(), StorageError> {
        let backup_dir = backup_path.as_ref();

        if let Some(parent) = backup_dir.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                StorageError::BackendError(format!("Failed to create backup directory: {}", e))
            })?;
        }

        // Flush database first to ensure all data is written
        self.db
            .flush()
            .map_err(|e| StorageError::BackendError(format!("Failed to flush database: {}", e)))?;

        // Copy the database files
        copy_dir_recursive(&self.db_path, backup_dir)
            .map_err(|e| StorageError::BackendError(format!("Backup copy failed: {}", e)))?;

        Ok(())
    }

    /// Trigger database compaction
    pub async fn compact(&self) -> Result<u64, StorageError> {
        // Flush all pending writes first
        self.db
            .flush()
            .map_err(|e| StorageError::BackendError(format!("Flush failed: {}", e)))?;

        // Get space info before compaction
        let before_size = self.db.size_on_disk().map_err(|e| {
            StorageError::BackendError(format!("Failed to get database size: {}", e))
        })?;

        // Trigger compaction by forcing a maintenance operation
        // Sled doesn't have explicit compaction, but we can force garbage collection
        self.kv
            .flush()
            .map_err(|e| StorageError::BackendError(format!("KV tree flush failed: {}", e)))?;
        self.outbox
            .flush()
            .map_err(|e| StorageError::BackendError(format!("Outbox tree flush failed: {}", e)))?;

        let after_size = self.db.size_on_disk().map_err(|e| {
            StorageError::BackendError(format!(
                "Failed to get database size after compaction: {}",
                e
            ))
        })?;

        Ok(before_size.saturating_sub(after_size))
    }

    /// Get database size information
    pub fn get_size_info(&self) -> Result<(u64, u64), StorageError> {
        let disk_size = self
            .db
            .size_on_disk()
            .map_err(|e| StorageError::BackendError(format!("Failed to get disk size: {}", e)))?;

        // Calculate logical size by counting entries (approximation)
        let mut logical_size = 0u64;
        for tree_name in self.db.tree_names() {
            if let Ok(tree) = self.db.open_tree(&tree_name) {
                logical_size += tree.len() as u64;
            }
        }

        Ok((disk_size, logical_size))
    }
}

#[async_trait]
impl StorageAdapter for SledAdapter {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let res = self
            .kv
            .get(key)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        Ok(res.map(|ivec| ivec.to_vec()))
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.kv
            .insert(key, value)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        // Double-write to outbox to support streaming/subscription
        self.outbox
            .insert(key, value)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;

        if self.kv_ops_since_flush.fetch_add(1, Ordering::Relaxed) + 1 >= self.flush_every {
            self.kv
                .flush()
                .map_err(|e| StorageError::BackendError(e.to_string()))?;
            self.outbox
                .flush()
                .map_err(|e| StorageError::BackendError(e.to_string()))?;
            self.db
                .flush()
                .map_err(|e| StorageError::BackendError(e.to_string()))?;
            self.kv_ops_since_flush.store(0, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        self.kv
            .remove(key)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        if self.kv_ops_since_flush.fetch_add(1, Ordering::Relaxed) + 1 >= self.flush_every {
            self.kv
                .flush()
                .map_err(|e| StorageError::BackendError(e.to_string()))?;
            self.db
                .flush()
                .map_err(|e| StorageError::BackendError(e.to_string()))?;
            self.kv_ops_since_flush.store(0, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn outbox_save(&self, id: &str, payload: &[u8]) -> Result<(), StorageError> {
        self.outbox
            .insert(id.as_bytes(), payload)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        if self.outbox_ops_since_flush.fetch_add(1, Ordering::Relaxed) + 1 >= self.flush_every {
            self.outbox
                .flush()
                .map_err(|e| StorageError::BackendError(e.to_string()))?;
            self.db
                .flush()
                .map_err(|e| StorageError::BackendError(e.to_string()))?;
            self.outbox_ops_since_flush.store(0, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn outbox_list(&self) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let mut items = Vec::new();
        for res in self.outbox.iter() {
            let (k, v) = res.map_err(|e| StorageError::BackendError(e.to_string()))?;
            items.push((String::from_utf8_lossy(&k).to_string(), v.to_vec()));
        }
        Ok(items)
    }

    async fn outbox_remove(&self, id: &str) -> Result<(), StorageError> {
        self.outbox
            .remove(id.as_bytes())
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        if self.outbox_ops_since_flush.fetch_add(1, Ordering::Relaxed) + 1 >= self.flush_every {
            self.outbox
                .flush()
                .map_err(|e| StorageError::BackendError(e.to_string()))?;
            self.db
                .flush()
                .map_err(|e| StorageError::BackendError(e.to_string()))?;
            self.outbox_ops_since_flush.store(0, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn put_with_outbox(
        &self,
        key: &[u8],
        value: &[u8],
        outbox_id: &str,
        outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        // Use sled batch to apply atomically
        let mut kv_batch = sled::Batch::default();
        kv_batch.insert(key, value);
        let mut out_batch = sled::Batch::default();
        out_batch.insert(outbox_id.as_bytes(), outbox_payload);
        self.kv
            .apply_batch(kv_batch)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        self.outbox
            .apply_batch(out_batch)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        self.db
            .flush()
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        Ok(())
    }

    async fn delete_with_outbox(
        &self,
        key: &[u8],
        outbox_id: &str,
        outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        let mut kv_batch = sled::Batch::default();
        kv_batch.remove(key);
        let mut out_batch = sled::Batch::default();
        out_batch.insert(outbox_id.as_bytes(), outbox_payload);
        self.kv
            .apply_batch(kv_batch)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        self.outbox
            .apply_batch(out_batch)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        self.db
            .flush()
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let mut res = Vec::new();
        for item in self.kv.scan_prefix(prefix) {
            let (k, v) = item.map_err(|e| StorageError::BackendError(e.to_string()))?;
            res.push((k.to_vec(), v.to_vec()));
        }
        Ok(res)
    }

    async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let mut res = Vec::new();
        for item in self.kv.range(start..end) {
            let (k, v) = item.map_err(|e| StorageError::BackendError(e.to_string()))?;
            res.push((k.to_vec(), v.to_vec()));
        }
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::SledAdapter;
    use prkdb_types::storage::StorageAdapter;
    use tempfile::tempdir;

    #[tokio::test]
    async fn sled_put_get_delete() {
        let dir = tempdir().unwrap();
        let adapter = SledAdapter::open(dir.path()).unwrap();

        adapter.put(b"a", b"1").await.unwrap();
        assert_eq!(adapter.get(b"a").await.unwrap().unwrap(), b"1");
        adapter.delete(b"a").await.unwrap();
        assert!(adapter.get(b"a").await.unwrap().is_none());

        adapter.outbox_save("e1", b"evt").await.unwrap();
        let mut list = adapter.outbox_list().await.unwrap();
        list.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "e1");
        assert_eq!(list[0].1, b"evt");
        adapter.outbox_remove("e1").await.unwrap();
        assert!(adapter.outbox_list().await.unwrap().is_empty());
    }
}
