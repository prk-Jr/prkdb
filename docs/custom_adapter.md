# Writing a custom storage adapter

PrkDB’s persistence port is `prkdb_core::storage::StorageAdapter`. Implement it for any backend (RocksDB, Redis, HTTP service, etc.) and pass it into `PrkDb::builder().with_storage(...)`. The trait is async-first and has optional hooks for migrations and CDC outbox.

## Minimal in-memory adapter (async + outbox)
```rust
use async_trait::async_trait;
use prkdb_core::error::StorageError;
use prkdb_core::storage::StorageAdapter;
use tokio::sync::RwLock;
use std::{collections::HashMap, sync::Arc};

#[derive(Clone, Default)]
pub struct MemoryAdapter {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    data: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
    outbox: RwLock<Vec<(String, Vec<u8>)>>,
}

#[async_trait]
impl StorageAdapter for MemoryAdapter {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.inner.data.read().await.get(key).cloned())
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.inner.data.write().await.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        self.inner.data.write().await.remove(key);
        Ok(())
    }

    // CDC outbox support (used by PrkDB for replication/drains)
    async fn outbox_save(&self, id: &str, payload: &[u8]) -> Result<(), StorageError> {
        self.inner.outbox.write().await.push((id.to_string(), payload.to_vec()));
        Ok(())
    }
    async fn outbox_list(&self) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        Ok(self.inner.outbox.read().await.clone())
    }
    async fn outbox_remove(&self, id: &str) -> Result<(), StorageError> {
        let mut ob = self.inner.outbox.write().await;
        ob.retain(|(k, _)| k != id);
        Ok(())
    }
    async fn put_with_outbox(
        &self,
        key: &[u8],
        value: &[u8],
        outbox_id: &str,
        outbox_payload: &[u8],
    ) -> Result<(), StorageError> {
        // Simple atomic-ish implementation for the demo.
        let mut data = self.inner.data.write().await;
        let mut ob = self.inner.outbox.write().await;
        data.insert(key.to_vec(), value.to_vec());
        ob.push((outbox_id.to_string(), outbox_payload.to_vec()));
        Ok(())
    }
}
```

## Wiring it up
```rust
use prkdb::prelude::*;
use prkdb_macros::Collection;

#[derive(Collection, Clone)]
struct Event {
    #[id]
    id: u64,
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_target(false).init();

    let adapter = MemoryAdapter::default();
    let db = PrkDb::builder()
        .with_storage(adapter.clone())
        .register_collection::<Event>()
        .build()?;

    db.collection::<Event>()
        .put(Event { id: 1, name: "demo".into() })
        .await?;

    let fetched = db.collection::<Event>().get(&1).await?.unwrap();
    tracing::info!("Fetched event: {:?}", fetched);
    tracing::info!("Outbox entries: {:?}", adapter.outbox_list().await?);
    Ok(())
}
```

## Notes
- Implement `scan_prefix`/`scan_range` if your backend supports range queries (used by scans and some admin commands).
- Implement `migrate_table` if your adapter can run schema DDL (SQL backends should do this to support `register_table_collection`).
- `tests/adapter_matrix.rs` shows how multiple adapters are exercised; `tests/custom_adapter.rs` demonstrates plugging a bespoke one.
- Keep the adapter Send + Sync; use async locks (e.g., `tokio::sync::RwLock`) to avoid blocking async runtimes.
