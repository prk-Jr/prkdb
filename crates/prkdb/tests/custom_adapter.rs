use async_trait::async_trait;
use prkdb_macros::Collection;
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone, Default)]
struct MemoryAdapter {
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
        self.inner
            .data
            .write()
            .await
            .insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        self.inner.data.write().await.remove(key);
        Ok(())
    }

    async fn outbox_save(&self, id: &str, payload: &[u8]) -> Result<(), StorageError> {
        self.inner
            .outbox
            .write()
            .await
            .push((id.to_string(), payload.to_vec()));
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
        let mut data = self.inner.data.write().await;
        let mut ob = self.inner.outbox.write().await;
        data.insert(key.to_vec(), value.to_vec());
        ob.push((outbox_id.to_string(), outbox_payload.to_vec()));
        Ok(())
    }
}

#[derive(Collection, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Widget {
    #[id]
    id: u64,
    name: String,
}

#[tokio::test]
async fn custom_adapter_put_get_and_outbox() {
    let adapter = MemoryAdapter::default();
    let db = prkdb::PrkDb::builder()
        .with_storage(adapter.clone())
        .register_collection::<Widget>()
        .build()
        .expect("build db");

    let col = db.collection::<Widget>();
    col.put(Widget {
        id: 7,
        name: "hello".into(),
    })
    .await
    .expect("put");

    let got = col.get(&7).await.expect("get result").expect("found");
    assert_eq!(got.name, "hello");

    let outbox = adapter.outbox_list().await.expect("outbox list");
    assert_eq!(outbox.len(), 1);
    assert!(
        outbox[0].0.contains("Widget"),
        "outbox id should include type hint"
    );
}
