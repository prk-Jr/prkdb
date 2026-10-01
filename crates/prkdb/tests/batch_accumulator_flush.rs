//! STO-07 end to end: a batched CollectionHandle's flush reports storage failures.

use prkdb::prelude::*;
use prkdb::DbError;
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;

struct RefusesWrites;

#[async_trait::async_trait]
impl StorageAdapter for RefusesWrites {
    async fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(None)
    }
    async fn put(&self, _: &[u8], _: &[u8]) -> Result<(), StorageError> {
        Err(StorageError::BackendError("refused".into()))
    }
    async fn delete(&self, _: &[u8]) -> Result<(), StorageError> {
        Ok(())
    }
}

#[derive(Collection, serde::Serialize, serde::Deserialize, Clone, Debug)]
struct Item {
    #[id]
    id: u64,
}

#[tokio::test]
async fn a_batched_handle_flush_reports_failed_writes() {
    let db = PrkDb::builder()
        .with_storage(RefusesWrites)
        .build()
        .unwrap();
    let handle = db
        .collection::<Item>()
        .with_batching(prkdb_core::batch_config::BatchConfig {
            linger_ms: 1,
            max_batch_size: 4,
            ..Default::default()
        });
    handle.put(Item { id: 1 }).await.unwrap(); // accepted into the buffer
    let err = handle
        .flush()
        .await
        .expect_err("the write failed; flush must say so");
    assert!(err.to_string().contains("refused"), "{err}");
}

/// Two clones of one batched handle share its buffer: when the batch holding both writes
/// fails, each clone's flush says so, not only the first to be answered.
#[tokio::test]
async fn every_clone_of_a_batched_handle_sees_the_failure() {
    let db = PrkDb::builder()
        .with_storage(RefusesWrites)
        .build()
        .unwrap();
    let a = db
        .collection::<Item>()
        .with_batching(prkdb_core::batch_config::BatchConfig {
            linger_ms: 1,
            max_batch_size: 4,
            ..Default::default()
        });
    let b = a.clone();
    a.put(Item { id: 1 }).await.unwrap();
    b.put(Item { id: 2 }).await.unwrap();
    let (ra, rb) = tokio::join!(a.flush(), b.flush());
    for r in [ra, rb] {
        let err = r.expect_err("the batch failed; every flush over it must say so");
        // The storage error keeps its variant through the batch executor.
        assert!(
            matches!(err, DbError::Storage(StorageError::BackendError(ref m)) if m.contains("refused")),
            "{err:?}"
        );
    }
}
