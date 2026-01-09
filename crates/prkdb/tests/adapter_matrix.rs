use prkdb::prelude::*;
use prkdb_storage_segmented::SegmentedLogAdapter;
use prkdb_storage_sled::SledAdapter;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct Sample {
    #[id]
    id: u64,
    data: String,
}

#[tokio::test]
async fn sled_adapter_put_get_delete() {
    let tmp = TempDir::new().expect("tmp dir");
    let storage = SledAdapter::open(tmp.path()).expect("open sled");
    let db = PrkDb::builder()
        .with_storage(storage)
        .register_collection::<Sample>()
        .build()
        .unwrap();

    let coll = db.collection::<Sample>();
    coll.put(Sample {
        id: 1,
        data: "sled".into(),
    })
    .await
    .unwrap();

    assert_eq!(
        coll.get(&1).await.unwrap(),
        Some(Sample {
            id: 1,
            data: "sled".into()
        })
    );

    coll.delete(&1).await.unwrap();
    assert!(coll.get(&1).await.unwrap().is_none());
}

#[tokio::test]
async fn segmented_adapter_put_get_delete() {
    let tmp = TempDir::new().expect("tmp dir");
    let storage = SegmentedLogAdapter::new(tmp.path(), 1024, None, 8)
        .await
        .expect("create segmented adapter");

    // Use storage API directly (outbox is not supported on this adapter).
    storage.put(b"k", b"v").await.unwrap();
    assert_eq!(storage.get(b"k").await.unwrap(), Some(b"v".to_vec()));
    storage.delete(b"k").await.unwrap();
    assert!(storage.get(b"k").await.unwrap().is_none());

    // Outbox defaults should return an error.
    assert!(storage.outbox_save("one", b"payload").await.is_err());
}
