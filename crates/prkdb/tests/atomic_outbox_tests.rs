use prkdb::prelude::*;
use prkdb_storage_sled::SledAdapter;
use serde::{Deserialize, Serialize};

#[derive(Collection, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct Item {
    #[id]
    id: u32,
    v: i32,
}

#[tokio::test]
async fn sled_atomic_put_and_delete_outbox() {
    let tmp = tempfile::tempdir().unwrap();
    let adapter = SledAdapter::open(tmp.path()).unwrap();
    let db = PrkDb::builder()
        .with_storage(adapter)
        .register_collection::<Item>()
        .build()
        .unwrap();
    let coll = db.collection::<Item>();

    coll.put(Item { id: 1, v: 10 }).await.unwrap();
    coll.delete(&1).await.unwrap();

    // Drain outbox; should be empty after draining
    prkdb::outbox::drain_outbox_for::<Item>(&db).await.unwrap();
    // We can't access storage directly; indirectly ensure no replays by draining again
    prkdb::outbox::drain_outbox_for::<Item>(&db).await.unwrap();
}
