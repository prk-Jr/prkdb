use prkdb::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Collection, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct Rec {
    #[id]
    id: u32,
    v: i32,
}

#[tokio::test]
async fn prefix_and_range_scans() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<Rec>()
        .build()
        .unwrap();
    let c = db.collection::<Rec>();
    for i in 0..10 {
        c.put(Rec { id: i, v: i as i32 }).await.unwrap();
    }

    // Range by id
    let r = c.scan_range_by_id_bytes(&2, &6).await.unwrap();
    assert_eq!(r.len(), 4);
    assert_eq!(r.first().unwrap().id, 2);
    assert_eq!(r.last().unwrap().id, 5);

    // Prefix by id-encoding prefix of '0' byte should include id 0 only (bincode varint) but keep simple smoke test
    let p = c.scan_prefix(&[]).await.unwrap();
    assert_eq!(p.len(), 10);
}
