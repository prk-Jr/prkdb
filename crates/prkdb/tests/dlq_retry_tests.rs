use prkdb::compute::Context;
use prkdb::outbox::{
    dlq_is_empty_for, drain_dlq_for, make_dlq_id_for_type, save_dlq_event_for, DlqRecord,
    OutboxRecord,
};
use prkdb::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Collection, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct Ev {
    #[id]
    id: u64,
}

struct FlakyHandler {
    fails: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ComputeHandler<Ev, PrkDb> for FlakyHandler {
    async fn on_put(
        &self,
        _item: &Ev,
        _ctx: &Context<PrkDb>,
    ) -> Result<(), prkdb_types::error::ComputeError> {
        if self.fails.fetch_sub(1, Ordering::SeqCst) > 0 {
            return Err(prkdb_types::error::ComputeError::Handler("fail".into()));
        }
        Ok(())
    }
    async fn on_delete(
        &self,
        _id: &u64,
        _ctx: &Context<PrkDb>,
    ) -> Result<(), prkdb_types::error::ComputeError> {
        Ok(())
    }
}

#[tokio::test]
async fn dlq_retry_flow() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<Ev>()
        .with_compute_handler::<Ev, _>(FlakyHandler {
            fails: Arc::new(AtomicUsize::new(2)),
        })
        .build()
        .unwrap();

    // Put an event, then manually place a DLQ record simulating a previous failure
    db.collection::<Ev>().put(Ev { id: 1 }).await.unwrap();
    let dlq_id = make_dlq_id_for_type::<Ev>();
    let rec = DlqRecord {
        event: OutboxRecord::Put(Ev { id: 1 }),
        attempts: 0,
        last_error: None,
    };
    save_dlq_event_for::<Ev>(&db, &dlq_id, &rec).await.unwrap();

    // Retry twice; first will re-enqueue with attempts=1, second should succeed and remove
    drain_dlq_for::<Ev>(&db, 3).await.unwrap();
    drain_dlq_for::<Ev>(&db, 3).await.unwrap();

    // Ensure outbox (DLQ entries for Ev) is empty now
    assert!(dlq_is_empty_for::<Ev>(&db).await.unwrap());
}
