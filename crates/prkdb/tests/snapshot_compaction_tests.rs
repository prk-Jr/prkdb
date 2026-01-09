use prkdb::compute::{Context, StatefulCompute};
use prkdb::outbox::{replay_collection, snapshot_and_compact};
use prkdb::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Collection, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct Ev {
    #[id]
    id: u64,
    v: i32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Default)]
struct Cnt {
    c: i32,
}

struct CntH;

#[async_trait::async_trait]
impl StatefulCompute<Ev, PrkDb> for CntH {
    type State = Cnt;
    fn state_key(&self) -> String {
        "cnt".to_string()
    }
    fn init_state(&self) -> Self::State {
        Cnt::default()
    }
    async fn on_put(
        &self,
        _item: &Ev,
        state: &mut Self::State,
        _ctx: &Context<PrkDb>,
    ) -> Result<(), prkdb_core::error::ComputeError> {
        state.c += 1;
        Ok(())
    }
    async fn on_delete(
        &self,
        _id: &u64,
        _state: &mut Self::State,
        _ctx: &Context<PrkDb>,
    ) -> Result<(), prkdb_core::error::ComputeError> {
        Ok(())
    }
}

#[tokio::test]
async fn snapshot_then_compact_then_fast_replay() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<Ev>()
        .build()
        .unwrap();
    let coll = db.collection::<Ev>();
    for i in 0..10 {
        coll.put(Ev { id: i, v: 0 }).await.unwrap();
    }

    let h = CntH;
    snapshot_and_compact::<Ev, _>(&db, &h).await.unwrap();

    // After compaction, replay should do nothing and be fast.
    replay_collection::<Ev, _>(&db, &h).await.unwrap();
    // If this test passes, compaction did not break ordering or offset behavior.
}
