use prkdb::compute::{Context, StatefulCompute};
use prkdb::outbox::{load_state, replay_collection};
use prkdb::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Collection, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct Event {
    #[id]
    id: u64,
    delta: i64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Default)]
struct SumState {
    total: i64,
}

struct SumHandler;

#[async_trait::async_trait]
impl StatefulCompute<Event, PrkDb> for SumHandler {
    type State = SumState;

    fn state_key(&self) -> String {
        "sum".to_string()
    }
    fn init_state(&self) -> Self::State {
        SumState::default()
    }

    async fn on_put(
        &self,
        item: &Event,
        state: &mut Self::State,
        _ctx: &Context<PrkDb>,
    ) -> Result<(), prkdb_core::error::ComputeError> {
        state.total += item.delta;
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
async fn stateful_replay_builds_sum() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<Event>()
        .build()
        .unwrap();

    let events = db.collection::<Event>();
    events.put(Event { id: 1, delta: 2 }).await.unwrap();
    events.put(Event { id: 2, delta: 3 }).await.unwrap();
    events.put(Event { id: 3, delta: -1 }).await.unwrap();

    let handler = SumHandler;
    replay_collection::<Event, _>(&db, &handler).await.unwrap();
    let state = load_state::<_, Event>(&db, &handler).await.unwrap();
    assert_eq!(state.total, 4);
}
