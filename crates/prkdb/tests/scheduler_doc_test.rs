use prkdb::compute::{Context, StatefulCompute};
use prkdb::prelude::*;
use prkdb::scheduler::spawn_snapshot_job;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Collection, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct Evt {
    #[id]
    id: u64,
    v: i32,
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct S {
    n: i32,
}

struct H;

#[async_trait::async_trait]
impl StatefulCompute<Evt, PrkDb> for H {
    type State = S;
    fn state_key(&self) -> String {
        "scheduler_state".to_string()
    }
    fn init_state(&self) -> Self::State {
        S::default()
    }
    async fn on_put(
        &self,
        _item: &Evt,
        s: &mut S,
        _ctx: &Context<PrkDb>,
    ) -> Result<(), prkdb_types::error::ComputeError> {
        s.n += 1;
        Ok(())
    }
    async fn on_delete(
        &self,
        _id: &u64,
        _s: &mut S,
        _ctx: &Context<PrkDb>,
    ) -> Result<(), prkdb_types::error::ComputeError> {
        Ok(())
    }
}

#[tokio::test]
async fn scheduler_spawns() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<Evt>()
        .build()
        .unwrap();
    let _handle = spawn_snapshot_job::<Evt, _>(
        db.clone(),
        H,
        Duration::from_millis(10),
        Duration::from_millis(5),
    );
    // Allow the job to tick at least once
    tokio::time::sleep(Duration::from_millis(25)).await;
}
