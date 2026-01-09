use prkdb::compute::{ComputeHandler, Context};
use prkdb::{prelude::*, storage::InMemoryAdapter};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct User {
    #[id]
    id: u64,
    name: String,
}

impl User {
    fn new(id: u64, name: &str) -> Self {
        Self {
            id,
            name: name.into(),
        }
    }
}

struct CounterHandler {
    puts: Arc<Mutex<u64>>,
    dels: Arc<Mutex<u64>>,
}

#[async_trait::async_trait]
impl ComputeHandler<User, PrkDb> for CounterHandler {
    async fn on_put(
        &self,
        _item: &User,
        _ctx: &Context<PrkDb>,
    ) -> Result<(), prkdb_types::error::ComputeError> {
        *self.puts.lock().unwrap() += 1;
        Ok(())
    }
    async fn on_delete(
        &self,
        _id: &u64,
        _ctx: &Context<PrkDb>,
    ) -> Result<(), prkdb_types::error::ComputeError> {
        *self.dels.lock().unwrap() += 1;
        Ok(())
    }
}

#[tokio::test]
async fn cdc_outbox_and_drain_publish() {
    let puts = Arc::new(Mutex::new(0u64));
    let dels = Arc::new(Mutex::new(0u64));
    let handler = CounterHandler {
        puts: puts.clone(),
        dels: dels.clone(),
    };

    let db = Builder::new()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<User>()
        .with_compute_handler::<User, _>(handler)
        .build()
        .unwrap();

    let coll = db.collection::<User>();

    coll.put(User::new(1, "a")).await.unwrap();
    coll.put(User::new(2, "b")).await.unwrap();
    coll.delete(&1).await.unwrap();

    assert_eq!(*puts.lock().unwrap(), 2);
    assert_eq!(*dels.lock().unwrap(), 1);

    // Drain outbox to publish again (simulates replay)
    prkdb::outbox::drain_outbox_for::<User>(&db).await.unwrap();

    assert_eq!(*puts.lock().unwrap(), 4);
    assert_eq!(*dels.lock().unwrap(), 2);
}
