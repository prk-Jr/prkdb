use async_trait::async_trait;
use prkdb::compute::Context;
use prkdb::prelude::*;
use prkdb::storage::InMemoryAdapter;
use prkdb::PrkDb;
use prkdb_types::collection::ChangeEvent;
use serde::{Deserialize, Serialize};
use tokio::time::{timeout, Duration};

// --- Test Models ---
#[derive(Collection, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct User {
    #[id]
    id: u32,
    name: String,
}

#[derive(Collection, Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Default)]
struct UserStats {
    #[id]
    id: u32,
    user_count: i64,
}

#[derive(Collection, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct AuditLog {
    #[id]
    id: u64,
    message: String,
}

// --- Test Handlers ---
struct UserCounterHandler;

#[async_trait]
impl ComputeHandler<User, PrkDb> for UserCounterHandler {
    async fn on_put(
        &self,
        _item: &User,
        ctx: &Context<PrkDb>,
    ) -> Result<(), prkdb_types::error::ComputeError> {
        let stats_handle = ctx.db.collection::<UserStats>();

        // Try to fetch stats record, increment user_count and persist.
        let mut stats = stats_handle.get(&1).await.unwrap().unwrap_or_default();
        stats.id = 1;
        stats.user_count += 1;
        stats_handle.put(stats).await.unwrap();
        Ok(())
    }

    async fn on_delete(
        &self,
        _id: &u32,
        ctx: &Context<PrkDb>,
    ) -> Result<(), prkdb_types::error::ComputeError> {
        let stats_handle = ctx.db.collection::<UserStats>();
        if let Ok(Some(mut stats)) = stats_handle.get(&1).await {
            stats.user_count -= 1;
            stats_handle.put(stats).await.unwrap();
        }
        Ok(())
    }
}

struct AuditLoggerHandler;

#[async_trait]
impl ComputeHandler<User, PrkDb> for AuditLoggerHandler {
    async fn on_put(
        &self,
        item: &User,
        ctx: &Context<PrkDb>,
    ) -> Result<(), prkdb_types::error::ComputeError> {
        let log_message = format!("User {} was created/updated.", item.name);
        let log_id = item.id as u64;
        ctx.db
            .collection::<AuditLog>()
            .put(AuditLog {
                id: log_id,
                message: log_message,
            })
            .await
            .unwrap();
        Ok(())
    }
    async fn on_delete(
        &self,
        id: &u32,
        ctx: &Context<PrkDb>,
    ) -> Result<(), prkdb_types::error::ComputeError> {
        let log_message = format!("User with ID {} was deleted.", id);
        let log_id = (*id as u64) + 1_000_000;
        ctx.db
            .collection::<AuditLog>()
            .put(AuditLog {
                id: log_id,
                message: log_message,
            })
            .await
            .unwrap();
        Ok(())
    }
}

// Helper to build a standard DB for tests.
fn build_test_db() -> PrkDb {
    PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<User>()
        .register_collection::<UserStats>()
        .register_collection::<AuditLog>()
        .build()
        .unwrap()
}

// --- Test Cases ---

#[tokio::test]
async fn test_get_non_existent_item_returns_none() {
    let db = build_test_db();
    let users = db.collection::<User>();
    let result = users.get(&999).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_put_and_get() {
    let db = build_test_db();
    let users = db.collection::<User>();
    let user_in = User {
        id: 42,
        name: "Alice".to_string(),
    };

    users.put(user_in.clone()).await.unwrap();
    let maybe_user = users.get(&42).await.unwrap();
    println!("Retrieved: {:?}", maybe_user);
    let user_out = maybe_user.expect("User should exist after put");

    assert_eq!(user_out, user_in);
}

#[tokio::test]
async fn test_delete_removes_item() {
    let db = build_test_db();
    let users = db.collection::<User>();
    let user_in = User {
        id: 42,
        name: "Alice".to_string(),
    };
    users.put(user_in).await.unwrap();

    assert!(users.get(&42).await.unwrap().is_some());
    users.delete(&42).await.unwrap();
    assert!(users.get(&42).await.unwrap().is_none());
}

#[tokio::test]
async fn test_db_put_get() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<User>()
        .build()
        .unwrap();
    let users = db.collection::<User>();
    let user = User {
        id: 42,
        name: "Alice".to_string(),
    };
    users.put(user.clone()).await.unwrap();
    let retrieved = users.get(&42).await.unwrap().unwrap();
    assert_eq!(retrieved, user);
}

#[tokio::test]
async fn test_update_overwrites_existing_item() {
    let db = build_test_db();
    let users = db.collection::<User>();

    users
        .put(User {
            id: 42,
            name: "Alice".to_string(),
        })
        .await
        .unwrap();
    users
        .put(User {
            id: 42,
            name: "Alicia".to_string(),
        })
        .await
        .unwrap();

    let user_out = users
        .get(&42)
        .await
        .unwrap()
        .expect("User should exist after update");
    assert_eq!(user_out.name, "Alicia");
}

#[tokio::test]
async fn test_watch_receives_put_and_update_events() {
    let db = build_test_db();
    let users = db.collection::<User>();
    let mut watcher = users.watch();

    let user_alice = User {
        id: 42,
        name: "Alice".to_string(),
    };
    users.put(user_alice.clone()).await.unwrap();
    let event1 = watcher.recv().await.unwrap();
    assert_eq!(event1, ChangeEvent::Put(user_alice));

    let user_alicia = User {
        id: 42,
        name: "Alicia".to_string(),
    };
    users.put(user_alicia.clone()).await.unwrap();
    let event2 = watcher.recv().await.unwrap();
    assert_eq!(event2, ChangeEvent::Put(user_alicia));
}

#[tokio::test]
async fn test_compute_handler_trigger_on_delete() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<User>()
        .register_collection::<UserStats>()
        .with_compute_handler(UserCounterHandler)
        .build()
        .unwrap();

    let users = db.collection::<User>();
    let stats_handle = db.collection::<UserStats>();

    users
        .put(User {
            id: 1,
            name: "Alice".to_string(),
        })
        .await
        .unwrap();
    users
        .put(User {
            id: 2,
            name: "Bob".to_string(),
        })
        .await
        .unwrap();

    let initial_stats = stats_handle
        .get(&1)
        .await
        .unwrap()
        .expect("Stats should exist after put");
    assert_eq!(initial_stats.user_count, 2);

    // Act
    users.delete(&1).await.unwrap();

    // Assert that the user count was decremented
    let final_stats = stats_handle
        .get(&1)
        .await
        .unwrap()
        .expect("Stats should still exist");
    assert_eq!(final_stats.user_count, 1);
}

#[tokio::test]
async fn test_multiple_compute_handlers_are_triggered() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<User>()
        .register_collection::<UserStats>()
        .register_collection::<AuditLog>()
        .with_compute_handler(UserCounterHandler)
        .with_compute_handler(AuditLoggerHandler)
        .build()
        .unwrap();

    let users = db.collection::<User>();
    let stats = db.collection::<UserStats>();
    let logs = db.collection::<AuditLog>();

    users
        .put(User {
            id: 1,
            name: "Bob".to_string(),
        })
        .await
        .unwrap();

    let final_stats = stats
        .get(&1)
        .await
        .unwrap()
        .expect("Stats should have been created");
    assert_eq!(final_stats.user_count, 1);

    let final_log = logs
        .get(&1)
        .await
        .unwrap()
        .expect("Audit log should have been created");
    assert_eq!(final_log.message, "User Bob was created/updated.");
}

#[tokio::test]
async fn test_watchers_are_isolated_by_collection() {
    let db = build_test_db();
    let users = db.collection::<User>();
    let mut user_watcher = users.watch();

    let logs = db.collection::<AuditLog>();

    logs.put(AuditLog {
        id: 1,
        message: "system start".to_string(),
    })
    .await
    .unwrap();

    let result = timeout(Duration::from_millis(50), user_watcher.recv()).await;
    assert!(
        result.is_err(),
        "User watcher should not receive an event for an AuditLog put"
    );
}

#[tokio::test]
async fn test_slow_watcher_misses_intermediate_events() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<User>()
        .build()
        .unwrap();

    let users = db.collection::<User>();
    let mut slow_watcher = users.watch();

    users
        .put(User {
            id: 0,
            name: "User 0".to_string(),
        })
        .await
        .unwrap();
    let first_event = slow_watcher.recv().await.unwrap();
    assert_eq!(first_event.unwrap_put().id, 0);

    for i in 1..20 {
        users
            .put(User {
                id: i,
                name: format!("User {}", i),
            })
            .await
            .unwrap();
    }

    let next_event_result = slow_watcher.recv().await;
    assert!(matches!(
        next_event_result,
        Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
    ));
}
