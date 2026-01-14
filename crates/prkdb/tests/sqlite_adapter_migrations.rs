use prkdb::prelude::*;
use prkdb_orm::dialect::SqlDialect;
use prkdb_orm_macros::Table;
use prkdb_storage_sql::SqliteAdapter;
use prkdb_types::consumer::{AutoOffsetReset, Consumer, ConsumerConfig};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tokio::time::{sleep, Duration};

#[derive(Collection, Table, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[table(name = "items", primary_key = "id")]
struct Item {
    #[id]
    #[auto_increment]
    id: i64,
    name: String,
}

#[tokio::test]
async fn sqlite_adapter_runs_migrations_and_crud() {
    // Use a temp file to ensure we can inspect the underlying database.
    let tmp = NamedTempFile::new().expect("create temp db file");
    let db_url = format!("sqlite://{}", tmp.path().to_string_lossy());

    let storage = SqliteAdapter::connect(&db_url).await.unwrap();
    let db = PrkDb::builder()
        .with_schema_dialect(SqlDialect::Sqlite)
        .with_storage(storage)
        .register_table_collection::<Item>()
        .build_async()
        .await
        .expect("builder should run migrations");

    let items = db.collection::<Item>();
    items
        .put(Item {
            id: 1,
            name: "alpha".to_string(),
        })
        .await
        .unwrap();
    items
        .put(Item {
            id: 2,
            name: "beta".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(
        items.get(&1).await.unwrap(),
        Some(Item {
            id: 1,
            name: "alpha".to_string()
        })
    );

    items.delete(&2).await.unwrap();
    assert!(items.get(&2).await.unwrap().is_none());

    // Verify the DDL actually ran against the SQLite backend.
    let pool = sqlx::SqlitePool::connect(&db_url).await.unwrap();
    let table_row =
        sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name='items'")
            .fetch_optional(&pool)
            .await
            .unwrap();
    assert!(
        table_row.is_some(),
        "items table should be created via migrations"
    );
}

#[tokio::test]
async fn sqlite_consumer_sees_put_events() {
    let tmp = NamedTempFile::new().expect("create temp db file");
    let db_url = format!("sqlite://{}", tmp.path().to_string_lossy());

    let storage = SqliteAdapter::connect(&db_url).await.unwrap();
    let db = PrkDb::builder()
        .with_schema_dialect(SqlDialect::Sqlite)
        .with_storage(storage)
        .register_table_collection::<Item>()
        .build_async()
        .await
        .unwrap();

    // Start a consumer before producing, so it will see the new events.
    let config = ConsumerConfig {
        group_id: "g1".to_string(),
        consumer_id: Some("c1".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        max_poll_records: 10,
        auto_offset_reset: AutoOffsetReset::Earliest,
        dead_letter_topic: None,
        max_retries: 3,
        retry_backoff: Duration::from_millis(100),
    };
    let mut consumer = db.consumer::<Item>(config).await.unwrap();

    // Produce a couple of items.
    let items = db.collection::<Item>();
    items
        .put(Item {
            id: 1,
            name: "first".into(),
        })
        .await
        .unwrap();
    items
        .put(Item {
            id: 2,
            name: "second".into(),
        })
        .await
        .unwrap();

    // Poll until we see events or exhaust attempts.
    let mut received = Vec::new();
    for _ in 0..5 {
        let polled = consumer.poll().await.unwrap();
        if !polled.is_empty() {
            received.extend(polled.into_iter().map(|rec| rec.value));
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }

    assert!(
        !received.is_empty(),
        "consumer should receive events from puts"
    );
    assert!(
        received.iter().any(|i| i.name == "first"),
        "expected to see 'first' item"
    );
    assert!(
        received.iter().any(|i| i.name == "second"),
        "expected to see 'second' item"
    );
}

#[tokio::test]
async fn sqlite_put_with_outbox_is_atomic() {
    let tmp = NamedTempFile::new().expect("create temp db file");
    let db_url = format!("sqlite://{}", tmp.path().to_string_lossy());
    let adapter = SqliteAdapter::connect(&db_url).await.unwrap();

    // use put_with_outbox via the adapter directly
    adapter
        .put_with_outbox(b"a", b"1", "ob1", b"payload")
        .await
        .unwrap();

    let val = adapter.get(b"a").await.unwrap().unwrap();
    assert_eq!(val, b"1");
    let outbox = adapter.outbox_list().await.unwrap();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].0, "ob1");
    assert_eq!(outbox[0].1, b"payload");
}
