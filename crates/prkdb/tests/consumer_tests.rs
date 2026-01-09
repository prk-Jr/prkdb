//! Integration tests for consumer functionality

use prkdb::prelude::*;
use prkdb::storage::InMemoryAdapter;
use prkdb_core::consumer::{AutoOffsetReset, Consumer, ConsumerConfig, Offset};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Order {
    #[id]
    id: u64,
    customer: String,
    amount: f64,
}

#[tokio::test]
async fn consumer_poll_earliest() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<Order>()
        .build()
        .unwrap();

    // Put some events
    let collection = db.collection::<Order>();
    for i in 1..=5 {
        collection
            .put(Order {
                id: i,
                customer: format!("customer{}", i),
                amount: i as f64 * 100.0,
            })
            .await
            .unwrap();
    }

    // Create consumer starting from earliest
    let config = ConsumerConfig {
        group_id: "test-group".to_string(),
        auto_offset_reset: AutoOffsetReset::Earliest,
        auto_commit: false,
        ..Default::default()
    };

    let mut consumer = db.consumer::<Order>(config).await.unwrap();

    // Poll events
    let records = consumer.poll().await.unwrap();
    assert!(!records.is_empty());
    assert_eq!(records[0].value.id, 1);
}

#[tokio::test]
async fn consumer_poll_latest() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<Order>()
        .build()
        .unwrap();

    // Put some events before consumer starts
    let collection = db.collection::<Order>();
    for i in 1..=3 {
        collection
            .put(Order {
                id: i,
                customer: format!("customer{}", i),
                amount: i as f64 * 100.0,
            })
            .await
            .unwrap();
    }

    // Create consumer starting from latest
    let config = ConsumerConfig {
        group_id: "test-group".to_string(),
        auto_offset_reset: AutoOffsetReset::Latest,
        auto_commit: false,
        ..Default::default()
    };

    let mut consumer = db.consumer::<Order>(config).await.unwrap();

    // First poll should return empty (all events are before latest offset)
    let records = consumer.poll().await.unwrap();
    assert_eq!(records.len(), 0);

    // Add new event
    collection
        .put(Order {
            id: 4,
            customer: "customer4".to_string(),
            amount: 400.0,
        })
        .await
        .unwrap();

    // Now poll should get the new event
    let records = consumer.poll().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].value.id, 4);
}

#[tokio::test]
async fn consumer_manual_commit() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<Order>()
        .build()
        .unwrap();

    let collection = db.collection::<Order>();
    collection
        .put(Order {
            id: 1,
            customer: "customer1".to_string(),
            amount: 100.0,
        })
        .await
        .unwrap();

    let config = ConsumerConfig {
        group_id: "commit-test-group".to_string(),
        auto_offset_reset: AutoOffsetReset::Earliest,
        auto_commit: false,
        ..Default::default()
    };

    let mut consumer = db.consumer::<Order>(config.clone()).await.unwrap();

    // Poll and commit
    let records = consumer.poll().await.unwrap();
    assert_eq!(records.len(), 1);

    let result = consumer.commit().await.unwrap();
    assert!(matches!(
        result,
        prkdb_core::consumer::CommitResult::Success
    ));

    // Close consumer
    drop(consumer);

    // Create new consumer with same group
    let consumer2 = db.consumer::<Order>(config).await.unwrap();

    // Should start from committed offset
    let committed = consumer2.committed().await.unwrap();
    assert!(committed.is_some());
}

#[tokio::test]
async fn consumer_auto_commit() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<Order>()
        .build()
        .unwrap();

    let collection = db.collection::<Order>();
    for i in 1..=3 {
        collection
            .put(Order {
                id: i,
                customer: format!("customer{}", i),
                amount: i as f64 * 100.0,
            })
            .await
            .unwrap();
    }

    let config = ConsumerConfig {
        group_id: "auto-commit-group".to_string(),
        auto_offset_reset: AutoOffsetReset::Earliest,
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        ..Default::default()
    };

    let mut consumer = db.consumer::<Order>(config).await.unwrap();

    // Poll events
    let records = consumer.poll().await.unwrap();
    assert!(!records.is_empty());

    // Wait for auto-commit
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Poll again to trigger auto-commit check
    let _ = consumer.poll().await;

    // Check committed offset
    let committed = consumer.committed().await.unwrap();
    assert!(committed.is_some());
}

#[tokio::test]
async fn consumer_seek() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<Order>()
        .build()
        .unwrap();

    let collection = db.collection::<Order>();
    for i in 1..=10 {
        collection
            .put(Order {
                id: i,
                customer: format!("customer{}", i),
                amount: i as f64 * 100.0,
            })
            .await
            .unwrap();
    }

    let config = ConsumerConfig {
        group_id: "seek-test-group".to_string(),
        auto_offset_reset: AutoOffsetReset::Earliest,
        auto_commit: false,
        ..Default::default()
    };

    let mut consumer = db.consumer::<Order>(config).await.unwrap();

    // Seek to offset 5
    consumer.seek(Offset::from_value(5)).await.unwrap();
    assert_eq!(consumer.position(), Offset::from_value(5));

    // Poll should start from offset 5
    let records = consumer.poll().await.unwrap();
    if !records.is_empty() {
        assert!(records[0].offset >= 5);
    }
}

#[tokio::test]
async fn consumer_multiple_groups() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<Order>()
        .build()
        .unwrap();

    let collection = db.collection::<Order>();
    for i in 1..=5 {
        collection
            .put(Order {
                id: i,
                customer: format!("customer{}", i),
                amount: i as f64 * 100.0,
            })
            .await
            .unwrap();
    }

    // Create two consumers in different groups
    let config1 = ConsumerConfig {
        group_id: "group1".to_string(),
        auto_offset_reset: AutoOffsetReset::Earliest,
        auto_commit: false,
        ..Default::default()
    };

    let config2 = ConsumerConfig {
        group_id: "group2".to_string(),
        auto_offset_reset: AutoOffsetReset::Earliest,
        auto_commit: false,
        ..Default::default()
    };

    let mut consumer1 = db.consumer::<Order>(config1).await.unwrap();
    let mut consumer2 = db.consumer::<Order>(config2).await.unwrap();

    // Both should read all events independently
    let records1 = consumer1.poll().await.unwrap();
    let records2 = consumer2.poll().await.unwrap();

    assert!(!records1.is_empty());
    assert!(!records2.is_empty());

    // Commit only consumer1
    consumer1.commit().await.unwrap();

    // Consumer2's committed offset should still be None
    let committed2 = consumer2.committed().await.unwrap();
    assert!(committed2.is_none());
}

#[tokio::test]
async fn consumer_max_poll_records() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<Order>()
        .build()
        .unwrap();

    let collection = db.collection::<Order>();
    for i in 1..=20 {
        collection
            .put(Order {
                id: i,
                customer: format!("customer{}", i),
                amount: i as f64 * 100.0,
            })
            .await
            .unwrap();
    }

    let config = ConsumerConfig {
        group_id: "max-poll-group".to_string(),
        auto_offset_reset: AutoOffsetReset::Earliest,
        auto_commit: false,
        max_poll_records: 5,
        ..Default::default()
    };

    let mut consumer = db.consumer::<Order>(config).await.unwrap();

    // First poll should return at most 5 records
    let records = consumer.poll().await.unwrap();
    assert!(records.len() <= 5);
}

#[tokio::test]
async fn consumer_close_commits() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<Order>()
        .build()
        .unwrap();

    let collection = db.collection::<Order>();
    collection
        .put(Order {
            id: 1,
            customer: "customer1".to_string(),
            amount: 100.0,
        })
        .await
        .unwrap();

    let config = ConsumerConfig {
        group_id: "close-test-group".to_string(),
        auto_offset_reset: AutoOffsetReset::Earliest,
        auto_commit: false,
        ..Default::default()
    };

    let mut consumer = db.consumer::<Order>(config.clone()).await.unwrap();
    let records = consumer.poll().await.unwrap();
    assert!(!records.is_empty());

    // Close should commit
    consumer.close().await.unwrap();

    // Create new consumer and check committed offset
    let consumer2 = db.consumer::<Order>(config).await.unwrap();
    let committed = consumer2.committed().await.unwrap();
    assert!(committed.is_some());
}

#[tokio::test]
async fn consumer_concurrent_consumption() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<Order>()
        .build()
        .unwrap();

    let collection = db.collection::<Order>();
    for i in 1..=10 {
        collection
            .put(Order {
                id: i,
                customer: format!("customer{}", i),
                amount: i as f64 * 100.0,
            })
            .await
            .unwrap();
    }

    // Create multiple consumers in same group (simulating concurrent consumers)
    let config = ConsumerConfig {
        group_id: "concurrent-group".to_string(),
        auto_offset_reset: AutoOffsetReset::Earliest,
        auto_commit: true,
        ..Default::default()
    };

    let db1 = db.clone();
    let db2 = db.clone();
    let config1 = config.clone();
    let config2 = config.clone();

    let handle1 = tokio::spawn(async move {
        let mut consumer = db1.consumer::<Order>(config1).await.unwrap();
        let mut total = 0;
        for _ in 0..3 {
            let records = consumer.poll().await.unwrap();
            total += records.len();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        total
    });

    let handle2 = tokio::spawn(async move {
        let mut consumer = db2.consumer::<Order>(config2).await.unwrap();
        let mut total = 0;
        for _ in 0..3 {
            let records = consumer.poll().await.unwrap();
            total += records.len();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        total
    });

    let (count1, count2) = tokio::join!(handle1, handle2);

    // Both consumers should read events (though they might overlap)
    assert!(count1.unwrap() > 0 || count2.unwrap() > 0);
}
