//! Integration tests for consumer functionality

use prkdb::prelude::*;
use prkdb::storage::InMemoryAdapter;
use prkdb_types::consumer::{AutoOffsetReset, Consumer, ConsumerConfig, Offset};
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
        prkdb_types::consumer::CommitResult::Success
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

/// The versioned position format must retain existing committed-offset values.
#[tokio::test]
async fn offset_store_reads_legacy_bincode_offsets() {
    use prkdb::consumer::StorageOffsetStore;
    use prkdb_types::consumer::OffsetStore;
    use prkdb_types::storage::StorageAdapter;
    use std::sync::Arc;

    let storage = Arc::new(InMemoryAdapter::new());
    let store = StorageOffsetStore::new(storage.clone());
    for (partition, offset) in [0, 1, 250, 251, 65_535, u64::MAX].into_iter().enumerate() {
        let key = format!("__consumer_offset:legacy:events:{partition}");
        let bytes =
            bincode::serde::encode_to_vec(Offset(offset), bincode::config::standard()).unwrap();
        storage.put(key.as_bytes(), &bytes).await.unwrap();
        assert_eq!(
            store
                .get_offset("legacy", "events", partition as u32)
                .await
                .unwrap(),
            Some(Offset(offset))
        );
        assert_eq!(
            store
                .get_position("legacy", "events", partition as u32)
                .await
                .unwrap(),
            Some(prkdb_types::consumer::Position {
                offset: Offset(offset),
                check: None
            })
        );
    }
}

#[tokio::test]
async fn offset_store_reads_versioned_positions_through_offset_api() {
    use prkdb::consumer::StorageOffsetStore;
    use prkdb_types::consumer::OffsetStore;
    use prkdb_types::storage::StorageAdapter;
    use std::sync::Arc;

    let storage = Arc::new(InMemoryAdapter::new());
    let store = StorageOffsetStore::new(storage.clone());
    let mut bytes = vec![1];
    bytes.extend_from_slice(&42u64.to_le_bytes());
    bytes.push(1);
    bytes.extend_from_slice(&0x1234_5678u32.to_le_bytes());
    storage
        .put(b"__consumer_offset:group:events:0", &bytes)
        .await
        .unwrap();
    assert_eq!(
        store.get_offset("group", "events", 0).await.unwrap(),
        Some(Offset(42))
    );
    assert_eq!(
        *store
            .get_group_offsets("group")
            .await
            .unwrap()
            .get("__consumer_offset:group:events:0")
            .unwrap(),
        Offset(42)
    );
}

#[tokio::test]
async fn offset_store_writes_fixed_versioned_positions_without_a_check() {
    use prkdb::consumer::StorageOffsetStore;
    use prkdb_types::consumer::OffsetStore;
    use prkdb_types::storage::StorageAdapter;
    use std::sync::Arc;

    let storage = Arc::new(InMemoryAdapter::new());
    let store = StorageOffsetStore::new(storage.clone());
    store
        .save_offset("group", "events", 2, Offset(u64::MAX))
        .await
        .unwrap();
    let bytes = storage
        .get(b"__consumer_offset:group:events:2")
        .await
        .unwrap()
        .unwrap();
    let mut expected = vec![1];
    expected.extend_from_slice(&u64::MAX.to_le_bytes());
    expected.push(0);
    expected.extend_from_slice(&0u32.to_le_bytes());
    assert_eq!(bytes, expected);
    assert_eq!(
        store.get_offset("group", "events", 2).await.unwrap(),
        Some(Offset(u64::MAX))
    );
}

#[tokio::test]
async fn offset_store_refuses_malformed_versioned_positions() {
    use prkdb::consumer::StorageOffsetStore;
    use prkdb_types::consumer::OffsetStore;
    use prkdb_types::storage::StorageAdapter;
    use std::sync::Arc;

    let storage = Arc::new(InMemoryAdapter::new());
    let store = StorageOffsetStore::new(storage.clone());
    let mut valid = vec![1];
    valid.extend_from_slice(&42u64.to_le_bytes());
    valid.push(1);
    valid.extend_from_slice(&7u32.to_le_bytes());
    let mut unknown_version = valid.clone();
    unknown_version[0] = 2;
    let mut unknown_check = valid.clone();
    unknown_check[9] = 2;
    let mut trailing = valid.clone();
    trailing.push(0);
    for bytes in [
        unknown_version,
        unknown_check,
        valid[..13].to_vec(),
        trailing,
    ] {
        storage
            .put(b"__consumer_offset:group:events:0", &bytes)
            .await
            .unwrap();
        assert!(
            store.get_offset("group", "events", 0).await.is_err(),
            "malformed position was accepted: {bytes:?}"
        );
        assert!(store.get_group_offsets("group").await.is_err());
    }
}

#[tokio::test]
async fn offset_store_round_trips_checked_positions_and_clears_checks_on_manual_offsets() {
    use prkdb::consumer::StorageOffsetStore;
    use prkdb_types::consumer::{OffsetStore, Position};
    use std::sync::Arc;

    let store = StorageOffsetStore::new(Arc::new(InMemoryAdapter::new()));
    for check in [Some(0), Some(u32::MAX), None] {
        let position = Position {
            offset: Offset(u64::MAX),
            check,
        };
        store
            .save_position("group", "events", 0, position)
            .await
            .unwrap();
        assert_eq!(
            store.get_position("group", "events", 0).await.unwrap(),
            Some(position)
        );
        assert_eq!(
            store.get_offset("group", "events", 0).await.unwrap(),
            Some(position.offset)
        );
    }
    store
        .save_position(
            "group",
            "events",
            0,
            Position {
                offset: Offset(42),
                check: Some(7),
            },
        )
        .await
        .unwrap();
    store
        .save_offset("group", "events", 0, Offset(43))
        .await
        .unwrap();
    assert_eq!(
        store.get_position("group", "events", 0).await.unwrap(),
        Some(Position {
            offset: Offset(43),
            check: None
        })
    );
    assert_eq!(
        store.get_position("absent", "events", 0).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn offset_store_position_defaults_keep_legacy_implementors_compatible() {
    use async_trait::async_trait;
    use dashmap::DashMap;
    use prkdb::consumer::StorageOffsetStore;
    use prkdb_types::consumer::{ConsumerGroupId, OffsetStore, Position};
    use prkdb_types::error::Error;
    use std::sync::Arc;

    // Deliberately implements only the original methods. Position support must remain
    // source-compatible and preserve offset-only semantics for a caller-managed store.
    struct LegacyStore(StorageOffsetStore);
    #[async_trait]
    impl OffsetStore for LegacyStore {
        async fn get_offset(
            &self,
            group: &str,
            topic: &str,
            p: u32,
        ) -> Result<Option<Offset>, Error> {
            self.0.get_offset(group, topic, p).await
        }
        async fn save_offset(
            &self,
            group: &str,
            topic: &str,
            p: u32,
            offset: Offset,
        ) -> Result<(), Error> {
            self.0.save_offset(group, topic, p, offset).await
        }
        async fn list_groups(&self) -> Result<Vec<ConsumerGroupId>, Error> {
            self.0.list_groups().await
        }
        async fn get_group_offsets(&self, group: &str) -> Result<DashMap<String, Offset>, Error> {
            self.0.get_group_offsets(group).await
        }
        async fn delete_group(&self, group: &str) -> Result<(), Error> {
            self.0.delete_group(group).await
        }
    }
    let store: Arc<dyn OffsetStore> = Arc::new(LegacyStore(StorageOffsetStore::new(Arc::new(
        InMemoryAdapter::new(),
    ))));
    assert_eq!(
        store.get_position("group", "events", 0).await.unwrap(),
        None
    );
    store
        .save_position(
            "group",
            "events",
            0,
            Position {
                offset: Offset(42),
                check: Some(7),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_offset("group", "events", 0).await.unwrap(),
        Some(Offset(42))
    );
    assert_eq!(
        store.get_position("group", "events", 0).await.unwrap(),
        Some(Position {
            offset: Offset(42),
            check: None
        })
    );
}

#[tokio::test]
async fn offset_group_decode_errors_preserve_deserialization_classification() {
    use prkdb::consumer::StorageOffsetStore;
    use prkdb_types::consumer::OffsetStore;
    use prkdb_types::error::{Error, StorageError};
    use prkdb_types::storage::StorageAdapter;
    use std::sync::Arc;
    let storage = Arc::new(InMemoryAdapter::new());
    let store = StorageOffsetStore::new(storage.clone());
    let mut unknown_version = vec![0; 14];
    unknown_version[0] = 2;
    let mut unknown_kind = vec![0; 14];
    unknown_kind[0] = 1;
    unknown_kind[9] = 2;
    for bytes in [vec![1, 0], unknown_version, unknown_kind] {
        storage
            .put(b"__consumer_offset:group:events:0", &bytes)
            .await
            .unwrap();
        assert!(matches!(
            store.get_group_offsets("group").await.unwrap_err(),
            Error::Deserialization(_)
        ));
        assert!(matches!(
            store.get_offset("group", "events", 0).await.unwrap_err(),
            Error::Storage(StorageError::Deserialization(_))
        ));
    }
}

#[tokio::test]
async fn offset_group_backend_scan_errors_preserve_storage_classification() {
    use prkdb::consumer::StorageOffsetStore;
    use prkdb_types::consumer::OffsetStore;
    use prkdb_types::error::{Error, StorageError};
    use prkdb_types::storage::StorageAdapter;
    use std::sync::Arc;
    struct FailingScan {
        inner: InMemoryAdapter,
        decode_error: bool,
    }
    #[async_trait::async_trait]
    impl StorageAdapter for FailingScan {
        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
            self.inner.get(key).await
        }
        async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
            self.inner.put(key, value).await
        }
        async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
            self.inner.delete(key).await
        }
        async fn scan_prefix(
            &self,
            _prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
            Err(if self.decode_error {
                StorageError::Deserialization("injected scan decode failure".into())
            } else {
                StorageError::BackendError("injected scan failure".into())
            })
        }
    }
    for decode_error in [false, true] {
        let store = StorageOffsetStore::new(Arc::new(FailingScan {
            inner: InMemoryAdapter::new(),
            decode_error,
        }));
        let error = store.get_group_offsets("group").await.unwrap_err();
        if decode_error {
            assert!(matches!(
                error,
                Error::Storage(StorageError::Deserialization(_))
            ));
        } else {
            assert!(matches!(
                error,
                Error::Storage(StorageError::BackendError(_))
            ));
        }
    }
}
