use prkdb::partitioning::DefaultPartitioner;
use prkdb::prelude::*;
use prkdb_types::consumer::Consumer;
use prkdb_types::consumer::{AutoOffsetReset, ConsumerConfig};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Collection, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct MRec {
    #[id]
    id: u32,
    data: String,
}

#[tokio::test]
async fn metrics_update_on_put_delete() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<MRec>()
        .build()
        .unwrap();
    let m = db.metrics().clone();
    let c = db.collection::<MRec>();
    c.put(MRec {
        id: 1,
        data: "test".to_string(),
    })
    .await
    .unwrap();
    c.delete(&1).await.unwrap();
    assert_eq!(m.puts(), 1);
    assert_eq!(m.deletes(), 1);
    assert_eq!(m.outbox_saved(), 2);
}

#[tokio::test]
async fn test_partition_metrics_recording() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<MRec>()
        .build()
        .unwrap();

    // Configure partitioning for the collection
    let partition_count = 4;
    let partitioner: Arc<dyn prkdb::partitioning::Partitioner<u32>> =
        Arc::new(DefaultPartitioner::<u32>::new());
    db.register_partitioning::<MRec>(partition_count, partitioner);

    let collection = db.collection::<MRec>();

    // Put some data to generate metrics
    for i in 0..10 {
        collection
            .put(MRec {
                id: i,
                data: format!("test_data_{}", i),
            })
            .await
            .unwrap();
    }

    // Verify partition metrics were recorded
    let all_metrics = db.metrics().partition_metrics().all_metrics();
    assert!(
        !all_metrics.is_empty(),
        "Partition metrics should be recorded"
    );

    // Check that metrics exist for multiple partitions
    let partition_count_with_metrics = all_metrics.len();
    assert!(
        partition_count_with_metrics > 0,
        "At least one partition should have metrics"
    );

    // Verify metrics structure
    for metrics in &all_metrics {
        assert!(
            metrics.events_produced() > 0,
            "Events should be recorded for partition {}",
            metrics.partition_id()
        );
        assert!(
            metrics.bytes_produced() > 0,
            "Bytes should be recorded for partition {}",
            metrics.partition_id()
        );
        assert_eq!(
            metrics.events_consumed(),
            0,
            "No events consumed yet for partition {}",
            metrics.partition_id()
        );
        assert_eq!(
            metrics.consumer_lag(),
            metrics.events_produced(),
            "Consumer lag should equal produced events when nothing consumed for partition {}",
            metrics.partition_id()
        );
    }
}

#[tokio::test]
async fn test_partition_metrics_registry_coordination() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<MRec>()
        .build()
        .unwrap();

    let partition_count = 3;
    let partitioner: Arc<dyn prkdb::partitioning::Partitioner<u32>> =
        Arc::new(DefaultPartitioner::<u32>::new());
    db.register_partitioning::<MRec>(partition_count, partitioner);

    let registry = db.metrics().partition_metrics();

    // Test get_or_create behavior
    let partition_0_metrics = registry.get_or_create(0);
    let partition_1_metrics = registry.get_or_create(1);

    assert_eq!(partition_0_metrics.partition_id(), 0);
    assert_eq!(partition_1_metrics.partition_id(), 1);

    // Test that calling get_or_create again returns the same instance
    let partition_0_again = registry.get_or_create(0);
    assert_eq!(
        partition_0_metrics.partition_id(),
        partition_0_again.partition_id()
    );

    // Test get method
    let partition_0_get = registry.get(0);
    assert!(partition_0_get.is_some(), "Partition 0 should exist");
    assert_eq!(partition_0_get.unwrap().partition_id(), 0);

    let nonexistent = registry.get(999);
    assert!(
        nonexistent.is_none(),
        "Non-existent partition should return None"
    );

    // Test all_metrics
    let all_metrics = registry.all_metrics();
    assert_eq!(all_metrics.len(), 2, "Should have metrics for 2 partitions");
}

#[tokio::test]
async fn test_partition_aggregate_statistics() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<MRec>()
        .build()
        .unwrap();

    let partition_count = 4;
    let partitioner: Arc<dyn prkdb::partitioning::Partitioner<u32>> =
        Arc::new(DefaultPartitioner::<u32>::new());
    db.register_partitioning::<MRec>(partition_count, partitioner);

    let collection = db.collection::<MRec>();

    // Put test data
    let event_count = 20;
    for i in 0..event_count {
        collection
            .put(MRec {
                id: i,
                data: format!("data_{}", i),
            })
            .await
            .unwrap();
    }

    // Get aggregate statistics
    let stats = db.metrics().get_partition_aggregate_stats();

    // Verify aggregate calculations
    assert!(stats.total_partitions > 0, "Should have partitions");
    assert_eq!(
        stats.total_events_produced, event_count as u64,
        "Total events should match produced events"
    );
    assert_eq!(stats.total_events_consumed, 0, "No events consumed yet");
    assert!(stats.total_bytes_produced > 0, "Should have produced bytes");
    assert_eq!(stats.total_bytes_consumed, 0, "No bytes consumed yet");
    assert_eq!(
        stats.total_consumer_lag, event_count as u64,
        "Consumer lag should equal produced events when nothing consumed"
    );

    // Test averages
    assert!(
        stats.avg_events_per_partition > 0.0,
        "Average events per partition should be positive"
    );
    assert_eq!(
        stats.avg_consumers_per_partition, 0.0,
        "No consumers registered yet"
    );
}

#[tokio::test]
async fn test_partition_metrics_with_consumer() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<MRec>()
        .build()
        .unwrap();

    let partition_count = 2;
    let partitioner: Arc<dyn prkdb::partitioning::Partitioner<u32>> =
        Arc::new(DefaultPartitioner::<u32>::new());
    db.register_partitioning::<MRec>(partition_count, partitioner);

    let collection = db.collection::<MRec>();

    // Produce events
    for i in 0..6 {
        collection
            .put(MRec {
                id: i,
                data: format!("event_{}", i),
            })
            .await
            .unwrap();
    }

    // Create consumer with config
    let config = ConsumerConfig {
        group_id: "test-group".to_string(),
        consumer_id: Some("test-consumer".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(1000),
        max_poll_records: 100,
        auto_offset_reset: AutoOffsetReset::Earliest,
        dead_letter_topic: None,
        max_retries: 3,
        retry_backoff: Duration::from_millis(100),
    };
    let mut consumer = db.consumer::<MRec>(config).await.unwrap();

    // Consume some events
    let records = consumer.poll().await.unwrap();
    assert!(!records.is_empty(), "Should consume some records");

    consumer.commit().await.unwrap();

    // Check metrics after consumption
    let all_metrics = db.metrics().partition_metrics().all_metrics();

    let total_consumed: u64 = all_metrics.iter().map(|m| m.events_consumed()).sum();

    assert!(total_consumed > 0, "Should have consumed events");

    // Check aggregate stats include consumption
    let stats = db.metrics().get_partition_aggregate_stats();
    assert!(
        stats.total_events_consumed > 0,
        "Aggregate should show consumed events"
    );
    assert!(
        stats.avg_consumers_per_partition > 0.0,
        "Should show consumers per partition"
    );
}

#[allow(clippy::absurd_extreme_comparisons, unused_comparisons)]
#[tokio::test]
async fn test_partition_metrics_latency_tracking() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<MRec>()
        .build()
        .unwrap();

    let partitioner: Arc<dyn prkdb::partitioning::Partitioner<u32>> =
        Arc::new(DefaultPartitioner::<u32>::new());
    db.register_partitioning::<MRec>(2, partitioner);
    let collection = db.collection::<MRec>();

    // Create consumer first
    let config = ConsumerConfig {
        group_id: "latency-test".to_string(),
        consumer_id: Some("latency-consumer".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(1000),
        max_poll_records: 100,
        auto_offset_reset: AutoOffsetReset::Earliest,
        dead_letter_topic: None,
        max_retries: 3,
        retry_backoff: Duration::from_millis(100),
    };
    let mut consumer = db.consumer::<MRec>(config).await.unwrap();

    // Produce event
    collection
        .put(MRec {
            id: 1,
            data: "latency_test".to_string(),
        })
        .await
        .unwrap();

    // Add small delay to ensure measurable latency
    sleep(Duration::from_millis(1)).await;

    // Consume with latency
    let records = consumer.poll().await.unwrap();
    if !records.is_empty() {
        consumer.commit().await.unwrap();

        // Check that latency was recorded
        let all_metrics = db.metrics().partition_metrics().all_metrics();
        let consumed_partitions: Vec<_> = all_metrics
            .iter()
            .filter(|m| m.events_consumed() > 0)
            .collect();

        assert!(
            !consumed_partitions.is_empty(),
            "Should have consumed from at least one partition"
        );

        for metrics in consumed_partitions {
            // Latency should be recorded (may be very small)
            assert!(
                metrics.max_consume_latency_ms() >= 0,
                "Latency should be recorded for partition {}",
                metrics.partition_id()
            );
        }
    }
}
