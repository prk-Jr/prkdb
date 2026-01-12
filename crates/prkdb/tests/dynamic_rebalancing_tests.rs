use prkdb::partitioning::DefaultPartitioner;
use prkdb::prelude::*;
use prkdb_types::consumer::{AutoOffsetReset, Consumer, ConsumerConfig};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Collection, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct TestEvent {
    #[id]
    id: u32,
    data: String,
}

#[tokio::test]
async fn test_consumer_join_triggers_rebalancing() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<TestEvent>()
        .build()
        .unwrap();

    // Setup partitioning
    let partition_count = 4;
    let partitioner = Arc::new(DefaultPartitioner::<u32>::new());
    db.register_partitioning::<TestEvent>(partition_count, partitioner);

    let collection = db.collection::<TestEvent>();

    // Produce some test data
    for i in 0..8 {
        collection
            .put(TestEvent {
                id: i,
                data: format!("event_{}", i),
            })
            .await
            .unwrap();
    }

    // Start with single consumer
    let config1 = ConsumerConfig {
        group_id: "test-group".to_string(),
        consumer_id: Some("consumer-1".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        max_poll_records: 100,
        auto_offset_reset: AutoOffsetReset::Earliest,
    };

    let mut consumer1 = db.consumer::<TestEvent>(config1).await.unwrap();

    // Check initial assignment
    let assignment1 = db.get_consumer_group_assignment("test-group");
    assert!(assignment1.is_some(), "Should have assignment for group");
    let assignment1 = assignment1.unwrap();
    assert_eq!(assignment1.assignments.len(), 1, "Should have 1 consumer");
    assert_eq!(
        assignment1.assignments.get("consumer-1").unwrap().len(),
        4,
        "Consumer 1 should have all 4 partitions"
    );
    assert_eq!(assignment1.generation, 0, "Should be generation 0");

    // Add second consumer (should trigger rebalancing)
    let config2 = ConsumerConfig {
        group_id: "test-group".to_string(),
        consumer_id: Some("consumer-2".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        max_poll_records: 100,
        auto_offset_reset: AutoOffsetReset::Earliest,
    };

    let mut consumer2 = db.consumer::<TestEvent>(config2).await.unwrap();

    // Check rebalanced assignment
    let assignment2 = db.get_consumer_group_assignment("test-group");
    assert!(assignment2.is_some(), "Should have assignment for group");
    let assignment2 = assignment2.unwrap();
    assert_eq!(assignment2.assignments.len(), 2, "Should have 2 consumers");
    assert_eq!(
        assignment2.generation, 1,
        "Should be generation 1 after rebalancing"
    );

    // Check that partitions are distributed
    let consumer1_partitions = assignment2.assignments.get("consumer-1").unwrap().len();
    let consumer2_partitions = assignment2.assignments.get("consumer-2").unwrap().len();
    assert_eq!(
        consumer1_partitions + consumer2_partitions,
        4,
        "All partitions should be assigned"
    );
    assert!(
        (1..=3).contains(&consumer1_partitions),
        "Consumer 1 should have 1-3 partitions"
    );
    assert!(
        (1..=3).contains(&consumer2_partitions),
        "Consumer 2 should have 1-3 partitions"
    );

    // Clean up
    consumer1.close().await.unwrap();
    consumer2.close().await.unwrap();
}

#[tokio::test]
async fn test_consumer_leave_triggers_rebalancing() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<TestEvent>()
        .build()
        .unwrap();

    // Setup partitioning
    let partition_count = 4;
    let partitioner = Arc::new(DefaultPartitioner::<u32>::new());
    db.register_partitioning::<TestEvent>(partition_count, partitioner);

    // Create multiple consumers
    let config1 = ConsumerConfig {
        group_id: "test-group".to_string(),
        consumer_id: Some("consumer-1".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        max_poll_records: 100,
        auto_offset_reset: AutoOffsetReset::Earliest,
    };

    let config2 = ConsumerConfig {
        group_id: "test-group".to_string(),
        consumer_id: Some("consumer-2".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        max_poll_records: 100,
        auto_offset_reset: AutoOffsetReset::Earliest,
    };

    let mut consumer1 = db.consumer::<TestEvent>(config1).await.unwrap();
    let mut consumer2 = db.consumer::<TestEvent>(config2).await.unwrap();

    // Verify initial balanced assignment
    let initial_assignment = db.get_consumer_group_assignment("test-group").unwrap();
    assert_eq!(
        initial_assignment.assignments.len(),
        2,
        "Should have 2 consumers"
    );
    assert_eq!(initial_assignment.generation, 1, "Should be generation 1");

    // Close one consumer (should trigger rebalancing)
    consumer2.close().await.unwrap();

    // Small delay to allow rebalancing to complete
    sleep(Duration::from_millis(10)).await;

    // Check that remaining consumer gets all partitions
    let final_assignment = db.get_consumer_group_assignment("test-group").unwrap();
    assert_eq!(
        final_assignment.assignments.len(),
        1,
        "Should have 1 consumer remaining"
    );
    assert_eq!(
        final_assignment.generation, 2,
        "Should be generation 2 after rebalancing"
    );
    assert_eq!(
        final_assignment
            .assignments
            .get("consumer-1")
            .unwrap()
            .len(),
        4,
        "Consumer 1 should have all 4 partitions"
    );

    // Clean up
    consumer1.close().await.unwrap();
}

#[tokio::test]
async fn test_generation_based_coordination() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<TestEvent>()
        .build()
        .unwrap();

    // Setup partitioning
    let partition_count = 2;
    let partitioner = Arc::new(DefaultPartitioner::<u32>::new());
    db.register_partitioning::<TestEvent>(partition_count, partitioner);

    // Create consumer sequence to test generation increments
    let config1 = ConsumerConfig {
        group_id: "gen-test".to_string(),
        consumer_id: Some("consumer-1".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        max_poll_records: 100,
        auto_offset_reset: AutoOffsetReset::Earliest,
    };

    // Generation 0: Single consumer
    let mut consumer1 = db.consumer::<TestEvent>(config1).await.unwrap();
    let gen0 = db.get_consumer_group_assignment("gen-test").unwrap();
    assert_eq!(gen0.generation, 0, "Should start at generation 0");

    // Generation 1: Add second consumer
    let config2 = ConsumerConfig {
        group_id: "gen-test".to_string(),
        consumer_id: Some("consumer-2".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        max_poll_records: 100,
        auto_offset_reset: AutoOffsetReset::Earliest,
    };

    let mut consumer2 = db.consumer::<TestEvent>(config2).await.unwrap();
    let gen1 = db.get_consumer_group_assignment("gen-test").unwrap();
    assert_eq!(gen1.generation, 1, "Should increment to generation 1");

    // Generation 2: Add third consumer
    let config3 = ConsumerConfig {
        group_id: "gen-test".to_string(),
        consumer_id: Some("consumer-3".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        max_poll_records: 100,
        auto_offset_reset: AutoOffsetReset::Earliest,
    };

    let mut consumer3 = db.consumer::<TestEvent>(config3).await.unwrap();
    let gen2 = db.get_consumer_group_assignment("gen-test").unwrap();
    assert_eq!(gen2.generation, 2, "Should increment to generation 2");

    // Generation 3: Remove middle consumer
    consumer2.close().await.unwrap();
    sleep(Duration::from_millis(10)).await;
    let gen3 = db.get_consumer_group_assignment("gen-test").unwrap();
    assert_eq!(gen3.generation, 3, "Should increment to generation 3");
    assert_eq!(
        gen3.assignments.len(),
        2,
        "Should have 2 consumers remaining"
    );

    // Clean up
    consumer1.close().await.unwrap();
    consumer3.close().await.unwrap();
}

#[tokio::test]
async fn test_rebalancing_preserves_consumption() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<TestEvent>()
        .build()
        .unwrap();

    // Setup partitioning
    let partition_count = 4;
    let partitioner = Arc::new(DefaultPartitioner::<u32>::new());
    db.register_partitioning::<TestEvent>(partition_count, partitioner);

    let collection = db.collection::<TestEvent>();

    // Produce test events
    let event_count = 12u32;
    for i in 0..event_count {
        collection
            .put(TestEvent {
                id: i,
                data: format!("test_event_{}", i),
            })
            .await
            .unwrap();
    }

    // Start with single consumer and consume some events
    let config1 = ConsumerConfig {
        group_id: "consume-test".to_string(),
        consumer_id: Some("consumer-1".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        max_poll_records: 100,
        auto_offset_reset: AutoOffsetReset::Earliest,
    };

    let mut consumer1 = db.consumer::<TestEvent>(config1).await.unwrap();

    // Consume some events
    let records1 = consumer1.poll().await.unwrap();
    consumer1.commit().await.unwrap();
    let consumed_count_before = records1.len();
    assert!(
        consumed_count_before > 0,
        "Should have consumed some events"
    );

    // Add second consumer (triggers rebalancing)
    let config2 = ConsumerConfig {
        group_id: "consume-test".to_string(),
        consumer_id: Some("consumer-2".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        max_poll_records: 100,
        auto_offset_reset: AutoOffsetReset::Earliest,
    };

    let mut consumer2 = db.consumer::<TestEvent>(config2).await.unwrap();

    // Both consumers should now consume remaining events
    let records1_after = consumer1.poll().await.unwrap();
    let records2_after = consumer2.poll().await.unwrap();

    let total_consumed = consumed_count_before + records1_after.len() + records2_after.len();

    // Should not exceed total produced events (no duplicate consumption)
    assert!(
        total_consumed <= event_count as usize,
        "Should not consume more events than produced"
    );

    // Should consume remaining events efficiently
    assert!(
        total_consumed >= (event_count - 2) as usize,
        "Should consume most/all events"
    ); // Allow small margin for timing

    // Clean up
    consumer1.close().await.unwrap();
    consumer2.close().await.unwrap();
}

#[tokio::test]
async fn test_multiple_consumer_groups_independent_rebalancing() {
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<TestEvent>()
        .build()
        .unwrap();

    // Setup partitioning
    let partition_count = 3;
    let partitioner = Arc::new(DefaultPartitioner::<u32>::new());
    db.register_partitioning::<TestEvent>(partition_count, partitioner);

    // Create consumers in group A
    let config_a1 = ConsumerConfig {
        group_id: "group-a".to_string(),
        consumer_id: Some("consumer-a1".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        max_poll_records: 100,
        auto_offset_reset: AutoOffsetReset::Earliest,
    };

    let config_a2 = ConsumerConfig {
        group_id: "group-a".to_string(),
        consumer_id: Some("consumer-a2".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        max_poll_records: 100,
        auto_offset_reset: AutoOffsetReset::Earliest,
    };

    // Create consumers in group B
    let config_b1 = ConsumerConfig {
        group_id: "group-b".to_string(),
        consumer_id: Some("consumer-b1".to_string()),
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        max_poll_records: 100,
        auto_offset_reset: AutoOffsetReset::Earliest,
    };

    let mut consumer_a1 = db.consumer::<TestEvent>(config_a1).await.unwrap();
    let mut consumer_b1 = db.consumer::<TestEvent>(config_b1).await.unwrap();

    // Check initial states
    let assignment_a = db.get_consumer_group_assignment("group-a").unwrap();
    let assignment_b = db.get_consumer_group_assignment("group-b").unwrap();

    assert_eq!(
        assignment_a.generation, 0,
        "Group A should be at generation 0"
    );
    assert_eq!(
        assignment_b.generation, 0,
        "Group B should be at generation 0"
    );
    assert_eq!(
        assignment_a.assignments.len(),
        1,
        "Group A should have 1 consumer"
    );
    assert_eq!(
        assignment_b.assignments.len(),
        1,
        "Group B should have 1 consumer"
    );

    // Add consumer to group A only (should not affect group B)
    let mut consumer_a2 = db.consumer::<TestEvent>(config_a2).await.unwrap();

    let assignment_a_after = db.get_consumer_group_assignment("group-a").unwrap();
    let assignment_b_after = db.get_consumer_group_assignment("group-b").unwrap();

    assert_eq!(
        assignment_a_after.generation, 1,
        "Group A should increment to generation 1"
    );
    assert_eq!(
        assignment_b_after.generation, 0,
        "Group B should remain at generation 0"
    );
    assert_eq!(
        assignment_a_after.assignments.len(),
        2,
        "Group A should have 2 consumers"
    );
    assert_eq!(
        assignment_b_after.assignments.len(),
        1,
        "Group B should still have 1 consumer"
    );

    // Clean up
    consumer_a1.close().await.unwrap();
    consumer_a2.close().await.unwrap();
    consumer_b1.close().await.unwrap();
}
