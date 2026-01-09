use prkdb::partitioning::{
    AssignmentStrategy, DefaultPartitioner, PartitionCoordinator, PartitionMetadata, Partitioner,
};
use prkdb::prelude::*;
use prkdb::storage::InMemoryAdapter;
use prkdb_types::collection::Collection;
use prkdb_types::consumer::{AutoOffsetReset, Consumer, ConsumerConfig, Offset};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
struct MyKey(u64);

#[test]
fn test_default_partitioner_distribution() {
    let partitioner = DefaultPartitioner::<MyKey>::new();
    let num_partitions = 10;
    let num_keys = 1000;
    let mut counts = vec![0; num_partitions as usize];

    for i in 0..num_keys {
        let key = MyKey(i);
        let partition = partitioner.partition(&key, num_partitions);
        counts[partition as usize] += 1;
    }

    // Check that all partitions have some keys
    assert!(counts.iter().all(|&c| c > 0));

    // A very basic check for distribution. A perfect distribution would be 100 keys per partition.
    // We'll check if the counts are within a reasonable range.
    let expected_avg = num_keys as f64 / num_partitions as f64;
    for &count in &counts {
        let deviation = (count as f64 - expected_avg).abs() / expected_avg;
        assert!(
            deviation < 0.5,
            "Partition count {} is too far from average {}",
            count,
            expected_avg
        );
    }
}

#[test]
fn test_partitioner_zero_partitions() {
    let partitioner = DefaultPartitioner::<MyKey>::new();
    let key = MyKey(42);
    assert_eq!(partitioner.partition(&key, 0), 0);
}

#[test]
fn test_partitioner_single_partition() {
    let partitioner = DefaultPartitioner::<MyKey>::new();
    let key = MyKey(42);
    assert_eq!(partitioner.partition(&key, 1), 0);
}

#[test]
fn test_partition_metadata_creation() {
    let metadata = PartitionMetadata::new(5, "orders".to_string());
    assert_eq!(metadata.id, 5);
    assert_eq!(metadata.collection, "orders");
    assert_eq!(metadata.leader, None);
    assert_eq!(metadata.replicas.len(), 0);
    assert_eq!(metadata.record_count, 0);
    assert_eq!(metadata.size_bytes, 0);
}

#[test]
fn test_partition_assignment_round_robin() {
    let coordinator = PartitionCoordinator::new(AssignmentStrategy::RoundRobin);
    let consumers = vec![
        "consumer-1".to_string(),
        "consumer-2".to_string(),
        "consumer-3".to_string(),
    ];
    let assignment = coordinator.assign(&consumers, 9, None);

    assert_eq!(assignment.generation, 0);
    assert_eq!(assignment.assignments.len(), 3);

    // Each consumer should get 3 partitions in round-robin fashion
    let c1_parts = assignment.get_partitions("consumer-1");
    let c2_parts = assignment.get_partitions("consumer-2");
    let c3_parts = assignment.get_partitions("consumer-3");

    assert_eq!(c1_parts, vec![0, 3, 6]);
    assert_eq!(c2_parts, vec![1, 4, 7]);
    assert_eq!(c3_parts, vec![2, 5, 8]);

    // Verify is_assigned works
    assert!(assignment.is_assigned("consumer-1", 0));
    assert!(assignment.is_assigned("consumer-2", 1));
    assert!(!assignment.is_assigned("consumer-1", 1));
}

#[test]
fn test_partition_assignment_range() {
    let coordinator = PartitionCoordinator::new(AssignmentStrategy::Range);
    let consumers = vec!["consumer-1".to_string(), "consumer-2".to_string()];
    let assignment = coordinator.assign(&consumers, 10, None);

    let c1_parts = assignment.get_partitions("consumer-1");
    let c2_parts = assignment.get_partitions("consumer-2");

    // Range assignment should give consecutive partitions
    assert_eq!(c1_parts, vec![0, 1, 2, 3, 4]);
    assert_eq!(c2_parts, vec![5, 6, 7, 8, 9]);
}

#[test]
fn test_partition_assignment_sticky() {
    let coordinator = PartitionCoordinator::new(AssignmentStrategy::Sticky);
    let consumers = vec!["consumer-1".to_string(), "consumer-2".to_string()];

    // Initial assignment
    let assignment1 = coordinator.assign(&consumers, 6, None);
    let c1_initial = assignment1.get_partitions("consumer-1");
    let c2_initial = assignment1.get_partitions("consumer-2");

    // Add a new consumer - sticky should preserve existing assignments
    let consumers2 = vec![
        "consumer-1".to_string(),
        "consumer-2".to_string(),
        "consumer-3".to_string(),
    ];
    let assignment2 = coordinator.assign(&consumers2, 6, Some(&assignment1));

    let c1_after = assignment2.get_partitions("consumer-1");
    let c2_after = assignment2.get_partitions("consumer-2");
    let c3_after = assignment2.get_partitions("consumer-3");

    // Existing consumers should keep their partitions
    assert!(c1_after.iter().all(|p| c1_initial.contains(p)));
    assert!(c2_after.iter().all(|p| c2_initial.contains(p)));

    // All partitions should be assigned
    let mut all_parts = HashSet::new();
    all_parts.extend(c1_after);
    all_parts.extend(c2_after);
    all_parts.extend(c3_after);
    assert_eq!(all_parts.len(), 6);

    // Generation should increment
    assert_eq!(assignment2.generation, 1);
}

#[test]
fn test_partition_assignment_empty_consumers() {
    let coordinator = PartitionCoordinator::new(AssignmentStrategy::RoundRobin);
    let assignment = coordinator.assign(&[], 10, None);

    assert_eq!(assignment.assignments.len(), 0);
    assert_eq!(assignment.generation, 0);
}

#[test]
fn test_partition_assignment_uneven_distribution() {
    let coordinator = PartitionCoordinator::new(AssignmentStrategy::Range);
    let consumers = vec![
        "consumer-1".to_string(),
        "consumer-2".to_string(),
        "consumer-3".to_string(),
    ];
    let assignment = coordinator.assign(&consumers, 10, None);

    let c1_parts = assignment.get_partitions("consumer-1");
    let c2_parts = assignment.get_partitions("consumer-2");
    let c3_parts = assignment.get_partitions("consumer-3");

    // 10 partitions / 3 consumers = 3, 3, 4 (remainder distributed)
    assert_eq!(c1_parts.len(), 4); // Gets extra partition
    assert_eq!(c2_parts.len(), 3);
    assert_eq!(c3_parts.len(), 3);

    // Verify all partitions are assigned
    let mut all_parts = HashSet::new();
    all_parts.extend(c1_parts);
    all_parts.extend(c2_parts);
    all_parts.extend(c3_parts);
    assert_eq!(all_parts.len(), 10);
}

#[tokio::test]
async fn test_partitioned_collection_handle() {
    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct TestItem {
        id: u64,
        value: String,
    }

    impl Collection for TestItem {
        type Id = u64;
        fn id(&self) -> &Self::Id {
            &self.id
        }
    }

    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<TestItem>()
        .build()
        .unwrap();
    let handle = db.collection::<TestItem>().with_partitions(4);

    // Insert items - they should be distributed across partitions
    for i in 0..10 {
        let item = TestItem {
            id: i,
            value: format!("value-{}", i),
        };
        handle.put(item).await.unwrap();
    }

    // Verify we can retrieve items
    for i in 0..10 {
        let retrieved = handle.get(&i).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().value, format!("value-{}", i));
    }
}

#[tokio::test]
async fn test_consumer_with_partitions() {
    use prkdb::consumer::PrkConsumer;

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct TestItem {
        id: u64,
        value: String,
    }

    impl Collection for TestItem {
        type Id = u64;
        fn id(&self) -> &Self::Id {
            &self.id
        }
    }

    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<TestItem>()
        .build()
        .unwrap();
    let config = ConsumerConfig {
        group_id: "test-group".to_string(),
        consumer_id: Some("consumer-1".to_string()),
        auto_offset_reset: AutoOffsetReset::Earliest,
        ..Default::default()
    };

    // Create consumer with specific partitions
    let consumer = PrkConsumer::<TestItem>::with_partitions(db.clone(), config, vec![0, 1, 2])
        .await
        .unwrap();

    assert_eq!(consumer.assigned_partitions(), &[0, 1, 2]);
    assert_eq!(consumer.position(), Offset::zero());
}

#[tokio::test]
async fn test_consumer_partition_reassignment() {
    use prkdb::consumer::PrkConsumer;

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct TestItem {
        id: u64,
        value: String,
    }

    impl Collection for TestItem {
        type Id = u64;
        fn id(&self) -> &Self::Id {
            &self.id
        }
    }

    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<TestItem>()
        .build()
        .unwrap();
    let config = ConsumerConfig {
        group_id: "test-group".to_string(),
        consumer_id: Some("consumer-1".to_string()),
        auto_offset_reset: AutoOffsetReset::Earliest,
        ..Default::default()
    };

    let mut consumer = PrkConsumer::<TestItem>::with_partitions(db.clone(), config, vec![0, 1])
        .await
        .unwrap();

    assert_eq!(consumer.assigned_partitions(), &[0, 1]);

    // Reassign to different partitions
    consumer.reassign_partitions(vec![2, 3, 4]).await.unwrap();
    assert_eq!(consumer.assigned_partitions(), &[2, 3, 4]);
}
