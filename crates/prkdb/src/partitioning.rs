//! Partitioning support for horizontal scaling
//!
//! This module provides partitioning functionality to distribute data across
//! multiple partitions for improved scalability and parallel processing.

use prkdb_types::collection::Collection;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use ahash::AHasher;

/// Partition identifier
pub type PartitionId = u32;

/// A trait for determining the partition for a given key.
pub trait Partitioner<K: Hash>: Send + Sync {
    /// Returns the partition number for the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to partition.
    /// * `num_partitions` - The total number of partitions.
    ///
    /// # Returns
    ///
    /// The partition number (0-indexed).
    fn partition(&self, key: &K, num_partitions: u32) -> u32;
}

/// A default partitioner that uses ahash for consistent hashing.
#[derive(Debug, Default, Clone)]
pub struct DefaultPartitioner<K: Hash + Send + Sync> {
    _marker: PhantomData<K>,
}

impl<K: Hash + Send + Sync> DefaultPartitioner<K> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<K: Hash + Send + Sync> Partitioner<K> for DefaultPartitioner<K> {
    fn partition(&self, key: &K, num_partitions: u32) -> u32 {
        if num_partitions == 0 {
            return 0;
        }
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        (hasher.finish() % num_partitions as u64) as u32
    }
}

/// Metadata about a partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetadata {
    /// Partition ID
    pub id: PartitionId,
    /// Collection name this partition belongs to
    pub collection: String,
    /// Leader node ID (for replication)
    pub leader: Option<String>,
    /// Replica node IDs
    pub replicas: Vec<String>,
    /// Approximate number of records in this partition
    pub record_count: u64,
    /// Approximate size in bytes
    pub size_bytes: u64,
}

impl PartitionMetadata {
    pub fn new(id: PartitionId, collection: String) -> Self {
        Self {
            id,
            collection,
            leader: None,
            replicas: Vec::new(),
            record_count: 0,
            size_bytes: 0,
        }
    }
}

/// Configuration for a partitioned collection.
#[derive(Debug, Clone)]
pub struct PartitionedCollection<C: Collection, P: Partitioner<C::Id>> {
    pub num_partitions: u32,
    pub partitioner: P,
    _marker: PhantomData<C>,
}

impl<C: Collection, P: Partitioner<C::Id>> PartitionedCollection<C, P> {
    pub fn new(num_partitions: u32, partitioner: P) -> Self {
        Self {
            num_partitions,
            partitioner,
            _marker: PhantomData,
        }
    }

    /// Get the partition for a given collection item's ID.
    pub fn partition_for(&self, id: &C::Id) -> u32 {
        self.partitioner.partition(id, self.num_partitions)
    }
}

/// Partition assignment strategy for consumers
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AssignmentStrategy {
    /// Round-robin assignment of partitions to consumers
    #[default]
    RoundRobin,
    /// Range-based assignment (consecutive partitions)
    Range,
    /// Sticky assignment (minimize partition movement on rebalance)
    Sticky,
}

/// Partition assignment for a consumer group
#[derive(Debug, Clone)]
pub struct PartitionAssignment {
    /// Consumer ID to assigned partitions mapping
    pub assignments: HashMap<String, Vec<PartitionId>>,
    /// Generation number (incremented on each rebalance)
    pub generation: u64,
}

impl PartitionAssignment {
    pub fn new() -> Self {
        Self {
            assignments: HashMap::new(),
            generation: 0,
        }
    }

    /// Get partitions assigned to a specific consumer
    pub fn get_partitions(&self, consumer_id: &str) -> Vec<PartitionId> {
        self.assignments
            .get(consumer_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Check if a consumer is assigned a specific partition
    pub fn is_assigned(&self, consumer_id: &str, partition: PartitionId) -> bool {
        self.assignments
            .get(consumer_id)
            .map(|parts| parts.contains(&partition))
            .unwrap_or(false)
    }
}

impl Default for PartitionAssignment {
    fn default() -> Self {
        Self::new()
    }
}

/// Partition assignment coordinator
pub struct PartitionCoordinator {
    strategy: AssignmentStrategy,
}

impl PartitionCoordinator {
    pub fn new(strategy: AssignmentStrategy) -> Self {
        Self { strategy }
    }

    /// Assign partitions to consumers based on the strategy
    pub fn assign(
        &self,
        consumer_ids: &[String],
        num_partitions: u32,
        current_assignment: Option<&PartitionAssignment>,
    ) -> PartitionAssignment {
        if consumer_ids.is_empty() || num_partitions == 0 {
            return PartitionAssignment::new();
        }

        let mut assignment = PartitionAssignment {
            assignments: HashMap::new(),
            generation: current_assignment.map(|a| a.generation + 1).unwrap_or(0),
        };

        match self.strategy {
            AssignmentStrategy::RoundRobin => {
                self.assign_round_robin(consumer_ids, num_partitions, &mut assignment)
            }
            AssignmentStrategy::Range => {
                self.assign_range(consumer_ids, num_partitions, &mut assignment)
            }
            AssignmentStrategy::Sticky => self.assign_sticky(
                consumer_ids,
                num_partitions,
                current_assignment,
                &mut assignment,
            ),
        }

        assignment
    }

    fn assign_round_robin(
        &self,
        consumer_ids: &[String],
        num_partitions: u32,
        assignment: &mut PartitionAssignment,
    ) {
        for partition in 0..num_partitions {
            let consumer_idx = (partition as usize) % consumer_ids.len();
            let consumer_id = &consumer_ids[consumer_idx];
            assignment
                .assignments
                .entry(consumer_id.clone())
                .or_default()
                .push(partition);
        }
    }

    fn assign_range(
        &self,
        consumer_ids: &[String],
        num_partitions: u32,
        assignment: &mut PartitionAssignment,
    ) {
        let partitions_per_consumer = num_partitions / consumer_ids.len() as u32;
        let remainder = num_partitions % consumer_ids.len() as u32;

        let mut start = 0;
        for (idx, consumer_id) in consumer_ids.iter().enumerate() {
            let extra = if (idx as u32) < remainder { 1 } else { 0 };
            let count = partitions_per_consumer + extra;
            let partitions: Vec<PartitionId> = (start..start + count).collect();
            assignment
                .assignments
                .insert(consumer_id.clone(), partitions);
            start += count;
        }
    }

    fn assign_sticky(
        &self,
        consumer_ids: &[String],
        num_partitions: u32,
        current_assignment: Option<&PartitionAssignment>,
        assignment: &mut PartitionAssignment,
    ) {
        // If no current assignment, fall back to round-robin
        let Some(current) = current_assignment else {
            return self.assign_round_robin(consumer_ids, num_partitions, assignment);
        };

        // Track which partitions are already assigned
        let mut assigned_partitions = HashSet::new();
        let consumer_set: HashSet<_> = consumer_ids.iter().cloned().collect();

        // Keep existing assignments for consumers that are still active
        for (consumer_id, partitions) in &current.assignments {
            if consumer_set.contains(consumer_id) {
                assignment
                    .assignments
                    .insert(consumer_id.clone(), partitions.clone());
                assigned_partitions.extend(partitions.iter().copied());
            }
        }

        // Assign unassigned partitions using round-robin
        let unassigned: Vec<PartitionId> = (0..num_partitions)
            .filter(|p| !assigned_partitions.contains(p))
            .collect();

        for (idx, partition) in unassigned.into_iter().enumerate() {
            let consumer_idx = idx % consumer_ids.len();
            let consumer_id = &consumer_ids[consumer_idx];
            assignment
                .assignments
                .entry(consumer_id.clone())
                .or_default()
                .push(partition);
        }
    }
}

impl Default for PartitionCoordinator {
    fn default() -> Self {
        Self::new(AssignmentStrategy::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_partitioner() {
        let partitioner = DefaultPartitioner::<u64>::new();
        let partition = partitioner.partition(&42, 10);
        assert!(partition < 10);

        // Same key should always map to same partition
        let partition2 = partitioner.partition(&42, 10);
        assert_eq!(partition, partition2);
    }

    #[test]
    fn test_round_robin_assignment() {
        let coordinator = PartitionCoordinator::new(AssignmentStrategy::RoundRobin);
        let consumers = vec!["c1".to_string(), "c2".to_string(), "c3".to_string()];
        let assignment = coordinator.assign(&consumers, 9, None);

        assert_eq!(assignment.assignments.len(), 3);
        assert_eq!(assignment.get_partitions("c1"), vec![0, 3, 6]);
        assert_eq!(assignment.get_partitions("c2"), vec![1, 4, 7]);
        assert_eq!(assignment.get_partitions("c3"), vec![2, 5, 8]);
    }

    #[test]
    fn test_range_assignment() {
        let coordinator = PartitionCoordinator::new(AssignmentStrategy::Range);
        let consumers = vec!["c1".to_string(), "c2".to_string()];
        let assignment = coordinator.assign(&consumers, 10, None);

        assert_eq!(assignment.get_partitions("c1"), vec![0, 1, 2, 3, 4]);
        assert_eq!(assignment.get_partitions("c2"), vec![5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_sticky_assignment() {
        let coordinator = PartitionCoordinator::new(AssignmentStrategy::Sticky);
        let consumers = vec!["c1".to_string(), "c2".to_string()];

        // Initial assignment
        let assignment1 = coordinator.assign(&consumers, 6, None);

        // Add a new consumer - existing assignments should be preserved
        let consumers2 = vec!["c1".to_string(), "c2".to_string(), "c3".to_string()];
        let assignment2 = coordinator.assign(&consumers2, 6, Some(&assignment1));

        // c1 and c2 should keep their partitions
        let c1_parts = assignment2.get_partitions("c1");
        let c2_parts = assignment2.get_partitions("c2");

        assert!(c1_parts
            .iter()
            .all(|p| assignment1.get_partitions("c1").contains(p)));
        assert!(c2_parts
            .iter()
            .all(|p| assignment1.get_partitions("c2").contains(p)));
    }

    #[test]
    fn test_partition_metadata() {
        let metadata = PartitionMetadata::new(0, "orders".to_string());
        assert_eq!(metadata.id, 0);
        assert_eq!(metadata.collection, "orders");
        assert_eq!(metadata.record_count, 0);
    }
}
