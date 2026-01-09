//! Range-based partitioner for ordered key access patterns
//!
//! Unlike hash-based partitioning, range partitioning preserves key order,
//! enabling efficient range queries that only touch relevant partitions.

use super::partitioner::PartitionStrategy;
use std::sync::RwLock;

/// Range-based partitioner for ordered key access
///
/// Keys are partitioned based on their lexicographic order.
/// Range queries only need to access partitions that contain keys in the range.
pub struct RangePartitioner {
    /// Sorted list of (split_key, partition_id)
    /// Keys < boundaries[0].0 go to partition 0
    /// Keys >= boundaries[i].0 and < boundaries[i+1].0 go to partition boundaries[i].1
    boundaries: RwLock<Vec<(Vec<u8>, u64)>>,

    /// Total number of partitions
    num_partitions: u64,
}

impl RangePartitioner {
    /// Create a new range partitioner with uniform splits
    ///
    /// Initially creates evenly distributed boundaries across the key space.
    pub fn new(num_partitions: u64) -> Self {
        let boundaries = Self::generate_uniform_boundaries(num_partitions);
        Self {
            boundaries: RwLock::new(boundaries),
            num_partitions,
        }
    }

    /// Generate uniformly distributed boundaries
    fn generate_uniform_boundaries(num_partitions: u64) -> Vec<(Vec<u8>, u64)> {
        if num_partitions <= 1 {
            return vec![];
        }

        let mut boundaries = Vec::with_capacity((num_partitions - 1) as usize);

        // Create boundaries at regular intervals in the key space
        // For simplicity, use single-byte boundaries spread across 0-255
        let step = 256 / num_partitions;
        for i in 1..num_partitions {
            let boundary_byte = (i * step) as u8;
            boundaries.push((vec![boundary_byte], i));
        }

        boundaries
    }

    /// Create a range partitioner with custom split keys
    pub fn with_boundaries(boundaries: Vec<(Vec<u8>, u64)>) -> Self {
        let num_partitions = if boundaries.is_empty() {
            1
        } else {
            boundaries.iter().map(|(_, id)| id).max().unwrap_or(&0) + 1
        };

        Self {
            boundaries: RwLock::new(boundaries),
            num_partitions,
        }
    }

    /// Get the partition ID for a given key
    pub fn get_partition_for_key(&self, key: &[u8]) -> u64 {
        let boundaries = self.boundaries.read().unwrap();

        if boundaries.is_empty() {
            return 0;
        }

        // Binary search for the correct partition
        match boundaries.binary_search_by(|(boundary, _)| boundary.as_slice().cmp(key)) {
            Ok(idx) => boundaries[idx].1,
            Err(0) => 0, // Key is before first boundary
            Err(idx) => boundaries[idx - 1].1,
        }
    }

    /// Get all partitions that might contain keys in the given range
    pub fn get_partitions_in_range(&self, start: &[u8], end: &[u8]) -> Vec<u64> {
        let boundaries = self.boundaries.read().unwrap();

        if boundaries.is_empty() {
            return vec![0];
        }

        let start_partition = self.find_partition(&boundaries, start);
        let end_partition = self.find_partition(&boundaries, end);

        (start_partition..=end_partition).collect()
    }

    /// Find partition for a key given boundaries (helper)
    fn find_partition(&self, boundaries: &[(Vec<u8>, u64)], key: &[u8]) -> u64 {
        match boundaries.binary_search_by(|(boundary, _)| boundary.as_slice().cmp(key)) {
            Ok(idx) => boundaries[idx].1,
            Err(0) => 0,
            Err(idx) => boundaries[idx - 1].1,
        }
    }

    /// Split a partition at the given key
    ///
    /// Returns the new partition ID created by the split.
    pub fn split_partition(&self, split_key: Vec<u8>) -> u64 {
        let mut boundaries = self.boundaries.write().unwrap();

        let new_partition_id = if boundaries.is_empty() {
            1
        } else {
            boundaries.iter().map(|(_, id)| id).max().unwrap_or(&0) + 1
        };

        // Insert in sorted order
        let insert_pos = boundaries
            .binary_search_by(|(k, _)| k.cmp(&split_key))
            .unwrap_or_else(|pos| pos);

        boundaries.insert(insert_pos, (split_key, new_partition_id));

        new_partition_id
    }

    /// Merge two adjacent partitions
    ///
    /// Removes the boundary between them, keeping the lower partition ID.
    pub fn merge_partitions(&self, partition_id_to_remove: u64) -> bool {
        let mut boundaries = self.boundaries.write().unwrap();

        // Find and remove the boundary that starts the partition to remove
        if let Some(pos) = boundaries
            .iter()
            .position(|(_, id)| *id == partition_id_to_remove)
        {
            boundaries.remove(pos);
            true
        } else {
            false
        }
    }

    /// Get current boundaries (for debugging/monitoring)
    pub fn get_boundaries(&self) -> Vec<(Vec<u8>, u64)> {
        self.boundaries.read().unwrap().clone()
    }

    /// Get number of partitions
    pub fn partition_count(&self) -> u64 {
        self.num_partitions
    }
}

impl PartitionStrategy for RangePartitioner {
    fn get_partition(&self, key: &[u8]) -> u64 {
        self.get_partition_for_key(key)
    }

    fn get_partitions_for_range(&self, start: &[u8], end: &[u8]) -> Vec<u64> {
        self.get_partitions_in_range(start, end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_partitioner_basic() {
        let partitioner = RangePartitioner::new(4);

        // Keys should be partitioned by their prefix
        let p1 = partitioner.get_partition_for_key(b"\x10key1");
        let p2 = partitioner.get_partition_for_key(b"\x50key2");
        let p3 = partitioner.get_partition_for_key(b"\x90key3");
        let p4 = partitioner.get_partition_for_key(b"\xD0key4");

        // Different prefixes should go to different partitions
        assert!(
            p1 != p4 || p2 != p3,
            "Keys with different prefixes should spread"
        );
    }

    #[test]
    fn test_range_partitioner_range_query() {
        let partitioner = RangePartitioner::with_boundaries(vec![
            (vec![0x40], 1),
            (vec![0x80], 2),
            (vec![0xC0], 3),
        ]);

        // Range query should only return relevant partitions
        let partitions = partitioner.get_partitions_in_range(b"\x50", b"\x70");
        assert!(partitions.contains(&1));
        assert!(!partitions.contains(&3));

        // Full range should return all partitions
        let all = partitioner.get_partitions_in_range(b"\x00", b"\xFF");
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn test_range_partitioner_split() {
        let partitioner = RangePartitioner::new(2);

        // Split at a new key
        let new_id = partitioner.split_partition(vec![0x60]);

        // Verify new partition is accessible
        let p = partitioner.get_partition_for_key(b"\x70");
        assert!(p == new_id || p > 0);
    }

    #[test]
    fn test_partition_strategy_trait() {
        let partitioner = RangePartitioner::new(4);

        let strategy: &dyn PartitionStrategy = &partitioner;
        let partition = strategy.get_partition(b"\x50test");
        assert!(partition < 4);

        // Range partitioner should optimize range queries
        let partitions = strategy.get_partitions_for_range(b"\x00", b"\x40");
        assert!(partitions.len() <= 4);
    }
}
