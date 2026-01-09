//! Partitioning strategies for distributing keys across nodes
//!
//! Provides multiple partitioning strategies:
//! - `ConsistentHashRing`: Minimizes data movement during rebalancing
//! - `Partitioner`: Simple hash-based partitioning (legacy)

use super::config::NodeId;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Trait for partitioning strategies
pub trait PartitionStrategy: Send + Sync {
    /// Get the partition ID for a given key
    fn get_partition(&self, key: &[u8]) -> u64;

    /// Get all partitions that might contain keys in the given range
    fn get_partitions_for_range(&self, start: &[u8], end: &[u8]) -> Vec<u64>;
}

/// Consistent hash ring for minimal data movement during rebalancing
///
/// Uses virtual nodes (vnodes) to achieve even distribution across nodes.
/// When nodes are added or removed, only ~1/n of data needs to move.
pub struct ConsistentHashRing {
    /// The hash ring: hash_value -> (node_id, partition_id)
    ring: BTreeMap<u64, (NodeId, u64)>,

    /// Number of virtual nodes per physical node
    vnodes_per_node: usize,

    /// Set of nodes currently in the ring
    nodes: HashSet<NodeId>,

    /// Number of partitions
    num_partitions: u64,
}

impl ConsistentHashRing {
    /// Create a new consistent hash ring
    ///
    /// # Arguments
    /// * `num_partitions` - Total number of partitions
    /// * `vnodes_per_node` - Virtual nodes per physical node (higher = better balance)
    pub fn new(num_partitions: u64, vnodes_per_node: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            vnodes_per_node,
            nodes: HashSet::new(),
            num_partitions,
        }
    }

    /// Create a new ring with initial nodes
    pub fn with_nodes(num_partitions: u64, vnodes_per_node: usize, nodes: Vec<NodeId>) -> Self {
        let mut ring = Self::new(num_partitions, vnodes_per_node);
        for node in nodes {
            ring.add_node(node);
        }
        ring
    }

    /// Add a node to the ring
    pub fn add_node(&mut self, node: NodeId) {
        if self.nodes.contains(&node) {
            return;
        }

        self.nodes.insert(node);

        // Add virtual nodes
        for vnode_idx in 0..self.vnodes_per_node {
            let hash = self.hash_vnode(node, vnode_idx);
            // Map to partition based on position in ring
            let partition = hash % self.num_partitions;
            self.ring.insert(hash, (node, partition));
        }
    }

    /// Remove a node from the ring
    pub fn remove_node(&mut self, node: NodeId) {
        if !self.nodes.remove(&node) {
            return;
        }

        // Remove all virtual nodes for this physical node
        self.ring.retain(|_, (n, _)| *n != node);
    }

    /// Get the node responsible for a given key
    pub fn get_node(&self, key: &[u8]) -> Option<NodeId> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = self.hash_key(key);

        // Find the first node with hash >= key hash (clockwise walk)
        if let Some((_, (node, _))) = self.ring.range(hash..).next() {
            return Some(*node);
        }

        // Wrap around to first node
        self.ring.values().next().map(|(node, _)| *node)
    }

    /// Get the partition ID for a given key
    pub fn get_partition_for_key(&self, key: &[u8]) -> u64 {
        let hash = self.hash_key(key);
        hash % self.num_partitions
    }

    /// Get all nodes in the ring
    pub fn nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.nodes.iter()
    }

    /// Get the number of nodes
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Hash a virtual node identifier
    fn hash_vnode(&self, node: NodeId, vnode_idx: usize) -> u64 {
        let mut data = Vec::with_capacity(16);
        data.extend_from_slice(&node.to_le_bytes());
        data.extend_from_slice(&(vnode_idx as u64).to_le_bytes());
        seahash::hash(&data)
    }

    /// Hash a key
    fn hash_key(&self, key: &[u8]) -> u64 {
        seahash::hash(key)
    }
}

impl PartitionStrategy for ConsistentHashRing {
    fn get_partition(&self, key: &[u8]) -> u64 {
        self.get_partition_for_key(key)
    }

    fn get_partitions_for_range(&self, _start: &[u8], _end: &[u8]) -> Vec<u64> {
        // For hash-based partitioning, range queries need all partitions
        // (keys are scattered by hash, not ordered)
        (0..self.num_partitions).collect()
    }
}

/// Hash-based partitioner for distributing keys across nodes (legacy)
pub struct Partitioner {
    /// Number of partitions
    num_partitions: usize,

    /// Mapping from partition ID to node ID
    partition_map: HashMap<usize, NodeId>,
}

impl Partitioner {
    /// Create a new partitioner with the given number of partitions
    pub fn new(num_partitions: usize, nodes: Vec<NodeId>) -> Self {
        let mut partition_map = HashMap::new();

        // Simple round-robin assignment of partitions to nodes
        for partition_id in 0..num_partitions {
            let node_index = partition_id % nodes.len();
            partition_map.insert(partition_id, nodes[node_index]);
        }

        Self {
            num_partitions,
            partition_map,
        }
    }

    /// Get the partition ID for a given key
    pub fn get_partition(&self, key: &[u8]) -> usize {
        // Simple hash-based partitioning
        let hash = ahash::AHasher::default();
        use std::hash::{Hash, Hasher};
        let mut hasher = hash;
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_partitions
    }

    /// Get the node ID responsible for a given key
    pub fn get_node(&self, key: &[u8]) -> Option<NodeId> {
        let partition = self.get_partition(key);
        self.partition_map.get(&partition).copied()
    }

    /// Get the node ID responsible for a given partition
    pub fn get_node_for_partition(&self, partition_id: usize) -> Option<NodeId> {
        self.partition_map.get(&partition_id).copied()
    }

    /// Rebalance partitions when nodes are added or removed
    pub fn rebalance(&mut self, nodes: Vec<NodeId>) {
        self.partition_map.clear();
        for partition_id in 0..self.num_partitions {
            let node_index = partition_id % nodes.len();
            self.partition_map.insert(partition_id, nodes[node_index]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consistent_hash_ring_basic() {
        let mut ring = ConsistentHashRing::new(16, 100);
        ring.add_node(1);
        ring.add_node(2);
        ring.add_node(3);

        // Keys should map consistently
        let key = b"test_key";
        let node1 = ring.get_node(key);
        let node2 = ring.get_node(key);
        assert_eq!(node1, node2);

        // Partition should be consistent
        let p1 = ring.get_partition_for_key(key);
        let p2 = ring.get_partition_for_key(key);
        assert_eq!(p1, p2);
    }

    #[test]
    fn test_consistent_hash_ring_add_node() {
        let mut ring = ConsistentHashRing::new(16, 100);
        ring.add_node(1);
        ring.add_node(2);

        // Record mappings for some keys
        let keys: Vec<&[u8]> = vec![b"key1", b"key2", b"key3", b"key4", b"key5"];
        let before: Vec<_> = keys.iter().map(|k| ring.get_node(k)).collect();

        // Add a new node
        ring.add_node(3);

        // Some mappings should change, but not all
        let after: Vec<_> = keys.iter().map(|k| ring.get_node(k)).collect();
        let changed = before
            .iter()
            .zip(after.iter())
            .filter(|(b, a)| b != a)
            .count();

        // With consistent hashing, only ~1/n keys should move
        // We're being lenient here since we only have 5 keys
        assert!(changed <= 3, "Too many keys moved: {}", changed);
    }

    #[test]
    fn test_consistent_hash_ring_remove_node() {
        let mut ring = ConsistentHashRing::new(16, 100);
        ring.add_node(1);
        ring.add_node(2);
        ring.add_node(3);

        ring.remove_node(2);

        assert_eq!(ring.node_count(), 2);

        // All keys should still route to remaining nodes
        let node = ring.get_node(b"test");
        assert!(node == Some(1) || node == Some(3));
    }

    #[test]
    fn test_partition_strategy_trait() {
        let ring = ConsistentHashRing::with_nodes(16, 100, vec![1, 2, 3]);

        let strategy: &dyn PartitionStrategy = &ring;
        let partition = strategy.get_partition(b"test");
        assert!(partition < 16);

        let partitions = strategy.get_partitions_for_range(b"a", b"z");
        assert_eq!(partitions.len(), 16); // Hash-based returns all partitions
    }
}
