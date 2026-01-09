use super::config::{ClusterConfig, NodeId};
use super::partitioner::Partitioner;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Router for directing client requests to the correct node
pub struct Router {
    /// Cluster configuration
    config: ClusterConfig,

    /// Partitioner for key distribution
    partitioner: Arc<RwLock<Partitioner>>,

    /// Current leader ID (updated by Raft consensus)
    current_leader: Arc<RwLock<Option<NodeId>>>,
}

impl Router {
    /// Create a new router
    pub fn new(config: ClusterConfig, num_partitions: usize) -> Self {
        let nodes: Vec<NodeId> = config.nodes.iter().map(|(id, _)| *id).collect();
        let partitioner = Partitioner::new(num_partitions, nodes);

        Self {
            config,
            partitioner: Arc::new(RwLock::new(partitioner)),
            current_leader: Arc::new(RwLock::new(None)),
        }
    }

    /// Get the node ID responsible for a given key
    pub async fn route_key(&self, key: &[u8]) -> Option<NodeId> {
        let partitioner = self.partitioner.read().await;
        partitioner.get_node(key)
    }

    /// Check if the local node is responsible for a given key
    pub async fn is_local_key(&self, key: &[u8]) -> bool {
        if let Some(node_id) = self.route_key(key).await {
            node_id == self.config.local_node_id
        } else {
            false
        }
    }

    /// Update the current leader
    pub async fn set_leader(&self, leader_id: Option<NodeId>) {
        let mut current_leader = self.current_leader.write().await;
        *current_leader = leader_id;
    }

    /// Get the current leader
    pub async fn get_leader(&self) -> Option<NodeId> {
        let current_leader = self.current_leader.read().await;
        *current_leader
    }
}
