use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// Unique identifier for a node in the cluster
pub type NodeId = u64;

/// Configuration for the Raft cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// ID of this node
    pub local_node_id: NodeId,

    /// Address to listen on for Raft RPCs
    pub listen_addr: SocketAddr,

    /// Map of all nodes in the cluster (ID -> Address)
    pub nodes: Vec<(NodeId, SocketAddr)>,

    /// Election timeout in milliseconds (min)
    pub election_timeout_min_ms: u64,

    /// Election timeout in milliseconds (max)
    pub election_timeout_max_ms: u64,

    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,

    /// Partition ID this node belongs to
    #[serde(default)]
    pub partition_id: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            local_node_id: 1,
            listen_addr: "127.0.0.1:50051".parse().unwrap(),
            nodes: vec![(1, "127.0.0.1:50051".parse().unwrap())],
            election_timeout_min_ms: 500,
            election_timeout_max_ms: 1000,
            heartbeat_interval_ms: 100,
            partition_id: 0,
        }
    }
}
