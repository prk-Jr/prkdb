pub mod batch_config;
pub mod command;
pub mod config;
pub mod grpc_service; // NEW: gRPC service for client data operations
pub mod node;
pub mod partition_manager;
pub mod partitioner;
pub mod range_partitioner;
pub mod router;
pub mod rpc_client;
pub mod server;
pub mod service;
pub mod state_machine;

// Re-export proto types from prkdb-proto
// This maintains backward compatibility for existing code using crate::raft::rpc::*
pub mod rpc {
    pub use prkdb_proto::raft::*;
}

pub use batch_config::RaftBatchConfig;
pub use config::{ClusterConfig, NodeId};
pub use grpc_service::PrkDbGrpcService;
pub use node::{RaftNode, RaftState};
pub use partition_manager::PartitionManager;
pub use partitioner::{ConsistentHashRing, PartitionStrategy, Partitioner};
pub use range_partitioner::RangePartitioner;
pub use router::Router;
pub use rpc_client::RpcClientPool;
pub use state_machine::{PrkDbStateMachine, StateMachine};
