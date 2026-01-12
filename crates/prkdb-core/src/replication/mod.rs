//! Replication module for PrkDB.
//!
//! This module provides leader-follower replication capabilities.

pub mod follower_server;
pub mod manager;
pub mod protocol;
pub mod replica_client;

// Re-export types from prkdb-types
pub use prkdb_types::replication::{AckLevel, Change, ReplicationConfig};

// Re-export local implementations
pub use follower_server::{FollowerServer, FollowerServerError};
pub use manager::{ReplicationError, ReplicationManager};
pub use protocol::{HealthCheckRequest, HealthCheckResponse, ReplicateRequest, ReplicateResponse};
pub use replica_client::{ReplicaClient, ReplicaClientError};
