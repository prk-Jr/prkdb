//! # prkdb-proto
//!
//! Protocol buffer definitions for PrkDB.
//!
//! This crate provides the gRPC service definitions and message types
//! shared between client and server components:
//!
//! - `RaftService` - Internal Raft consensus protocol
//! - `PrkDbService` - Client-facing data operations (Put, Get, Delete, etc.)
//!
//! ## Usage
//!
//! ```rust,ignore
//! use prkdb_proto::raft::{PutRequest, PutResponse};
//! use prkdb_proto::raft::prk_db_service_client::PrkDbServiceClient;
//! ```

/// Generated protobuf types and gRPC service definitions
pub mod raft {
    tonic::include_proto!("raft");
}

// Re-export commonly used types for convenience
pub use raft::{
    // Client service
    prk_db_service_client::PrkDbServiceClient,
    prk_db_service_server::{PrkDbService, PrkDbServiceServer},

    // Raft service (internal)
    raft_service_client::RaftServiceClient,
    raft_service_server::{RaftService, RaftServiceServer},

    AppendEntriesRequest,
    AppendEntriesResponse,
    BatchPutRequest,
    BatchPutResponse,
    DeleteRequest,
    DeleteResponse,
    GetRequest,
    GetResponse,
    HealthRequest,
    HealthResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    KvPair,
    LogEntry,
    MetadataRequest,
    MetadataResponse,
    NodeInfo,
    PartitionInfo,

    PreVoteRequest,
    PreVoteResponse,
    // Common messages
    PutRequest,
    PutResponse,
    ReadIndexRequest,
    ReadIndexResponse,
    ReadMode,
    // Raft messages
    RequestVoteRequest,
    RequestVoteResponse,
    // Admin messages for consumer/replication
    ResetConsumerOffsetRequest,
    ResetConsumerOffsetResponse,
    StartReplicationRequest,
    StartReplicationResponse,
    StopReplicationRequest,
    StopReplicationResponse,
};
