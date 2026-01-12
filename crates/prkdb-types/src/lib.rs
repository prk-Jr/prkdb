//! # prkdb-types
//!
//! Core domain types and traits for PrkDB.
//!
//! This crate provides the foundational types that are shared across all PrkDB crates:
//! - Collection traits and types
//! - Storage adapter traits
//! - Error types
//! - Index abstractions
//! - Consumer/replication protocol types
//!
//! ## Design Philosophy
//!
//! This crate intentionally has minimal dependencies to:
//! 1. Enable lightweight client libraries
//! 2. Allow mock implementations for testing
//! 3. Provide clear separation between domain types and implementation

pub mod collection;
pub mod consumer;
pub mod error;
pub mod index;
pub mod replication;
pub mod snapshot;
pub mod storage;

// Re-exports for convenience
pub use collection::{
    Auditable, ChangeEvent, Collection, Hooks, SoftDeletable, Timestamped, Validatable,
    ValidationError, Versioned, VersionedRecord, WithComputed,
};
pub use consumer::{
    AutoOffsetReset, CommitResult, Consumer, ConsumerConfig, ConsumerGroupId, ConsumerRecord,
    Offset, OffsetStore,
};
pub use error::{ComputeError, ConsumerError, Error, StorageError};
pub use index::{IndexDef, Indexed, IndexedStorage};
pub use replication::{AckLevel, Change, ReplicationConfig};
pub use storage::{StorageAdapter, TransactionalStorageAdapter};
