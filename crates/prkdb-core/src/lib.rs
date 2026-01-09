pub mod batch_config;
pub mod batching;
pub mod buffer_pool; // Phase 5.2: Buffer pooling for serialization
pub mod collection;
pub mod compute;
pub mod consumer;
pub mod error;
pub mod index; // Secondary index support
pub mod replication;
pub mod serialization;
pub mod storage;
pub mod wal;

pub use collection::{
    Auditable, Collection, Hooks, SoftDeletable, Timestamped, Validatable, ValidationError,
    Versioned, VersionedRecord, WithComputed,
};
pub use index::{IndexDef, Indexed, IndexedStorage};
