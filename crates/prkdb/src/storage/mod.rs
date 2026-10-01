pub mod cache;
pub mod checkpoint; // Empty until Task 2.14 writes an index snapshot
pub mod collection_partitioned_adapter; // Per-collection routing over the one WAL (D11)
pub mod config;
pub mod format; // FORMAT marker and open rules (D3)
pub mod lock; // Data-directory lock (STO-10)
pub mod migrations; // Migration registry (D4)
pub mod recovery;
pub mod snapshot;
pub mod wal_adapter;
pub mod writer_liveness; // Client-side time bounds for the WAL write path

mod in_memory;
pub use in_memory::InMemoryAdapter;

pub use config::CompactionConfig;

// Export WAL adapters
pub use collection_partitioned_adapter::CollectionPartitionedAdapter;
pub use wal_adapter::WalStorageAdapter;
