pub mod cache;
pub mod collection_partitioned_adapter; // Kafka-style collection partitioning for 4-7x performance!
pub mod config;
pub mod recovery;
pub mod sharded_wal_adapter; // Phase 2: Multi-WAL sharding for 5-10x performance
pub mod wal_adapter;
pub mod write_queue; // Phase 2: Dedicated sync writer

// Re-export the old InMemoryAdapter from storage_old_inmemory
pub use crate::storage_old_inmemory::InMemoryAdapter;

// Export WAL adapters
pub use collection_partitioned_adapter::CollectionPartitionedAdapter;
pub use sharded_wal_adapter::ShardedWalAdapter;
pub use wal_adapter::WalStorageAdapter;
