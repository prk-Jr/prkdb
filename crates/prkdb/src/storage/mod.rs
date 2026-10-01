pub mod cache;
pub mod checkpoint; // Empty until Task 2.14 writes an index snapshot
pub mod collection_partitioned_adapter; // Kafka-style collection partitioning for 4-7x performance!
pub mod config;
pub mod format; // FORMAT marker and open rules (D3)
pub mod migrations; // Migration registry (D4)
pub mod partitioned_streaming_adapter; // Phase 24C: Multi-partition for 1+ GB/s
pub mod recovery;
pub mod sharded_wal_adapter; // Phase 2: Multi-WAL sharding for 5-10x performance
pub mod snapshot;
pub mod streaming_adapter; // Phase 24: High-throughput streaming (2x+ Kafka performance)
pub mod wal_adapter;
pub mod write_queue; // Phase 2: Dedicated sync writer
pub mod writer_liveness; // Client-side time bounds for the WAL write path

mod in_memory;
pub use in_memory::InMemoryAdapter;

pub use config::CompactionConfig;

// Export WAL adapters
pub use collection_partitioned_adapter::CollectionPartitionedAdapter;
pub use partitioned_streaming_adapter::{
    PartitionStrategy, PartitionedStreamingAdapter, PartitionedStreamingConfig,
};
pub use sharded_wal_adapter::ShardedWalAdapter;
pub use streaming_adapter::{StreamingConfig, StreamingRecord, StreamingStorageAdapter};
pub use wal_adapter::WalStorageAdapter;
