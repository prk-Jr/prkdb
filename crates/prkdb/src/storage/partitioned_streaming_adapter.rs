//! Partitioned Streaming Adapter - Phase 24C
//!
//! Multi-partition streaming for horizontal scaling to 1+ GB/s.
//! Each partition is an independent StreamingStorageAdapter with its own WAL.
//!
//! # Features
//! - **Partition parallelism**: N independent write paths
//! - **Lock-free routing**: Hash-based or round-robin partition selection
//! - **Linear scaling**: Near-linear throughput scaling with partitions
//!
//! # Example
//! ```ignore
//! let adapter = PartitionedStreamingAdapter::new(config, 8).await?;
//!
//! // Writes are distributed across partitions
//! let offset = adapter.append_batch(records).await?;
//! ```

use crate::storage::streaming_adapter::{
    StreamingConfig, StreamingRecord, StreamingStorageAdapter,
};
use prkdb_types::error::StorageError;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Configuration for partitioned streaming
#[derive(Debug, Clone)]
pub struct PartitionedStreamingConfig {
    /// Base directory for all partitions
    pub base_dir: PathBuf,
    /// Number of partitions
    pub partition_count: usize,
    /// Segments per partition
    pub segments_per_partition: usize,
    /// Sync after each batch (slower but durable)
    pub sync_each_batch: bool,
}

impl Default for PartitionedStreamingConfig {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("partitioned_streaming"),
            partition_count: 4,
            segments_per_partition: 2,
            sync_each_batch: false,
        }
    }
}

/// Partition routing strategy
#[derive(Debug, Clone, Copy)]
pub enum PartitionStrategy {
    /// Round-robin across partitions (best for throughput)
    RoundRobin,
    /// Hash-based routing (best for key ordering)
    KeyHash,
}

/// Multi-partition streaming adapter for horizontal scaling
///
/// Distributes writes across N independent partitions for linear scaling.
/// Each partition is a separate StreamingStorageAdapter with its own WAL.
pub struct PartitionedStreamingAdapter {
    inner: Arc<PartitionedInner>,
}

struct PartitionedInner {
    partitions: Vec<StreamingStorageAdapter>,
    partition_count: usize,
    next_partition: AtomicUsize,
    total_records: AtomicU64,
    strategy: PartitionStrategy,
}

impl Clone for PartitionedStreamingAdapter {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl PartitionedStreamingAdapter {
    /// Create a new partitioned streaming adapter
    pub async fn new(config: PartitionedStreamingConfig) -> Result<Self, StorageError> {
        Self::with_strategy(config, PartitionStrategy::RoundRobin).await
    }

    /// Create with specific routing strategy
    pub async fn with_strategy(
        config: PartitionedStreamingConfig,
        strategy: PartitionStrategy,
    ) -> Result<Self, StorageError> {
        let mut partitions = Vec::with_capacity(config.partition_count);

        for i in 0..config.partition_count {
            let partition_dir = config.base_dir.join(format!("partition_{}", i));

            let streaming_config = StreamingConfig {
                log_dir: partition_dir,
                segment_count: config.segments_per_partition,
                sync_each_batch: config.sync_each_batch,
                ..Default::default()
            };

            let adapter = StreamingStorageAdapter::new(streaming_config).await?;
            partitions.push(adapter);
        }

        Ok(Self {
            inner: Arc::new(PartitionedInner {
                partitions,
                partition_count: config.partition_count,
                next_partition: AtomicUsize::new(0),
                total_records: AtomicU64::new(0),
                strategy,
            }),
        })
    }

    /// Append a batch of records
    ///
    /// Records are routed to a partition based on the configured strategy.
    /// Returns a global offset combining partition ID and local offset.
    pub async fn append_batch(&self, records: Vec<StreamingRecord>) -> Result<u64, StorageError> {
        if records.is_empty() {
            return Ok(self.inner.total_records.load(Ordering::SeqCst));
        }

        // Select partition
        let partition_id = match self.inner.strategy {
            PartitionStrategy::RoundRobin => {
                self.inner.next_partition.fetch_add(1, Ordering::Relaxed)
                    % self.inner.partition_count
            }
            PartitionStrategy::KeyHash => {
                // Hash first record's key to select partition
                if let Some(first) = records.first() {
                    let mut hash: u64 = 0;
                    for byte in &first.key {
                        hash = hash.wrapping_mul(31).wrapping_add(*byte as u64);
                    }
                    (hash as usize) % self.inner.partition_count
                } else {
                    0
                }
            }
        };

        // Append to selected partition
        let record_count = records.len() as u64;
        let local_offset = self.inner.partitions[partition_id]
            .append_batch(records)
            .await?;

        // Update total count
        let global_offset = self
            .inner
            .total_records
            .fetch_add(record_count, Ordering::SeqCst);

        // Encode partition ID into offset (top 8 bits = partition, rest = local offset)
        let encoded_offset = ((partition_id as u64) << 56) | (local_offset & 0x00FFFFFFFFFFFFFF);

        Ok(encoded_offset)
    }

    /// Append to all partitions in parallel (maximum throughput)
    ///
    /// Splits the batch evenly across all partitions and writes in parallel.
    pub async fn append_batch_parallel(
        &self,
        records: Vec<StreamingRecord>,
    ) -> Result<u64, StorageError> {
        if records.is_empty() {
            return Ok(self.inner.total_records.load(Ordering::SeqCst));
        }

        let record_count = records.len();
        let partition_count = self.inner.partition_count;
        let chunk_size = (record_count + partition_count - 1) / partition_count;

        // Split records into chunks for each partition
        let chunks: Vec<Vec<StreamingRecord>> = records
            .into_iter()
            .collect::<Vec<_>>()
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();

        // Write to all partitions in parallel
        let mut handles = Vec::with_capacity(chunks.len());

        for (i, chunk) in chunks.into_iter().enumerate() {
            if i >= partition_count {
                break;
            }
            let partition = self.inner.partitions[i].clone();
            handles.push(tokio::spawn(
                async move { partition.append_batch(chunk).await },
            ));
        }

        // Wait for all writes to complete
        for handle in handles {
            handle
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))??;
        }

        // Return global offset
        Ok(self
            .inner
            .total_records
            .fetch_add(record_count as u64, Ordering::SeqCst))
    }

    /// Get total records written across all partitions
    pub fn total_records(&self) -> u64 {
        self.inner.total_records.load(Ordering::SeqCst)
    }

    /// Get partition count
    pub fn partition_count(&self) -> usize {
        self.inner.partition_count
    }

    /// Sync all partitions to disk
    pub async fn sync(&self) -> Result<(), StorageError> {
        for partition in &self.inner.partitions {
            partition.sync().await?;
        }
        Ok(())
    }

    /// Get per-partition offsets
    pub fn partition_offsets(&self) -> Vec<u64> {
        self.inner
            .partitions
            .iter()
            .map(|p| p.current_offset())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_partitioned_basic() {
        let dir = tempfile::tempdir().unwrap();
        let config = PartitionedStreamingConfig {
            base_dir: dir.path().to_path_buf(),
            partition_count: 4,
            segments_per_partition: 2,
            ..Default::default()
        };

        let adapter = PartitionedStreamingAdapter::new(config).await.unwrap();

        let records: Vec<StreamingRecord> = (0..100)
            .map(|i| StreamingRecord {
                key: format!("key_{}", i).into_bytes(),
                value: vec![i as u8; 100],
            })
            .collect();

        let offset = adapter.append_batch(records).await.unwrap();
        assert!(offset > 0 || adapter.total_records() > 0);
    }

    #[tokio::test]
    async fn test_partitioned_parallel() {
        let dir = tempfile::tempdir().unwrap();
        let config = PartitionedStreamingConfig {
            base_dir: dir.path().to_path_buf(),
            partition_count: 4,
            ..Default::default()
        };

        let adapter = PartitionedStreamingAdapter::new(config).await.unwrap();

        let records: Vec<StreamingRecord> = (0..1000)
            .map(|i| StreamingRecord {
                key: format!("key_{}", i).into_bytes(),
                value: vec![i as u8; 100],
            })
            .collect();

        adapter.append_batch_parallel(records).await.unwrap();
        assert_eq!(adapter.total_records(), 1000);
    }
}
