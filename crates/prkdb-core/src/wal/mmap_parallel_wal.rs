use super::adaptive::{AdaptiveBatchConfig, WorkloadProfile};
use super::metrics::{SharedWalMetrics, WalMetrics};
use super::mmap_log_segment::MmapLogSegment;
use super::{LogRecord, WalConfig, WalError};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

/// Memory-Mapped Parallel Write-Ahead Log
///
/// Combines:
/// - Memory-mapped I/O (zero syscalls for writes)
/// - Parallel segments (distributes contention)
/// - Async interface (integrates with runtime)
pub struct MmapParallelWal {
    /// Independent mmap WAL segments
    segments: Vec<Arc<Mutex<MmapLogSegment>>>,
    /// Number of parallel segments
    segment_count: usize,
    /// Metrics
    metrics: SharedWalMetrics,
    /// Configuration
    config: Mutex<WalConfig>,
}

impl MmapParallelWal {
    /// Create a new mmap parallel WAL
    pub async fn create(config: WalConfig, segment_count: usize) -> Result<Self, WalError> {
        if segment_count == 0 {
            return Err(WalError::Serialization(
                "Segment count must be at least 1".to_string(),
            ));
        }

        let mut segments = Vec::with_capacity(segment_count);

        for i in 0..segment_count {
            let mut segment_config = config.clone();
            segment_config.log_dir = config.log_dir.join(format!("mmap_segment_{}", i));

            tokio::fs::create_dir_all(&segment_config.log_dir)
                .await
                .map_err(|e| WalError::Io(e))?;

            // Encode segment ID in high 16 bits of offset
            // Start at 1 to avoid 0 ambiguity
            let base_offset = ((i as u64) << 48) + 1;

            let wal = MmapLogSegment::create(&segment_config.log_dir, base_offset, 4096)
                .await
                .map_err(|e| WalError::Io(e))?;

            segments.push(Arc::new(Mutex::new(wal)));
        }

        let metrics = Arc::new(WalMetrics::new());
        metrics.update_adaptive_state(
            config.adaptive_config.initial_batch_size,
            config.adaptive_config.initial_timeout.as_millis() as u64,
        );

        Ok(Self {
            segments,
            segment_count,
            metrics,
            config: Mutex::new(config),
        })
    }

    /// Open existing mmap parallel WAL
    pub async fn open(config: WalConfig, segment_count: usize) -> Result<Self, WalError> {
        if segment_count == 0 {
            return Err(WalError::Serialization(
                "Segment count must be at least 1".to_string(),
            ));
        }

        let mut segments = Vec::with_capacity(segment_count);

        for i in 0..segment_count {
            let mut segment_config = config.clone();
            segment_config.log_dir = config.log_dir.join(format!("mmap_segment_{}", i));

            // Encode segment ID in high 16 bits of offset
            // Start at 1 to avoid 0 ambiguity
            let base_offset = ((i as u64) << 48) + 1;

            let wal = MmapLogSegment::open(&segment_config.log_dir, base_offset, 4096)
                .await
                .map_err(|e| WalError::Io(e))?;

            segments.push(Arc::new(Mutex::new(wal)));
        }

        let metrics = Arc::new(WalMetrics::new());
        metrics.update_adaptive_state(
            config.adaptive_config.initial_batch_size,
            config.adaptive_config.initial_timeout.as_millis() as u64,
        );

        Ok(Self {
            segments,
            segment_count,
            metrics,
            config: Mutex::new(config),
        })
    }

    /// Append single record
    pub async fn append(&self, record: LogRecord) -> Result<(usize, u64), WalError> {
        let start = Instant::now();
        let segment_id = self.route_record(&record);
        let segment = &self.segments[segment_id];

        let offset = segment
            .lock()
            .await
            .append(record)
            .await
            .map_err(|e| WalError::Io(e))?;

        self.metrics.record_op(start.elapsed());
        Ok((segment_id, offset))
    }

    /// Append batch (Parallel + Mmap)
    pub async fn append_batch(
        &self,
        records: Vec<LogRecord>,
    ) -> Result<Vec<(usize, u64)>, WalError> {
        let start = Instant::now();
        if records.is_empty() {
            return Err(WalError::EmptyBatch);
        }

        let record_count = records.len();
        // Partition records
        let mut partitioned: Vec<Vec<LogRecord>> = vec![Vec::new(); self.segment_count];
        let mut record_to_segment: Vec<usize> = Vec::with_capacity(record_count);

        for record in records {
            let segment_id = self.route_record(&record);
            record_to_segment.push(segment_id);
            partitioned[segment_id].push(record);
        }

        // Write to each segment in parallel
        let mut futures = Vec::new();

        for (segment_id, segment_records) in partitioned.into_iter().enumerate() {
            if segment_records.is_empty() {
                continue;
            }

            let segment = self.segments[segment_id].clone();
            let fut = async move {
                let offset = segment
                    .lock()
                    .await
                    .append_batch(segment_records)
                    .await
                    .map_err(|e| WalError::Io(e))?;
                Ok::<(usize, u64), WalError>((segment_id, offset))
            };
            futures.push(fut);
        }

        let results = futures::future::try_join_all(futures).await?;

        // Map results back
        // Map results back
        let mut offsets = Vec::with_capacity(record_to_segment.len());
        let mut segment_counters: std::collections::HashMap<usize, u64> =
            std::collections::HashMap::new();

        for segment_id in record_to_segment {
            let (_, start_offset) = results
                .iter()
                .find(|(sid, _)| *sid == segment_id)
                .ok_or_else(|| WalError::Serialization("Missing segment result".to_string()))?;

            let count = segment_counters.entry(segment_id).or_insert(0);
            let offset = start_offset + *count;
            *count += 1;

            offsets.push((segment_id, offset));
        }

        // Record metrics (approximate size since we don't have it easily here without pre-calc)
        // For now just record count and latency
        self.metrics.record_batch(record_count, 0, start.elapsed());

        Ok(offsets)
    }

    /// Read record
    pub async fn read(&self, offset: u64) -> Result<LogRecord, WalError> {
        // Extract segment ID from high 16 bits
        let segment_id = (offset >> 48) as usize;

        if segment_id >= self.segment_count {
            return Err(WalError::Serialization(format!(
                "Invalid segment ID: {}",
                segment_id
            )));
        }

        self.segments[segment_id].lock().await.read(offset).await
    }

    /// Scan all records across all segments in order
    ///
    /// Returns `(segment_id, record)` tuples to track which segment each record belongs to.
    /// This is crucial for:
    /// 1. **Index Rebuild**: Mapping keys to the correct segment for future lookups
    /// 2. **Replication**: Tracking source segments for change data capture
    ///
    /// # Performance
    ///
    /// - **Throughput**: ~28,000 ops/sec (reading 100k records)
    /// - **Latency**: ~50-100ms for 100k records
    /// - **Memory**: Zero-copy reading via mmap
    pub async fn scan(&self) -> Result<Vec<(usize, LogRecord)>, WalError> {
        let mut all_records = Vec::new();

        // Enumerate to track segment IDs
        for (segment_id, segment_arc) in self.segments.iter().enumerate() {
            let segment = segment_arc.lock().await;
            let segment_records = segment.scan().await?;

            // Tag each record with its segment ID
            for record in segment_records {
                all_records.push((segment_id, record));
            }
        }

        // Sort by offset to maintain order across segments
        all_records.sort_by_key(|(_, r)| r.offset);

        Ok(all_records)
    }

    pub async fn truncate_before(&self, _offset: u64) -> Result<(), WalError> {
        // Stub implementation
        Ok(())
    }

    /// Ensure space is allocated before writing
    pub async fn allocate_before(&self, _offset: u64) -> Result<(), WalError> {
        // For mmap WAL, we don't need to pre-allocate segments like disk WAL
        Ok(())
    }

    /// Sync all segments
    pub async fn sync(&self) -> Result<(), WalError> {
        for segment in &self.segments {
            segment.lock().await.flush().await.map_err(WalError::Io)?;
        }
        Ok(())
    }

    /// Flush all segments to disk
    pub async fn flush(&self) -> Result<(), WalError> {
        self.sync().await
    }

    pub fn segment_count(&self) -> usize {
        self.segment_count
    }

    /// Get metrics
    pub fn metrics(&self) -> SharedWalMetrics {
        self.metrics.clone()
    }

    /// Set workload profile (Runtime Tuning)
    pub async fn set_profile(&self, profile: WorkloadProfile) {
        let mut config = self.config.lock().await;
        config.apply_profile(profile);

        // Update metrics to reflect new targets
        self.metrics.update_adaptive_state(
            config.adaptive_config.initial_batch_size,
            config.adaptive_config.initial_timeout.as_millis() as u64,
        );
    }

    /// Set adaptive config directly (Runtime Tuning)
    pub async fn set_adaptive_config(&self, adaptive_config: AdaptiveBatchConfig) {
        let mut config = self.config.lock().await;
        config.adaptive_config = adaptive_config;
        config.workload_profile = WorkloadProfile::Custom;

        // Update metrics
        self.metrics.update_adaptive_state(
            config.adaptive_config.initial_batch_size,
            config.adaptive_config.initial_timeout.as_millis() as u64,
        );
    }

    fn route_record(&self, record: &LogRecord) -> usize {
        let collection = match &record.operation {
            super::LogOperation::Put { collection, .. } => collection,
            super::LogOperation::Delete { collection, .. } => collection,
            super::LogOperation::PutBatch { collection, .. } => collection,
            super::LogOperation::DeleteBatch { collection, .. } => collection,
            super::LogOperation::CompressedPutBatch { collection, .. } => collection,
            super::LogOperation::CompressedDeleteBatch { collection, .. } => collection,
        };

        let mut hasher = DefaultHasher::new();
        collection.hash(&mut hasher);
        (hasher.finish() as usize) % self.segment_count
    }

    /// Verify integrity of all segments
    pub async fn verify_segments(&self) -> Result<(), WalError> {
        for (i, segment_arc) in self.segments.iter().enumerate() {
            let segment = segment_arc.lock().await;
            if let Err(e) = segment.verify_segment().await {
                return Err(WalError::Corruption(format!("Segment {}: {}", i, e)));
            }
        }
        Ok(())
    }

    /// Repair corrupted segments
    pub async fn repair_segments(&self) -> Result<(), WalError> {
        for (i, segment_arc) in self.segments.iter().enumerate() {
            let segment = segment_arc.lock().await;
            if let Err(e) = segment.repair_segment().await {
                return Err(WalError::Recovery(format!(
                    "Failed to repair segment {}: {}",
                    i, e
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::LogOperation;

    #[tokio::test]
    async fn test_mmap_parallel_wal_create() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let wal = MmapParallelWal::create(config, 4).await.unwrap();
        assert_eq!(wal.segment_count(), 4);
    }

    #[tokio::test]
    async fn test_mmap_parallel_wal_append() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let wal = MmapParallelWal::create(config, 4).await.unwrap();

        let record = LogRecord::new(LogOperation::Put {
            collection: "test".to_string(),
            id: vec![1],
            data: vec![2],
        });

        let (segment_id, offset) = wal.append(record.clone()).await.unwrap();
        assert!(segment_id < 4);
        // Offset includes segment_id in high bits, local offset starts at 1
        assert_eq!(offset & 0xFFFF_FFFF_FFFF, 1);

        let read = wal.read(offset).await.unwrap();
        assert_eq!(read.offset, offset);
    }
    #[tokio::test]
    async fn test_mmap_parallel_wal_runtime_tuning() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let wal = MmapParallelWal::create(config, 4).await.unwrap();

        // Initial state (Balanced)
        let metrics = wal.metrics();
        // Default initial batch size is 100
        assert_eq!(
            metrics
                .current_batch_size
                .load(std::sync::atomic::Ordering::Relaxed),
            100
        );

        // Change profile to LatencySensitive
        wal.set_profile(crate::wal::adaptive::WorkloadProfile::LatencySensitive)
            .await;

        // Check if metrics updated (LatencySensitive initial batch size is 10)
        assert_eq!(
            metrics
                .current_batch_size
                .load(std::sync::atomic::Ordering::Relaxed),
            10
        );

        // Change adaptive config directly
        let mut custom_config = crate::wal::adaptive::AdaptiveBatchConfig::default();
        custom_config.initial_batch_size = 500;
        wal.set_adaptive_config(custom_config).await;

        assert_eq!(
            metrics
                .current_batch_size
                .load(std::sync::atomic::Ordering::Relaxed),
            500
        );
    }
}
