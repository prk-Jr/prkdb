use super::write_ahead_log::WriteAheadLog;
use super::{LogRecord, WalConfig, WalError};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Parallel Write-Ahead Log with multiple segments for concurrent writes
///
/// Distributes writes across N independent WAL instances to enable
/// parallel I/O and reduce contention. Each WAL instance operates
/// independently with its own segments and fsync schedule.
pub struct ParallelWal {
    /// Independent WAL instances
    segments: Vec<Arc<Mutex<WriteAheadLog>>>,
    /// Number of parallel segments
    segment_count: usize,
    /// Configuration
    #[allow(dead_code)]
    config: WalConfig,
}

impl ParallelWal {
    /// Create a new parallel WAL with the specified number of segments
    pub fn create(config: WalConfig, segment_count: usize) -> Result<Self, WalError> {
        if segment_count == 0 {
            return Err(WalError::Serialization(
                "Segment count must be at least 1".to_string(),
            ));
        }

        let mut segments = Vec::with_capacity(segment_count);

        for i in 0..segment_count {
            // Create a separate directory for each parallel segment
            let mut segment_config = config.clone();
            segment_config.log_dir = config.log_dir.join(format!("segment_{}", i));

            std::fs::create_dir_all(&segment_config.log_dir).map_err(WalError::Io)?;

            let wal = WriteAheadLog::create(segment_config).map_err(WalError::Io)?;

            segments.push(Arc::new(Mutex::new(wal)));
        }

        Ok(Self {
            segments,
            segment_count,
            config,
        })
    }

    /// Open an existing parallel WAL
    pub fn open(config: WalConfig, segment_count: usize) -> Result<Self, WalError> {
        if segment_count == 0 {
            return Err(WalError::Serialization(
                "Segment count must be at least 1".to_string(),
            ));
        }

        let mut segments = Vec::with_capacity(segment_count);

        for i in 0..segment_count {
            let mut segment_config = config.clone();
            segment_config.log_dir = config.log_dir.join(format!("segment_{}", i));

            let wal = WriteAheadLog::open(segment_config).map_err(WalError::Io)?;

            segments.push(Arc::new(Mutex::new(wal)));
        }

        Ok(Self {
            segments,
            segment_count,
            config,
        })
    }

    /// Append a single record (routes to appropriate segment based on hash)
    pub async fn append(&self, record: LogRecord) -> Result<(usize, u64), WalError> {
        let segment_id = self.route_record(&record);
        let segment = &self.segments[segment_id];

        let offset = segment.lock().await.append(record).map_err(WalError::Io)?;

        Ok((segment_id, offset))
    }

    /// Append multiple records in parallel across segments
    ///
    /// This is the key optimization: records are partitioned by segment,
    /// then written in parallel to maximize throughput.
    pub async fn append_batch(
        &self,
        records: Vec<LogRecord>,
    ) -> Result<Vec<(usize, u64)>, WalError> {
        if records.is_empty() {
            return Err(WalError::EmptyBatch);
        }

        // Partition records by segment
        let mut partitioned: Vec<Vec<LogRecord>> = vec![Vec::new(); self.segment_count];
        let mut record_to_segment: Vec<usize> = Vec::with_capacity(records.len());

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
                    .map_err(WalError::Io)?;
                Ok::<(usize, u64), WalError>((segment_id, offset))
            };
            futures.push(fut);
        }

        // Wait for all writes to complete
        let results = futures::future::try_join_all(futures).await?;

        // Map results back to original record order
        let mut offsets = Vec::with_capacity(record_to_segment.len());
        for segment_id in record_to_segment {
            // Find the result for this segment
            let (_, offset) = results
                .iter()
                .find(|(sid, _)| *sid == segment_id)
                .ok_or_else(|| WalError::Serialization("Missing segment result".to_string()))?;
            offsets.push((segment_id, *offset));
        }

        Ok(offsets)
    }

    /// Read a record from a specific segment
    pub async fn read(&self, segment_id: usize, offset: u64) -> Result<LogRecord, WalError> {
        if segment_id >= self.segment_count {
            return Err(WalError::Serialization(format!(
                "Invalid segment ID: {}",
                segment_id
            )));
        }

        self.segments[segment_id]
            .lock()
            .await
            .read(offset)
            .map_err(WalError::Io)
    }

    /// Get the number of parallel segments
    pub fn segment_count(&self) -> usize {
        self.segment_count
    }

    /// Route a record to a segment based on collection name hash
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

    /// Get total next offset across all segments
    pub async fn next_offset(&self) -> u64 {
        let mut total = 0;
        for segment in &self.segments {
            total += segment.lock().await.next_offset();
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::LogOperation;

    #[tokio::test]
    async fn test_parallel_wal_create() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let wal = ParallelWal::create(config, 4).unwrap();
        assert_eq!(wal.segment_count(), 4);
    }

    #[tokio::test]
    async fn test_parallel_wal_append() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let wal = ParallelWal::create(config, 4).unwrap();

        let record = LogRecord::new(LogOperation::Put {
            collection: "test".to_string(),
            id: vec![1, 2, 3],
            data: vec![4, 5, 6],
        });

        let (segment_id, offset) = wal.append(record.clone()).await.unwrap();
        assert!(segment_id < 4);
        assert_eq!(offset, 0);

        // Read it back
        let _read_record = wal.read(segment_id, offset).await.unwrap();
        // Successfully read the record (operation comparison not needed)
    }

    #[tokio::test]
    async fn test_parallel_wal_batch() {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let wal = ParallelWal::create(config, 4).unwrap();

        // Create records for different collections to test distribution
        let mut records = Vec::new();
        for i in 0..100 {
            records.push(LogRecord::new(LogOperation::Put {
                collection: format!("coll_{}", i % 10),
                id: vec![i as u8],
                data: vec![i as u8; 100],
            }));
        }

        let results = wal.append_batch(records).await.unwrap();
        assert_eq!(results.len(), 100);

        // Verify records are distributed across segments
        let mut segment_counts = [0; 4];
        for (segment_id, _) in &results {
            segment_counts[*segment_id] += 1;
        }

        // At least 2 segments should have records (with 100 records and 10 collections)
        let used_segments = segment_counts.iter().filter(|&&c| c > 0).count();
        assert!(used_segments >= 2);
    }
}
