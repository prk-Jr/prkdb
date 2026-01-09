use super::{config::WalConfig, log_record::LogRecord, log_segment::LogSegment};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, ErrorKind};
use std::sync::{Arc, RwLock};

/// Write-Ahead Log manager
///
/// Coordinates multiple log segments, handles segment rolling, and provides
/// a unified append/read API.
pub struct WriteAheadLog {
    config: WalConfig,

    /// All segments (offset → segment)
    segments: RwLock<BTreeMap<u64, Arc<LogSegment>>>,

    /// Currently active (writable) segment
    active_segment: RwLock<Arc<LogSegment>>,
}

impl WriteAheadLog {
    /// Create a new WAL
    pub fn create(config: WalConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.log_dir)?;

        let active_segment = Arc::new(LogSegment::create(
            &config.log_dir,
            0,
            config.index_interval_bytes,
        )?);

        let mut segments = BTreeMap::new();
        segments.insert(0, Arc::clone(&active_segment));

        Ok(Self {
            config,
            segments: RwLock::new(segments),
            active_segment: RwLock::new(active_segment),
        })
    }

    /// Open an existing WAL
    pub fn open(config: WalConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.log_dir)?;

        // Find all segment files
        let mut segment_offsets = Vec::new();

        for entry in fs::read_dir(&config.log_dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(ext) = path.extension() {
                if ext == "log" {
                    if let Some(filename) = path.file_stem() {
                        if let Some(name_str) = filename.to_str() {
                            if let Ok(offset) = name_str.parse::<u64>() {
                                segment_offsets.push(offset);
                            }
                        }
                    }
                }
            }
        }

        // Sort segments by offset
        segment_offsets.sort();

        if segment_offsets.is_empty() {
            // No existing segments, create a new one
            return Self::create(config);
        }

        // Open all segments
        let mut segments = BTreeMap::new();

        for offset in segment_offsets {
            let segment = Arc::new(LogSegment::open(
                &config.log_dir,
                offset,
                config.index_interval_bytes,
            )?);
            segments.insert(offset, segment);
        }

        // Last segment is active
        let active_offset = *segments.keys().last().unwrap();
        let active_segment = Arc::clone(segments.get(&active_offset).unwrap());

        Ok(Self {
            config,
            segments: RwLock::new(segments),
            active_segment: RwLock::new(active_segment),
        })
    }

    /// Append a record to the log (with compression if configured)
    pub fn append(&self, record: LogRecord) -> io::Result<u64> {
        // Note: Compression should be applied when creating the record
        // via LogRecord::new_with_compression() before calling append()

        let active = self.active_segment.read().unwrap();

        // Check if need to roll segment
        if active.size() >= self.config.segment_bytes {
            drop(active); // Release read lock
            self.roll_segment()?;

            // Get new active segment
            let active = self.active_segment.read().unwrap();
            active.append(record)
        } else {
            active.append(record)
        }
    }

    /// Append multiple records atomically in a batch
    ///
    /// This is optimized for batch writes:
    /// - Single write lock acquisition
    /// - Single fsync call (if sync enabled)
    /// - Better throughput for multiple records
    ///
    /// Returns the offset of the last record written
    pub fn append_batch(&self, records: Vec<LogRecord>) -> io::Result<u64> {
        if records.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Empty batch provided",
            ));
        }

        let active = self.active_segment.read().unwrap();

        // Check if need to roll segment (simplified check based on first record)
        // In a real implementation, we might need to split the batch across segments
        if active.size() >= self.config.segment_bytes {
            drop(active); // Release read lock
            self.roll_segment()?;

            // Get new active segment
            let active = self.active_segment.read().unwrap();
            active.append_batch(records)
        } else {
            active.append_batch(records)
        }
    }

    /// Read a record by offset
    pub fn read(&self, offset: u64) -> io::Result<LogRecord> {
        let segments = self.segments.read().unwrap();

        // Find segment containing this offset
        let mut segment_offset = 0;
        for (base_offset, _) in segments.iter() {
            if *base_offset <= offset {
                segment_offset = *base_offset;
            } else {
                break;
            }
        }

        let segment = segments
            .get(&segment_offset)
            .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "segment not found"))?;

        segment.read(offset)
    }

    /// Roll to a new active segment
    fn roll_segment(&self) -> io::Result<()> {
        let mut active_lock = self.active_segment.write().unwrap();
        let mut segments_lock = self.segments.write().unwrap();

        let next_offset = active_lock.next_offset();

        // Create new segment
        let new_segment = Arc::new(LogSegment::create(
            &self.config.log_dir,
            next_offset,
            self.config.index_interval_bytes,
        )?);

        // Add old segment to segments list
        let old_segment = std::mem::replace(&mut *active_lock, Arc::clone(&new_segment));
        segments_lock.insert(old_segment.base_offset, old_segment);

        // New segment is now active
        segments_lock.insert(next_offset, Arc::clone(&new_segment));

        Ok(())
    }

    /// Get the next offset that will be assigned
    pub fn next_offset(&self) -> u64 {
        self.active_segment.read().unwrap().next_offset()
    }

    /// Get number of segments
    pub fn segment_count(&self) -> usize {
        self.segments.read().unwrap().len()
    }

    /// Get compression config (useful for creating records)
    pub fn compression_config(&self) -> &crate::wal::compression::CompressionConfig {
        &self.config.compression
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{LogOperation, WalConfig};
    use std::env;

    #[test]
    fn test_wal_create_append_read() -> io::Result<()> {
        let dir = env::temp_dir().join("test_wal");
        let _ = fs::remove_dir_all(&dir);

        let config = WalConfig {
            log_dir: dir.clone(),
            ..WalConfig::test_config()
        };

        let wal = WriteAheadLog::create(config)?;

        // Append records
        let record1 = LogRecord::new(LogOperation::Put {
            collection: "users".to_string(),
            id: b"user1".to_vec(),
            data: b"Alice".to_vec(),
        });

        let offset1 = wal.append(record1)?;
        assert_eq!(offset1, 0);

        let record2 = LogRecord::new(LogOperation::Delete {
            collection: "users".to_string(),
            id: b"user2".to_vec(),
        });

        let offset2 = wal.append(record2)?;
        assert_eq!(offset2, 1);

        // Read back
        let read1 = wal.read(0)?;
        assert_eq!(read1.offset, 0);

        let read2 = wal.read(1)?;
        assert_eq!(read2.offset, 1);

        // Clean up
        fs::remove_dir_all(&dir)?;

        Ok(())
    }

    #[test]
    fn test_wal_segment_rolling() -> io::Result<()> {
        let dir = env::temp_dir().join("test_wal_rolling");
        let _ = fs::remove_dir_all(&dir);

        let config = WalConfig {
            log_dir: dir.clone(),
            segment_bytes: 200, // Small segment for testing
            ..WalConfig::test_config()
        };

        let wal = WriteAheadLog::create(config)?;

        // Append enough records to trigger roll
        for i in 0..10 {
            let record = LogRecord::new(LogOperation::Put {
                collection: "test".to_string(),
                id: format!("id{}", i).into_bytes(),
                data: vec![0u8; 50], // 50 bytes of data
            });
            wal.append(record)?;
        }

        // Should have created multiple segments
        assert!(wal.segment_count() > 1, "should have rolled segments");

        // Clean up
        fs::remove_dir_all(&dir)?;

        Ok(())
    }
}
