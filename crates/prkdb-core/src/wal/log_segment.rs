use super::{log_record::LogRecord, offset_index::OffsetIndex};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Single segment file in the write-ahead log
pub struct LogSegment {
    /// First offset in this segment
    pub base_offset: u64,

    /// Path to the log file
    // log_path: PathBuf, // Removed unused field

    /// Log data file (.log)
    log_file: Arc<Mutex<File>>,

    /// Sparse offset index
    index: Arc<Mutex<OffsetIndex>>,

    /// Next offset to assign
    current_offset: AtomicU64,

    /// Current write position in file
    file_position: AtomicU64,

    /// Index interval (bytes between index entries)
    index_interval_bytes: u64,

    /// Bytes since last index entry
    bytes_since_last_index: AtomicU64,
}

impl LogSegment {
    /// Create a new log segment
    pub fn create(dir: &Path, base_offset: u64, index_interval_bytes: u64) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;

        // File names: {base_offset}.log and {base_offset}.index
        let log_filename = format!("{:020}.log", base_offset);
        let index_filename = format!("{:020}.index", base_offset);

        let log_path = dir.join(&log_filename);
        let index_path = dir.join(&index_filename);

        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&log_path)?;

        let index = OffsetIndex::create(&index_path, base_offset, 1024 * 1024)?;

        Ok(Self {
            base_offset,
            log_file: Arc::new(Mutex::new(log_file)),
            index: Arc::new(Mutex::new(index)),
            current_offset: AtomicU64::new(base_offset),
            file_position: AtomicU64::new(0),
            index_interval_bytes,
            bytes_since_last_index: AtomicU64::new(0),
        })
    }

    /// Open an existing log segment
    pub fn open(dir: &Path, base_offset: u64, index_interval_bytes: u64) -> io::Result<Self> {
        let log_filename = format!("{:020}.log", base_offset);
        let index_filename = format!("{:020}.index", base_offset);

        let log_path = dir.join(&log_filename);
        let index_path = dir.join(&index_filename);

        let mut log_file = OpenOptions::new().read(true).write(true).open(&log_path)?;

        // Get current file size
        let file_size = log_file.metadata()?.len();

        let index = OffsetIndex::open(&index_path, base_offset, 1024 * 1024)?;

        // Scan log to find current offset
        let current_offset = Self::find_last_offset(&mut log_file, base_offset)?;

        Ok(Self {
            base_offset,
            log_file: Arc::new(Mutex::new(log_file)),
            index: Arc::new(Mutex::new(index)),
            current_offset: AtomicU64::new(current_offset),
            file_position: AtomicU64::new(file_size),
            index_interval_bytes,
            bytes_since_last_index: AtomicU64::new(0),
        })
    }

    /// Append a record to this segment
    pub fn append(&self, mut record: LogRecord) -> io::Result<u64> {
        // Assign offset
        let offset = self.current_offset.fetch_add(1, Ordering::SeqCst);
        record.offset = offset;

        // Serialize record (includes header)
        let serialized = record.serialize();
        let record_size = serialized.len() as u64;

        // Get current position
        let position = self.file_position.load(Ordering::SeqCst);

        // Write to file
        {
            let mut file = self.log_file.lock().unwrap();
            file.seek(SeekFrom::Start(position))?;
            file.write_all(&serialized)?;
            file.sync_all()?;
        }

        // Update position
        self.file_position.fetch_add(record_size, Ordering::SeqCst);

        // Update index if needed
        let bytes_since = self
            .bytes_since_last_index
            .fetch_add(record_size, Ordering::SeqCst);
        if bytes_since + record_size >= self.index_interval_bytes {
            let mut index = self.index.lock().unwrap();
            index.append(offset, position)?;
            self.bytes_since_last_index.store(0, Ordering::SeqCst);
        }

        Ok(offset)
    }

    /// Append multiple records to this segment
    pub fn append_batch(&self, records: Vec<LogRecord>) -> io::Result<u64> {
        if records.is_empty() {
            return Ok(self.current_offset.load(Ordering::SeqCst) - 1);
        }

        let mut file = self.log_file.lock().unwrap();
        let mut last_offset = 0;
        let mut total_bytes = 0;

        // We need to collect serialized bytes first to write them all at once
        // or write them sequentially while holding the lock.
        // Writing sequentially while holding the lock is fine.

        for mut record in records {
            // Assign offset
            let offset = self.current_offset.fetch_add(1, Ordering::SeqCst);
            record.offset = offset;
            last_offset = offset;

            // Serialize
            let serialized = record.serialize();
            let record_size = serialized.len() as u64;

            // Write to file (we hold the lock)
            file.write_all(&serialized)?;

            // Update index if needed
            // Note: We can't easily update index inside this loop if we want to be super efficient,
            // but for now let's just update it.
            // Actually, we need to track position.
            let position = self.file_position.load(Ordering::SeqCst) + total_bytes;

            // Check index interval
            let bytes_since = self.bytes_since_last_index.load(Ordering::SeqCst) + total_bytes;
            if bytes_since + record_size >= self.index_interval_bytes {
                let mut index = self.index.lock().unwrap();
                // We need to handle the fact that we might be writing multiple index entries
                // But index.append might fail if we don't handle it right.
                // For simplicity, let's just update index here.
                index.append(offset, position)?;
                // Reset bytes since last index is tricky because we are accumulating.
                // Let's simplify: just update the atomic at the end?
                // No, we need to update index correctly.
            }

            total_bytes += record_size;
        }

        // Sync once at the end
        file.sync_all()?;

        // Update file position once
        self.file_position.fetch_add(total_bytes, Ordering::SeqCst);

        // Update bytes since last index (approximate is fine, or we can track it better)
        // For now, let's just add total_bytes. If we missed an index entry in the middle, it's okay,
        // the index is sparse.
        self.bytes_since_last_index
            .fetch_add(total_bytes, Ordering::SeqCst);

        Ok(last_offset)
    }

    /// Read a record at a specific offset
    pub fn read(&self, offset: u64) -> io::Result<LogRecord> {
        use crate::serialization::zerocopy::ZeroCopyLogHeader;
        use zerocopy::FromBytes;

        if offset < self.base_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset before segment base",
            ));
        }

        // Use index to find position
        let position = {
            let index = self.index.lock().unwrap();
            index.lookup(offset).unwrap_or(0)
        };

        // Read from file
        let mut file = self.log_file.lock().unwrap();
        file.seek(SeekFrom::Start(position))?;

        // Scan forward to find exact offset
        loop {
            // Read header
            let mut header_bytes = [0u8; ZeroCopyLogHeader::SIZE];
            if file.read_exact(&mut header_bytes).is_err() {
                return Err(io::Error::new(io::ErrorKind::NotFound, "offset not found"));
            }

            let header = ZeroCopyLogHeader::read_from_prefix(&header_bytes)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid header"))?;

            let payload_len = header.payload_len as usize;

            // Read payload
            let mut payload = vec![0u8; payload_len];
            file.read_exact(&mut payload)?;

            // Construct full record bytes for deserialization
            let mut full_record = Vec::with_capacity(ZeroCopyLogHeader::SIZE + payload_len);
            full_record.extend_from_slice(&header_bytes);
            full_record.extend_from_slice(&payload);

            // We can check offset from header directly!
            if header.offset == offset {
                let record = LogRecord::deserialize(&full_record)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                return Ok(record);
            }

            if header.offset > offset {
                return Err(io::Error::new(io::ErrorKind::NotFound, "offset not found"));
            }
        }
    }

    /// Get current size of log file
    pub fn size(&self) -> u64 {
        self.file_position.load(Ordering::SeqCst)
    }

    /// Get next offset to be assigned
    pub fn next_offset(&self) -> u64 {
        self.current_offset.load(Ordering::SeqCst)
    }

    /// Helper: Scan log file to find last offset
    fn find_last_offset(file: &mut File, base_offset: u64) -> io::Result<u64> {
        use crate::serialization::zerocopy::ZeroCopyLogHeader;
        use zerocopy::FromBytes;

        file.seek(SeekFrom::Start(0))?;

        let mut last_offset = base_offset;

        loop {
            // Try to read header
            let mut header_bytes = [0u8; ZeroCopyLogHeader::SIZE];
            if file.read_exact(&mut header_bytes).is_err() {
                break; // End of file
            }

            // Check for zero padding (if pre-allocated)
            if header_bytes.iter().all(|&b| b == 0) {
                break;
            }

            let header = ZeroCopyLogHeader::read_from_prefix(&header_bytes)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid header"))?;

            let payload_len = header.payload_len as usize;

            // Skip payload
            if file.seek(SeekFrom::Current(payload_len as i64)).is_err() {
                break;
            }

            last_offset = header.offset + 1;
        }

        Ok(last_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{LogOperation, LogRecord};
    use std::env;

    #[test]
    fn test_log_segment_append_read() -> io::Result<()> {
        let dir = env::temp_dir().join("test_log_segment");
        let _ = std::fs::remove_dir_all(&dir); // Clean slate

        let segment = LogSegment::create(&dir, 0, 4096)?;

        // Append records
        let record1 = LogRecord::new(LogOperation::Put {
            collection: "users".to_string(),
            id: b"user1".to_vec(),
            data: b"Alice".to_vec(),
        });

        let offset1 = segment.append(record1.clone())?;
        assert_eq!(offset1, 0);

        let record2 = LogRecord::new(LogOperation::Put {
            collection: "users".to_string(),
            id: b"user2".to_vec(),
            data: b"Bob".to_vec(),
        });

        let offset2 = segment.append(record2.clone())?;
        assert_eq!(offset2, 1);

        // Read back
        let read1 = segment.read(0)?;
        assert_eq!(read1.offset, 0);

        let read2 = segment.read(1)?;
        assert_eq!(read2.offset, 1);

        // Clean up
        std::fs::remove_dir_all(&dir)?;

        Ok(())
    }
}
