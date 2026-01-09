use super::{log_record::LogRecord, offset_index::OffsetIndex};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// Async single segment file in the write-ahead log
///
/// This version uses tokio::fs::File for true async I/O,
/// eliminating the overhead of blocking I/O in async context.
pub struct AsyncLogSegment {
    /// First offset in this segment
    pub base_offset: u64,

    /// Log data file (.log) - async version
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

impl AsyncLogSegment {
    /// Create a new async log segment
    pub async fn create(
        dir: &Path,
        base_offset: u64,
        index_interval_bytes: u64,
    ) -> std::io::Result<Self> {
        tokio::fs::create_dir_all(dir).await?;

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
            .open(&log_path)
            .await?;

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

    /// Open an existing async log segment
    pub async fn open(
        dir: &Path,
        base_offset: u64,
        index_interval_bytes: u64,
    ) -> std::io::Result<Self> {
        let log_filename = format!("{:020}.log", base_offset);
        let index_filename = format!("{:020}.index", base_offset);

        let log_path = dir.join(&log_filename);
        let index_path = dir.join(&index_filename);

        let mut log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&log_path)
            .await?;

        // Get current file size
        let file_size = log_file.metadata().await?.len();

        let index = OffsetIndex::open(&index_path, base_offset, 1024 * 1024)?;

        // Find last offset by scanning file
        let last_offset = Self::find_last_offset(&mut log_file, base_offset).await?;

        Ok(Self {
            base_offset,
            log_file: Arc::new(Mutex::new(log_file)),
            index: Arc::new(Mutex::new(index)),
            current_offset: AtomicU64::new(last_offset + 1),
            file_position: AtomicU64::new(file_size),
            index_interval_bytes,
            bytes_since_last_index: AtomicU64::new(0),
        })
    }

    /// Append a record to this segment (async)
    pub async fn append(&self, mut record: LogRecord) -> std::io::Result<u64> {
        let offset = self.current_offset.fetch_add(1, Ordering::SeqCst);
        record.offset = offset;

        // Serialize record
        let config = bincode::config::standard();
        let serialized = bincode::encode_to_vec(&record, config)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        let record_size = serialized.len() as u32;
        let mut buffer = Vec::with_capacity(4 + serialized.len());
        buffer.extend_from_slice(&record_size.to_le_bytes());
        buffer.extend_from_slice(&serialized);

        // Write to file (async)
        let mut file = self.log_file.lock().await;
        file.write_all(&buffer).await?;
        file.sync_all().await?; // Async fsync

        let bytes_written = buffer.len() as u64;
        let file_pos = self
            .file_position
            .fetch_add(bytes_written, Ordering::SeqCst);

        // Update index if needed
        let bytes_since_index = self
            .bytes_since_last_index
            .fetch_add(bytes_written, Ordering::SeqCst);

        if bytes_since_index >= self.index_interval_bytes {
            let mut index = self.index.lock().await;
            index.append(offset, file_pos)?;
            self.bytes_since_last_index.store(0, Ordering::SeqCst);
        }

        Ok(offset)
    }

    /// Append multiple records to this segment (async batch)
    pub async fn append_batch(&self, records: Vec<LogRecord>) -> std::io::Result<u64> {
        if records.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot append empty batch",
            ));
        }

        let start_offset = self.current_offset.load(Ordering::SeqCst);
        let num_records = records.len();
        let mut buffer = Vec::with_capacity(records.len() * 1024);

        // Serialize all records
        for (i, mut record) in records.into_iter().enumerate() {
            let offset = start_offset + i as u64;
            record.offset = offset;

            let config = bincode::config::standard();
            let serialized = bincode::encode_to_vec(&record, config)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

            let record_size = serialized.len() as u32;
            buffer.extend_from_slice(&record_size.to_le_bytes());
            buffer.extend_from_slice(&serialized);
        }

        let last_offset = start_offset + num_records as u64 - 1;

        // Single async write for entire batch
        let mut file = self.log_file.lock().await;
        file.write_all(&buffer).await?;
        file.sync_all().await?; // Async fsync

        let bytes_written = buffer.len() as u64;
        let file_pos = self
            .file_position
            .fetch_add(bytes_written, Ordering::SeqCst);
        self.current_offset.store(last_offset + 1, Ordering::SeqCst);

        // Update index
        let bytes_since_index = self
            .bytes_since_last_index
            .fetch_add(bytes_written, Ordering::SeqCst);

        if bytes_since_index >= self.index_interval_bytes {
            let mut index = self.index.lock().await;
            index.append(last_offset, file_pos)?;
            self.bytes_since_last_index.store(0, Ordering::SeqCst);
        }

        Ok(last_offset)
    }

    /// Read a record at a specific offset (async)
    pub async fn read(&self, offset: u64) -> std::io::Result<LogRecord> {
        if offset < self.base_offset {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Offset {} is before base offset {}",
                    offset, self.base_offset
                ),
            ));
        }

        // Get approximate file position from index
        let index = self.index.lock().await;
        let file_pos = index.lookup(offset).unwrap_or(0) as u64;
        drop(index);

        // Scan from that position
        let mut file = self.log_file.lock().await;
        file.seek(std::io::SeekFrom::Start(file_pos)).await?;

        loop {
            // Read record size
            let mut size_buf = [0u8; 4];
            match file.read_exact(&mut size_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Offset {} not found", offset),
                    ));
                }
                Err(e) => return Err(e),
            }

            let record_size = u32::from_le_bytes(size_buf) as usize;

            // Read record data
            let mut record_buf = vec![0u8; record_size];
            file.read_exact(&mut record_buf).await?;

            // Deserialize
            let config = bincode::config::standard();
            let (record, _): (LogRecord, usize) = bincode::decode_from_slice(&record_buf, config)
                .map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
            })?;

            if record.offset == offset {
                return Ok(record);
            } else if record.offset > offset {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Offset {} not found (skipped)", offset),
                ));
            }
            // Continue scanning
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

    /// Helper: Scan log file to find last offset (async)
    async fn find_last_offset(file: &mut File, base_offset: u64) -> std::io::Result<u64> {
        file.seek(std::io::SeekFrom::Start(0)).await?;
        let mut last_offset = base_offset.saturating_sub(1);

        loop {
            // Read record size
            let mut size_buf = [0u8; 4];
            match file.read_exact(&mut size_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let record_size = u32::from_le_bytes(size_buf) as usize;

            // Read record
            let mut record_buf = vec![0u8; record_size];
            file.read_exact(&mut record_buf).await?;

            // Deserialize to get offset
            let config = bincode::config::standard();
            if let Ok((record, _)) = bincode::decode_from_slice::<LogRecord, _>(&record_buf, config)
            {
                last_offset = record.offset;
            }
        }

        Ok(last_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{LogOperation, LogRecord};

    #[tokio::test]
    async fn test_async_log_segment_append_read() {
        let dir = tempfile::tempdir().unwrap();

        let segment = AsyncLogSegment::create(dir.path(), 0, 4096).await.unwrap();

        // Append a record
        let record = LogRecord::new(LogOperation::Put {
            collection: "test".to_string(),
            id: vec![1, 2, 3],
            data: vec![4, 5, 6],
        });

        let offset = segment.append(record.clone()).await.unwrap();
        assert_eq!(offset, 0);

        // Read it back
        let read_record = segment.read(offset).await.unwrap();
        assert_eq!(read_record.offset, offset);
    }

    #[tokio::test]
    async fn test_async_log_segment_batch() {
        let dir = tempfile::tempdir().unwrap();

        let segment = AsyncLogSegment::create(dir.path(), 100, 4096)
            .await
            .unwrap();

        // Create batch
        let records: Vec<_> = (0..10)
            .map(|i| {
                LogRecord::new(LogOperation::Put {
                    collection: "test".to_string(),
                    id: vec![i],
                    data: vec![i; 100],
                })
            })
            .collect();

        let last_offset = segment.append_batch(records).await.unwrap();
        assert_eq!(last_offset, 109); // 100 + 9

        // Read back
        let record = segment.read(105).await.unwrap();
        assert_eq!(record.offset, 105);
    }
}
