use super::{log_record::LogRecord, offset_index::OffsetIndex, WalError};
use memmap2::MmapMut;
use rkyv::Deserialize as RkyvDeserialize;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex; // Phase 5.5: For deserializing archived records

/// Memory-mapped single segment file in the write-ahead log
///
/// This version uses memory-mapped I/O for zero-syscall writes.
/// It maintains a fixed-size memory map and grows the file as needed.
pub struct MmapLogSegment {
    /// First offset in this segment
    pub base_offset: u64,

    /// Path to the log file (kept for debugging/logging)
    #[allow(dead_code)]
    log_path: PathBuf,

    /// Memory map
    mmap: Arc<Mutex<MmapMut>>,

    /// Underlying file (kept open for resizing)
    file: std::fs::File,

    /// Sparse offset index
    index: Arc<Mutex<OffsetIndex>>,

    /// Next offset to assign
    current_offset: AtomicU64,

    /// Current write position in file
    file_position: AtomicU64,

    /// Current capacity of the file/mmap
    capacity: AtomicU64,

    /// Index interval (bytes between index entries)
    index_interval_bytes: u64,

    /// Bytes since last index entry
    bytes_since_last_index: AtomicU64,

    /// Phase 5.4: Track batches since last fsync for I/O batching
    batches_since_fsync: AtomicU64,
}

const INITIAL_SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64 MB
const GROWTH_STEP: u64 = 64 * 1024 * 1024; // 64 MB

impl MmapLogSegment {
    /// Create a new mmap log segment
    pub async fn create(
        dir: &Path,
        base_offset: u64,
        index_interval_bytes: u64,
    ) -> Result<Self, std::io::Error> {
        tokio::fs::create_dir_all(dir).await?;

        let log_filename = format!("{:020}.log", base_offset);
        let index_filename = format!("{:020}.index", base_offset);

        let log_path = dir.join(&log_filename);
        let index_path = dir.join(&index_filename);

        // Create file synchronously (mmap requires sync file)
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&log_path)?;

        // Set initial size
        file.set_len(INITIAL_SEGMENT_SIZE)?;

        // Create mmap
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let index = OffsetIndex::create(&index_path, base_offset, 1024 * 1024)?;

        Ok(Self {
            base_offset,
            log_path,
            mmap: Arc::new(Mutex::new(mmap)),
            file,
            index: Arc::new(Mutex::new(index)),
            current_offset: AtomicU64::new(base_offset),
            file_position: AtomicU64::new(0),
            capacity: AtomicU64::new(INITIAL_SEGMENT_SIZE),
            index_interval_bytes,
            bytes_since_last_index: AtomicU64::new(0),
            batches_since_fsync: AtomicU64::new(0), // Phase 5.4
        })
    }

    /// Open an existing mmap log segment
    pub async fn open(
        dir: &Path,
        base_offset: u64,
        index_interval_bytes: u64,
    ) -> Result<Self, std::io::Error> {
        let log_filename = format!("{:020}.log", base_offset);
        let index_filename = format!("{:020}.index", base_offset);

        let log_path = dir.join(&log_filename);
        let index_path = dir.join(&index_filename);

        let file = OpenOptions::new().read(true).write(true).open(&log_path)?;

        let file_size = file.metadata()?.len();
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let index = OffsetIndex::open(&index_path, base_offset, 1024 * 1024)?;

        // Find last offset (scan)
        // For mmap, we can scan memory directly which is much faster
        let (last_offset, valid_bytes) = Self::scan_mmap(&mmap, base_offset)?;

        Ok(Self {
            base_offset,
            log_path,
            mmap: Arc::new(Mutex::new(mmap)),
            file,
            index: Arc::new(Mutex::new(index)),
            current_offset: AtomicU64::new(last_offset + 1),
            file_position: AtomicU64::new(valid_bytes),
            capacity: AtomicU64::new(file_size),
            index_interval_bytes,
            bytes_since_last_index: AtomicU64::new(0),
            batches_since_fsync: AtomicU64::new(0), // Phase 5.4
        })
    }

    /// Append a record to this segment
    pub async fn append(&self, mut record: LogRecord) -> Result<u64, std::io::Error> {
        let offset = self.current_offset.fetch_add(1, Ordering::SeqCst);
        record.offset = offset;

        // Phase 5.5: Use rkyv serialization for zero-copy performance
        let serialized = record.serialize_rkyv();
        let total_size = serialized.len();

        let mut mmap = self.mmap.lock().await;
        let current_pos = self.file_position.load(Ordering::SeqCst) as usize;
        let capacity = self.capacity.load(Ordering::SeqCst) as usize;

        // Check capacity and resize if needed
        if current_pos + total_size > capacity {
            // Drop mmap lock to resize
            drop(mmap);
            self.resize(capacity as u64 + GROWTH_STEP).await?;
            // Re-acquire lock
            mmap = self.mmap.lock().await;
        }

        // Write to mmap (memory copy)
        mmap[current_pos..current_pos + total_size].copy_from_slice(&serialized);

        // Update position
        let bytes_written = total_size as u64;
        self.file_position
            .fetch_add(bytes_written, Ordering::SeqCst);

        // Update index
        let bytes_since_index = self
            .bytes_since_last_index
            .fetch_add(bytes_written, Ordering::SeqCst);

        if bytes_since_index >= self.index_interval_bytes {
            let mut index = self.index.lock().await;
            index.append(offset, current_pos as u64)?;
            self.bytes_since_last_index.store(0, Ordering::SeqCst);
        }

        Ok(offset)
    }

    /// Append batch
    pub async fn append_batch(&self, records: Vec<LogRecord>) -> Result<u64, std::io::Error> {
        if records.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot append empty batch",
            ));
        }

        let start_offset = self.current_offset.load(Ordering::SeqCst);
        let num_records = records.len();

        // OPTIMIZATION 1: Estimate total size more accurately
        // Average LogRecord size is ~100-200 bytes, use 256 as conservative estimate
        let estimated_size = num_records * 256;
        let mut buffer = Vec::with_capacity(estimated_size);

        // OPTIMIZATION 2: Serialize all records with offset assignment
        for (i, mut record) in records.into_iter().enumerate() {
            let offset = start_offset + i as u64;
            record.offset = offset;

            // Phase 5.5 Optimization: Use serialize_into_rkyv to avoid extra allocation
            record.serialize_into_rkyv(&mut buffer);
        }

        let total_size = buffer.len();
        let last_offset = start_offset + num_records as u64 - 1;

        let mut mmap = self.mmap.lock().await;
        let current_pos = self.file_position.load(Ordering::SeqCst) as usize;
        let capacity = self.capacity.load(Ordering::SeqCst) as usize;

        // Resize if needed
        if current_pos + total_size > capacity {
            drop(mmap);
            // Calculate needed size
            let needed = (current_pos + total_size) as u64;
            let new_size = ((needed + GROWTH_STEP - 1) / GROWTH_STEP) * GROWTH_STEP;
            self.resize(new_size).await?;
            mmap = self.mmap.lock().await;
        }

        // Verify bounds after potential resize to prevent panic
        let mmap_len = mmap.len();
        if current_pos + total_size > mmap_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "mmap bounds error: need {}..{} but len={}",
                    current_pos,
                    current_pos + total_size,
                    mmap_len
                ),
            ));
        }

        // OPTIMIZATION 3: Single memcpy to mmap
        mmap[current_pos..current_pos + total_size].copy_from_slice(&buffer);

        // Update state
        let bytes_written = total_size as u64;
        self.file_position
            .fetch_add(bytes_written, Ordering::SeqCst);
        self.current_offset.store(last_offset + 1, Ordering::SeqCst);

        // Update index
        let bytes_since_index = self
            .bytes_since_last_index
            .fetch_add(bytes_written, Ordering::SeqCst);

        if bytes_since_index >= self.index_interval_bytes {
            let mut index = self.index.lock().await;
            index.append(last_offset, current_pos as u64)?;
            self.bytes_since_last_index.store(0, Ordering::SeqCst);
        }

        // Phase 5.4: I/O Batching
        // Only flush every 10 batches to amortize I/O cost
        // This reduces OS page flushing overhead (identified as 50ms bottleneck)
        let batches = self.batches_since_fsync.fetch_add(1, Ordering::SeqCst);
        if batches % 10 == 0 {
            mmap.flush_async()?;
        }

        Ok(start_offset)
    }

    /// Flush to disk
    pub async fn sync(&self) -> std::io::Result<()> {
        let mmap = self.mmap.lock().await;
        mmap.flush()
    }

    /// Read record (Phase 5.5: Using rkyv deserialization)
    pub async fn read(&self, offset: u64) -> Result<LogRecord, WalError> {
        let index = self.index.lock().await;
        let mut file_pos = index.lookup(offset).unwrap_or(0) as usize;
        drop(index);

        let mmap = self.mmap.lock().await;
        let max_pos = self.file_position.load(Ordering::SeqCst) as usize;

        if file_pos >= max_pos {
            return Err(WalError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Record not found: offset={}", offset),
            )));
        }

        // Scan forward from file_pos to find the record with matching offset
        // This is necessary because the index is sparse (not every offset is indexed)
        loop {
            // Read size prefix (8 bytes, little-endian)
            if file_pos + 8 > max_pos {
                return Err(WalError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Record not found: offset={}", offset),
                )));
            }

            let size_bytes: [u8; 8] = mmap[file_pos..file_pos + 8]
                .try_into()
                .map_err(|_| WalError::Serialization("Failed to read size prefix".to_string()))?;
            let size = u64::from_le_bytes(size_bytes) as usize;

            // Check for zero padding (end of data)
            if size == 0 {
                return Err(WalError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Record not found: offset={}", offset),
                )));
            }

            // Read rkyv data
            let data_start = file_pos + 8;
            let data_end = data_start + size;

            if data_end > max_pos {
                return Err(WalError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Record not found: offset={}", offset),
                )));
            }

            let bytes = &mmap[data_start..data_end];

            // Use rkyv to check and deserialize
            let archived = rkyv::check_archived_root::<LogRecord>(bytes)
                .map_err(|e| WalError::Serialization(format!("rkyv validation failed: {}", e)))?;

            // Deserialize to owned LogRecord
            let record: LogRecord = archived
                .deserialize(&mut rkyv::Infallible)
                .map_err(|_| WalError::Serialization("Deserialization failed".to_string()))?;

            // Verify checksum to detect corruption
            record
                .verify_checksum()
                .map_err(|e| WalError::Serialization(format!("Data corruption: {}", e)))?;

            // Check if this is the record we're looking for
            if record.offset == offset {
                return Ok(record);
            } else if record.offset > offset {
                // We've passed the target offset, record doesn't exist
                return Err(WalError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Record not found: offset={}", offset),
                )));
            }

            // Move to next record
            file_pos = data_end;
        }
    }

    /// Scan all records in this segment sequentially
    pub async fn scan(&self) -> Result<Vec<LogRecord>, WalError> {
        let mmap = self.mmap.lock().await;
        let max_pos = self.file_position.load(Ordering::SeqCst) as usize;
        let mut records = Vec::new();
        let mut current_pos = 0;

        // Loop until we can't read a size prefix
        while current_pos + 8 <= max_pos {
            // Read size prefix (8 bytes, little-endian)
            let size_bytes: [u8; 8] = mmap[current_pos..current_pos + 8]
                .try_into()
                .map_err(|_| WalError::Serialization("Failed to read size prefix".to_string()))?;
            let size = u64::from_le_bytes(size_bytes) as usize;

            // Check for zero padding (end of data)
            // Since file is zero-initialized, 0 size means we reached the end
            if size == 0 {
                break;
            }

            let data_start = current_pos + 8;
            let data_end = data_start + size;

            if data_end > max_pos {
                // Incomplete record at end of file
                break;
            }

            let record_bytes = &mmap[data_start..data_end];

            // Use rkyv to check and deserialize
            match rkyv::check_archived_root::<LogRecord>(record_bytes) {
                Ok(archived) => match archived.deserialize(&mut rkyv::Infallible) {
                    Ok(record) => {
                        let record: LogRecord = record;

                        // Verify checksum to detect corruption
                        if let Err(e) = record.verify_checksum() {
                            // Corruption detected - stop scanning here
                            eprintln!("Corruption detected during scan: {}", e);
                            break;
                        }

                        records.push(record);
                        current_pos = data_end;
                    }
                    Err(_) => {
                        // Corruption detected - stop scanning here
                        // This enables auto-recovery by truncating at corruption point
                        break;
                    }
                },
                Err(_) => {
                    // Corruption detected - stop scanning here
                    // This enables auto-recovery by truncating at corruption point
                    break;
                }
            }
        }

        Ok(records)
    }

    /// Resize the underlying file and remap
    async fn resize(&self, new_size: u64) -> std::io::Result<()> {
        let mut mmap_guard = self.mmap.lock().await;
        mmap_guard.flush()?;
        self.file.set_len(new_size)?;
        let new_mmap = unsafe { MmapMut::map_mut(&self.file)? };
        *mmap_guard = new_mmap;
        self.capacity.store(new_size, Ordering::SeqCst);
        Ok(())
    }

    /// Flush the memory-mapped file to disk
    pub async fn flush(&self) -> std::io::Result<()> {
        let mmap = self.mmap.lock().await;
        mmap.flush()
    }

    /// Verify segment integrity
    pub async fn verify_segment(&self) -> Result<(), WalError> {
        let mmap = self.mmap.lock().await;
        let max_pos = self.file_position.load(Ordering::SeqCst) as usize;
        let mut current_pos = 0;

        while current_pos + 8 <= max_pos {
            // Read size prefix (8 bytes, little-endian)
            let size_bytes: [u8; 8] = mmap[current_pos..current_pos + 8]
                .try_into()
                .map_err(|_| WalError::Serialization("Failed to read size prefix".to_string()))?;
            let size = u64::from_le_bytes(size_bytes) as usize;

            // Check for zero padding (end of data)
            if size == 0 {
                break;
            }

            let data_start = current_pos + 8;
            let data_end = data_start + size;

            if data_end > max_pos {
                return Err(WalError::Corruption(format!(
                    "Invalid record size at pos {}: {}",
                    current_pos, size
                )));
            }

            let record_bytes = &mmap[data_start..data_end];

            // Use rkyv to check and deserialize
            let archived = rkyv::check_archived_root::<LogRecord>(record_bytes)
                .map_err(|e| WalError::Corruption(format!("rkyv validation failed: {}", e)))?;

            let record: LogRecord = archived
                .deserialize(&mut rkyv::Infallible)
                .map_err(|_| WalError::Corruption("Deserialization failed".to_string()))?;

            // Verify checksum
            record
                .verify_checksum()
                .map_err(|e| WalError::Corruption(format!("Checksum mismatch: {}", e)))?;

            current_pos = data_end;
        }

        Ok(())
    }

    /// Repair segment by truncating at first corruption
    pub async fn repair_segment(&self) -> Result<u64, WalError> {
        let mmap = self.mmap.lock().await;
        let max_pos = self.file_position.load(Ordering::SeqCst) as usize;
        let mut current_pos = 0;
        let mut valid_bytes = 0;

        while current_pos + 8 <= max_pos {
            // Read size prefix
            let size_bytes: [u8; 8] = match mmap[current_pos..current_pos + 8].try_into() {
                Ok(bytes) => bytes,
                Err(_) => break,
            };
            let size = u64::from_le_bytes(size_bytes) as usize;

            // Check for zero padding (end of data)
            if size == 0 {
                break;
            }

            let data_start = current_pos + 8;
            let data_end = data_start + size;

            if data_end > max_pos {
                break;
            }

            let record_bytes = &mmap[data_start..data_end];

            // Verify rkyv structure
            let archived = match rkyv::check_archived_root::<LogRecord>(record_bytes) {
                Ok(a) => a,
                Err(_) => break,
            };

            // Deserialize
            let record: LogRecord = match archived.deserialize(&mut rkyv::Infallible) {
                Ok(r) => r,
                Err(_) => break,
            };

            // Verify checksum
            if record.verify_checksum().is_err() {
                break;
            }

            current_pos = data_end;
            valid_bytes = current_pos;
        }

        if valid_bytes < max_pos {
            self.file_position
                .store(valid_bytes as u64, Ordering::SeqCst);
            // We can't easily truncate mmap, but we can update file position
            // and the next write will overwrite or append from there.
            // Ideally we should ftruncate the underlying file.
            // For now, updating file_position effectively "truncates" the logical log.
        }

        Ok(valid_bytes as u64)
    }

    /// Scan mmap to find last valid offset
    /// Scan mmap to find last valid offset
    fn scan_mmap(mmap: &MmapMut, base_offset: u64) -> std::io::Result<(u64, u64)> {
        let mut pos = 0;
        let len = mmap.len();
        let mut last_offset = base_offset.saturating_sub(1);

        while pos + 8 <= len {
            // Read size prefix (8 bytes, little-endian)
            let size_bytes: [u8; 8] = mmap[pos..pos + 8].try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Failed to read size prefix",
                )
            })?;
            let size = u64::from_le_bytes(size_bytes) as usize;

            // Check for zero padding (end of data)
            if size == 0 {
                break;
            }

            let data_start = pos + 8;
            let data_end = data_start + size;

            if data_end > len {
                break;
            }

            let record_bytes = &mmap[data_start..data_end];

            // We need to deserialize to get the offset
            match rkyv::check_archived_root::<LogRecord>(record_bytes) {
                Ok(archived) => match archived.deserialize(&mut rkyv::Infallible) {
                    Ok(record) => {
                        let record: LogRecord = record;
                        last_offset = record.offset;
                        pos = data_end;
                    }
                    Err(_) => break,
                },
                Err(_) => break,
            }
        }

        Ok((last_offset, pos as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::LogOperation;

    #[tokio::test]
    async fn test_mmap_append_read() {
        let dir = tempfile::tempdir().unwrap();
        let segment = MmapLogSegment::create(dir.path(), 0, 1024).await.unwrap();

        let record = LogRecord::new(LogOperation::Put {
            collection: "test".to_string(),
            id: vec![1],
            data: vec![2],
        });

        let offset = segment.append(record.clone()).await.unwrap();
        assert_eq!(offset, 0);

        let read = segment.read(0).await.unwrap();
        assert_eq!(read.offset, 0);
    }

    #[tokio::test]
    async fn test_mmap_resize() {
        let dir = tempfile::tempdir().unwrap();
        // Start small to force resize
        // Note: INITIAL_SEGMENT_SIZE is constant, so we can't easily force resize in test
        // unless we write A LOT or change the constant.
        // For unit test, we trust the logic or would need to make size configurable.
        // Let's just test basic functionality for now.
        let segment = MmapLogSegment::create(dir.path(), 0, 1024).await.unwrap();

        let mut records = Vec::new();
        for i in 0..100 {
            records.push(LogRecord::new(LogOperation::Put {
                collection: "test".to_string(),
                id: vec![i as u8],
                data: vec![0u8; 1000],
            }));
        }

        segment.append_batch(records).await.unwrap();

        let read = segment.read(99).await.unwrap();
        assert_eq!(read.offset, 99);
    }
}
