//! Snapshot management for backup and restore
//!
//! Provides functionality to create full database snapshots and restore from them.

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use prkdb_types::error::StorageError;
pub use prkdb_types::snapshot::{CompressionType, SnapshotHeader};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

pub const SNAPSHOT_VERSION: u32 = 1;

/// Helper to write snapshots
pub struct SnapshotWriter {
    writer: Box<dyn Write>,
}

impl SnapshotWriter {
    pub fn new(path: &Path, header: SnapshotHeader) -> Result<Self, StorageError> {
        let file = File::create(path).map_err(|e| StorageError::BackendError(e.to_string()))?;
        let mut buf_writer = BufWriter::new(file);

        // Serialize header first (uncompressed) using Bincode 2
        let config = bincode::config::standard();
        let header_bytes = bincode::encode_to_vec(&header, config)
            .map_err(|e| StorageError::Internal(format!("Failed to serialize header: {}", e)))?;

        // Write header length (u32) then header
        let len = header_bytes.len() as u32;
        buf_writer
            .write_all(&len.to_le_bytes())
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        buf_writer
            .write_all(&header_bytes)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;

        let writer: Box<dyn Write> = match header.compression {
            CompressionType::None => Box::new(buf_writer),
            CompressionType::Gzip => Box::new(GzEncoder::new(buf_writer, Compression::default())),
        };

        Ok(Self { writer })
    }

    pub fn write_entry(&mut self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        // Simple length-prefixed format: key_len(u32) | key | val_len(u32) | val
        let key_len = key.len() as u32;
        let val_len = value.len() as u32;

        self.writer
            .write_all(&key_len.to_le_bytes())
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        self.writer
            .write_all(key)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        self.writer
            .write_all(&val_len.to_le_bytes())
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        self.writer
            .write_all(value)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;

        Ok(())
    }

    pub fn finish(mut self) -> Result<(), StorageError> {
        self.writer
            .flush()
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        Ok(())
    }
}

/// Helper to read snapshots
pub struct SnapshotReader {
    reader: Box<dyn Read>,
    pub header: SnapshotHeader,
}

impl SnapshotReader {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let file = File::open(path).map_err(|e| StorageError::BackendError(e.to_string()))?;
        let mut buf_reader = BufReader::new(file);

        // Read header length
        let mut len_bytes = [0u8; 4];
        buf_reader
            .read_exact(&mut len_bytes)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        let len = u32::from_le_bytes(len_bytes) as usize;

        // Read header
        let mut header_bytes = vec![0u8; len];
        buf_reader
            .read_exact(&mut header_bytes)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;

        let config = bincode::config::standard();
        let header: SnapshotHeader = bincode::decode_from_slice(&header_bytes, config)
            .map_err(|e| StorageError::Internal(format!("Failed to deserialize header: {}", e)))?
            .0;

        let reader: Box<dyn Read> = match header.compression {
            CompressionType::None => Box::new(buf_reader),
            CompressionType::Gzip => Box::new(GzDecoder::new(buf_reader)),
        };

        Ok(Self { reader, header })
    }

    /// Returns next entry as (key, value). Returns None on EOF.
    pub fn next_entry(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, StorageError> {
        let mut len_bytes = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut len_bytes) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(StorageError::BackendError(e.to_string()));
        }
        let key_len = u32::from_le_bytes(len_bytes) as usize;
        let mut key = vec![0u8; key_len];
        self.reader
            .read_exact(&mut key)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;

        self.reader
            .read_exact(&mut len_bytes)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        let val_len = u32::from_le_bytes(len_bytes) as usize;
        let mut val = vec![0u8; val_len];
        self.reader
            .read_exact(&mut val)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;

        Ok(Some((key, val)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_snapshot_write_read_no_compression() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snap_none.bin");

        let header = SnapshotHeader::new(100, 2, CompressionType::None);
        let mut writer = SnapshotWriter::new(&path, header).unwrap();
        writer.write_entry(b"key1", b"val1").unwrap();
        writer.write_entry(b"key2", b"val2").unwrap();
        writer.finish().unwrap();

        let mut reader = SnapshotReader::open(&path).unwrap();
        assert_eq!(reader.header.max_offset, 100);
        assert_eq!(reader.header.index_entries, 2);
        assert_eq!(reader.header.compression, CompressionType::None);

        let (k1, v1) = reader.next_entry().unwrap().unwrap();
        assert_eq!(k1, b"key1");
        assert_eq!(v1, b"val1");

        let (k2, v2) = reader.next_entry().unwrap().unwrap();
        assert_eq!(k2, b"key2");
        assert_eq!(v2, b"val2");

        assert!(reader.next_entry().unwrap().is_none());
    }

    #[test]
    fn test_snapshot_write_read_gzip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snap_gzip.bin");

        let header = SnapshotHeader::new(200, 1, CompressionType::Gzip);
        let mut writer = SnapshotWriter::new(&path, header).unwrap();
        // Write enough data to verify compression actually does something (though hard to detect from outside without checking size)
        let large_val = vec![b'a'; 1000];
        writer.write_entry(b"key1", &large_val).unwrap();
        writer.finish().unwrap();

        let mut reader = SnapshotReader::open(&path).unwrap();
        assert_eq!(reader.header.compression, CompressionType::Gzip);

        let (k1, v1) = reader.next_entry().unwrap().unwrap();
        assert_eq!(k1, b"key1");
        assert_eq!(v1, large_val);
    }
}
