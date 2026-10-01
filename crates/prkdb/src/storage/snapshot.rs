//! Snapshot management for backup and restore
//!
//! Provides functionality to create full database snapshots and restore from them.

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use prkdb_types::codec::{decode_with_limit, MAX_HEADER_BYTES, MAX_RECORD_BYTES};
use prkdb_types::error::StorageError;
pub use prkdb_types::snapshot::{CompressionType, SnapshotHeader};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

pub const SNAPSHOT_VERSION: u32 = 1;

type SnapshotEntry = (Vec<u8>, Vec<u8>);

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
    /// Entries returned so far; never more than `header.index_entries`.
    entries_read: u64,
}

impl SnapshotReader {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let file = File::open(path).map_err(|e| StorageError::BackendError(e.to_string()))?;
        Self::from_reader(BufReader::new(file))
    }

    /// Reads a snapshot from any byte stream (the file `open` reads, or bytes in memory).
    pub fn from_reader(mut buf_reader: impl Read + 'static) -> Result<Self, StorageError> {
        // Read header length
        let mut len_bytes = [0u8; 4];
        buf_reader
            .read_exact(&mut len_bytes)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        // A header is a few fixed-size fields; a larger length is corruption, refused
        // before it sizes an allocation.
        if len > MAX_HEADER_BYTES {
            return Err(StorageError::Corruption(format!(
                "snapshot header of {len} bytes exceeds the {MAX_HEADER_BYTES}-byte limit"
            )));
        }

        // Read header
        let mut header_bytes = vec![0u8; len];
        buf_reader
            .read_exact(&mut header_bytes)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;

        let header: SnapshotHeader = decode_with_limit::<_, MAX_HEADER_BYTES>(&header_bytes)
            .map_err(|e| StorageError::Internal(format!("Failed to deserialize header: {}", e)))?
            .0;

        let reader: Box<dyn Read> = match header.compression {
            CompressionType::None => Box::new(buf_reader),
            CompressionType::Gzip => Box::new(GzDecoder::new(buf_reader)),
        };

        Ok(Self {
            reader,
            header,
            entries_read: 0,
        })
    }

    /// Returns next entry as (key, value). Returns None on EOF.
    ///
    /// Bounded whatever the file says, including a small gzip stream that inflates without
    /// limit: a key or value over [`MAX_RECORD_BYTES`] is refused before it is read, and
    /// the stream may hold no more than the header's `index_entries` entries (the key count
    /// the writer saw; deletes during the snapshot can only make it hold fewer). So reading
    /// a whole snapshot decompresses at most `index_entries` x 2 x [`MAX_RECORD_BYTES`].
    pub fn next_entry(&mut self) -> Result<Option<SnapshotEntry>, StorageError> {
        let mut len_bytes = [0u8; 4];
        if self.entries_read == self.header.index_entries {
            let mut probe = [0u8; 1];
            return match self.reader.read(&mut probe) {
                Ok(0) => Ok(None),
                Ok(_) => Err(StorageError::Corruption(format!(
                    "snapshot holds more than the {} entries its header declares",
                    self.header.index_entries
                ))),
                Err(e) => Err(StorageError::BackendError(e.to_string())),
            };
        }
        if let Err(e) = self.reader.read_exact(&mut len_bytes) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(StorageError::BackendError(e.to_string()));
        }
        let key = read_prefixed(&mut self.reader, u32::from_le_bytes(len_bytes))?;

        self.reader
            .read_exact(&mut len_bytes)
            .map_err(|e| StorageError::BackendError(e.to_string()))?;
        let val = read_prefixed(&mut self.reader, u32::from_le_bytes(len_bytes))?;

        self.entries_read += 1;
        Ok(Some((key, val)))
    }
}

/// Reads exactly `len` bytes, growing the buffer as bytes arrive rather than allocating
/// the declared length up front, and refusing a length over [`MAX_RECORD_BYTES`] (no
/// stored key or value is larger) before reading anything: a corrupt length, or a gzip
/// stream that inflates without limit, cannot drive a 4 GiB entry.
fn read_prefixed(reader: &mut dyn Read, len: u32) -> Result<Vec<u8>, StorageError> {
    if len as usize > MAX_RECORD_BYTES {
        return Err(StorageError::Corruption(format!(
            "snapshot entry declares {len} bytes, over the {MAX_RECORD_BYTES}-byte record limit"
        )));
    }
    let mut buf = Vec::new();
    reader
        .take(u64::from(len))
        .read_to_end(&mut buf)
        .map_err(|e| StorageError::BackendError(e.to_string()))?;
    if buf.len() != len as usize {
        return Err(StorageError::Corruption(format!(
            "snapshot entry declares {len} bytes, file has {}",
            buf.len()
        )));
    }
    Ok(buf)
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
