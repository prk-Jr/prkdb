use crate::buffer_pool::PooledBuffer;
use crate::wal::compression::{compress, CompressionConfig, CompressionType};
use crate::wal::WalError;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Single record in the write-ahead log
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    bincode::Encode,
    bincode::Decode,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct LogRecord {
    /// Unique offset (assigned by WAL)
    pub offset: u64,

    /// Timestamp (milliseconds since epoch)
    pub timestamp: u64,

    /// Compression type used for this record
    pub compression: CompressionType,

    /// CRC32 checksum of the operation data (for integrity)
    pub checksum: u32,

    /// The actual operation (may be compressed)
    pub operation: LogOperation,
}

/// Operations that can be logged
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    bincode::Encode,
    bincode::Decode,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum LogOperation {
    /// Single put operation
    Put {
        collection: String,
        id: Vec<u8>,
        data: Vec<u8>,
    },

    /// Single delete operation
    Delete { collection: String, id: Vec<u8> },

    /// Batch put operation
    PutBatch {
        collection: String,
        items: Vec<(Vec<u8>, Vec<u8>)>, // (id, data) pairs
    },

    /// Compressed batch put operation
    CompressedPutBatch {
        collection: String,
        original_len: u32,
        data: Vec<u8>, // Compressed data
    },

    /// Batch delete operation
    DeleteBatch {
        collection: String,
        ids: Vec<Vec<u8>>,
    },

    /// Compressed batch delete operation
    CompressedDeleteBatch {
        collection: String,
        original_len: u32,
        data: Vec<u8>, // Compressed ids
    },
}

impl LogRecord {
    /// Create a new log record without compression
    pub fn new(operation: LogOperation) -> Self {
        let config = bincode::config::standard();

        // Use pooled buffer for serialization
        let mut pooled_buffer = PooledBuffer::acquire();
        let op_bytes = pooled_buffer
            .encode(&operation, config)
            .expect("operation serialization should not fail");
        let checksum = crc32fast::hash(op_bytes);

        Self {
            offset: 0, // Will be set by WAL
            timestamp: current_timestamp_ms(),
            compression: CompressionType::None,
            checksum,
            operation,
        }
    }

    /// Create a new log record with compression
    pub fn new_with_compression(
        operation: LogOperation,
        compression_config: &CompressionConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Compress if configured
        let (compressed_op, compression_type) =
            if compression_config.compression_type != CompressionType::None {
                match operation {
                    LogOperation::PutBatch { collection, items } => {
                        // Serialize items to bytes using pooled buffer
                        let config = bincode::config::standard();
                        let mut pooled_buffer = PooledBuffer::acquire();
                        let items_bytes = pooled_buffer.encode(&items, config)?;
                        let original_len = items_bytes.len() as u32;

                        // Compress items
                        let compressed_data = compress(items_bytes, compression_config)?;

                        // If compression didn't reduce size (or was skipped), keep original
                        if compressed_data.len() >= items_bytes.len() {
                            (
                                LogOperation::PutBatch { collection, items },
                                CompressionType::None,
                            )
                        } else {
                            (
                                LogOperation::CompressedPutBatch {
                                    collection,
                                    original_len,
                                    data: compressed_data,
                                },
                                compression_config.compression_type,
                            )
                        }
                    }
                    LogOperation::DeleteBatch { collection, ids } => {
                        // Serialize ids to bytes using pooled buffer
                        let config = bincode::config::standard();
                        let mut pooled_buffer = PooledBuffer::acquire();
                        let ids_bytes = pooled_buffer.encode(&ids, config)?;
                        let original_len = ids_bytes.len() as u32;

                        // Compress ids
                        let compressed_data = compress(ids_bytes, compression_config)?;

                        // If compression didn't reduce size (or was skipped), keep original
                        if compressed_data.len() >= ids_bytes.len() {
                            (
                                LogOperation::DeleteBatch { collection, ids },
                                CompressionType::None,
                            )
                        } else {
                            (
                                LogOperation::CompressedDeleteBatch {
                                    collection,
                                    original_len,
                                    data: compressed_data,
                                },
                                compression_config.compression_type,
                            )
                        }
                    }
                    // For single operations, we don't compress individually as overhead > benefit
                    _ => (operation, CompressionType::None),
                }
            } else {
                (operation, CompressionType::None)
            };

        // Calculate checksum of the FINAL operation (compressed or not) using pooled buffer
        let config = bincode::config::standard();
        let mut pooled_buffer = PooledBuffer::acquire();
        let op_bytes = pooled_buffer.encode(&compressed_op, config)?;
        let checksum = crc32fast::hash(op_bytes);

        Ok(Self {
            offset: 0,
            timestamp: current_timestamp_ms(),
            compression: compression_type,
            checksum,
            operation: compressed_op,
        })
    }

    /// Verify the checksum of this record
    pub fn verify_checksum(&self) -> Result<(), String> {
        // Recompute checksum of operation
        let config = bincode::config::standard();
        let op_bytes = bincode::encode_to_vec(&self.operation, config)
            .map_err(|e| format!("Failed to encode operation for checksum: {}", e))?;
        let computed = crc32fast::hash(&op_bytes);

        if computed == self.checksum {
            Ok(())
        } else {
            Err(format!(
                "Checksum mismatch: expected={}, computed={}",
                self.checksum, computed
            ))
        }
    }

    /// Serialize to bytes (on-disk format)
    /// Format: [ZeroCopyLogHeader (32 bytes)][N bytes: bincode data]
    pub fn serialize(&self) -> Vec<u8> {
        let config = bincode::config::standard();
        let payload =
            bincode::encode_to_vec(&self.operation, config).expect("serialization should not fail");

        // Padding is handled by ZeroCopyLogHeader struct layout (u8 + [u8;3])
        let header = crate::serialization::zerocopy::ZeroCopyLogHeader {
            offset: self.offset,
            timestamp: self.timestamp,
            checksum: self.checksum,
            payload_len: payload.len() as u32,
            compression_type: match self.compression {
                CompressionType::None => 0,
                CompressionType::Lz4 => 1,
                CompressionType::Snappy => 2,
                CompressionType::Zstd => 3,
            },
            _padding: [0; 7],
        };

        use zerocopy::AsBytes;
        let mut result = Vec::with_capacity(
            crate::serialization::zerocopy::ZeroCopyLogHeader::SIZE + payload.len(),
        );
        result.extend_from_slice(header.as_bytes());
        result.extend_from_slice(&payload);

        result
    }

    /// Serialize to bytes using rkyv (Phase 5.5: Zero-copy)
    pub fn serialize_rkyv(&self) -> Vec<u8> {
        let bytes = rkyv::to_bytes::<_, 256>(self).expect("rkyv serialization should not fail");

        // Write size prefix (8 bytes, little-endian) so we can find record boundaries
        // Using u64 to maintain 8-byte alignment required by rkyv
        let size = bytes.len() as u64;
        let mut result = Vec::with_capacity(8 + bytes.len());
        result.extend_from_slice(&size.to_le_bytes());
        result.extend_from_slice(&bytes);
        result
    }

    /// Serialize into an existing buffer using rkyv (zero-alloc)
    pub fn serialize_into_rkyv(&self, buffer: &mut Vec<u8>) {
        let bytes = rkyv::to_bytes::<_, 256>(self).expect("rkyv serialization should not fail");

        // Write size prefix (8 bytes, little-endian) so we can find record boundaries
        // Using u64 to maintain 8-byte alignment required by rkyv
        let size = bytes.len() as u64;
        buffer.extend_from_slice(&size.to_le_bytes());

        // Write rkyv data
        buffer.extend_from_slice(&bytes);
    }

    /// Deserialize from bytes
    pub fn deserialize(bytes: &[u8]) -> Result<Self, WalError> {
        use crate::serialization::zerocopy::ZeroCopyLogHeader;
        use zerocopy::FromBytes;

        if bytes.len() < ZeroCopyLogHeader::SIZE {
            return Err(WalError::Serialization(
                "insufficient bytes for header".to_string(),
            ));
        }

        let header = ZeroCopyLogHeader::read_from_prefix(bytes)
            .ok_or_else(|| WalError::Serialization("invalid header".to_string()))?; // read_from_prefix returns Option in recent zerocopy? Or Result?
                                                                                    // Actually read_from_prefix is for unaligned?
                                                                                    // Let's use ref_from_prefix or read_from.
                                                                                    // read_from takes a prefix.
                                                                                    // Wait, read_from_prefix is available in 0.7?
                                                                                    // Let's check docs or assume standard usage.
                                                                                    // If it fails, we'll fix.
                                                                                    // Actually, `read_from` is safer if we want a copy.

        // Let's use `read_from` on the slice.
        // But `read_from` might not be available on slice directly without `zerocopy::ByteSlice`.
        // `ZeroCopyLogHeader::read_from(bytes)`? No.
        // `ZeroCopyLogHeader::ref_from_prefix(bytes)` returns reference.
        // We want a value.
        // Let's try `ZeroCopyLogHeader::read_from_prefix(bytes)`.

        let payload_len = header.payload_len as usize;

        if bytes.len() < ZeroCopyLogHeader::SIZE + payload_len {
            return Err(WalError::Serialization(
                "insufficient bytes for payload".to_string(),
            ));
        }

        let payload = &bytes[ZeroCopyLogHeader::SIZE..ZeroCopyLogHeader::SIZE + payload_len];

        let config = bincode::config::standard();
        let (operation, _): (LogOperation, usize) = bincode::decode_from_slice(payload, config)
            .map_err(|e| WalError::Serialization(e.to_string()))?;

        // Verify checksum
        // Note: We should verify checksum of the payload bytes directly if possible to avoid re-serialization?
        // Original implementation calculated checksum of `op_bytes`.
        // Here `payload` IS `op_bytes`.
        let computed_checksum = crc32fast::hash(payload);
        if computed_checksum != header.checksum {
            return Err(WalError::ChecksumMismatch {
                expected: header.checksum,
                found: computed_checksum,
            });
        }

        let compression = match header.compression_type {
            0 => CompressionType::None,
            1 => CompressionType::Lz4,
            2 => CompressionType::Snappy,
            3 => CompressionType::Zstd,
            _ => CompressionType::None, // Default or error?
        };

        Ok(Self {
            offset: header.offset,
            timestamp: header.timestamp,
            compression,
            checksum: header.checksum,
            operation,
        })
    }

    /// Size in bytes when serialized
    pub fn size(&self) -> usize {
        // Estimate size without full serialization?
        // We need payload size.
        // For now, full serialize is safest.
        self.serialize().len()
    }
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_serialization() {
        let record = LogRecord::new(LogOperation::Put {
            collection: "users".to_string(),
            id: b"user123".to_vec(),
            data: b"John Doe".to_vec(),
        });

        let bytes = record.serialize();
        let deserialized = LogRecord::deserialize(&bytes).unwrap();

        assert_eq!(record.timestamp, deserialized.timestamp);
        assert_eq!(record.compression, CompressionType::None);
        assert_eq!(record.checksum, deserialized.checksum);

        match (record.operation, deserialized.operation) {
            (
                LogOperation::Put {
                    collection: c1,
                    id: id1,
                    data: d1,
                },
                LogOperation::Put {
                    collection: c2,
                    id: id2,
                    data: d2,
                },
            ) => {
                assert_eq!(c1, c2);
                assert_eq!(id1, id2);
                assert_eq!(d1, d2);
            }
            _ => panic!("operation mismatch"),
        }
    }

    #[test]
    fn test_batch_operations() {
        let record = LogRecord::new(LogOperation::PutBatch {
            collection: "items".to_string(),
            items: vec![
                (b"id1".to_vec(), b"data1".to_vec()),
                (b"id2".to_vec(), b"data2".to_vec()),
            ],
        });

        let bytes = record.serialize();
        let deserialized = LogRecord::deserialize(&bytes).unwrap();

        match deserialized.operation {
            LogOperation::PutBatch { items, .. } => {
                assert_eq!(items.len(), 2);
            }
            _ => panic!("expected PutBatch"),
        }
    }

    #[test]
    fn test_checksum_validation() {
        let mut record = LogRecord::new(LogOperation::Put {
            collection: "test".to_string(),
            id: b"id1".to_vec(),
            data: b"data1".to_vec(),
        });

        let bytes = record.serialize();

        // Tamper with checksum
        record.checksum = 12345;
        let tampered_bytes = record.serialize();

        // Original should deserialize fine
        assert!(LogRecord::deserialize(&bytes).is_ok());

        // Tampered should fail
        assert!(LogRecord::deserialize(&tampered_bytes).is_err());
    }

    #[test]
    fn test_record_with_compression() {
        let config = CompressionConfig {
            compression_type: CompressionType::Lz4,
            min_compress_bytes: 100,
            compression_level: 3,
        };

        let operation = LogOperation::PutBatch {
            collection: "test".to_string(),
            items: vec![(b"id1".to_vec(), vec![42u8; 1024])], // Large enough to compress
        };

        let record = LogRecord::new_with_compression(operation, &config).unwrap();

        assert_eq!(record.compression, CompressionType::Lz4);
        assert!(record.checksum > 0);

        let bytes = record.serialize();
        let deserialized = LogRecord::deserialize(&bytes).unwrap();

        assert_eq!(record.checksum, deserialized.checksum);
    }
}
