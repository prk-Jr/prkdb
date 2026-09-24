use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CompressionError {
    #[error("Compression failed: {0}")]
    CompressionFailed(String),

    #[error("Decompression failed: {0}")]
    DecompressionFailed(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Unsupported compression type: {0:?}")]
    UnsupportedType(CompressionType),
}

#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    bincode::Encode,
    bincode::Decode,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug))]
pub enum CompressionType {
    None = 0,
    Lz4 = 1,
    Snappy = 2,
    Zstd = 3,
}

impl CompressionType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(CompressionType::None),
            1 => Some(CompressionType::Lz4),
            2 => Some(CompressionType::Snappy),
            3 => Some(CompressionType::Zstd),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// Type of compression to use
    pub compression_type: CompressionType,

    /// Don't compress messages smaller than this threshold (bytes)
    pub min_compress_bytes: usize,

    /// Compression level (for Zstd, 1-22, default 3)
    pub compression_level: i32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            compression_type: CompressionType::Lz4, // Fast by default
            min_compress_bytes: 256,                // Skip tiny messages
            compression_level: 3,                   // Balanced
        }
    }
}

impl CompressionConfig {
    /// Configuration optimized for maximum throughput
    pub fn throughput_optimized() -> Self {
        Self {
            compression_type: CompressionType::Lz4,
            min_compress_bytes: 512, // Higher threshold
            compression_level: 1,    // Fastest
        }
    }

    /// Configuration optimized for maximum compression ratio
    pub fn compression_optimized() -> Self {
        Self {
            compression_type: CompressionType::Zstd,
            min_compress_bytes: 128, // Lower threshold
            compression_level: 9,    // Higher compression
        }
    }

    /// Balanced configuration
    pub fn balanced() -> Self {
        Self::default()
    }

    /// No compression configuration
    pub fn none() -> Self {
        Self {
            compression_type: CompressionType::None,
            min_compress_bytes: usize::MAX,
            compression_level: 0,
        }
    }
}

/// Compress data using the specified configuration
pub fn compress(data: &[u8], config: &CompressionConfig) -> Result<Vec<u8>, CompressionError> {
    // Skip compression for small messages
    if data.len() < config.min_compress_bytes {
        return Ok(data.to_vec());
    }

    match config.compression_type {
        CompressionType::None => Ok(data.to_vec()),

        CompressionType::Lz4 => {
            let mut encoder = lz4::EncoderBuilder::new()
                .level(4) // Fast compression
                .build(Vec::new())
                .map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;

            encoder.write_all(data)?;
            let (compressed, result) = encoder.finish();
            result?;
            Ok(compressed)
        }

        CompressionType::Snappy => {
            let mut encoder = snap::write::FrameEncoder::new(Vec::new());
            encoder.write_all(data)?;
            encoder
                .into_inner()
                .map_err(|e| CompressionError::Io(e.into_error()))
        }

        CompressionType::Zstd => {
            let mut encoder = zstd::Encoder::new(Vec::new(), config.compression_level)
                .map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;

            encoder.write_all(data)?;
            encoder.finish().map_err(CompressionError::Io)
        }
    }
}

/// Decompress data using the specified compression type
pub fn decompress(
    data: &[u8],
    compression_type: CompressionType,
) -> Result<Vec<u8>, CompressionError> {
    match compression_type {
        CompressionType::None => Ok(data.to_vec()),

        CompressionType::Lz4 => {
            let mut decoder = lz4::Decoder::new(data)
                .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))?;

            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;
            Ok(decompressed)
        }

        CompressionType::Snappy => {
            let mut decoder = snap::read::FrameDecoder::new(data);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;
            Ok(decompressed)
        }

        CompressionType::Zstd => {
            let mut decoder = zstd::Decoder::new(data)
                .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))?;

            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;
            Ok(decompressed)
        }
    }
}

/// Decompresses `data`, refusing to produce more than `max_len` bytes.
///
/// A compressed blob can claim to expand to an arbitrary size (a "decompression bomb");
/// `decompress` above reads to completion before anything can check that, so a small
/// malicious input can force an unbounded allocation. This reads through a `Take`
/// adapter capped at `max_len + 1` bytes, so the decoder itself never accumulates more
/// than that bound in memory, and returns an error the instant the output would exceed
/// `max_len` rather than after decompressing all of it.
pub fn decompress_bounded(
    data: &[u8],
    compression_type: CompressionType,
    max_len: usize,
) -> Result<Vec<u8>, CompressionError> {
    match compression_type {
        CompressionType::None => {
            if data.len() > max_len {
                return Err(CompressionError::DecompressionFailed(format!(
                    "decompressed output of {} bytes exceeds the {max_len}-byte limit",
                    data.len()
                )));
            }
            Ok(data.to_vec())
        }
        CompressionType::Lz4 => {
            let decoder = lz4::Decoder::new(data)
                .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))?;
            read_bounded(decoder, max_len)
        }
        CompressionType::Snappy => {
            let decoder = snap::read::FrameDecoder::new(data);
            read_bounded(decoder, max_len)
        }
        CompressionType::Zstd => {
            let decoder = zstd::Decoder::new(data)
                .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))?;
            read_bounded(decoder, max_len)
        }
    }
}

/// Reads `reader` to completion through a bound of `max_len + 1` bytes, erroring if the
/// output turns out to exceed `max_len`. The `+ 1` lets a decompressed size of exactly
/// `max_len` succeed while still detecting anything larger without reading further.
fn read_bounded<R: Read>(reader: R, max_len: usize) -> Result<Vec<u8>, CompressionError> {
    let mut limited = reader.take(max_len as u64 + 1);
    let mut out = Vec::new();
    limited.read_to_end(&mut out)?;
    if out.len() > max_len {
        return Err(CompressionError::DecompressionFailed(format!(
            "decompressed output exceeds the {max_len}-byte limit"
        )));
    }
    Ok(out)
}

/// Calculate compression ratio (original_size / compressed_size)
pub fn compression_ratio(original_size: usize, compressed_size: usize) -> f64 {
    if compressed_size == 0 {
        0.0
    } else {
        original_size as f64 / compressed_size as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_test_data(size: usize) -> Vec<u8> {
        // Generate somewhat compressible data (realistic)
        let pattern = b"The quick brown fox jumps over the lazy dog. ";
        pattern.iter().cycle().take(size).copied().collect()
    }

    #[test]
    fn test_lz4_compression() {
        let data = generate_test_data(1024);
        let config = CompressionConfig {
            compression_type: CompressionType::Lz4,
            min_compress_bytes: 256,
            compression_level: 3,
        };

        let compressed = compress(&data, &config).unwrap();
        let decompressed = decompress(&compressed, CompressionType::Lz4).unwrap();

        assert_eq!(data, decompressed);
        assert!(compressed.len() < data.len(), "Data should be compressed");
    }

    #[test]
    fn test_snappy_compression() {
        let data = generate_test_data(1024);
        let config = CompressionConfig {
            compression_type: CompressionType::Snappy,
            min_compress_bytes: 256,
            compression_level: 0,
        };

        let compressed = compress(&data, &config).unwrap();
        let decompressed = decompress(&compressed, CompressionType::Snappy).unwrap();

        assert_eq!(data, decompressed);
        assert!(compressed.len() < data.len(), "Data should be compressed");
    }

    #[test]
    fn test_zstd_compression() {
        let data = generate_test_data(1024);
        let config = CompressionConfig {
            compression_type: CompressionType::Zstd,
            min_compress_bytes: 256,
            compression_level: 3,
        };

        let compressed = compress(&data, &config).unwrap();
        let decompressed = decompress(&compressed, CompressionType::Zstd).unwrap();

        assert_eq!(data, decompressed);
        assert!(compressed.len() < data.len(), "Data should be compressed");
    }

    #[test]
    fn test_no_compression() {
        let data = generate_test_data(1024);
        let config = CompressionConfig {
            compression_type: CompressionType::None,
            min_compress_bytes: 256,
            compression_level: 0,
        };

        let compressed = compress(&data, &config).unwrap();
        assert_eq!(data, compressed, "No compression should return original");
    }

    #[test]
    fn test_threshold_skips_small_data() {
        let data = generate_test_data(128); // Below threshold
        let config = CompressionConfig {
            compression_type: CompressionType::Lz4,
            min_compress_bytes: 256,
            compression_level: 3,
        };

        let compressed = compress(&data, &config).unwrap();
        assert_eq!(data, compressed, "Small data should not be compressed");
    }

    #[test]
    fn test_compression_ratio() {
        assert_eq!(compression_ratio(1000, 500), 2.0);
        assert_eq!(compression_ratio(1000, 250), 4.0);
        assert_eq!(compression_ratio(1000, 1000), 1.0);
    }

    #[test]
    fn decompress_bounded_accepts_output_within_the_limit() {
        let data = generate_test_data(4096);
        let config = CompressionConfig {
            compression_type: CompressionType::Lz4,
            min_compress_bytes: 0,
            compression_level: 3,
        };
        let compressed = compress(&data, &config).unwrap();
        let out = decompress_bounded(&compressed, CompressionType::Lz4, data.len()).unwrap();
        assert_eq!(out, data);
    }

    /// A decompression bomb: a small compressed input whose real decompressed size is
    /// far larger than the caller's claimed bound. `decompress_bounded` must error
    /// without ever materializing the full output.
    #[test]
    fn decompress_bounded_rejects_a_bomb_without_allocating_the_full_output() {
        let huge_zeros = vec![0u8; 64 * 1024 * 1024]; // 64 MiB of highly compressible data
        let config = CompressionConfig {
            compression_type: CompressionType::Lz4,
            min_compress_bytes: 0,
            compression_level: 3,
        };
        let compressed = compress(&huge_zeros, &config).unwrap();
        assert!(
            compressed.len() < huge_zeros.len() / 100,
            "the point of the test is a compressed input much smaller than its real \
             decompressed size, got {} bytes for {} real bytes",
            compressed.len(),
            huge_zeros.len()
        );
        // Claim a decompressed size far below the real one; `decompress_bounded` must
        // stop within `max_len + 1` bytes of output rather than reading to completion.
        let max_len = 4096;
        let err = decompress_bounded(&compressed, CompressionType::Lz4, max_len).unwrap_err();
        assert!(matches!(err, CompressionError::DecompressionFailed(_)));
    }

    #[test]
    fn decompress_bounded_rejects_uncompressed_data_over_the_limit() {
        let data = vec![7u8; 100];
        let err = decompress_bounded(&data, CompressionType::None, 10).unwrap_err();
        assert!(matches!(err, CompressionError::DecompressionFailed(_)));
    }
}
