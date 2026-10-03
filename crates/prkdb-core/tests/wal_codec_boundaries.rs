//! Public compression and payload-boundary contracts missed by the WAL core mutation job.
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::frame::{
    decode_frame, encode_frame, Decoded, FrameFault, FrameKind, FRAME_HEADER_LEN, MAX_PAYLOAD_LEN,
};
use prkdb_core::wal::{compress, decompress_bounded, CompressionConfig, CompressionType};

#[test]
fn throughput_preset_keeps_its_higher_threshold_and_fast_level() {
    let cfg = CompressionConfig::throughput_optimized();
    assert_eq!(
        (
            cfg.compression_type,
            cfg.min_compress_bytes,
            cfg.compression_level
        ),
        (CompressionType::Lz4, 512, 1)
    );
    // This size sits between the default and throughput thresholds: the preset
    // must leave it alone, whereas replacing the preset with Default compresses it.
    let data = vec![b'x'; 384];
    assert_eq!(compress(&data, &cfg).unwrap(), data);
}

#[test]
fn compression_preset_uses_zstd_at_its_lower_threshold() {
    let cfg = CompressionConfig::compression_optimized();
    assert_eq!(
        (
            cfg.compression_type,
            cfg.min_compress_bytes,
            cfg.compression_level
        ),
        (CompressionType::Zstd, 128, 9)
    );
    let data = vec![b'x'; 128];
    let encoded = compress(&data, &cfg).unwrap();
    assert!(encoded.len() < data.len());
    assert_eq!(
        decompress_bounded(&encoded, CompressionType::Zstd, data.len()).unwrap(),
        data
    );
}

#[test]
fn compression_starts_at_the_threshold_inclusively() {
    let cfg = CompressionConfig {
        compression_type: CompressionType::Lz4,
        min_compress_bytes: 256,
        compression_level: 3,
    };
    let below = vec![b'x'; 255];
    assert_eq!(compress(&below, &cfg).unwrap(), below);
    let at = vec![b'x'; 256];
    let encoded = compress(&at, &cfg).unwrap();
    assert!(encoded.len() < at.len());
    assert_eq!(
        decompress_bounded(&encoded, CompressionType::Lz4, at.len()).unwrap(),
        at
    );
}

#[test]
fn uncompressed_decompression_accepts_the_exact_bound_including_zero() {
    let data = b"abc";
    assert_eq!(
        decompress_bounded(data, CompressionType::None, data.len()).unwrap(),
        data
    );
    assert!(decompress_bounded(data, CompressionType::None, data.len() - 1).is_err());
    assert!(decompress_bounded(&[], CompressionType::None, 0)
        .unwrap()
        .is_empty());
}

#[test]
fn a_tiny_batch_records_the_codec_actually_used() {
    let batch = Batch {
        ops: vec![BatchOp::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }],
    };
    let cfg = CompressionConfig::default();
    let encoded = batch.encode(&cfg).unwrap();
    let raw_len = u32::from_le_bytes(encoded[2..6].try_into().unwrap()) as usize;
    assert!(
        raw_len < cfg.min_compress_bytes,
        "fixture must be below the threshold"
    );
    assert_eq!(encoded[1], CompressionType::None as u8);
    assert_eq!(Batch::decode(&encoded).unwrap(), batch);
}

#[test]
fn an_exact_limit_frame_length_is_validated_before_payload_completeness() {
    // A header alone distinguishes a legal-but-incomplete payload from an illegal
    // length, without allocating a 64 MiB payload or running a checksum over it.
    let mut header = Vec::new();
    encode_frame(&mut header, 1, FrameKind::Batch, b"x");
    header.truncate(FRAME_HEADER_LEN);
    header[..4].copy_from_slice(&(MAX_PAYLOAD_LEN as u32).to_le_bytes());
    assert_eq!(decode_frame(&header), Decoded::Fault(FrameFault::Truncated));
    let oversized = MAX_PAYLOAD_LEN as u32 + 1;
    header[..4].copy_from_slice(&oversized.to_le_bytes());
    assert_eq!(
        decode_frame(&header),
        Decoded::Fault(FrameFault::BadLength(oversized))
    );
}
