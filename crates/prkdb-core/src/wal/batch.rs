//! Versioned payload of one atomic write batch (Task 2.5), carried as the payload of a
//! `FrameKind::Batch` frame.
//!
//! Layout, little-endian: `version u8 (=1) | codec u8 | raw_len u32 | body`, where `codec`
//! is the existing `CompressionType` discriminant and `body` is the op list, compressed
//! when `codec != 0`. `raw_len` is the length of the *uncompressed* op list, checked
//! against what decompression actually produces.
//!
//! **Deviation 2 (from the decision record, which had no compression):**
//! `WalConfig::compression` defaults to LZ4 today; dropping it silently would add another
//! knob that does nothing (root cause 4). So the batch codec compresses when the caller's
//! `CompressionConfig` says to and the payload clears its `min_compress_bytes` threshold —
//! the same rule `compress()` already applies — and records the codec actually used, not
//! the one requested, so a small payload that `compress()` left alone is never handed to
//! the wrong decompressor.
//!
//! Ops: `u32 count`, then per op a tag byte: `1 Put: u32 klen | key | u32 vlen | value`,
//! `2 Delete: u32 klen | key`. Tags 3-5 (outbox put, outbox remove, event) are added in
//! Tasks 2.19-2.20, before the format is frozen by the golden directory in Task 2.24.

use super::frame::MAX_PAYLOAD_LEN;
use super::{compress, decompress_bounded, CompressionConfig, CompressionType, WalError};

const BATCH_VERSION: u8 = 1;
const TAG_PUT: u8 = 1;
const TAG_DELETE: u8 = 2;
/// Smallest possible encoded op: a `Delete` with a zero-length key (tag byte + 4-byte
/// length prefix). Used only to cap a preallocation, not to reject anything.
const MIN_OP_LEN: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Batch {
    pub ops: Vec<BatchOp>,
}

fn encode_ops(ops: &[BatchOp]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(ops.len() as u32).to_le_bytes());
    for op in ops {
        match op {
            BatchOp::Put { key, value } => {
                buf.push(TAG_PUT);
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(value);
            }
            BatchOp::Delete { key } => {
                buf.push(TAG_DELETE);
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
            }
        }
    }
    buf
}

/// Reads a `u32` length-prefixed slice starting at `*pos`, advancing `*pos` past it.
/// Rejects a length that would read past the end of `raw`.
fn take_bytes<'a>(raw: &'a [u8], pos: &mut usize) -> Result<&'a [u8], WalError> {
    let len_bytes = raw
        .get(*pos..*pos + 4)
        .ok_or_else(|| WalError::Serialization("batch op: missing length prefix".to_string()))?;
    let len = u32::from_le_bytes(len_bytes.try_into().expect("4-byte slice")) as usize;
    *pos += 4;
    let bytes = raw
        .get(*pos..*pos + len)
        .ok_or_else(|| WalError::Serialization("batch op: length runs past the end".to_string()))?;
    *pos += len;
    Ok(bytes)
}

fn decode_ops(raw: &[u8]) -> Result<Batch, WalError> {
    let count_bytes = raw
        .get(0..4)
        .ok_or_else(|| WalError::Serialization("batch ops: missing count".to_string()))?;
    let count = u32::from_le_bytes(count_bytes.try_into().expect("4-byte slice")) as usize;
    let mut pos = 4;
    // Cap the preallocation at what the remaining bytes could actually encode, so a
    // bogus `count` (e.g. u32::MAX) on a small input can't force a huge allocation
    // before the loop below ever reads a byte past the real data.
    let max_ops_by_size = raw.len().saturating_sub(4) / MIN_OP_LEN;
    let mut ops = Vec::with_capacity(count.min(max_ops_by_size));

    for _ in 0..count {
        let tag = *raw
            .get(pos)
            .ok_or_else(|| WalError::Serialization("batch op: missing tag byte".to_string()))?;
        pos += 1;
        match tag {
            TAG_PUT => {
                let key = take_bytes(raw, &mut pos)?.to_vec();
                let value = take_bytes(raw, &mut pos)?.to_vec();
                ops.push(BatchOp::Put { key, value });
            }
            TAG_DELETE => {
                let key = take_bytes(raw, &mut pos)?.to_vec();
                ops.push(BatchOp::Delete { key });
            }
            other => {
                return Err(WalError::Serialization(format!(
                    "batch op: unknown tag {other}"
                )))
            }
        }
    }

    if pos != raw.len() {
        return Err(WalError::Serialization(
            "batch ops: trailing bytes after the last op".to_string(),
        ));
    }

    Ok(Batch { ops })
}

impl Batch {
    pub fn encode(&self, compression: &CompressionConfig) -> Result<Vec<u8>, WalError> {
        let raw = encode_ops(&self.ops);
        let raw_len = raw.len() as u32;

        // `compress()` itself skips compression below `min_compress_bytes`, returning the
        // input unchanged. Mirror that check here so the codec byte we write always
        // matches what actually ended up in `body` — writing the requested codec for data
        // `compress()` left alone would make `decode` hand it to the wrong decompressor.
        let should_compress = compression.compression_type != CompressionType::None
            && raw.len() >= compression.min_compress_bytes;

        let (codec, body) = if should_compress {
            let compressed = compress(&raw, compression)
                .map_err(|e| WalError::Serialization(format!("batch compress: {e}")))?;
            (compression.compression_type, compressed)
        } else {
            (CompressionType::None, raw)
        };

        let mut out = Vec::with_capacity(1 + 1 + 4 + body.len());
        out.push(BATCH_VERSION);
        out.push(codec as u8);
        out.extend_from_slice(&raw_len.to_le_bytes());
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// Rejects unknown versions, codecs and tags, lengths past the end, and trailing
    /// bytes.
    pub fn decode(bytes: &[u8]) -> Result<Batch, WalError> {
        let header = bytes.get(0..6).ok_or_else(|| {
            WalError::Serialization("batch payload shorter than its header".to_string())
        })?;
        let version = header[0];
        if version != BATCH_VERSION {
            return Err(WalError::Serialization(format!(
                "unsupported batch version {version}"
            )));
        }
        let codec_byte = header[1];
        let raw_len = u32::from_le_bytes(header[2..6].try_into().expect("4-byte slice")) as usize;
        let body = &bytes[6..];

        // Reject an oversized claimed size *before* decompressing anything: `raw_len` is
        // attacker-controlled (it comes from the frame we're decoding), and bounding it
        // here means the bounded decompress below never has to trust more than
        // MAX_PAYLOAD_LEN worth of output, regardless of what the compressed body
        // itself could be coaxed into producing.
        if raw_len > MAX_PAYLOAD_LEN {
            return Err(WalError::Serialization(format!(
                "batch raw_len {raw_len} exceeds the {MAX_PAYLOAD_LEN}-byte limit"
            )));
        }

        let codec = CompressionType::from_u8(codec_byte)
            .ok_or_else(|| WalError::Serialization(format!("unknown batch codec {codec_byte}")))?;

        // Bounded to `raw_len`: a compressed `body` that decompresses to more than the
        // batch itself claims is a decompression bomb, and this must not be allowed to
        // allocate past that claim before noticing.
        let raw = if codec == CompressionType::None {
            body.to_vec()
        } else {
            decompress_bounded(body, codec, raw_len)
                .map_err(|e| WalError::Serialization(format!("batch decompress: {e}")))?
        };

        if raw.len() != raw_len {
            return Err(WalError::Serialization(format!(
                "batch raw_len {raw_len} does not match decoded length {}",
                raw.len()
            )));
        }

        decode_ops(&raw)
    }
}
