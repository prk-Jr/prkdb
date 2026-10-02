//! Record batch codec (Task 2.15b.2, streaming log design note §4.2): the payload of a
//! `FrameKind::Records` frame, one append of 1 to 65,536 stream records.
//!
//! # Layout
//!
//! Little-endian throughout. An 18-byte header, never compressed, then the body:
//!
//! | Offset | Size | Field            | Meaning                                                    |
//! |-------:|-----:|------------------|------------------------------------------------------------|
//! | 0      | 1    | `version`        | `1`                                                        |
//! | 1      | 1    | `codec`          | `CompressionType` actually used: 0 none, 1 LZ4, 2 Snappy, 3 Zstd |
//! | 2      | 4    | `raw_len` u32    | length of the uncompressed body, at most `MAX_PAYLOAD_LEN` |
//! | 6      | 8    | `append_time_ms` i64 | wall clock when the append was encoded, ms since the Unix epoch |
//! | 14     | 4    | `count` u32      | number of records, `1..=65_536`                            |
//! | 18     | ..   | `body`           | `raw_len` bytes when `codec` is 0, else a stream of that codec that decompresses to exactly `raw_len` bytes |
//!
//! The uncompressed body is the records in order; a record's index in the batch (its
//! `idx` in the offset `lsn << 16 | idx`) is its position. Each record:
//!
//! | Size | Field          | Present                                                   |
//! |-----:|----------------|-----------------------------------------------------------|
//! | 1    | `flags`        | always: bit 0 `has_key`, bit 1 `has_headers`, bits 2-7 zero |
//! | 4    | `key_len` u32  | if `has_key`                                              |
//! | n    | `key`          | if `has_key`                                              |
//! | 4    | `value_len` u32| always                                                    |
//! | n    | `value`        | always                                                    |
//! | 2    | `header_count` u16 | if `has_headers`, and then at least 1                 |
//! |      | per header: `name_len` u16, `name` (UTF-8), `val_len` u32, `val` | if `has_headers` |
//!
//! A record with no key and no headers costs 5 bytes plus its value. `has_key` with
//! `key_len = 0` is a present, empty key (`Some(vec![])`), distinct from no key.
//!
//! Worked example (pinned by `the_layout_is_pinned_byte_by_byte`): `append_time_ms =
//! 1000`, record 0 = key `k`, value `v`, header `h = x`; record 1 = no key, empty value,
//! no headers; uncompressed.
//!
//! ```text
//! 01 00 1A000000 E803000000000000 02000000     version 1, codec 0, raw_len 26, time 1000, count 2
//! 03 01000000 6B 01000000 76 0100 0100 68 01000000 78    record 0 (21 bytes)
//! 00 00000000                                  record 1 (5 bytes)
//! ```
//!
//! # Encoding
//!
//! One encoding per batch, so a decoded batch re-encodes (uncompressed) to the same bytes:
//! `has_headers` is set exactly when the record has headers. The body is compressed when
//! the caller's `CompressionConfig` names a codec and the body reaches its
//! `min_compress_bytes`, as `batch.rs` does, and is then kept only if it came out
//! smaller; `codec` records what was actually stored. `encode` refuses
//! (`WalError::InvalidRecords`) 0 or more than 65,536 records, a header name over 65,535
//! bytes, more than 65,535 headers on one record, and an uncompressed body over
//! `MAX_PAYLOAD_LEN`, which `decode` would refuse.
//!
//! # Decoding untrusted bytes
//!
//! `decode` is a fuzz target (`records_decode`) and allocates nothing a length field
//! claims beyond what the input holds: `raw_len` is checked against `MAX_PAYLOAD_LEN`
//! before decompressing, decompression is bounded by `raw_len` (`decompress_bounded`),
//! every length is checked against the bytes left before it is sliced, and the record
//! and header vectors are preallocated only up to what the remaining bytes could encode.
//! It refuses an unknown version or codec, a `count` of 0 or above 65,536, a body whose
//! length is not `raw_len`, set reserved flag bits, `has_headers` with no headers,
//! invalid UTF-8 header names, lengths past the end, and trailing bytes.
//!
//! The codec has no checksum of its own: a `Records` frame's CRC covers the payload.

use super::frame::MAX_PAYLOAD_LEN;
use super::{compress, decompress_bounded, CompressionConfig, CompressionType, WalError};
use std::borrow::Cow;

/// The only record batch version.
pub const RECORDS_VERSION: u8 = 1;
/// The most records one batch (one frame) holds: 2¹⁶, the range of an offset's `idx`.
pub const MAX_RECORDS: usize = 1 << 16;
/// `version(1) + codec(1) + raw_len(4) + append_time_ms(8) + count(4)`.
pub const RECORDS_HEADER_LEN: usize = 18;

const FLAG_HAS_KEY: u8 = 0b01;
const FLAG_HAS_HEADERS: u8 = 0b10;
const RESERVED_FLAGS: u8 = !(FLAG_HAS_KEY | FLAG_HAS_HEADERS);
/// Smallest encoded record: flags and an empty value's length.
const MIN_RECORD_LEN: usize = 1 + 4;
/// Smallest encoded header: an empty name's and an empty value's lengths.
const MIN_HEADER_LEN: usize = 2 + 4;

/// One stream record. `idx` is not stored: it is the record's position in its batch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Record {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    /// In order; names may repeat.
    pub headers: Vec<(String, Vec<u8>)>,
}

/// One append: its records and the time it was encoded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecordBatch {
    pub append_time_ms: i64,
    pub records: Vec<Record>,
}

/// The fixed header, validated.
struct Header {
    codec: CompressionType,
    raw_len: usize,
    append_time_ms: i64,
    count: usize,
}

fn malformed(reason: impl std::fmt::Display) -> WalError {
    WalError::Serialization(format!("record batch: {reason}"))
}

fn invalid(reason: impl Into<String>) -> WalError {
    WalError::InvalidRecords(reason.into())
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("4-byte slice"))
}

/// Reads and checks the header: version, codec, `raw_len <= MAX_PAYLOAD_LEN` and
/// `count` in range. Touches nothing past byte 18.
fn read_header(bytes: &[u8]) -> Result<Header, WalError> {
    let Some(header) = bytes.get(..RECORDS_HEADER_LEN) else {
        return Err(malformed(format!(
            "{} bytes, shorter than the {RECORDS_HEADER_LEN}-byte header",
            bytes.len()
        )));
    };
    if header[0] != RECORDS_VERSION {
        return Err(malformed(format!("unsupported version {}", header[0])));
    }
    let codec = CompressionType::from_u8(header[1])
        .ok_or_else(|| malformed(format!("unknown codec {}", header[1])))?;
    let raw_len = u32_at(header, 2) as usize;
    if raw_len > MAX_PAYLOAD_LEN {
        return Err(malformed(format!(
            "raw_len {raw_len} exceeds the {MAX_PAYLOAD_LEN}-byte limit"
        )));
    }
    let append_time_ms = i64::from_le_bytes(header[6..14].try_into().expect("8-byte slice"));
    let count = u32_at(header, 14) as usize;
    if count == 0 || count > MAX_RECORDS {
        return Err(malformed(format!(
            "count {count} is outside 1..={MAX_RECORDS}"
        )));
    }
    Ok(Header {
        codec,
        raw_len,
        append_time_ms,
        count,
    })
}

/// A cursor over the uncompressed body. Every read checks the bytes left first.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, len: usize, record: usize, what: &str) -> Result<&'a [u8], WalError> {
        if len > self.remaining() {
            return Err(malformed(format!(
                "record {record}: {what} of {len} bytes runs past the end of the body"
            )));
        }
        let bytes = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(bytes)
    }

    fn u8(&mut self, record: usize, what: &str) -> Result<u8, WalError> {
        Ok(self.take(1, record, what)?[0])
    }

    fn u16(&mut self, record: usize, what: &str) -> Result<u16, WalError> {
        let b = self.take(2, record, what)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    fn u32(&mut self, record: usize, what: &str) -> Result<u32, WalError> {
        let b = self.take(4, record, what)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// A `u32` length, then that many bytes.
    fn bytes32(&mut self, record: usize, what: &str) -> Result<&'a [u8], WalError> {
        let len = self.u32(record, what)? as usize;
        self.take(len, record, what)
    }

    fn record(&mut self, i: usize) -> Result<Record, WalError> {
        let flags = self.u8(i, "flags")?;
        if flags & RESERVED_FLAGS != 0 {
            return Err(malformed(format!(
                "record {i}: reserved flag bits set ({flags:#04x})"
            )));
        }
        let key = if flags & FLAG_HAS_KEY != 0 {
            Some(self.bytes32(i, "key")?.to_vec())
        } else {
            None
        };
        let value = self.bytes32(i, "value")?.to_vec();
        let headers = if flags & FLAG_HAS_HEADERS != 0 {
            self.headers(i)?
        } else {
            Vec::new()
        };
        Ok(Record {
            key,
            value,
            headers,
        })
    }

    fn headers(&mut self, i: usize) -> Result<Vec<(String, Vec<u8>)>, WalError> {
        let count = self.u16(i, "header_count")? as usize;
        if count == 0 {
            return Err(malformed(format!(
                "record {i}: has_headers with header_count 0"
            )));
        }
        let mut headers = Vec::with_capacity(count.min(self.remaining() / MIN_HEADER_LEN));
        for _ in 0..count {
            let name_len = self.u16(i, "header name length")? as usize;
            let name = self.take(name_len, i, "header name")?;
            let name = std::str::from_utf8(name)
                .map_err(|e| malformed(format!("record {i}: header name is not UTF-8: {e}")))?;
            let value = self.bytes32(i, "header value")?;
            headers.push((name.to_owned(), value.to_vec()));
        }
        Ok(headers)
    }
}

impl Record {
    /// This record's encoded length, or why it cannot be encoded.
    fn encoded_len(&self, i: usize) -> Result<usize, WalError> {
        let mut len = MIN_RECORD_LEN + self.value.len();
        if let Some(key) = &self.key {
            len = len.saturating_add(4 + key.len());
        }
        if !self.headers.is_empty() {
            if self.headers.len() > u16::MAX as usize {
                return Err(invalid(format!(
                    "record {i} has {} headers; at most {} fit",
                    self.headers.len(),
                    u16::MAX
                )));
            }
            len = len.saturating_add(2);
            for (name, value) in &self.headers {
                if name.len() > u16::MAX as usize {
                    return Err(invalid(format!(
                        "record {i}: a header name of {} bytes is over the {}-byte limit",
                        name.len(),
                        u16::MAX
                    )));
                }
                len = len
                    .saturating_add(MIN_HEADER_LEN + name.len())
                    .saturating_add(value.len());
            }
        }
        Ok(len)
    }

    /// Appends this record. Lengths were checked by `encoded_len`: the whole body is at
    /// most `MAX_PAYLOAD_LEN`, so every `u32` length fits.
    fn write(&self, out: &mut Vec<u8>) {
        let mut flags = 0;
        if self.key.is_some() {
            flags |= FLAG_HAS_KEY;
        }
        if !self.headers.is_empty() {
            flags |= FLAG_HAS_HEADERS;
        }
        out.push(flags);
        if let Some(key) = &self.key {
            out.extend_from_slice(&(key.len() as u32).to_le_bytes());
            out.extend_from_slice(key);
        }
        out.extend_from_slice(&(self.value.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.value);
        if !self.headers.is_empty() {
            out.extend_from_slice(&(self.headers.len() as u16).to_le_bytes());
            for (name, value) in &self.headers {
                out.extend_from_slice(&(name.len() as u16).to_le_bytes());
                out.extend_from_slice(name.as_bytes());
                out.extend_from_slice(&(value.len() as u32).to_le_bytes());
                out.extend_from_slice(value);
            }
        }
    }
}

impl RecordBatch {
    /// The uncompressed body's length, after checking every limit `decode` enforces.
    fn body_len(&self) -> Result<usize, WalError> {
        let count = self.records.len();
        if count == 0 || count > MAX_RECORDS {
            return Err(invalid(format!(
                "{count} records; a batch holds 1..={MAX_RECORDS}"
            )));
        }
        let mut len = 0usize;
        for (i, record) in self.records.iter().enumerate() {
            len = len.saturating_add(record.encoded_len(i)?);
            if len > MAX_PAYLOAD_LEN {
                return Err(invalid(format!(
                    "the uncompressed body exceeds the {MAX_PAYLOAD_LEN}-byte limit at \
                     record {i}; split the batch"
                )));
            }
        }
        Ok(len)
    }

    fn write_header(&self, out: &mut Vec<u8>, codec: CompressionType, raw_len: usize) {
        out.push(RECORDS_VERSION);
        out.push(codec as u8);
        out.extend_from_slice(&(raw_len as u32).to_le_bytes());
        out.extend_from_slice(&self.append_time_ms.to_le_bytes());
        out.extend_from_slice(&(self.records.len() as u32).to_le_bytes());
    }

    fn write_body(&self, out: &mut Vec<u8>) {
        for record in &self.records {
            record.write(out);
        }
    }

    /// Encodes the batch (layout in the module docs), compressing the body as
    /// `compression` says when that makes it smaller.
    pub fn encode(&self, compression: &CompressionConfig) -> Result<Vec<u8>, WalError> {
        let raw_len = self.body_len()?;
        let try_compress = compression.compression_type != CompressionType::None
            && raw_len >= compression.min_compress_bytes;

        if !try_compress {
            // One allocation: the header and the body written in place.
            let mut out = Vec::with_capacity(RECORDS_HEADER_LEN + raw_len);
            self.write_header(&mut out, CompressionType::None, raw_len);
            self.write_body(&mut out);
            debug_assert_eq!(out.len(), RECORDS_HEADER_LEN + raw_len);
            return Ok(out);
        }

        let mut raw = Vec::with_capacity(raw_len);
        self.write_body(&mut raw);
        debug_assert_eq!(raw.len(), raw_len);
        let compressed = compress(&raw, compression)
            .map_err(|e| WalError::Serialization(format!("record batch compress: {e}")))?;
        let (codec, body) = if compressed.len() < raw.len() {
            (compression.compression_type, compressed)
        } else {
            (CompressionType::None, raw)
        };
        let mut out = Vec::with_capacity(RECORDS_HEADER_LEN + body.len());
        self.write_header(&mut out, codec, raw_len);
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// Decodes a payload `encode` wrote, refusing anything else (module docs).
    pub fn decode(bytes: &[u8]) -> Result<RecordBatch, WalError> {
        let header = read_header(bytes)?;
        let body = &bytes[RECORDS_HEADER_LEN..];
        let raw: Cow<'_, [u8]> = if header.codec == CompressionType::None {
            Cow::Borrowed(body)
        } else {
            // Bounded by `raw_len` (itself at most MAX_PAYLOAD_LEN): a body that would
            // decompress to more is refused before it is all produced.
            Cow::Owned(
                decompress_bounded(body, header.codec, header.raw_len)
                    .map_err(|e| malformed(format!("decompress: {e}")))?,
            )
        };
        if raw.len() != header.raw_len {
            return Err(malformed(format!(
                "raw_len {} does not match the body's {} bytes",
                header.raw_len,
                raw.len()
            )));
        }

        let mut reader = Reader { buf: &raw, pos: 0 };
        let mut records = Vec::with_capacity(header.count.min(raw.len() / MIN_RECORD_LEN));
        for i in 0..header.count {
            records.push(reader.record(i)?);
        }
        if reader.remaining() != 0 {
            return Err(malformed(format!(
                "{} trailing bytes after record {}",
                reader.remaining(),
                header.count - 1
            )));
        }
        Ok(RecordBatch {
            append_time_ms: header.append_time_ms,
            records,
        })
    }

    /// The header only: `(append_time_ms, count)`, without decompressing or reading the
    /// body. It checks what the header alone can show (version, codec, `raw_len`,
    /// `count`); a body fault is found by `decode`.
    pub fn peek_header(bytes: &[u8]) -> Result<(i64, u32), WalError> {
        let header = read_header(bytes)?;
        Ok((header.append_time_ms, header.count as u32))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::frame::{decode_frame, encode_frame, Decoded, FrameKind};
    use crate::wal::CompressionType;
    use proptest::prelude::*;

    fn none() -> CompressionConfig {
        CompressionConfig::none()
    }

    /// Compresses whatever the size, so small test batches exercise each codec.
    fn always(codec: CompressionType) -> CompressionConfig {
        CompressionConfig {
            compression_type: codec,
            min_compress_bytes: 0,
            compression_level: 3,
        }
    }

    const ALL_CODECS: [CompressionType; 4] = [
        CompressionType::None,
        CompressionType::Lz4,
        CompressionType::Snappy,
        CompressionType::Zstd,
    ];

    fn rec(key: Option<&[u8]>, value: &[u8], headers: &[(&str, &[u8])]) -> Record {
        Record {
            key: key.map(<[u8]>::to_vec),
            value: value.to_vec(),
            headers: headers
                .iter()
                .map(|(n, v)| (n.to_string(), v.to_vec()))
                .collect(),
        }
    }

    /// Every combination of key and headers, an empty key, an empty value, an empty
    /// header name and a repeated header name.
    fn sample() -> RecordBatch {
        RecordBatch {
            append_time_ms: 1_700_000_000_123,
            records: vec![
                rec(Some(b"user:1"), b"alice", &[("trace", b"abc"), ("v", b"2")]),
                rec(None, b"no key, no headers", &[]),
                rec(Some(b"user:2"), b"bob", &[]),
                rec(
                    None,
                    b"headers only",
                    &[("", b""), ("dup", b"1"), ("dup", b"2")],
                ),
                rec(Some(b""), b"", &[]),
            ],
        }
    }

    /// Large and repetitive enough that every compressor shrinks it.
    fn compressible() -> RecordBatch {
        RecordBatch {
            append_time_ms: -5,
            records: (0..50)
                .map(|i| {
                    rec(
                        Some(format!("order:{i:04}").as_bytes()),
                        "pending ".repeat(8).as_bytes(),
                        &[("source", b"checkout")],
                    )
                })
                .collect(),
        }
    }

    /// A payload built field by field: `version | codec | raw_len | time | count | body`.
    fn payload(
        version: u8,
        codec: u8,
        raw_len: u32,
        time: i64,
        count: u32,
        body: &[u8],
    ) -> Vec<u8> {
        let mut out = vec![version, codec];
        out.extend_from_slice(&raw_len.to_le_bytes());
        out.extend_from_slice(&time.to_le_bytes());
        out.extend_from_slice(&count.to_le_bytes());
        out.extend_from_slice(body);
        out
    }

    /// An uncompressed payload of `count` records whose body is `body`.
    fn plain(count: u32, body: &[u8]) -> Vec<u8> {
        payload(1, 0, body.len() as u32, 7, count, body)
    }

    fn decode_err(bytes: &[u8]) -> String {
        match RecordBatch::decode(bytes) {
            Ok(batch) => panic!("decoded {batch:?}"),
            Err(e) => e.to_string(),
        }
    }

    /// The module docs' worked example, byte for byte.
    #[test]
    fn the_layout_is_pinned_byte_by_byte() {
        let batch = RecordBatch {
            append_time_ms: 1000,
            records: vec![rec(Some(b"k"), b"v", &[("h", b"x")]), rec(None, b"", &[])],
        };
        #[rustfmt::skip]
        let expected: Vec<u8> = vec![
            0x01,                                           // version
            0x00,                                           // codec: none
            0x1A, 0x00, 0x00, 0x00,                         // raw_len = 26
            0xE8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // append_time_ms = 1000
            0x02, 0x00, 0x00, 0x00,                         // count = 2
            // record 0
            0x03,                                           // flags: has_key | has_headers
            0x01, 0x00, 0x00, 0x00, b'k',                   // key_len = 1, key
            0x01, 0x00, 0x00, 0x00, b'v',                   // value_len = 1, value
            0x01, 0x00,                                     // header_count = 1
            0x01, 0x00, b'h',                               // name_len = 1, name
            0x01, 0x00, 0x00, 0x00, b'x',                   // val_len = 1, val
            // record 1
            0x00,                                           // flags: none
            0x00, 0x00, 0x00, 0x00,                         // value_len = 0
        ];
        assert_eq!(batch.encode(&none()).unwrap(), expected);
        assert_eq!(RecordBatch::decode(&expected).unwrap(), batch);
        assert_eq!(RECORDS_HEADER_LEN, 18);
    }

    #[test]
    fn round_trips_with_and_without_keys_headers_and_compression() {
        for batch in [sample(), compressible()] {
            for codec in ALL_CODECS {
                let bytes = batch.encode(&always(codec)).unwrap();
                assert_eq!(RecordBatch::decode(&bytes).unwrap(), batch, "{codec:?}");
            }
            let bytes = batch.encode(&CompressionConfig::default()).unwrap();
            assert_eq!(RecordBatch::decode(&bytes).unwrap(), batch);
        }
    }

    /// LZ4 shrinks a compressible batch, and the codec byte names the codec used.
    #[test]
    fn lz4_round_trips_and_records_the_codec_used() {
        let batch = compressible();
        let plain = batch.encode(&none()).unwrap();
        let lz4 = batch.encode(&always(CompressionType::Lz4)).unwrap();
        assert_eq!(plain[1], CompressionType::None as u8);
        assert_eq!(lz4[1], CompressionType::Lz4 as u8);
        assert!(lz4.len() < plain.len(), "{} vs {}", lz4.len(), plain.len());
        assert_eq!(
            lz4[2..6],
            plain[2..6],
            "raw_len is the uncompressed body's length"
        );
        assert_eq!(RecordBatch::decode(&lz4).unwrap(), batch);
    }

    /// Below the threshold, or when compression would not shrink the body, the body is
    /// stored as is and the codec byte says so.
    #[test]
    fn a_body_compression_does_not_shrink_is_stored_uncompressed() {
        let tiny = RecordBatch {
            append_time_ms: 0,
            records: vec![rec(None, b"x", &[])],
        };
        for cfg in [CompressionConfig::default(), always(CompressionType::Lz4)] {
            let bytes = tiny.encode(&cfg).unwrap();
            assert_eq!(bytes[1], CompressionType::None as u8, "{cfg:?}");
            assert_eq!(bytes, tiny.encode(&none()).unwrap());
        }
    }

    #[test]
    fn count_bounds_on_encode() {
        let empty = RecordBatch {
            append_time_ms: 0,
            records: Vec::new(),
        };
        assert!(matches!(
            empty.encode(&none()),
            Err(WalError::InvalidRecords(_))
        ));

        let mut full = RecordBatch {
            append_time_ms: 0,
            records: vec![Record::default(); MAX_RECORDS],
        };
        let bytes = full.encode(&none()).unwrap();
        assert_eq!(RecordBatch::peek_header(&bytes).unwrap(), (0, 65_536));
        assert_eq!(RecordBatch::decode(&bytes).unwrap(), full);

        full.records.push(Record::default());
        let err = full.encode(&none()).unwrap_err();
        assert!(matches!(err, WalError::InvalidRecords(_)), "{err}");
        assert!(err.to_string().contains("65537"), "{err}");
    }

    #[test]
    fn count_0_and_65_537_are_refused_on_decode() {
        assert!(decode_err(&plain(0, b"")).contains("count 0"));
        // 65,537 empty records: a body that would otherwise decode.
        let body = [0u8, 0, 0, 0, 0].repeat(65_537);
        assert!(decode_err(&plain(65_537, &body)).contains("count 65537"));
        let body = [0u8, 0, 0, 0, 0].repeat(65_536);
        assert_eq!(
            RecordBatch::decode(&plain(65_536, &body))
                .unwrap()
                .records
                .len(),
            65_536
        );
    }

    #[test]
    fn every_truncation_is_refused() {
        for (batch, codec) in [
            (sample(), CompressionType::None),
            (compressible(), CompressionType::Lz4),
            (compressible(), CompressionType::Snappy),
            (compressible(), CompressionType::Zstd),
        ] {
            let bytes = batch.encode(&always(codec)).unwrap();
            for len in 0..bytes.len() {
                assert!(
                    RecordBatch::decode(&bytes[..len]).is_err(),
                    "{codec:?}: prefix of {len} of {} bytes decoded",
                    bytes.len()
                );
            }
        }
    }

    /// A stored payload travels in a `Records` frame, so a reader sees the frame CRC
    /// first: every single-bit flip of the frame is refused before or by the codec.
    #[test]
    fn every_bit_flip_of_a_records_frame_is_refused() {
        for (batch, codec) in [
            (sample(), CompressionType::None),
            (compressible(), CompressionType::Lz4),
        ] {
            let mut frame = Vec::new();
            let payload = batch.encode(&always(codec)).unwrap();
            encode_frame(&mut frame, 9, FrameKind::Records, &payload);
            assert_eq!(decode_records_frame(&frame).unwrap(), batch);
            for i in 0..frame.len() {
                for bit in 0..8 {
                    let mut bad = frame.clone();
                    bad[i] ^= 1 << bit;
                    assert!(
                        decode_records_frame(&bad).is_err(),
                        "{codec:?}: byte {i} bit {bit}"
                    );
                }
            }
        }
    }

    /// The whole frame as a stream reader takes it: one `Records` frame, then its batch.
    fn decode_records_frame(buf: &[u8]) -> Result<RecordBatch, String> {
        match decode_frame(buf) {
            Decoded::Frame {
                kind: FrameKind::Records,
                payload,
                frame_len,
                ..
            } if frame_len == buf.len() => RecordBatch::decode(payload).map_err(|e| e.to_string()),
            other => Err(format!("{other:?}")),
        }
    }

    /// The codec alone has no checksum (the frame's covers it), so a flip inside a key,
    /// value or time decodes to a different batch. What it guarantees: a flip never
    /// decodes back to the same batch (the uncompressed encoding is canonical), and a flip
    /// in the version, codec, raw_len or count is always refused.
    #[test]
    fn a_bit_flip_in_the_payload_is_refused_or_changes_the_batch() {
        let batch = sample();
        let bytes = batch.encode(&none()).unwrap();
        for i in 0..bytes.len() {
            for bit in 0..8 {
                let mut bad = bytes.clone();
                bad[i] ^= 1 << bit;
                if let Ok(other) = RecordBatch::decode(&bad) {
                    assert!(
                        (6..14).contains(&i) || i >= RECORDS_HEADER_LEN,
                        "header byte {i} bit {bit} decoded"
                    );
                    assert_ne!(other, batch, "byte {i} bit {bit} decoded to the same batch");
                }
            }
        }
    }

    #[test]
    fn reserved_flag_bits_are_refused() {
        for bit in 2..8 {
            let body = [1u8 << bit, 0, 0, 0, 0];
            assert!(
                decode_err(&plain(1, &body)).contains("reserved"),
                "bit {bit}"
            );
        }
    }

    /// `has_headers` with zero headers is refused, so every batch has one encoding.
    #[test]
    fn has_headers_with_no_headers_is_refused() {
        let body = [0b10u8, 0, 0, 0, 0, 0, 0];
        assert!(decode_err(&plain(1, &body)).contains("header_count 0"));
    }

    #[test]
    fn raw_len_above_max_payload_len_is_refused_before_decompressing() {
        // Not an LZ4 stream at all: a decoder that decompressed first would report that.
        let bytes = payload(
            1,
            CompressionType::Lz4 as u8,
            MAX_PAYLOAD_LEN as u32 + 1,
            0,
            1,
            b"junk",
        );
        let err = decode_err(&bytes);
        assert!(err.contains("raw_len") && err.contains("limit"), "{err}");
        assert!(RecordBatch::peek_header(&bytes).is_err());
    }

    /// A body that decompresses to more than `raw_len` is refused without producing more
    /// than `raw_len + 1` bytes (`decompress_bounded`).
    #[test]
    fn a_body_that_decompresses_past_raw_len_is_refused() {
        let big = RecordBatch {
            append_time_ms: 0,
            records: vec![rec(None, &vec![0u8; 1 << 20], &[])],
        };
        for codec in [
            CompressionType::Lz4,
            CompressionType::Snappy,
            CompressionType::Zstd,
        ] {
            let mut bytes = big.encode(&always(codec)).unwrap();
            assert_eq!(bytes[1], codec as u8);
            bytes[2..6].copy_from_slice(&64u32.to_le_bytes());
            assert!(decode_err(&bytes).contains("decompress"), "{codec:?}");
        }
    }

    #[test]
    fn invalid_utf8_header_names_are_refused() {
        let mut body = vec![0b10u8, 0, 0, 0, 0]; // has_headers, empty value
        body.extend_from_slice(&1u16.to_le_bytes()); // one header
        body.extend_from_slice(&2u16.to_le_bytes());
        body.extend_from_slice(&[0xC3, 0x28]); // invalid UTF-8
        body.extend_from_slice(&0u32.to_le_bytes());
        assert!(decode_err(&plain(1, &body)).contains("UTF-8"));
    }

    #[test]
    fn trailing_bytes_are_refused() {
        let mut body = sample().encode(&none()).unwrap()[RECORDS_HEADER_LEN..].to_vec();
        body.push(0);
        assert!(decode_err(&plain(5, &body)).contains("trailing"));
        // Past the end of the declared body, too (raw_len no longer matches).
        let mut bytes = sample().encode(&none()).unwrap();
        bytes.push(0);
        assert!(RecordBatch::decode(&bytes).is_err());
    }

    #[test]
    fn lengths_past_the_end_are_refused() {
        let cases: [(&str, Vec<u8>); 4] = [
            ("key", vec![0b01, 0xFF, 0xFF, 0xFF, 0xFF]),
            ("value", vec![0, 9, 0, 0, 0, b'x']),
            ("header name", vec![0b10, 0, 0, 0, 0, 1, 0, 5, 0, b'a']),
            (
                "header value",
                vec![0b10, 0, 0, 0, 0, 1, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0x7F],
            ),
        ];
        for (what, body) in cases {
            let err = decode_err(&plain(1, &body));
            assert!(err.contains("past the end"), "{what}: {err}");
        }
        // A count larger than the records present.
        let body = [0u8, 0, 0, 0, 0];
        assert!(decode_err(&plain(2, &body)).contains("past the end"));
    }

    #[test]
    fn unknown_versions_and_codecs_are_refused() {
        let body = [0u8, 0, 0, 0, 0];
        for version in [0u8, 2, 255] {
            let bytes = payload(version, 0, 5, 0, 1, &body);
            assert!(decode_err(&bytes).contains("version"), "{version}");
            assert!(RecordBatch::peek_header(&bytes).is_err());
        }
        for codec in [4u8, 255] {
            let bytes = payload(1, codec, 5, 0, 1, &body);
            assert!(decode_err(&bytes).contains("codec"), "{codec}");
            assert!(RecordBatch::peek_header(&bytes).is_err());
        }
        assert!(decode_err(&[1, 0, 0]).contains("header"));
    }

    /// `peek_header` reads the uncompressed header only: a body that is not even a valid
    /// LZ4 stream does not stop it.
    #[test]
    fn peek_header_reads_time_and_count_without_decompressing() {
        let bytes = payload(1, CompressionType::Lz4 as u8, 100, -42, 3, b"not lz4");
        assert_eq!(RecordBatch::peek_header(&bytes).unwrap(), (-42, 3));
        assert!(RecordBatch::decode(&bytes).is_err());

        let batch = compressible();
        let bytes = batch.encode(&always(CompressionType::Lz4)).unwrap();
        assert_eq!(RecordBatch::peek_header(&bytes).unwrap(), (-5, 50));
        assert_eq!(
            RecordBatch::peek_header(&bytes[..RECORDS_HEADER_LEN]).unwrap(),
            (-5, 50)
        );
        assert!(RecordBatch::peek_header(&bytes[..RECORDS_HEADER_LEN - 1]).is_err());
        assert!(RecordBatch::peek_header(&plain(0, b"")).is_err());
    }

    #[test]
    fn encode_refuses_fields_too_long_for_their_length_prefix() {
        let long_name = RecordBatch {
            append_time_ms: 0,
            records: vec![Record {
                headers: vec![("n".repeat(u16::MAX as usize + 1), Vec::new())],
                ..Record::default()
            }],
        };
        let err = long_name.encode(&none()).unwrap_err();
        assert!(
            matches!(err, WalError::InvalidRecords(_)) && err.to_string().contains("name"),
            "{err}"
        );

        let many_headers = RecordBatch {
            append_time_ms: 0,
            records: vec![Record {
                headers: vec![(String::new(), Vec::new()); u16::MAX as usize + 1],
                ..Record::default()
            }],
        };
        let err = many_headers.encode(&none()).unwrap_err();
        assert!(
            matches!(err, WalError::InvalidRecords(_)) && err.to_string().contains("headers"),
            "{err}"
        );

        let mut at_limit = many_headers;
        at_limit.records[0].headers.pop();
        let bytes = at_limit.encode(&none()).unwrap();
        assert_eq!(RecordBatch::decode(&bytes).unwrap(), at_limit);
    }

    /// The uncompressed body is what `decode` bounds by `MAX_PAYLOAD_LEN`, so `encode`
    /// refuses a larger one even when it would compress under the limit.
    #[test]
    fn encode_refuses_a_body_over_max_payload_len() {
        let batch = RecordBatch {
            append_time_ms: 0,
            records: vec![rec(None, &vec![0u8; MAX_PAYLOAD_LEN], &[])],
        };
        for codec in [CompressionType::None, CompressionType::Lz4] {
            let err = batch.encode(&always(codec)).unwrap_err();
            assert!(
                matches!(err, WalError::InvalidRecords(_)),
                "{codec:?}: {err}"
            );
            assert!(err.to_string().contains("limit"), "{err}");
        }
    }

    fn record_strategy() -> impl Strategy<Value = Record> {
        (
            proptest::option::of(proptest::collection::vec(any::<u8>(), 0..16)),
            proptest::collection::vec(any::<u8>(), 0..64),
            proptest::collection::vec(
                (".{0,6}", proptest::collection::vec(any::<u8>(), 0..8)),
                0..3,
            ),
        )
            .prop_map(|(key, value, headers)| Record {
                key,
                value,
                headers,
            })
    }

    fn batch_strategy() -> impl Strategy<Value = RecordBatch> {
        (
            any::<i64>(),
            proptest::collection::vec(record_strategy(), 1..12),
        )
            .prop_map(|(append_time_ms, records)| RecordBatch {
                append_time_ms,
                records,
            })
    }

    fn config_strategy() -> impl Strategy<Value = CompressionConfig> {
        prop_oneof![
            Just(none()),
            Just(always(CompressionType::Lz4)),
            Just(always(CompressionType::Snappy)),
            Just(always(CompressionType::Zstd)),
            Just(CompressionConfig::default()),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn prop_round_trips(batch in batch_strategy(), cfg in config_strategy()) {
            let bytes = batch.encode(&cfg).unwrap();
            prop_assert_eq!(
                RecordBatch::peek_header(&bytes).unwrap(),
                (batch.append_time_ms, batch.records.len() as u32)
            );
            prop_assert_eq!(RecordBatch::decode(&bytes).unwrap(), batch);
        }

        #[test]
        fn prop_every_truncation_is_refused(
            batch in batch_strategy(),
            cfg in config_strategy(),
        ) {
            let bytes = batch.encode(&cfg).unwrap();
            for len in 0..bytes.len() {
                prop_assert!(RecordBatch::decode(&bytes[..len]).is_err(), "prefix {}", len);
            }
        }

        #[test]
        fn prop_every_frame_bit_flip_is_refused(
            batch in batch_strategy(),
            cfg in config_strategy(),
        ) {
            let mut frame = Vec::new();
            encode_frame(&mut frame, 3, FrameKind::Records, &batch.encode(&cfg).unwrap());
            for i in 0..frame.len() {
                for bit in 0..8 {
                    let mut bad = frame.clone();
                    bad[i] ^= 1 << bit;
                    prop_assert!(decode_records_frame(&bad).is_err(), "byte {} bit {}", i, bit);
                }
            }
        }

        #[test]
        fn prop_a_payload_bit_flip_never_decodes_to_the_same_batch(batch in batch_strategy()) {
            let bytes = batch.encode(&none()).unwrap();
            for i in 0..bytes.len() {
                for bit in 0..8 {
                    let mut bad = bytes.clone();
                    bad[i] ^= 1 << bit;
                    if let Ok(other) = RecordBatch::decode(&bad) {
                        prop_assert_ne!(&other, &batch, "byte {} bit {}", i, bit);
                    }
                }
            }
        }

        /// Arbitrary bytes never panic, and anything that decodes re-encodes to a batch
        /// that decodes the same.
        #[test]
        fn prop_arbitrary_bytes_never_panic(
            bytes in proptest::collection::vec(any::<u8>(), 0..256),
        ) {
            if let Ok(batch) = RecordBatch::decode(&bytes) {
                let again = batch.encode(&none()).unwrap();
                prop_assert_eq!(RecordBatch::decode(&again).unwrap(), batch);
            }
            let _ = RecordBatch::peek_header(&bytes);
        }
    }
}
