//! Bounded bincode decoding for bytes read from disk or the network.
//!
//! `bincode::config::standard()` has no size limit. Decoding a `String` or `Vec<u8>` with
//! it allocates the length the input declares before reading a single byte of the
//! contents, so a ten-byte input can declare a 2^62-byte string and abort the process
//! (or a `u64::MAX` one and panic with "capacity overflow"). Every decode of bytes this
//! program did not just produce itself goes through this module instead, which uses
//! `standard().with_limit::<N>()`: a declared length that would take the decode past `N`
//! bytes is a `DecodeError::LimitExceeded`, returned before anything is allocated.
//!
//! The limit is a decode-side check only. It does not change the wire format, so bytes
//! written with `standard()` decode here unchanged, and encoders keep using `standard()`
//! (`limit_does_not_change_the_wire_format` below).
//!
//! # Which limit
//!
//! - [`decode`] / [`decode_serde`]: one stored record, Raft log entry or message. Limited
//!   to [`MAX_RECORD_BYTES`], the WAL's largest frame payload (`prkdb_core`'s
//!   `MAX_PAYLOAD_LEN`, asserted equal there): no stored value can be larger than the
//!   frame that carried it.
//! - [`decode_with_limit`] / [`decode_serde_with_limit`]: a caller-chosen limit, for
//!   small fixed-shape values such as a file header ([`MAX_HEADER_BYTES`]).
//! - [`decode_file`] / [`decode_serde_file`]: a whole file that may legitimately be far
//!   larger than one record (a Raft snapshot, a persisted index). The limit scales with
//!   the input: the smallest of 1 MiB, 16 MiB, ... that is at least the input's length.
//!   For the types these hold (byte strings, strings, and maps and sequences of them) a
//!   valid encoding never declares more bytes than it contains, so a valid file always
//!   fits, and a hostile one can make the decoder allocate at most 16 times its own size.

use bincode::config::{self, Configuration, Limit};
use bincode::error::DecodeError;
use serde::de::DeserializeOwned;

/// The largest single record, Raft log entry or message decoded with [`decode`] or
/// [`decode_serde`]: 64 MiB, the WAL's largest frame payload.
pub const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

/// The limit for small fixed-shape values such as a file header: 64 KiB.
pub const MAX_HEADER_BYTES: usize = 64 * 1024;

/// `standard()` with a decode limit of `N` bytes. Same wire format as `standard()`.
pub fn bounded<const N: usize>() -> Configuration<config::LittleEndian, config::Varint, Limit<N>> {
    config::standard().with_limit::<N>()
}

/// Decodes a `bincode::Decode` value, declaring at most `N` bytes.
pub fn decode_with_limit<T: bincode::Decode<()>, const N: usize>(
    bytes: &[u8],
) -> Result<(T, usize), DecodeError> {
    bincode::decode_from_slice(bytes, bounded::<N>())
}

/// Decodes a serde value, declaring at most `N` bytes.
pub fn decode_serde_with_limit<T: DeserializeOwned, const N: usize>(
    bytes: &[u8],
) -> Result<(T, usize), DecodeError> {
    bincode::serde::decode_from_slice(bytes, bounded::<N>())
}

/// Decodes one record's `bincode::Decode` value (limit [`MAX_RECORD_BYTES`]).
pub fn decode<T: bincode::Decode<()>>(bytes: &[u8]) -> Result<(T, usize), DecodeError> {
    decode_with_limit::<T, MAX_RECORD_BYTES>(bytes)
}

/// Decodes one record's serde value (limit [`MAX_RECORD_BYTES`]).
pub fn decode_serde<T: DeserializeOwned>(bytes: &[u8]) -> Result<(T, usize), DecodeError> {
    decode_serde_with_limit::<T, MAX_RECORD_BYTES>(bytes)
}

/// Picks the smallest limit tier at least `bytes.len()` and runs `$decode::<T, TIER>`.
/// The tiers grow by 16x, so the limit is never more than 16 times the input.
macro_rules! scaled {
    ($decode:ident, $t:ty, $bytes:expr) => {{
        let bytes: &[u8] = $bytes;
        if bytes.len() <= 1 << 20 {
            return $decode::<$t, { 1 << 20 }>(bytes);
        }
        if bytes.len() <= 1 << 24 {
            return $decode::<$t, { 1 << 24 }>(bytes);
        }
        if bytes.len() <= 1 << 28 {
            return $decode::<$t, { 1 << 28 }>(bytes);
        }
        #[cfg(target_pointer_width = "64")]
        {
            if bytes.len() <= 1 << 32 {
                return $decode::<$t, { 1 << 32 }>(bytes);
            }
            if bytes.len() <= 1 << 36 {
                return $decode::<$t, { 1 << 36 }>(bytes);
            }
            if bytes.len() <= 1 << 40 {
                return $decode::<$t, { 1 << 40 }>(bytes);
            }
        }
        Err(DecodeError::LimitExceeded)
    }};
}

/// Decodes a whole file's `bincode::Decode` value; the limit scales with the input (see
/// the module docs for the types this is sound for).
pub fn decode_file<T: bincode::Decode<()>>(bytes: &[u8]) -> Result<(T, usize), DecodeError> {
    scaled!(decode_with_limit, T, bytes)
}

/// Decodes a whole file's serde value; the limit scales with the input (see the module
/// docs for the types this is sound for).
pub fn decode_serde_file<T: DeserializeOwned>(bytes: &[u8]) -> Result<(T, usize), DecodeError> {
    scaled!(decode_serde_with_limit, T, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Debug, PartialEq, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
    enum Shape {
        Bytes(Vec<u8>),
        Named { name: String, n: u32 },
    }

    /// `[variant 1][String length: 0xFD + u64 LE]`, nothing after: a ten-byte input
    /// declaring a `len`-byte string.
    fn declares_string_of(len: u64) -> Vec<u8> {
        let mut input = vec![1u8, 0xFD];
        input.extend_from_slice(&len.to_le_bytes());
        input
    }

    #[test]
    fn limit_does_not_change_the_wire_format() {
        let values = [
            Shape::Bytes(vec![7; 300]),
            Shape::Named {
                name: "orders".into(),
                n: 3,
            },
        ];
        for v in &values {
            let plain = bincode::encode_to_vec(v, config::standard()).unwrap();
            let limited = bincode::encode_to_vec(v, bounded::<MAX_RECORD_BYTES>()).unwrap();
            assert_eq!(plain, limited);
            let plain_serde = bincode::serde::encode_to_vec(v, config::standard()).unwrap();
            assert_eq!(plain_serde, plain);
            let (back, used) = decode::<Shape>(&plain).unwrap();
            assert_eq!((&back, used), (v, plain.len()));
            assert_eq!(&decode_serde::<Shape>(&plain).unwrap().0, v);
            assert_eq!(&decode_file::<Shape>(&plain).unwrap().0, v);
            assert_eq!(&decode_serde_file::<Shape>(&plain).unwrap().0, v);
        }
    }

    #[test]
    fn an_oversized_declared_length_is_an_error_not_an_allocation() {
        for len in [u64::MAX, 1 << 62, 1 << 40, (MAX_RECORD_BYTES as u64) + 1] {
            let input = declares_string_of(len);
            assert!(
                matches!(decode::<Shape>(&input), Err(DecodeError::LimitExceeded)),
                "native, {len:#x}"
            );
            assert!(
                matches!(
                    decode_serde::<Shape>(&input),
                    Err(DecodeError::LimitExceeded)
                ),
                "serde, {len:#x}"
            );
            assert!(
                matches!(
                    decode_file::<Shape>(&input),
                    Err(DecodeError::LimitExceeded)
                ),
                "file, {len:#x}"
            );
            assert!(
                matches!(
                    decode_serde_file::<Shape>(&input),
                    Err(DecodeError::LimitExceeded)
                ),
                "serde file, {len:#x}"
            );
        }
    }

    #[test]
    fn a_header_limit_refuses_what_a_record_limit_allows() {
        let input = declares_string_of(MAX_HEADER_BYTES as u64 + 1);
        assert!(matches!(
            decode_with_limit::<Shape, MAX_HEADER_BYTES>(&input),
            Err(DecodeError::LimitExceeded)
        ));
        // Under the record limit the same declaration is allowed, and fails only because
        // the bytes are not there.
        assert!(matches!(
            decode::<Shape>(&input),
            Err(DecodeError::UnexpectedEnd { .. })
        ));
    }

    #[test]
    fn a_file_larger_than_the_first_tier_still_decodes() {
        let mut map: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        map.insert(b"big".to_vec(), Some(vec![1; (1 << 20) + 10]));
        map.insert(b"gone".to_vec(), None);
        let bytes = bincode::serde::encode_to_vec(&map, config::standard()).unwrap();
        assert!(bytes.len() > 1 << 20);
        let (back, _): (HashMap<Vec<u8>, Option<Vec<u8>>>, _) = decode_serde_file(&bytes).unwrap();
        assert_eq!(back, map);

        let state = (5u64, 2u64, vec![9u8; (1 << 20) + 1]);
        let bytes = bincode::encode_to_vec(&state, config::standard()).unwrap();
        assert_eq!(decode_file::<(u64, u64, Vec<u8>)>(&bytes).unwrap().0, state);
    }
}
