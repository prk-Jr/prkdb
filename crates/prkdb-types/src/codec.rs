//! Bounded bincode decoding for bytes read from disk or the network.
//!
//! `bincode::config::standard()` has no size limit. Decoding a `String` or `Vec<u8>` with
//! it allocates the length the input declares before reading a single byte of the
//! contents, so a ten-byte input can declare a 2^62-byte string and abort the process
//! (or a `u64::MAX` one and panic with "capacity overflow"). Every decode of bytes this
//! program did not just produce itself goes through this module instead, which uses
//! `standard().with_limit::<N>()`: a decode that would claim more than `N` bytes is a
//! `DecodeError::LimitExceeded`, returned before the claimed allocation is made.
//!
//! The limit is a decode-side check only. It does not change the wire format, so bytes
//! written with `standard()` decode here unchanged, and encoders keep using `standard()`
//! (`limit_does_not_change_the_wire_format` below).
//!
//! # What bincode claims
//!
//! The limit is checked against a running count of "claimed" bytes, which is the decoded
//! value's in-memory size, not its wire size:
//!
//! - every primitive claims its `size_of` when decoded, on both the serde and the native
//!   path: a `u64` or a length prefix claims 8 even when its varint is one byte on the
//!   wire, so a valid value can claim up to 8 times its encoding (16 for `u128`);
//! - a `String`, `Vec<u8>` or serde byte buffer claims its declared length before
//!   allocating it, which is what stops the 2^62-byte string;
//! - on the native path only, a `Vec<T>` or map of anything else first claims
//!   `len * size_of::<T>()` and preallocates `len` slots, releasing the claim element by
//!   element. That transient claim can exceed 8 times the encoding (a
//!   `Vec<(Vec<u8>, Vec<u8>)>` of empty pairs is 2 bytes per element on the wire and 48
//!   in memory). The serde path's sequences and maps claim nothing up front and
//!   preallocate at most serde's cautious size hint.
//!
//! # Which limit
//!
//! - [`decode`] / [`decode_serde`] (records, Raft entries, messages) and [`decode_file`] /
//!   [`decode_serde_file`] (whole files: a Raft snapshot, a persisted index) all scale the
//!   limit with the input: the smallest of 1 MiB, 8 MiB, 64 MiB, ... that is at least
//!   [`CLAIM_PER_INPUT_BYTE`] (8) times the input's length. A valid value always fits
//!   when it claims at most 8 bytes per wire byte: every serde type without `u128`, and
//!   every native type whose only containers are `Vec<u8>` and `String`. A hostile input
//!   can make a decode claim at most `max(1 MiB, 64 x its length)`. The record decodes
//!   also stop at an absolute ceiling, [`MAX_RECORD_CLAIM`] (512 MiB = 8 x
//!   [`MAX_RECORD_BYTES`]): a record or Raft entry is never longer than a WAL frame
//!   payload, so a valid one never needs more, and an input whose scaled limit would
//!   exceed it is `LimitExceeded` without decoding. The file decodes have no ceiling.
//!   Every production caller uses the serde path or a native type of that shape
//!   (`(u64, u64, Vec<u8>)` for the Raft snapshot). The exception is
//!   `LogRecord::deserialize` (native, `Vec<(Vec<u8>, Vec<u8>)>` batches), which has no
//!   production caller: the legacy record format is decoded only by its own tests.
//! - [`decode_with_limit`] / [`decode_serde_with_limit`]: a fixed limit, for small
//!   fixed-shape values such as a file header ([`MAX_HEADER_BYTES`]).

use bincode::config::{self, Configuration, Limit};
use bincode::error::DecodeError;
use serde::de::DeserializeOwned;

/// The largest stored key or value: 64 MiB, the WAL's largest frame payload
/// (`prkdb_core`'s `MAX_PAYLOAD_LEN`, asserted equal there). Readers of length-prefixed
/// entries refuse a longer declared length before reading it.
pub const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

/// The limit for small fixed-shape values such as a file header: 64 KiB.
pub const MAX_HEADER_BYTES: usize = 64 * 1024;

/// The most bytes a valid value claims per byte of its encoding (a one-byte varint
/// decoded into a `u64` or `usize`); see the module docs.
pub const CLAIM_PER_INPUT_BYTE: usize = 8;

/// The most a record, Raft entry or message decode ([`decode`], [`decode_serde`]) may
/// claim: 512 MiB, [`CLAIM_PER_INPUT_BYTE`] x [`MAX_RECORD_BYTES`].
pub const MAX_RECORD_CLAIM: usize = CLAIM_PER_INPUT_BYTE * MAX_RECORD_BYTES;

const _: () = assert!(MAX_RECORD_CLAIM == 1 << 29);

/// Whether a record decode of `bytes` would need a limit over [`MAX_RECORD_CLAIM`].
fn over_record_ceiling(bytes: &[u8]) -> bool {
    bytes.len().saturating_mul(CLAIM_PER_INPUT_BYTE) > MAX_RECORD_CLAIM
}

/// `standard()` with a decode limit of `N` bytes. Same wire format as `standard()`.
pub fn bounded<const N: usize>() -> Configuration<config::LittleEndian, config::Varint, Limit<N>> {
    config::standard().with_limit::<N>()
}

/// Decodes a `bincode::Decode` value, claiming at most `N` bytes.
pub fn decode_with_limit<T: bincode::Decode<()>, const N: usize>(
    bytes: &[u8],
) -> Result<(T, usize), DecodeError> {
    bincode::decode_from_slice(bytes, bounded::<N>())
}

/// Decodes a serde value, claiming at most `N` bytes.
pub fn decode_serde_with_limit<T: DeserializeOwned, const N: usize>(
    bytes: &[u8],
) -> Result<(T, usize), DecodeError> {
    bincode::serde::decode_from_slice(bytes, bounded::<N>())
}

/// Runs `$decode::<T, TIER>` with the smallest tier (1 MiB x 8^k) that is at least
/// `CLAIM_PER_INPUT_BYTE * bytes.len()`. Tiers grow by 8x, so the limit is at most
/// `max(1 MiB, 64 x bytes.len())`.
macro_rules! scaled {
    ($decode:ident, $t:ty, $bytes:expr) => {{
        let bytes: &[u8] = $bytes;
        let need = bytes.len().saturating_mul(CLAIM_PER_INPUT_BYTE);
        if need <= 1 << 20 {
            return $decode::<$t, { 1 << 20 }>(bytes);
        }
        if need <= 1 << 23 {
            return $decode::<$t, { 1 << 23 }>(bytes);
        }
        if need <= 1 << 26 {
            return $decode::<$t, { 1 << 26 }>(bytes);
        }
        if need <= 1 << 29 {
            return $decode::<$t, { 1 << 29 }>(bytes);
        }
        #[cfg(target_pointer_width = "64")]
        {
            if need <= 1 << 32 {
                return $decode::<$t, { 1 << 32 }>(bytes);
            }
            if need <= 1 << 35 {
                return $decode::<$t, { 1 << 35 }>(bytes);
            }
            if need <= 1 << 38 {
                return $decode::<$t, { 1 << 38 }>(bytes);
            }
            if need <= 1 << 41 {
                return $decode::<$t, { 1 << 41 }>(bytes);
            }
        }
        Err(DecodeError::LimitExceeded)
    }};
}

/// Decodes one record's, Raft entry's or message's `bincode::Decode` value; the limit
/// scales with the input, up to [`MAX_RECORD_CLAIM`] (module docs).
pub fn decode<T: bincode::Decode<()>>(bytes: &[u8]) -> Result<(T, usize), DecodeError> {
    if over_record_ceiling(bytes) {
        return Err(DecodeError::LimitExceeded);
    }
    scaled!(decode_with_limit, T, bytes)
}

/// Decodes one record's, Raft entry's or message's serde value; the limit scales with
/// the input, up to [`MAX_RECORD_CLAIM`] (module docs).
pub fn decode_serde<T: DeserializeOwned>(bytes: &[u8]) -> Result<(T, usize), DecodeError> {
    if over_record_ceiling(bytes) {
        return Err(DecodeError::LimitExceeded);
    }
    scaled!(decode_serde_with_limit, T, bytes)
}

/// Decodes a whole file's `bincode::Decode` value; the limit scales with the input, with
/// no ceiling (module docs).
pub fn decode_file<T: bincode::Decode<()>>(bytes: &[u8]) -> Result<(T, usize), DecodeError> {
    scaled!(decode_with_limit, T, bytes)
}

/// Decodes a whole file's serde value; the limit scales with the input, with no ceiling
/// (module docs).
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

    /// Claims are the in-memory size: a million zero `u64`s are about 1 MB on the wire and
    /// claim 8 MB. A fixed 1 MiB limit refuses them on both paths; the scaled limit
    /// (8 x the input) decodes them.
    #[test]
    fn claims_are_in_memory_size_and_the_scaled_limit_covers_them() {
        let zeros = vec![0u64; 1_000_000];
        let bytes = bincode::encode_to_vec(&zeros, config::standard()).unwrap();
        assert!(bytes.len() < 2 * 1024 * 1024);
        assert!(matches!(
            decode_with_limit::<Vec<u64>, { 1 << 20 }>(&bytes),
            Err(DecodeError::LimitExceeded)
        ));
        assert!(matches!(
            decode_serde_with_limit::<Vec<u64>, { 1 << 20 }>(&bytes),
            Err(DecodeError::LimitExceeded)
        ));
        assert_eq!(decode::<Vec<u64>>(&bytes).unwrap().0, zeros);
        assert_eq!(decode_serde::<Vec<u64>>(&bytes).unwrap().0, zeros);
    }

    /// The record decodes stop at 512 MiB of claim: an input longer than a WAL frame
    /// payload (whose scaled limit would be over the ceiling) is refused without being
    /// decoded, while a valid record near the frame limit still decodes. The file decodes
    /// take the same over-long input.
    #[test]
    fn record_decodes_stop_at_the_ceiling_and_file_decodes_do_not() {
        let near = Shape::Bytes(vec![7; MAX_RECORD_BYTES - 64]);
        let bytes = bincode::encode_to_vec(&near, config::standard()).unwrap();
        assert!(bytes.len() <= MAX_RECORD_BYTES);
        assert_eq!(decode::<Shape>(&bytes).unwrap().0, near);
        assert_eq!(decode_serde::<Shape>(&bytes).unwrap().0, near);

        let over = Shape::Bytes(vec![7; MAX_RECORD_BYTES]);
        let bytes = bincode::encode_to_vec(&over, config::standard()).unwrap();
        assert!(bytes.len() > MAX_RECORD_BYTES);
        assert!(matches!(
            decode::<Shape>(&bytes),
            Err(DecodeError::LimitExceeded)
        ));
        assert!(matches!(
            decode_serde::<Shape>(&bytes),
            Err(DecodeError::LimitExceeded)
        ));
        assert_eq!(decode_file::<Shape>(&bytes).unwrap().0, over);
    }

    #[test]
    fn a_header_limit_refuses_what_a_record_limit_allows() {
        let input = declares_string_of(MAX_HEADER_BYTES as u64 + 1);
        assert!(matches!(
            decode_with_limit::<Shape, MAX_HEADER_BYTES>(&input),
            Err(DecodeError::LimitExceeded)
        ));
        // Under the scaled limit (1 MiB for a ten-byte input) the same declaration is
        // allowed, and fails only because the bytes are not there.
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
