//! Frame codec for the single ordered WAL (Task 2.5, decision record §7).
//!
//! Layout, little-endian: `len u32 | crc u32 | lsn u64 | kind u8 | payload`. The 17-byte
//! header is fixed; `len` is the payload length. The CRC covers `lsn | kind | payload`,
//! not `len`, so a torn write that truncates the payload is caught by the length check
//! before the CRC is even computed.
//!
//! **The CRC is checked before the kind (STO-11).** A frame whose CRC is wrong is torn
//! (`BadCrc`), whatever its kind byte says. A frame whose CRC is right but whose kind this
//! build does not know was written whole by a build that knows more kinds:
//! `UnsupportedKind`, which the log refuses and never truncates as a torn tail.
//!
//! **Deviation 1 (spec revision 11):** CRC-32 via `crc32fast` (already a `prkdb-core`
//! dependency, hardware-accelerated) instead of CRC-32C, to add no new dependency. The
//! frame header carries no algorithm field, so this choice is fixed for format 2.
//!
//! **Accepted risk: `len` is not itself covered by the CRC.** The CRC is computed over
//! `lsn | kind | payload`, where `payload`'s bounds come from `len`. If corruption
//! altered `len` to point at a different (wrong) boundary, and the bytes at that wrong
//! boundary happened to still satisfy the CRC over `lsn | kind | <wrong payload>`, the
//! corruption would go undetected. That coincidence needs a 32-bit CRC to collide, which
//! is a false-negative rate of roughly 2⁻³², the same order of magnitude LevelDB and
//! RocksDB accept for their own per-record CRCs, and far below realistic hardware
//! bit-flip rates. Folding `len` into the CRC input was considered and rejected: it would
//! not close this gap (a corrupted `len` still changes the CRC input consistently with
//! itself) and would only protect against a `len` that is corrupted alone while every
//! other byte, including the CRC, stays intact — a case decode_frame already catches via
//! its own bounds and (for `Batch`) `BadLength(0)`.

/// A frame's log sequence number: frames are numbered 1, 2, 3, ... in append order,
/// contiguously across segments. (Not a byte offset: [`crate::wal::RecordLoc`] holds
/// that.)
pub type Lsn = u64;

/// `len(4) + crc(4) + lsn(8) + kind(1)`.
pub const FRAME_HEADER_LEN: usize = 17;

/// Largest payload accepted on write and trusted on read (checked before allocating).
pub const MAX_PAYLOAD_LEN: usize = 64 * 1024 * 1024;

// Bounded decodes of stored records (`prkdb_types::codec`) use the same limit: no stored
// value can be larger than the frame that carried it.
const _: () = assert!(MAX_PAYLOAD_LEN == prkdb_types::codec::MAX_RECORD_BYTES);

/// What a frame's `kind` byte means. A CRC-valid frame of any other kind is
/// `FrameFault::UnsupportedKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    /// One atomic write batch (see `batch.rs`).
    Batch = 1,
    /// A record removed by compaction: header only, empty payload, keeps LSNs
    /// contiguous (Task 2.15).
    Elided = 2,
}

impl FrameKind {
    fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(FrameKind::Batch),
            2 => Some(FrameKind::Elided),
            _ => None,
        }
    }
}

/// Why `decode_frame` stopped trusting the bytes in front of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameFault {
    /// Fewer bytes left than a header, or than the header's length claims.
    Truncated,
    /// A header of all zeros: preallocated or never-written space. End of log.
    ZeroHeader,
    BadLength(u32),
    /// The CRC does not match `lsn | kind | payload`: a torn or corrupted frame.
    BadCrc,
    /// The CRC matches, but the kind byte is not one this build knows: a whole frame
    /// written by a later build (STO-11). Never a torn tail; the log refuses it.
    UnsupportedKind(u8),
    LsnGap {
        expected: Lsn,
        found: Lsn,
    },
}

/// The result of decoding the frame at the start of a buffer.
#[derive(Debug, PartialEq, Eq)]
pub enum Decoded<'a> {
    Frame {
        lsn: Lsn,
        kind: FrameKind,
        payload: &'a [u8],
        /// Total bytes consumed by this frame (header + payload).
        frame_len: usize,
    },
    Fault(FrameFault),
}

/// Appends one frame to `out`. Panics in debug if `payload.len() > MAX_PAYLOAD_LEN`;
/// callers check with `WalError::RecordTooLarge` first.
pub fn encode_frame(out: &mut Vec<u8>, lsn: Lsn, kind: FrameKind, payload: &[u8]) {
    debug_assert!(payload.len() <= MAX_PAYLOAD_LEN);
    let len = payload.len() as u32;

    let mut crc_input = Vec::with_capacity(8 + 1 + payload.len());
    crc_input.extend_from_slice(&lsn.to_le_bytes());
    crc_input.push(kind as u8);
    crc_input.extend_from_slice(payload);
    let crc = crc32fast::hash(&crc_input);

    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&lsn.to_le_bytes());
    out.push(kind as u8);
    out.extend_from_slice(payload);
}

/// Decodes the frame at the start of `buf`. Never allocates, never panics.
///
/// A header of all zeros is reported as `ZeroHeader` before any other check: it is what
/// preallocated or never-written space looks like, and is not a length or CRC problem.
/// An oversized claimed length is rejected (`BadLength`) before the length is used to
/// slice the buffer, so a bogus `len` never causes an out-of-bounds read.
///
/// Then the CRC, and only then the kind (STO-11): `BadCrc` for any frame whose bytes do
/// not match their CRC, `UnsupportedKind` for a CRC-valid frame of an unknown kind.
pub fn decode_frame(buf: &[u8]) -> Decoded<'_> {
    if buf.len() < FRAME_HEADER_LEN {
        return Decoded::Fault(FrameFault::Truncated);
    }

    let header = &buf[..FRAME_HEADER_LEN];
    if header.iter().all(|&b| b == 0) {
        return Decoded::Fault(FrameFault::ZeroHeader);
    }

    let len = u32::from_le_bytes(header[0..4].try_into().expect("4-byte slice"));
    let crc = u32::from_le_bytes(header[4..8].try_into().expect("4-byte slice"));
    let lsn = u64::from_le_bytes(header[8..16].try_into().expect("8-byte slice"));
    let kind_byte = header[16];

    if len as usize > MAX_PAYLOAD_LEN {
        return Decoded::Fault(FrameFault::BadLength(len));
    }

    let frame_len = FRAME_HEADER_LEN + len as usize;
    if buf.len() < frame_len {
        return Decoded::Fault(FrameFault::Truncated);
    }
    let payload = &buf[FRAME_HEADER_LEN..frame_len];

    // `lsn | kind` are the header's bytes 8..17, contiguous, so the CRC input needs no
    // copy.
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header[8..FRAME_HEADER_LEN]);
    hasher.update(payload);
    if hasher.finalize() != crc {
        return Decoded::Fault(FrameFault::BadCrc);
    }

    let Some(kind) = FrameKind::from_u8(kind_byte) else {
        return Decoded::Fault(FrameFault::UnsupportedKind(kind_byte));
    };

    // A zero-length payload is only legitimate for `Elided` (a compacted-away record,
    // header only). A `Batch` frame always carries at least an encoded op count, so a
    // zero length there is corruption, not a valid empty batch.
    if len == 0 && kind == FrameKind::Batch {
        return Decoded::Fault(FrameFault::BadLength(0));
    }

    Decoded::Frame {
        lsn,
        kind,
        payload,
        frame_len,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_frame() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, 7, FrameKind::Batch, b"hello");
        assert_eq!(
            decode_frame(&buf),
            Decoded::Frame {
                lsn: 7,
                kind: FrameKind::Batch,
                payload: b"hello",
                frame_len: FRAME_HEADER_LEN + 5,
            }
        );
    }

    #[test]
    fn an_all_zero_header_is_zero_header_not_bad_length() {
        let buf = [0u8; FRAME_HEADER_LEN];
        assert_eq!(decode_frame(&buf), Decoded::Fault(FrameFault::ZeroHeader));
    }

    #[test]
    fn short_buffer_is_truncated() {
        assert_eq!(
            decode_frame(&[1, 2, 3]),
            Decoded::Fault(FrameFault::Truncated)
        );
    }

    /// A frame with kind byte `kind` and a CRC computed over it, as a later build that
    /// knows `kind` would write it.
    fn frame_of_kind(lsn: Lsn, kind: u8, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_frame(&mut buf, lsn, FrameKind::Batch, payload);
        buf[16] = kind;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf[8..]);
        let crc = hasher.finalize();
        buf[4..8].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// STO-11: a CRC-valid frame of an unknown kind is `UnsupportedKind`, not a torn
    /// frame, even with an empty payload.
    #[test]
    fn a_valid_frame_of_an_unknown_kind_is_unsupported_kind() {
        for payload in [&b""[..], b"from a later build"] {
            assert_eq!(
                decode_frame(&frame_of_kind(1, 99, payload)),
                Decoded::Fault(FrameFault::UnsupportedKind(99))
            );
        }
    }

    /// STO-11: the CRC is checked first, so an unknown kind byte whose CRC does not
    /// match (a known frame with its kind byte flipped, or a torn one) is `BadCrc`.
    #[test]
    fn an_unknown_kind_with_a_bad_crc_is_bad_crc() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, 1, FrameKind::Batch, b"hello");
        buf[16] = 99;
        assert_eq!(decode_frame(&buf), Decoded::Fault(FrameFault::BadCrc));

        let mut buf = frame_of_kind(1, 99, b"hello");
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        assert_eq!(decode_frame(&buf), Decoded::Fault(FrameFault::BadCrc));
    }

    /// The CRC covers the kind byte: flipping `Batch` to `Elided` is `BadCrc`.
    #[test]
    fn a_known_kind_byte_flipped_to_another_known_kind_is_bad_crc() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, 1, FrameKind::Batch, b"hello");
        buf[16] = FrameKind::Elided as u8;
        assert_eq!(decode_frame(&buf), Decoded::Fault(FrameFault::BadCrc));
    }

    #[test]
    fn a_flipped_payload_bit_is_bad_crc() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, 1, FrameKind::Batch, b"hello");
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        assert_eq!(decode_frame(&buf), Decoded::Fault(FrameFault::BadCrc));
    }

    /// A `Batch` frame always encodes at least an op count, so a zero-length payload is
    /// corruption, not a valid empty batch — even though the CRC (over an empty payload)
    /// is internally consistent.
    #[test]
    fn a_zero_length_batch_payload_is_bad_length() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, 1, FrameKind::Batch, b"");
        assert_eq!(decode_frame(&buf), Decoded::Fault(FrameFault::BadLength(0)));
    }

    /// `Elided` is the one frame kind allowed to carry no payload (a compacted-away
    /// record keeping its LSN slot).
    #[test]
    fn a_zero_length_elided_payload_is_a_valid_frame() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, 1, FrameKind::Elided, b"");
        assert_eq!(
            decode_frame(&buf),
            Decoded::Frame {
                lsn: 1,
                kind: FrameKind::Elided,
                payload: b"",
                frame_len: FRAME_HEADER_LEN,
            }
        );
    }
}
