//! Frame codec for the single ordered WAL (Task 2.5, decision record §7).
//!
//! Layout, little-endian: `len u32 | crc u32 | lsn u64 | kind u8 | payload`. The 17-byte
//! header is fixed; `len` is the payload length. The CRC covers `lsn | kind | payload`,
//! not `len`, so a torn write that truncates the payload is caught by the length check
//! before the CRC is even computed.
//!
//! **Deviation 1 (spec revision 11):** CRC-32 via `crc32fast` (already a `prkdb-core`
//! dependency, hardware-accelerated) instead of CRC-32C, to add no new dependency. The
//! frame header carries no algorithm field, so this choice is fixed for format 2.

/// A frame's position in the log: its byte offset within its segment, once written.
pub type Lsn = u64;

/// `len(4) + crc(4) + lsn(8) + kind(1)`.
pub const FRAME_HEADER_LEN: usize = 17;

/// Largest payload accepted on write and trusted on read (checked before allocating).
pub const MAX_PAYLOAD_LEN: usize = 64 * 1024 * 1024;

/// What a frame's `kind` byte means. Unknown kinds are faults (`FrameFault::UnknownKind`).
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
    BadCrc,
    UnknownKind(u8),
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

    let kind = match FrameKind::from_u8(kind_byte) {
        Some(k) => k,
        None => return Decoded::Fault(FrameFault::UnknownKind(kind_byte)),
    };

    let frame_len = FRAME_HEADER_LEN + len as usize;
    if buf.len() < frame_len {
        return Decoded::Fault(FrameFault::Truncated);
    }
    let payload = &buf[FRAME_HEADER_LEN..frame_len];

    let mut crc_input = Vec::with_capacity(8 + 1 + payload.len());
    crc_input.extend_from_slice(&lsn.to_le_bytes());
    crc_input.push(kind_byte);
    crc_input.extend_from_slice(payload);
    if crc32fast::hash(&crc_input) != crc {
        return Decoded::Fault(FrameFault::BadCrc);
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

    #[test]
    fn unknown_kind_is_a_fault() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, 1, FrameKind::Batch, b"");
        buf[16] = 99;
        assert_eq!(
            decode_frame(&buf),
            Decoded::Fault(FrameFault::UnknownKind(99))
        );
    }

    #[test]
    fn a_flipped_payload_bit_is_bad_crc() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, 1, FrameKind::Batch, b"hello");
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        assert_eq!(decode_frame(&buf), Decoded::Fault(FrameFault::BadCrc));
    }
}
