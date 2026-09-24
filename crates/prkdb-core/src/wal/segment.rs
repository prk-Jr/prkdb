//! Segment header, file naming and verified scan for the single ordered WAL (Task 2.5).
//!
//! A segment file is named `{first_lsn:020}.wal` (so lexicographic order == LSN order)
//! and starts with a 24-byte header: `b"PRKDBWAL" | format u32 | reserved u32 (0) |
//! first_lsn u64`. `format` is `prkdb_core::format::FORMAT_VERSION`, the single version
//! number the program understands, so a data directory has exactly one format version.

use crate::format::FORMAT_VERSION;
use crate::vfs::VfsFile;
use crate::wal::frame::{
    decode_frame, Decoded, FrameFault, FrameKind, Lsn, FRAME_HEADER_LEN, MAX_PAYLOAD_LEN,
};
use crate::wal::WalError;
use std::io;
use std::path::Path;

pub const SEGMENT_MAGIC: [u8; 8] = *b"PRKDBWAL";
pub const SEGMENT_HEADER_LEN: u64 = 24;

/// Bytes read per chunk while scanning a segment, so a torn or hostile file never forces
/// the whole segment into memory before its first fault is found.
const SCAN_CHUNK: usize = 1024 * 1024;

/// `"{first_lsn:020}.wal"`.
pub fn segment_file_name(first_lsn: Lsn) -> String {
    format!("{first_lsn:020}.wal")
}

/// Inverse of [`segment_file_name`]; `None` for anything else, including a name with a
/// trailing extra suffix such as `.wal.tmp`.
pub fn parse_segment_file_name(name: &str) -> Option<Lsn> {
    let stem = name.strip_suffix(".wal")?;
    if stem.len() != 20 || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

/// Writes the header at offset 0. Does not sync.
pub fn write_segment_header(file: &dyn VfsFile, first_lsn: Lsn) -> io::Result<()> {
    let mut buf = [0u8; SEGMENT_HEADER_LEN as usize];
    buf[0..8].copy_from_slice(&SEGMENT_MAGIC);
    buf[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    // buf[12..16] stays zero: reserved.
    buf[16..24].copy_from_slice(&first_lsn.to_le_bytes());
    file.write_at(0, &buf)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentScan {
    pub first_lsn: Lsn,
    /// LSN the next frame appended to this segment must carry.
    pub next_lsn: Lsn,
    /// Offset just past the last good frame (>= SEGMENT_HEADER_LEN).
    pub valid_len: u64,
    pub file_len: u64,
    /// Where and why the scan stopped before `file_len`; `None` if every byte was a good
    /// frame.
    pub stopped: Option<(u64, FrameFault)>,
}

/// Callback `scan_segment` invokes for each good frame it finds.
pub type ScanVisitor<'a> = dyn FnMut(RecordLoc, FrameKind, &[u8]) -> Result<(), WalError> + 'a;

/// Where a frame lives, as handed to the visitor and stored in indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordLoc {
    pub lsn: Lsn,
    /// First LSN of the segment holding the frame (its file-name key).
    pub segment: Lsn,
    /// Byte offset of the frame header within the segment file.
    pub offset: u64,
    pub payload_len: u32,
}

fn read_header(file: &dyn VfsFile, path: &Path, first_lsn: Lsn) -> Result<(), WalError> {
    let file_len = file.len()?;
    if file_len < SEGMENT_HEADER_LEN {
        return Err(WalError::CorruptSegment {
            path: path.to_path_buf(),
            offset: 0,
            reason: format!(
                "file is {file_len} bytes, shorter than the {SEGMENT_HEADER_LEN}-byte segment header"
            ),
        });
    }

    let mut header = [0u8; SEGMENT_HEADER_LEN as usize];
    file.read_at(0, &mut header)?;

    if header[0..8] != SEGMENT_MAGIC {
        return Err(WalError::CorruptSegment {
            path: path.to_path_buf(),
            offset: 0,
            reason: "bad segment magic".to_string(),
        });
    }

    let format = u32::from_le_bytes(header[8..12].try_into().expect("4-byte slice"));
    if format != FORMAT_VERSION {
        return Err(WalError::UnsupportedFormat {
            path: path.to_path_buf(),
            found: format,
            supported: FORMAT_VERSION,
        });
    }

    let stored_first_lsn = u64::from_le_bytes(header[16..24].try_into().expect("8-byte slice"));
    if stored_first_lsn != first_lsn {
        return Err(WalError::CorruptSegment {
            path: path.to_path_buf(),
            offset: 16,
            reason: format!(
                "header first_lsn {stored_first_lsn} does not match the file name's {first_lsn}"
            ),
        });
    }

    Ok(())
}

/// Reads another chunk (up to `SCAN_CHUNK` bytes) onto the end of `buf`, if the file has
/// more bytes past what `buf` already holds. Returns `false` at EOF.
fn read_more(
    file: &dyn VfsFile,
    buf: &mut Vec<u8>,
    buf_pos: u64,
    file_len: u64,
) -> Result<bool, WalError> {
    let have = buf_pos + buf.len() as u64;
    if have >= file_len {
        return Ok(false);
    }
    let to_read = std::cmp::min(SCAN_CHUNK as u64, file_len - have) as usize;
    let start = buf.len();
    buf.resize(start + to_read, 0);
    file.read_at(have, &mut buf[start..])?;
    Ok(true)
}

/// Verifies the header (magic, format == `FORMAT_VERSION`, `first_lsn` == `first_lsn`),
/// then every frame in order, reading in `SCAN_CHUNK`-sized chunks (a frame spanning
/// chunks is read whole after checking its length against `MAX_PAYLOAD_LEN` and the file
/// length). Calls `visit` for each good frame. Header problems are errors:
/// `UnsupportedFormat` for a different format number, `CorruptSegment` for bad magic or
/// `first_lsn`. Frame problems are not errors: they end the scan and are reported in
/// `SegmentScan::stopped`; the caller decides between "torn tail, truncate" (last
/// segment) and "corruption, refuse" (earlier segment).
pub fn scan_segment(
    file: &dyn VfsFile,
    path: &Path,
    first_lsn: Lsn,
    visit: &mut ScanVisitor<'_>,
) -> Result<SegmentScan, WalError> {
    read_header(file, path, first_lsn)?;
    let file_len = file.len()?;

    let mut buf: Vec<u8> = Vec::new();
    let mut buf_pos = SEGMENT_HEADER_LEN;
    let mut cursor = SEGMENT_HEADER_LEN;
    let mut expected_lsn = first_lsn;
    let mut stopped: Option<(u64, FrameFault)> = None;

    loop {
        let local = (cursor - buf_pos) as usize;

        // Make sure at least a header's worth of bytes is available, or we've hit EOF.
        while buf.len() - local < FRAME_HEADER_LEN && read_more(file, &mut buf, buf_pos, file_len)?
        {
        }

        let avail = buf.len() - local;
        if avail == 0 {
            // Every byte up to file_len was consumed as a good frame.
            break;
        }
        if avail < FRAME_HEADER_LEN {
            stopped = Some((cursor, FrameFault::Truncated));
            break;
        }

        // Peek the claimed length so we can pull in the rest of a large frame before
        // decoding, without trusting an absurd length enough to allocate for it.
        let len = u32::from_le_bytes(buf[local..local + 4].try_into().expect("4-byte slice"));
        if len as usize > MAX_PAYLOAD_LEN {
            stopped = Some((cursor, FrameFault::BadLength(len)));
            break;
        }
        let frame_len = FRAME_HEADER_LEN + len as usize;

        while buf.len() - local < frame_len && read_more(file, &mut buf, buf_pos, file_len)? {}

        match decode_frame(&buf[local..]) {
            Decoded::Frame {
                lsn,
                kind,
                payload,
                frame_len,
            } => {
                if lsn != expected_lsn {
                    stopped = Some((
                        cursor,
                        FrameFault::LsnGap {
                            expected: expected_lsn,
                            found: lsn,
                        },
                    ));
                    break;
                }
                let loc = RecordLoc {
                    lsn,
                    segment: first_lsn,
                    offset: cursor,
                    payload_len: payload.len() as u32,
                };
                visit(loc, kind, payload)?;
                expected_lsn += 1;
                cursor += frame_len as u64;

                // Bound memory use: drop consumed bytes once the window grows past a
                // chunk, rather than keeping the whole segment buffered.
                let new_local = (cursor - buf_pos) as usize;
                if new_local >= SCAN_CHUNK {
                    buf.drain(0..new_local);
                    buf_pos = cursor;
                }
            }
            Decoded::Fault(fault) => {
                stopped = Some((cursor, fault));
                break;
            }
        }
    }

    Ok(SegmentScan {
        first_lsn,
        next_lsn: expected_lsn,
        valid_len: cursor,
        file_len,
        stopped,
    })
}

/// Reads one frame at `loc`, verifying CRC, kind and that its LSN is `loc.lsn`.
pub fn read_frame(file: &dyn VfsFile, path: &Path, loc: RecordLoc) -> Result<Vec<u8>, WalError> {
    let mut header = [0u8; FRAME_HEADER_LEN];
    file.read_at(loc.offset, &mut header)?;
    let len = u32::from_le_bytes(header[0..4].try_into().expect("4-byte slice"));
    if len as usize > MAX_PAYLOAD_LEN {
        return Err(WalError::RecordTooLarge {
            path: path.to_path_buf(),
            len: len as usize,
            max: MAX_PAYLOAD_LEN,
        });
    }

    let total = FRAME_HEADER_LEN + len as usize;
    let mut buf = vec![0u8; total];
    file.read_at(loc.offset, &mut buf)?;

    match decode_frame(&buf) {
        Decoded::Frame {
            lsn: found_lsn,
            payload,
            ..
        } => {
            if found_lsn != loc.lsn {
                return Err(WalError::CorruptSegment {
                    path: path.to_path_buf(),
                    offset: loc.offset,
                    reason: format!("expected lsn {} at this offset, found {found_lsn}", loc.lsn),
                });
            }
            Ok(payload.to_vec())
        }
        Decoded::Fault(fault) => Err(WalError::CorruptSegment {
            path: path.to_path_buf(),
            offset: loc.offset,
            reason: format!("{fault:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_file_name_and_parse_round_trip() {
        assert_eq!(segment_file_name(42), "00000000000000000042.wal");
        assert_eq!(
            parse_segment_file_name("00000000000000000042.wal"),
            Some(42)
        );
        assert_eq!(
            parse_segment_file_name("00000000000000000042.wal.tmp"),
            None
        );
        assert_eq!(parse_segment_file_name("checkpoint.json"), None);
        assert_eq!(
            parse_segment_file_name("42.wal"),
            None,
            "must be zero-padded to 20 digits"
        );
    }
}
