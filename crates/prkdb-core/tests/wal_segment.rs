//! Frame/segment/batch codecs (Task 2.5). StdVfs only; FaultFs tests live in prkdb-verify.

use prkdb_core::format::FORMAT_VERSION;
use prkdb_core::vfs::{OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::frame::{
    decode_frame, encode_frame, Decoded, FrameFault, FrameKind, FRAME_HEADER_LEN, MAX_PAYLOAD_LEN,
};
use prkdb_core::wal::segment::{
    parse_segment_file_name, read_frame, scan_segment, segment_file_name, write_segment_header,
    RecordLoc, SegmentScan, SEGMENT_HEADER_LEN,
};
use prkdb_core::wal::{compress, CompressionConfig, CompressionType, WalError};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn write_segment(dir: &Path, first: u64, payloads: &[Vec<u8>]) -> (PathBuf, Vec<u64>) {
    let path = dir.join(segment_file_name(first));
    let f = StdVfs.create(&path).unwrap();
    write_segment_header(f.as_ref(), first).unwrap();
    let (mut buf, mut starts) = (Vec::new(), Vec::new());
    for (i, p) in payloads.iter().enumerate() {
        starts.push(SEGMENT_HEADER_LEN + buf.len() as u64);
        encode_frame(&mut buf, first + i as u64, FrameKind::Batch, p);
    }
    f.write_at(SEGMENT_HEADER_LEN, &buf).unwrap();
    f.sync_data().unwrap();
    (path, starts)
}

#[allow(clippy::type_complexity)]
fn scan(path: &Path, first: u64) -> Result<(Vec<(u64, Vec<u8>)>, SegmentScan), WalError> {
    let f: Arc<dyn VfsFile> = StdVfs.open(path, OpenMode::Read).unwrap();
    let mut seen = Vec::new();
    let s = scan_segment(f.as_ref(), path, first, &mut |loc, kind, payload| {
        assert_eq!(kind, FrameKind::Batch);
        seen.push((loc.lsn, payload.to_vec()));
        Ok(())
    })?;
    Ok((seen, s))
}

fn payloads(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("payload-{i}").repeat(i + 1).into_bytes())
        .collect()
}

fn flip_byte(path: &Path, at: u64) {
    let f = StdVfs.open(path, OpenMode::ReadWrite).unwrap();
    let mut b = [0u8; 1];
    f.read_at(at, &mut b).unwrap();
    f.write_at(at, &[b[0] ^ 0xFF]).unwrap();
}

#[test]
fn frames_round_trip_in_lsn_order() {
    let dir = tempfile::tempdir().unwrap();
    let p = payloads(5);
    let (path, _) = write_segment(dir.path(), 41, &p);
    let (seen, s) = scan(&path, 41).unwrap();
    assert_eq!(seen, (41..46).zip(p).collect::<Vec<_>>());
    assert_eq!((s.first_lsn, s.next_lsn, s.stopped), (41, 46, None));
    assert_eq!(s.valid_len, s.file_len);
}

#[test]
fn a_flipped_payload_byte_stops_the_scan_at_that_frame() {
    let dir = tempfile::tempdir().unwrap();
    let (path, starts) = write_segment(dir.path(), 1, &payloads(3));
    flip_byte(&path, starts[1] + FRAME_HEADER_LEN as u64 + 2);
    let (seen, s) = scan(&path, 1).unwrap();
    assert_eq!(
        seen.len(),
        1,
        "only the frame before the corruption is good"
    );
    assert_eq!(s.stopped, Some((starts[1], FrameFault::BadCrc)));
    assert_eq!((s.valid_len, s.next_lsn), (starts[1], 2));
}

#[test]
fn a_short_tail_is_truncated_not_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let (path, starts) = write_segment(dir.path(), 1, &payloads(3));
    let f = StdVfs.open(&path, OpenMode::ReadWrite).unwrap();
    f.set_len(starts[2] + 5).unwrap();
    let (seen, s) = scan(&path, 1).unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(s.stopped, Some((starts[2], FrameFault::Truncated)));
}

#[test]
fn zeroed_space_after_the_last_frame_is_end_of_log() {
    let dir = tempfile::tempdir().unwrap();
    let (path, _) = write_segment(dir.path(), 1, &payloads(2));
    let f = StdVfs.open(&path, OpenMode::ReadWrite).unwrap();
    let len = f.len().unwrap();
    f.set_len(len + 4096).unwrap(); // what preallocation looks like
    let (seen, s) = scan(&path, 1).unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(s.stopped, Some((len, FrameFault::ZeroHeader)));
    assert_eq!(s.valid_len, len);
}

#[test]
fn an_lsn_gap_is_a_fault() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(segment_file_name(1));
    let f = StdVfs.create(&path).unwrap();
    write_segment_header(f.as_ref(), 1).unwrap();
    let mut buf = Vec::new();
    encode_frame(&mut buf, 1, FrameKind::Batch, b"a");
    let second = SEGMENT_HEADER_LEN + buf.len() as u64;
    encode_frame(&mut buf, 3, FrameKind::Batch, b"b");
    f.write_at(SEGMENT_HEADER_LEN, &buf).unwrap();
    let (_, s) = scan(&path, 1).unwrap();
    assert_eq!(
        s.stopped,
        Some((
            second,
            FrameFault::LsnGap {
                expected: 2,
                found: 3
            }
        ))
    );
}

#[test]
fn a_huge_length_is_rejected_without_allocating() {
    let mut buf = Vec::new();
    encode_frame(&mut buf, 7, FrameKind::Batch, b"x");
    buf[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(
        decode_frame(&buf),
        Decoded::Fault(FrameFault::BadLength(u32::MAX))
    );
}

#[test]
fn a_newer_format_is_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let (path, _) = write_segment(dir.path(), 1, &payloads(1));
    let f = StdVfs.open(&path, OpenMode::ReadWrite).unwrap();
    f.write_at(8, &(FORMAT_VERSION + 1).to_le_bytes()).unwrap();
    let err = scan(&path, 1).unwrap_err();
    assert!(
        matches!(err, WalError::UnsupportedFormat { found, supported, .. } if found == FORMAT_VERSION + 1 && supported == FORMAT_VERSION),
        "{err}"
    );
}

#[test]
fn read_frame_checks_the_lsn_it_was_asked_for() {
    let dir = tempfile::tempdir().unwrap();
    let (path, starts) = write_segment(dir.path(), 10, &payloads(2));
    let f = StdVfs.open(&path, OpenMode::Read).unwrap();
    let loc = RecordLoc {
        lsn: 11,
        segment: 10,
        offset: starts[1],
        payload_len: payloads(2)[1].len() as u32,
    };
    assert_eq!(read_frame(f.as_ref(), &path, loc).unwrap(), payloads(2)[1]);
    let wrong = RecordLoc { lsn: 12, ..loc };
    assert!(read_frame(f.as_ref(), &path, wrong).is_err());
}

#[test]
fn segment_names_sort_by_first_lsn() {
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
}

#[test]
fn batches_round_trip_with_and_without_compression() {
    let batch = Batch {
        ops: vec![
            BatchOp::Put {
                key: b"k1".to_vec(),
                value: vec![7; 4096],
            },
            BatchOp::Delete {
                key: b"k0".to_vec(),
            },
        ],
    };
    for cfg in [CompressionConfig::none(), CompressionConfig::default()] {
        assert_eq!(Batch::decode(&batch.encode(&cfg).unwrap()).unwrap(), batch);
    }
}

#[test]
fn a_batch_with_trailing_bytes_or_unknown_tag_is_corrupt() {
    let mut bytes = Batch {
        ops: vec![BatchOp::Delete { key: b"k".to_vec() }],
    }
    .encode(&CompressionConfig::none())
    .unwrap();
    bytes.push(0);
    assert!(Batch::decode(&bytes).is_err());
    let mut bad_tag = Batch {
        ops: vec![BatchOp::Delete { key: b"k".to_vec() }],
    }
    .encode(&CompressionConfig::none())
    .unwrap();
    let tag_at = 1 + 1 + 4 + 4; // version, codec, raw_len, count
    bad_tag[tag_at] = 99;
    assert!(Batch::decode(&bad_tag).is_err());
}

/// A crafted batch payload: a tiny compressed body (highly compressible zeros) whose
/// real decompressed size is far larger than the `raw_len` it claims. `Batch::decode`
/// must reject this without ever materializing the real, much larger output.
#[test]
fn a_batch_claiming_a_small_raw_len_but_decompressing_larger_is_rejected() {
    let huge = vec![0u8; 8 * 1024 * 1024]; // 8 MiB of highly compressible data
    let cfg = CompressionConfig {
        compression_type: CompressionType::Lz4,
        min_compress_bytes: 0,
        compression_level: 1,
    };
    let compressed = compress(&huge, &cfg).unwrap();
    assert!(
        compressed.len() < huge.len() / 10,
        "the point of the test is a compressed body much smaller than its real \
         decompressed size, got {} bytes for {} real bytes",
        compressed.len(),
        huge.len()
    );

    let mut bytes = Vec::new();
    bytes.push(1u8); // batch version
    bytes.push(CompressionType::Lz4 as u8);
    bytes.extend_from_slice(&100u32.to_le_bytes()); // claims only 100 raw bytes
    bytes.extend_from_slice(&compressed);

    assert!(
        Batch::decode(&bytes).is_err(),
        "a body that decompresses far past its claimed raw_len must be rejected"
    );
}

/// `raw_len` above `MAX_PAYLOAD_LEN` is rejected before any decompression is attempted.
#[test]
fn a_raw_len_above_the_payload_cap_is_rejected_before_decompressing() {
    let mut bytes = Vec::new();
    bytes.push(1u8); // batch version
    bytes.push(CompressionType::None as u8);
    bytes.extend_from_slice(&(MAX_PAYLOAD_LEN as u32 + 1).to_le_bytes());
    // No body bytes: if this were decompressed instead of rejected up front, decoding
    // would fail for a different reason (missing data) rather than the length check.
    let err = Batch::decode(&bytes).unwrap_err();
    assert!(format!("{err}").contains("exceeds"), "{err}");
}

/// A payload larger than the segment scanner's chunk size must still be read back whole
/// and unmangled, exercising the multi-chunk read path in `scan_segment`.
#[test]
fn a_frame_spanning_the_scan_chunk_boundary_is_read_intact() {
    let dir = tempfile::tempdir().unwrap();
    let big: Vec<u8> = (0..1_500_000u32).map(|i| (i % 251) as u8).collect(); // > 1 MiB
    let (path, _) = write_segment(dir.path(), 1, std::slice::from_ref(&big));
    let (seen, s) = scan(&path, 1).unwrap();
    assert_eq!(seen, vec![(1, big)]);
    assert_eq!(s.stopped, None);
    assert_eq!(s.valid_len, s.file_len);
}

/// The same oversized frame, torn well past the scan chunk boundary: the scan must stop
/// with `Truncated` at the frame's start, not panic or silently return a partial payload.
#[test]
fn a_torn_frame_spanning_the_scan_chunk_boundary_is_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let big = vec![0xCDu8; 1_500_000]; // > 1 MiB
    let (path, starts) = write_segment(dir.path(), 1, &[big]);
    let f = StdVfs.open(&path, OpenMode::ReadWrite).unwrap();
    // Truncate partway through the payload, past the 1 MiB scan chunk boundary.
    f.set_len(starts[0] + FRAME_HEADER_LEN as u64 + 1_200_000)
        .unwrap();
    let (seen, s) = scan(&path, 1).unwrap();
    assert_eq!(seen.len(), 0);
    assert_eq!(s.stopped, Some((starts[0], FrameFault::Truncated)));
}
