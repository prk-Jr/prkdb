//! Segment scan and random-read boundaries using real files and bounded fixtures.

use prkdb_core::vfs::{StdVfs, Vfs, VfsFile};
use prkdb_core::wal::frame::{
    encode_frame, FrameFault, FrameKind, FRAME_HEADER_LEN, MAX_PAYLOAD_LEN,
};
use prkdb_core::wal::segment::{
    read_frame, scan_segment, segment_file_name, write_segment_header, RecordLoc,
    SEGMENT_HEADER_LEN,
};
use prkdb_core::wal::WalError;
use std::path::Path;
use std::sync::Arc;

fn seed(path: &Path, payloads: &[Vec<u8>]) -> (Arc<dyn VfsFile>, Vec<RecordLoc>) {
    let file = StdVfs.create(path).unwrap();
    write_segment_header(&*file, 1).unwrap();
    let mut frames = Vec::new();
    let mut locations = Vec::new();
    for (index, payload) in payloads.iter().enumerate() {
        let lsn = index as u64 + 1;
        locations.push(RecordLoc {
            lsn,
            segment: 1,
            offset: SEGMENT_HEADER_LEN + frames.len() as u64,
            payload_len: payload.len() as u32,
        });
        encode_frame(&mut frames, lsn, FrameKind::Batch, payload);
    }
    file.write_at(SEGMENT_HEADER_LEN, &frames).unwrap();
    (file, locations)
}

#[test]
fn a_second_frame_header_crossing_a_scan_chunk_is_not_a_torn_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(segment_file_name(1));
    // The first read leaves eight bytes of the next frame header buffered.
    let payloads = vec![
        vec![0x31; (1 << 20) - FRAME_HEADER_LEN - 8],
        b"next".to_vec(),
    ];
    let (file, locations) = seed(&path, &payloads);
    let mut seen = Vec::new();
    let scan = scan_segment(&*file, &path, 1, &mut |loc, kind, payload| {
        assert_eq!(kind, FrameKind::Batch);
        assert!(
            payload == payloads[loc.lsn as usize - 1].as_slice(),
            "payload at lsn {} differs from the bytes written",
            loc.lsn
        );
        seen.push(loc);
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, locations);
    assert_eq!(scan.stopped, None);
    assert_eq!(scan.next_lsn, 3);
    assert_eq!(scan.valid_len, file.len().unwrap());
}

#[test]
fn a_second_frame_payload_crossing_a_scan_chunk_is_read_completely() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(segment_file_name(1));
    // The second frame starts halfway through the buffered chunk and extends past it.
    let payloads = vec![
        vec![0x42; (1 << 19) - FRAME_HEADER_LEN],
        vec![0x53; (3 << 18) - FRAME_HEADER_LEN],
    ];
    let (file, locations) = seed(&path, &payloads);
    let mut seen = Vec::new();
    let scan = scan_segment(&*file, &path, 1, &mut |loc, _, payload| {
        assert!(
            payload == payloads[loc.lsn as usize - 1].as_slice(),
            "payload at lsn {} differs from the bytes written",
            loc.lsn
        );
        seen.push(loc);
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, locations);
    assert_eq!(scan.stopped, None);
    assert_eq!(scan.next_lsn, 3);
    assert_eq!(scan.valid_len, file.len().unwrap());
}

#[test]
fn scanning_a_legal_maximum_length_header_reports_missing_payload() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(segment_file_name(1));
    let (file, _) = seed(&path, &[b"x".to_vec()]);
    file.write_at(SEGMENT_HEADER_LEN, &(MAX_PAYLOAD_LEN as u32).to_le_bytes())
        .unwrap();
    file.set_len(SEGMENT_HEADER_LEN + FRAME_HEADER_LEN as u64)
        .unwrap();
    let mut visits = 0;
    let scan = scan_segment(&*file, &path, 1, &mut |_, _, _| {
        visits += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(visits, 0);
    assert_eq!(
        scan.stopped,
        Some((SEGMENT_HEADER_LEN, FrameFault::Truncated))
    );
    assert_eq!(scan.valid_len, SEGMENT_HEADER_LEN);
    assert_eq!(scan.next_lsn, 1);
}

#[test]
fn an_oversized_location_is_rejected_before_reading_its_frame() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(segment_file_name(1));
    let (file, locations) = seed(&path, &[b"x".to_vec()]);
    let oversized = MAX_PAYLOAD_LEN + 1;
    let loc = RecordLoc {
        payload_len: oversized as u32,
        ..locations[0]
    };
    let error = read_frame(&*file, &path, loc).unwrap_err();
    assert!(
        matches!(error, WalError::RecordTooLarge { path: ref found, len, max }
        if found == &path && len == oversized && max == MAX_PAYLOAD_LEN),
        "{error}"
    );
}

#[test]
fn a_legal_maximum_location_reaches_frame_validation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(segment_file_name(1));
    let (file, locations) = seed(&path, &[b"x".to_vec()]);
    let loc = RecordLoc {
        payload_len: MAX_PAYLOAD_LEN as u32,
        ..locations[0]
    };
    // A legal location must reach the actual header: its stale length is corruption,
    // rather than RecordTooLarge. The tiny real header avoids a 64 MiB allocation.
    let error = read_frame(&*file, &path, loc).unwrap_err();
    assert!(
        matches!(error, WalError::CorruptSegment { path: ref found, offset, .. }
        if found == &path && offset == loc.offset),
        "{error}"
    );
}

#[test]
fn a_truncated_zero_suffix_cannot_be_fabricated_into_a_crc_valid_frame() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(segment_file_name(1));
    let (file, locations) = seed(&path, &[vec![9, 0, 0, 0]]);
    let loc = locations[0];
    assert_eq!(read_frame(&*file, &path, loc).unwrap(), vec![9, 0, 0, 0]);
    file.set_len(loc.offset + FRAME_HEADER_LEN as u64 + 1)
        .unwrap();
    // Zero-filling a short read would reconstruct the original CRC-valid payload,
    // even though three bytes are absent from disk. Length validation must refuse it.
    let error = read_frame(&*file, &path, loc).unwrap_err();
    assert!(
        matches!(error, WalError::CorruptSegment { path: ref found, offset, .. }
        if found == &path && offset == loc.offset),
        "{error}"
    );
}
