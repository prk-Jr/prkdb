//! Fuzz entry points (TST-07). Each must never panic, whatever the input; the cargo-fuzz
//! targets in `fuzz/` call these, and `tests/fuzz_corpus.rs` runs them over the seed
//! corpus (`fuzz/corpus/<target>/`) on stable in every CI run.
//!
//! Every decoder here reads bytes that come from disk or from the network, so a panic on
//! malformed input is a crash a corrupt file or a hostile peer can trigger.
//!
//! The WAL frame and checkpoint formats are checksummed, and a fuzzer that mutates a
//! seed almost always breaks the checksum, so it would only ever exercise the CRC check.
//! The targets that decode a checksummed format therefore run each input twice: as given,
//! and with its checksums recomputed ([`fix_frame_crcs`], [`fix_checkpoint_crc`]), which
//! is what lets mutations reach the parsers behind the check.

use prkdb_core::vfs::Vfs;
use prkdb_core::wal::batch::Batch;
use prkdb_core::wal::frame::{decode_frame, Decoded, FrameKind, FRAME_HEADER_LEN};
use prkdb_core::wal::records::RecordBatch;
use prkdb_core::wal::segment::{
    scan_segment, segment_file_name, write_segment_header, SEGMENT_HEADER_LEN,
};
use prkdb_core::wal::{CompressionConfig, CompressionType};
use std::path::Path;

/// A fuzz entry point: takes any bytes, must never panic.
pub type EntryPoint = fn(&[u8]);

/// Every entry point, by the name of its cargo-fuzz target and seed-corpus directory.
pub const TARGETS: [(&str, EntryPoint); 11] = [
    ("frame_decode", frame_decode),
    ("batch_decode", batch_decode),
    ("segment_scan", segment_scan),
    ("checkpoint_load", checkpoint_load),
    ("proto_decode", proto_decode),
    ("key_decode", key_decode),
    ("format_parse", format_parse),
    ("snapshot_restore", snapshot_restore),
    ("file_decode", file_decode),
    ("snapshot_entries", snapshot_entries),
    ("records_decode", records_decode),
];

/// Decodes a frame's payload as its kind says, as recovery (`Batch`) and a stream
/// reader (`Records`) do.
fn decode_payload(kind: FrameKind, payload: &[u8]) {
    match kind {
        FrameKind::Batch => {
            let _ = Batch::decode(payload);
        }
        FrameKind::Records => {
            let _ = RecordBatch::decode(payload);
        }
        FrameKind::Elided => {}
    }
}

/// Recomputes the CRC of every frame in `buf` whose claimed length fits, walking frames
/// by their length fields. A frame's CRC covers `lsn | kind | payload`, which are the
/// contiguous bytes from offset 8 of its header to its end.
pub fn fix_frame_crcs(buf: &mut [u8]) {
    let mut pos = 0usize;
    while buf.len() - pos >= FRAME_HEADER_LEN {
        let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().expect("4-byte slice"));
        let Some(end) = (pos + FRAME_HEADER_LEN).checked_add(len as usize) else {
            return;
        };
        if end > buf.len() {
            return;
        }
        let crc = crc32fast::hash(&buf[pos + 8..end]);
        buf[pos + 4..pos + 8].copy_from_slice(&crc.to_le_bytes());
        pos = end;
    }
}

/// Recomputes a checkpoint's trailing CRC (CRC-32 of every byte before it).
pub fn fix_checkpoint_crc(buf: &mut [u8]) {
    if buf.len() < 4 {
        return;
    }
    let (body, crc) = buf.split_at_mut(buf.len() - 4);
    crc.copy_from_slice(&crc32fast::hash(body).to_le_bytes());
}

fn with_fixed(data: &[u8], fix: fn(&mut [u8]), run: impl Fn(&[u8])) {
    run(data);
    let mut fixed = data.to_vec();
    fix(&mut fixed);
    if fixed != data {
        run(&fixed);
    }
}

/// Decodes consecutive frames from the start of `data` until one faults, and each good
/// frame's payload by its kind, as recovery and a stream reader do.
pub fn frame_decode(data: &[u8]) {
    with_fixed(data, fix_frame_crcs, |buf| {
        let mut rest = buf;
        while let Decoded::Frame {
            kind,
            payload,
            frame_len,
            ..
        } = decode_frame(rest)
        {
            decode_payload(kind, payload);
            rest = &rest[frame_len..];
        }
    });
}

/// `Batch::decode`: the versioned op list, including every decompressor.
pub fn batch_decode(data: &[u8]) {
    let _ = Batch::decode(data);
}

/// Scans `data` as a segment twice on an in-memory filesystem: once written after a
/// valid header for segment 1 (the frame path), once as the whole file (the header path).
/// The visitor decodes every `Batch` and `Records` payload, as recovery's replay and a
/// stream reader do.
pub fn segment_scan(data: &[u8]) {
    with_fixed(data, fix_frame_crcs, |body| {
        scan_file(body, true);
    });
    scan_file(data, false);
}

/// Scans `data` as segment 1, after a valid header if `header`, else as the whole file.
fn scan_file(data: &[u8], header: bool) {
    let fs = crate::faultfs::FaultFs::new();
    let dir = Path::new("/f");
    fs.create_dir_all(dir).expect("in-memory mkdir");
    let path = dir.join(segment_file_name(1));
    let file = fs.create(&path).expect("in-memory create");
    let body_at = if header {
        write_segment_header(file.as_ref(), 1).expect("in-memory write");
        SEGMENT_HEADER_LEN
    } else {
        0
    };
    if !data.is_empty() {
        file.write_at(body_at, data).expect("in-memory write");
    }
    let _ = scan_segment(file.as_ref(), &path, 1, &mut |_, kind, payload| {
        decode_payload(kind, payload);
        Ok(())
    });
}

/// `decode_checkpoint` (Task 2.14), and on success the log validation recovery runs next.
pub fn checkpoint_load(data: &[u8]) {
    use prkdb::storage::checkpoint::{decode_checkpoint, validate_against_log};
    with_fixed(data, fix_checkpoint_crc, |buf| {
        if let Ok((covered, entries)) = decode_checkpoint(buf) {
            let locs: Vec<_> = entries.iter().map(|(_, loc)| *loc).collect();
            let last = locs.iter().map(|l| l.lsn).fold(covered, u64::max);
            let segments: Vec<_> = locs.iter().map(|l| l.segment).collect();
            let _ = validate_against_log(covered, &locs, last, &segments);
        }
    });
}

/// The gRPC messages a node decodes from a peer or a client, and the Raft command each
/// replicated log entry carries.
pub fn proto_decode(data: &[u8]) {
    use prkdb::raft::command::Command;
    use prkdb_proto::raft;
    use prost::Message;
    if let Ok(req) = raft::AppendEntriesRequest::decode(data) {
        for entry in &req.entries {
            let _ = Command::deserialize(&entry.data);
        }
    }
    let _ = raft::InstallSnapshotRequest::decode(data);
    let _ = raft::RequestVoteRequest::decode(data);
    let _ = raft::PutRequest::decode(data);
    let _ = raft::BatchPutRequest::decode(data);
    let _ = Command::deserialize(data);
}

/// The key codec (KEY-01): `decode_key`, and `decode_id_hint` on the id bytes it yields
/// and on the raw input (the CLI calls it on bytes read from storage; `memcomparable`'s
/// own decoder panics on truncated input, which Task 2.12 guards against).
pub fn key_decode(data: &[u8]) {
    if let Ok((_, _, id)) = prkdb::keys::decode_key(data) {
        let _ = prkdb::keys::decode_id_hint(id);
    }
    let _ = prkdb::keys::decode_id_hint(data);
}

/// The data directory's `FORMAT` marker parser, through `read_format_with` on an
/// in-memory filesystem (the parser itself is private).
pub fn format_parse(data: &[u8]) {
    use prkdb::storage::format::{read_format_with, FORMAT_FILE};
    let fs = crate::faultfs::FaultFs::new();
    let dir = Path::new("/f");
    fs.create_dir_all(dir).expect("in-memory mkdir");
    let file = fs.create(&dir.join(FORMAT_FILE)).expect("in-memory create");
    if !data.is_empty() {
        file.write_at(0, data).expect("in-memory write");
    }
    let _ = read_format_with(&fs, dir);
}

/// The Raft state machine's snapshot parser (`PrkDbStateMachine::restore`), whose bytes
/// are a peer's `InstallSnapshotRequest.data`.
pub fn snapshot_restore(data: &[u8]) {
    let _ = prkdb::raft::state_machine::parse_snapshot(data);
}

/// The whole-file bincode decodes, whose limit scales with the input: the Raft
/// `snapshot.bin` (native) and the segmented adapter's `index.snapshot` (serde).
pub fn file_decode(data: &[u8]) {
    use prkdb_types::codec::{decode_file, decode_serde_file};
    use std::collections::HashMap;
    let _ = decode_file::<(u64, u64, Vec<u8>)>(data);
    let _ = decode_serde_file::<HashMap<Vec<u8>, Option<Vec<u8>>>>(data);
}

/// A backup snapshot read entry by entry (`SnapshotReader`), plain or gzip as its
/// header says.
pub fn snapshot_entries(data: &[u8]) {
    use prkdb::storage::snapshot::SnapshotReader;
    let Ok(mut reader) = SnapshotReader::from_reader(std::io::Cursor::new(data.to_vec())) else {
        return;
    };
    while let Ok(Some(_)) = reader.next_entry() {}
}

/// `RecordBatch::decode` (Task 2.15b.2): a stream frame's record batch, including every
/// decompressor. A batch that decodes must also agree with `peek_header`, and re-encode
/// to itself: byte for byte when it was stored uncompressed (the encoding is canonical),
/// and to a batch that decodes the same otherwise. A failed check panics, which is the
/// crash libFuzzer reports.
pub fn records_decode(data: &[u8]) {
    let peeked = RecordBatch::peek_header(data);
    let Ok(batch) = RecordBatch::decode(data) else {
        return;
    };
    let count = batch.records.len() as u32;
    assert_eq!(
        peeked.ok(),
        Some((batch.append_time_ms, count)),
        "peek_header disagrees with decode"
    );
    let again = batch
        .encode(&CompressionConfig::none())
        .expect("a decoded batch re-encodes");
    if data[1] == CompressionType::None as u8 {
        assert_eq!(
            again, data,
            "an uncompressed batch re-encodes to other bytes"
        );
    }
    assert_eq!(
        RecordBatch::decode(&again).ok().as_ref(),
        Some(&batch),
        "a re-encoded batch decodes differently"
    );
}
