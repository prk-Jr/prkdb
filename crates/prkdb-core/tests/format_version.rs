//! On-disk segment format versioning.
//!
//! Before this, WAL segments carried CRC32 per record but no magic number and no version
//! field. There was no way to detect a version mismatch, refuse a future format, or
//! migrate an old one — and every day of data written without one is data that cannot be
//! safely evolved. Cheapest thing to add, most expensive to retrofit.

use prkdb_core::wal::log_segment::{
    LogSegment, FORMAT_VERSION, PRKDB_WAL_MAGIC, SEGMENT_HEADER_LEN,
};
use prkdb_core::wal::{LogOperation, LogRecord};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

fn put(id: &str) -> LogRecord {
    LogRecord::new(LogOperation::Put {
        collection: "users".to_string(),
        id: id.as_bytes().to_vec(),
        data: format!("value-{id}").into_bytes(),
    })
}

fn log_path(dir: &Path, base: u64) -> std::path::PathBuf {
    dir.join(format!("{base:020}.log"))
}

fn index_path(dir: &Path, base: u64) -> std::path::PathBuf {
    dir.join(format!("{base:020}.index"))
}

#[test]
fn a_new_segment_starts_with_the_magic_and_version() {
    let dir = tempfile::tempdir().unwrap();
    let seg = LogSegment::create(dir.path(), 0, 4096).expect("create");
    seg.append(put("a")).expect("append");
    drop(seg);

    let bytes = std::fs::read(log_path(dir.path(), 0)).expect("read segment");
    assert!(
        bytes.len() >= SEGMENT_HEADER_LEN as usize,
        "a segment must be at least header-sized"
    );
    assert_eq!(&bytes[..8], &PRKDB_WAL_MAGIC, "magic must lead the file");
    assert_eq!(
        u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
        FORMAT_VERSION,
        "the version field must record the format actually written"
    );
    assert_eq!(
        &bytes[12..16],
        &[0, 0, 0, 0],
        "the reserved word must stay zero so flags can be added without a version bump"
    );
}

#[test]
fn records_round_trip_through_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let seg = LogSegment::create(dir.path(), 0, 4096).expect("create");
        for i in 0..5 {
            seg.append(put(&i.to_string())).expect("append");
        }
    }

    let seg = LogSegment::open(dir.path(), 0, 4096).expect("reopen a versioned segment");
    for i in 0..5u64 {
        let rec = seg
            .read(i)
            .unwrap_or_else(|e| panic!("reading offset {i} after reopen: {e}"));
        assert_eq!(rec.offset, i);
    }
}

/// The whole point of writing the version down: a build that meets a newer format must
/// refuse it rather than misparse it into plausible-looking records.
#[test]
fn a_future_format_version_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    {
        let seg = LogSegment::create(dir.path(), 0, 4096).expect("create");
        seg.append(put("a")).expect("append");
    }

    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .open(log_path(dir.path(), 0))
        .unwrap();
    f.seek(SeekFrom::Start(8)).unwrap();
    f.write_all(&(FORMAT_VERSION + 1).to_le_bytes()).unwrap();
    drop(f);

    let msg = match LogSegment::open(dir.path(), 0, 4096) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("a newer format must be refused, not read as if it were ours"),
    };
    assert!(
        msg.contains("newer than this build"),
        "the error must name the problem, got: {msg}"
    );
}

/// Segments written before headers existed must keep working. Refusing them would turn
/// this change into a data-loss event for anyone with an existing data directory.
#[test]
fn a_legacy_segment_without_a_header_still_opens() {
    let dir = tempfile::tempdir().unwrap();
    {
        let seg = LogSegment::create(dir.path(), 0, 4096).expect("create");
        for i in 0..3 {
            seg.append(put(&i.to_string())).expect("append");
        }
    }

    // Strip the header to reproduce the pre-header layout. The index is emptied rather
    // than deleted: its recorded positions were header-relative and so are wrong for the
    // stripped file, but OffsetIndex::open still requires the file to exist. An empty
    // index means every lookup misses and recovery scans from the start, which is the
    // path a real legacy segment takes.
    let path = log_path(dir.path(), 0);
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, &bytes[SEGMENT_HEADER_LEN as usize..]).unwrap();
    std::fs::write(index_path(dir.path(), 0), b"").unwrap();

    let seg = LogSegment::open(dir.path(), 0, 4096)
        .expect("a headerless segment must be treated as legacy, not rejected");
    let rec = seg.read(0).expect("legacy records must still be readable");
    assert_eq!(rec.offset, 0);
}

/// An empty file is not a format disagreement — nothing has been written yet.
#[test]
fn an_empty_segment_file_is_not_an_error() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(log_path(dir.path(), 0), b"").unwrap();
    std::fs::write(index_path(dir.path(), 0), b"").unwrap();

    LogSegment::open(dir.path(), 0, 4096).expect("an empty segment must open cleanly");
}

/// A file that is not ours at all must be refused rather than scanned as records.
#[test]
fn a_foreign_file_does_not_masquerade_as_a_segment() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(log_path(dir.path(), 0), b"NOTPRKDB\x00\x00\x00\x00garbage").unwrap();
    std::fs::write(index_path(dir.path(), 0), b"").unwrap();

    // No magic means "legacy", so the scan starts at 0 and must fail on the first record
    // header rather than inventing data.
    match LogSegment::open(dir.path(), 0, 4096) {
        Err(_) => {}
        Ok(seg) => {
            assert!(
                seg.read(0).is_err(),
                "garbage must not read back as a valid record"
            );
        }
    }
}
