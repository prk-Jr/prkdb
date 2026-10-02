//! TST-07: the fuzz entry points run on stable in every CI run, over the committed seed
//! corpus (`fuzz/corpus/<target>/`, written by `cargo run -p prkdb-verify --bin
//! fuzz_seeds`). The nightly `fuzz` job in ci.yml mutates the same seeds under libFuzzer.

use prkdb_verify::fuzz_entry::TARGETS;
use std::path::Path;

/// TST-07: every fuzz entry point accepts its whole seed corpus without panicking.
#[test]
fn fuzz_entries_accept_their_seed_corpus() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus");
    for (name, run) in TARGETS {
        let dir = root.join(name);
        let mut files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
            .map(|entry| entry.expect("corpus entry").path())
            .filter(|path| path.is_file())
            .collect();
        files.sort();
        assert!(!files.is_empty(), "{name} has no seed corpus");
        for path in files {
            let bytes = std::fs::read(&path).expect("corpus file");
            run(&bytes);
        }
    }
}

/// Every seed corpus directory belongs to an entry point, so a target cannot be renamed
/// or dropped while its seeds silently stop running.
#[test]
fn every_corpus_directory_has_an_entry_point() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus");
    for entry in std::fs::read_dir(&root).unwrap_or_else(|e| panic!("{}: {e}", root.display())) {
        let path = entry.expect("corpus dir").path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        assert!(
            TARGETS.iter().any(|(target, _)| *target == name),
            "fuzz/corpus/{name} has no entry in fuzz_entry::TARGETS"
        );
    }
}

/// Task 2.15b.2: the frame and segment corpora hold `Records` frames (kind 3) that decode,
/// so mutations of them reach the record batch decoder through both entry points.
#[test]
fn the_frame_corpora_hold_records_frames() {
    use prkdb_core::wal::frame::{decode_frame, Decoded, FrameKind};
    use prkdb_core::wal::records::RecordBatch;
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus");
    for name in ["frame_decode", "segment_scan"] {
        let dir = root.join(name);
        let mut records_frames = 0;
        for entry in std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
            let bytes = std::fs::read(entry.expect("corpus entry").path()).expect("corpus file");
            let mut rest = &bytes[..];
            while let Decoded::Frame {
                kind,
                payload,
                frame_len,
                ..
            } = decode_frame(rest)
            {
                if kind == FrameKind::Records && RecordBatch::decode(payload).is_ok() {
                    records_frames += 1;
                }
                rest = &rest[frame_len..];
            }
        }
        assert!(
            records_frames >= 2,
            "{name} holds {records_frames} decodable Records frames"
        );
    }
}
