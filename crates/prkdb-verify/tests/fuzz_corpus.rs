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

/// STREAM mutations must start from actual current-version encodings, not only errors.
#[test]
fn the_stream_manifest_corpus_contains_decodable_manifest_shapes() {
    let root =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/stream_manifest_parse");
    let mut partitions = Vec::new();
    for entry in std::fs::read_dir(root).unwrap() {
        let bytes = std::fs::read(entry.unwrap().path()).unwrap();
        let manifest = prkdb::stream_log::manifest::StreamManifest::decode(&bytes).unwrap();
        assert_eq!(manifest.encode().unwrap(), bytes);
        partitions.push(manifest.partitions);
    }
    partitions.sort_unstable();
    assert_eq!(partitions, [1, 64]);
}

/// Every production parser registered for stable replay also receives nightly mutations.
#[test]
fn nightly_fuzz_loop_covers_every_registered_target() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let workflow = std::fs::read_to_string(root.join(".github/workflows/ci.yml")).unwrap();
    let line = workflow
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("for t in "))
        .unwrap();
    let words = line
        .strip_prefix("for t in ")
        .unwrap()
        .strip_suffix("; do")
        .unwrap();
    let actual: std::collections::BTreeSet<_> = words.split_whitespace().collect();
    let expected: std::collections::BTreeSet<_> = TARGETS.iter().map(|(name, _)| *name).collect();
    assert_eq!(
        actual, expected,
        "nightly mutation coverage differs from stable parser coverage"
    );
    assert_eq!(
        words.split_whitespace().count(),
        expected.len(),
        "a target runs more than once"
    );
    assert!(
        expected.contains("consumer_position"),
        "new offset decoder has no mutation coverage"
    );
}

#[tokio::test]
async fn consumer_position_fuzz_decoder_exercises_actual_legacy_and_versioned_storage() {
    use prkdb_types::consumer::{Offset, Position};
    use prkdb_verify::fuzz_entry::decode_consumer_position;
    for value in [0, 1, 250, 251, u64::MAX] {
        let legacy =
            bincode::serde::encode_to_vec(Offset(value), bincode::config::standard()).unwrap();
        assert_eq!(
            decode_consumer_position(&legacy).await.unwrap(),
            Position {
                offset: Offset(value),
                check: None
            }
        );
        for check in [None, Some(0), Some(u32::MAX)] {
            let mut bytes = vec![1];
            bytes.extend(value.to_le_bytes());
            bytes.push(u8::from(check.is_some()));
            bytes.extend(check.unwrap_or(0).to_le_bytes());
            assert_eq!(
                decode_consumer_position(&bytes).await.unwrap(),
                Position {
                    offset: Offset(value),
                    check
                }
            );
        }
    }
}

#[tokio::test]
async fn consumer_position_fuzz_decoder_refuses_malformed_positions() {
    use prkdb_verify::fuzz_entry::decode_consumer_position;
    let mut version = vec![0; 14];
    version[0] = 2;
    let mut kind = vec![0; 14];
    kind[0] = 1;
    kind[9] = 2;
    for bytes in [vec![], vec![1, 0], version, kind, vec![1; 13], vec![1; 15]] {
        assert!(
            decode_consumer_position(&bytes).await.is_err(),
            "malformed position accepted: {bytes:?}"
        );
    }
}
