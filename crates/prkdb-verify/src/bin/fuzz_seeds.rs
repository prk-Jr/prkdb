//! `fuzz_seeds` — writes the seed corpus of every fuzz target (TST-07) to
//! `fuzz/corpus/<target>/seed-N`: a handful of valid inputs per target, for libFuzzer to
//! mutate and for `tests/fuzz_corpus.rs` to run on stable.
//!
//! A binary, not a test: a test that wrote into the source tree would modify the
//! repository on every CI run. Run it after a format change and commit the result:
//!
//! ```text
//! cargo run -p prkdb-verify --bin fuzz_seeds [-- <corpus dir>]
//! ```
//!
//! The output is deterministic (fixed keys, values and LSNs, no timestamps), so
//! regenerating without a format change leaves the tree clean. Only `seed-*` files are
//! replaced; anything else in a target's directory (inputs a local fuzz run saved there)
//! is left alone.

use anyhow::{bail, Context, Result};
use prkdb::keys::{collection_prefix, encode_id, encode_key, encode_record_key, CollectionId};
use prkdb::raft::command::Command;
use prkdb::storage::checkpoint::{checkpoint_dir, decode_checkpoint, encode_checkpoint};
use prkdb::storage::format::{ensure_format, FORMAT_FILE};
use prkdb::storage::WalStorageAdapter;
use prkdb_core::vfs::StdVfs;
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::frame::{decode_frame, encode_frame, Decoded, FrameKind, Lsn};
use prkdb_core::wal::segment::{segment_file_name, SEGMENT_HEADER_LEN};
use prkdb_core::wal::{CompressionConfig, CompressionType, RecordLoc, WalConfig};
use prkdb_proto::raft;
use prkdb_types::storage::StorageAdapter;
use prkdb_verify::fuzz_entry::TARGETS;
use prost::Message;
use std::path::{Path, PathBuf};

fn put(key: &str, value: &str) -> BatchOp {
    BatchOp::Put {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    }
}

fn small_batch() -> Batch {
    Batch {
        ops: vec![
            put("user:1", "alice"),
            put("user:2", "bob"),
            BatchOp::Delete {
                key: b"user:1".to_vec(),
            },
        ],
    }
}

/// Large and repetitive enough to clear any compressor's threshold and actually shrink.
fn big_batch() -> Batch {
    Batch {
        ops: (0..40)
            .map(|i| put(&format!("order:{i:04}"), &"pending ".repeat(8)))
            .collect(),
    }
}

fn compressed(codec: CompressionType) -> CompressionConfig {
    CompressionConfig {
        compression_type: codec,
        min_compress_bytes: 0,
        compression_level: 3,
    }
}

fn encode(batch: &Batch, codec: CompressionType) -> Result<Vec<u8>> {
    let cfg = if codec == CompressionType::None {
        CompressionConfig::none()
    } else {
        compressed(codec)
    };
    Ok(batch.encode(&cfg)?)
}

fn frame(lsn: Lsn, kind: FrameKind, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_frame(&mut out, lsn, kind, payload);
    out
}

/// A real data directory's first segment and checkpoint, from a small adapter.
struct RealFiles {
    segment: Vec<u8>,
    checkpoint: Vec<u8>,
}

async fn real_files() -> Result<RealFiles> {
    let dir = tempfile::tempdir()?;
    let cfg = WalConfig {
        log_dir: dir.path().to_path_buf(),
        segment_bytes: 64 * 1024,
        ..WalConfig::test_config()
    };
    {
        let db = WalStorageAdapter::open_async(cfg).await?;
        for i in 0..6 {
            db.put(
                format!("key-{i}").as_bytes(),
                format!("value-{i}").as_bytes(),
            )
            .await?;
        }
        db.delete(b"key-2").await?;
        db.put(b"key-3", b"value-3b").await?;
        db.save_checkpoint()?;
    }
    let segment = std::fs::read(dir.path().join(segment_file_name(1)))?;
    let ckpt_dir = checkpoint_dir(dir.path());
    let mut ckpts: Vec<PathBuf> = std::fs::read_dir(&ckpt_dir)
        .with_context(|| ckpt_dir.display().to_string())?
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<_, _>>()?;
    ckpts.sort();
    let Some(newest) = ckpts.last() else {
        bail!("save_checkpoint wrote no file in {}", ckpt_dir.display());
    };
    Ok(RealFiles {
        segment,
        checkpoint: std::fs::read(newest)?,
    })
}

fn frame_seeds() -> Result<Vec<Vec<u8>>> {
    let plain = encode(&small_batch(), CompressionType::None)?;
    let lz4 = encode(&big_batch(), CompressionType::Lz4)?;
    let mut two = frame(1, FrameKind::Batch, &plain);
    two.extend(frame(2, FrameKind::Elided, &[]));
    Ok(vec![
        frame(1, FrameKind::Batch, &plain),
        frame(7, FrameKind::Elided, &[]),
        frame(3, FrameKind::Batch, &lz4),
        two,
    ])
}

fn batch_seeds() -> Result<Vec<Vec<u8>>> {
    Ok(vec![
        encode(&small_batch(), CompressionType::None)?,
        encode(&big_batch(), CompressionType::Lz4)?,
        encode(&big_batch(), CompressionType::Snappy)?,
        encode(&big_batch(), CompressionType::Zstd)?,
        encode(&Batch::default(), CompressionType::None)?,
    ])
}

/// Segment bodies (the entry point writes a valid header for segment 1 first), and one
/// whole segment file (for the header path).
fn segment_seeds(real: &RealFiles) -> Result<Vec<Vec<u8>>> {
    let plain = encode(&small_batch(), CompressionType::None)?;
    let lz4 = encode(&big_batch(), CompressionType::Lz4)?;
    let mut two_batches = frame(1, FrameKind::Batch, &plain);
    two_batches.extend(frame(2, FrameKind::Batch, &lz4));
    let mut with_elided = frame(1, FrameKind::Elided, &[]);
    with_elided.extend(frame(2, FrameKind::Batch, &plain));
    let mut torn = two_batches.clone();
    let third = frame(3, FrameKind::Batch, &plain);
    torn.extend_from_slice(&third[..third.len() / 2]);
    Ok(vec![
        two_batches,
        with_elided,
        torn,
        real.segment[SEGMENT_HEADER_LEN as usize..].to_vec(),
        real.segment.clone(),
    ])
}

fn checkpoint_seeds(real: &RealFiles) -> Vec<Vec<u8>> {
    let loc = |lsn, offset| RecordLoc {
        lsn,
        segment: 1,
        offset,
        payload_len: 40,
    };
    vec![
        real.checkpoint.clone(),
        encode_checkpoint(0, &[]),
        encode_checkpoint(
            9,
            &[
                (b"a".to_vec(), loc(2, 24)),
                (b"b".to_vec(), loc(5, 120)),
                (b"c".to_vec(), loc(11, 400)),
            ],
        ),
    ]
}

fn proto_seeds() -> Vec<Vec<u8>> {
    let put_cmd = Command::Put {
        key: b"user:1".to_vec(),
        value: b"alice".to_vec(),
    };
    let entries = vec![
        raft::LogEntry {
            index: 4,
            term: 2,
            data: put_cmd.serialize(),
        },
        raft::LogEntry {
            index: 5,
            term: 2,
            data: Command::CreateCollection {
                name: "orders".into(),
                num_partitions: 3,
                replication_factor: 3,
            }
            .serialize(),
        },
    ];
    vec![
        raft::AppendEntriesRequest {
            term: 2,
            leader_id: 1,
            prev_log_index: 3,
            prev_log_term: 1,
            leader_commit: 3,
            entries,
        }
        .encode_to_vec(),
        raft::InstallSnapshotRequest {
            term: 2,
            leader_id: 1,
            last_included_index: 10,
            last_included_term: 2,
            offset: 0,
            data: b"snapshot chunk".to_vec(),
            done: true,
        }
        .encode_to_vec(),
        raft::RequestVoteRequest {
            term: 3,
            candidate_id: 2,
            last_log_index: 10,
            last_log_term: 2,
        }
        .encode_to_vec(),
        raft::PutRequest {
            key: b"user:1".to_vec(),
            value: b"alice".to_vec(),
        }
        .encode_to_vec(),
        raft::BatchPutRequest {
            pairs: vec![raft::KvPair {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        }
        .encode_to_vec(),
        put_cmd.serialize(),
        // RFT-11 regressions: a `CreateCollection` whose name declares u64::MAX and 2^62
        // bytes (panicked and aborted before the bounded decode), alone and as the data of
        // a replicated entry.
        oversized_command(u64::MAX),
        oversized_command(1 << 62),
        raft::AppendEntriesRequest {
            term: 2,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            entries: vec![raft::LogEntry {
                index: 1,
                term: 2,
                data: oversized_command(1 << 62),
            }],
        }
        .encode_to_vec(),
    ]
}

/// `Command::CreateCollection` (variant 2) whose name declares `len` bytes.
fn oversized_command(len: u64) -> Vec<u8> {
    let mut input = vec![2u8, 0xFD];
    input.extend_from_slice(&len.to_le_bytes());
    input
}

fn key_seeds() -> Result<Vec<Vec<u8>>> {
    Ok(vec![
        encode_key(b"", CollectionId(1), &encode_id(&"user-1")?)?,
        encode_record_key(b"tenant", CollectionId(7), &42u64)?,
        encode_record_key(b"", CollectionId(3), &"a string longer than eight bytes")?,
        encode_record_key(b"ns", CollectionId(2), &(5u32, "line"))?,
        collection_prefix(b"tenant", CollectionId(9)),
    ])
}

/// The marker this build writes, plus the variants the parser must accept.
fn format_seeds() -> Result<Vec<Vec<u8>>> {
    let dir = tempfile::tempdir()?;
    ensure_format(&StdVfs, dir.path())?;
    let current = std::fs::read(dir.path().join(FORMAT_FILE))?;
    Ok(vec![
        current,
        b"format = 2\ncreated_by = \"1.0.0\"\nchecksum = \"abc\"\n".to_vec(),
        b"format = \"3\"\n".to_vec(),
        b"created_by = \"0.6.0\"\n".to_vec(),
    ])
}

/// A seed the real decoder rejects only ever exercises an error path, so the generator
/// checks that the checksummed formats' seeds decode before writing anything.
fn check_valid(target: &str, seeds: &[Vec<u8>]) -> Result<()> {
    for (i, seed) in seeds.iter().enumerate() {
        let ok = match target {
            "frame_decode" => matches!(decode_frame(seed), Decoded::Frame { .. }),
            "batch_decode" => Batch::decode(seed).is_ok(),
            "checkpoint_load" => decode_checkpoint(seed).is_ok(),
            _ => true,
        };
        if !ok {
            bail!("{target} seed-{i} does not decode");
        }
    }
    Ok(())
}

fn write_seeds(root: &Path, target: &str, seeds: &[Vec<u8>]) -> Result<()> {
    let dir = root.join(target);
    std::fs::create_dir_all(&dir).with_context(|| dir.display().to_string())?;
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        let is_seed = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("seed-"));
        if is_seed {
            std::fs::remove_file(&path).with_context(|| path.display().to_string())?;
        }
    }
    for (i, seed) in seeds.iter().enumerate() {
        let path = dir.join(format!("seed-{i}"));
        std::fs::write(&path, seed).with_context(|| path.display().to_string())?;
    }
    println!("{target}: {} seeds", seeds.len());
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let root = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus"));
    let real = real_files().await?;
    for (target, _) in TARGETS {
        let seeds = match target {
            "frame_decode" => frame_seeds()?,
            "batch_decode" => batch_seeds()?,
            "segment_scan" => segment_seeds(&real)?,
            "checkpoint_load" => checkpoint_seeds(&real),
            "proto_decode" => proto_seeds(),
            "key_decode" => key_seeds()?,
            "format_parse" => format_seeds()?,
            other => bail!("no seed generator for fuzz target {other}"),
        };
        check_valid(target, &seeds)?;
        write_seeds(&root, target, &seeds)?;
    }
    Ok(())
}
