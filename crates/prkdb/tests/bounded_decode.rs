//! RFT-11: a length prefix read from disk or the network never sizes an allocation
//! unchecked. Before the fix, `Command::deserialize` on the ten-byte inputs below panicked
//! ("capacity overflow") or aborted the process (a 2^62-byte allocation), and every node
//! applying such a Raft entry would crash again on replay.
//!
//! An abort kills the test binary, so a regression shows up as this binary failing.

use prkdb::raft::command::Command;
use prkdb::storage::snapshot::{CompressionType, SnapshotHeader, SnapshotReader, SnapshotWriter};

/// `Command::CreateCollection` (variant 2) whose `name` declares `len` bytes, with nothing
/// after the length: `[2][0xFD][len: u64 LE]`.
fn command_declaring_name_of(len: u64) -> Vec<u8> {
    let mut input = vec![2u8, 0xFD];
    input.extend_from_slice(&len.to_le_bytes());
    input
}

#[test]
fn rft11_command_declaring_a_u64_max_length_is_rejected() {
    assert_eq!(
        Command::deserialize(&command_declaring_name_of(u64::MAX)),
        None
    );
}

#[test]
fn rft11_command_declaring_a_2_pow_62_length_is_rejected() {
    assert_eq!(
        Command::deserialize(&command_declaring_name_of(1 << 62)),
        None
    );
}

#[test]
fn rft11_commands_still_round_trip() {
    let commands = [
        Command::Put {
            key: b"k".to_vec(),
            value: vec![7; 4096],
        },
        Command::CreateCollection {
            name: "orders".into(),
            num_partitions: 3,
            replication_factor: 3,
        },
        Command::RevokePrincipal { name: "svc".into() },
    ];
    for c in commands {
        assert_eq!(Command::deserialize(&c.serialize()), Some(c));
    }
}

#[test]
fn rft11_snapshot_header_declaring_an_oversized_length_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snap.bin");
    std::fs::write(&path, u32::MAX.to_le_bytes()).unwrap();
    assert!(SnapshotReader::open(&path).is_err());
}

#[test]
fn rft11_snapshot_entry_declaring_an_oversized_length_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snap.bin");
    let writer =
        SnapshotWriter::new(&path, SnapshotHeader::new(1, 1, CompressionType::None)).unwrap();
    writer.finish().unwrap();
    // One entry whose key claims 4 GiB - 1 bytes, followed by three.
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    bytes.extend_from_slice(b"abc");
    std::fs::write(&path, &bytes).unwrap();

    let mut reader = SnapshotReader::open(&path).unwrap();
    assert!(reader.next_entry().is_err());
}
