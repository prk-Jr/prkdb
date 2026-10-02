//! `LOG_STATE`: where the log starts and how far compaction has dropped deletes
//! (Task 2.15).
//!
//! Compaction removes fully elided segments from the front of the log, so the log may
//! start after LSN 1. Without a durable record of that start, a first segment deleted by
//! accident would be indistinguishable from one compaction removed, and the open would
//! silently lose its records. `LOG_STATE` records it, together with the change-stream
//! compaction floor (`deletes_compacted_through`): the highest LSN of a `Delete` op a
//! compaction dropped. A change-stream cursor below it may have missed that delete and must
//! resynchronise from a snapshot. Dropped puts need no floor: a consumer that misses an
//! overwritten put still sees the last write to its key, or a dropped delete the floor
//! covers.
//!
//! An absent file means `log_start = 1, deletes_compacted_through = 0` (a log never
//! compacted). It is not part of `FORMAT` or of the index checkpoints.
//!
//! # Layout (little-endian)
//!
//! ```text
//! magic                      b"PRKDBLGS"
//! version                    u32   1
//! log_start                  u64   first LSN of the log; every earlier segment was removed
//! deletes_compacted_through  u64   highest LSN of a delete compaction dropped; 0 = none
//! reserved                   u64   written 0; a reader refuses any other value
//! crc                        u32   CRC-32 (crc32fast) of every preceding byte
//! ```
//!
//! A reader checks the magic and the version first and then the length that version
//! has, so a later version's file is refused by its version number, never misread. The
//! reserved field lets version 1 grow a value whose zero means "absent" without a new
//! version.
//!
//! Written atomically through `Vfs`: `LOG_STATE.tmp` create → write → `sync_data` →
//! rename → `sync_dir`.

use crate::vfs::{OpenMode, Vfs};
use crate::wal::frame::Lsn;
use crate::wal::WalError;
use std::path::Path;

/// The file's name, in the log directory.
pub const LOG_STATE_FILE: &str = "LOG_STATE";
/// Its temp file while being replaced.
pub const LOG_STATE_TMP_FILE: &str = "LOG_STATE.tmp";

const MAGIC: [u8; 8] = *b"PRKDBLGS";
/// The version this build writes and reads.
pub const LOG_STATE_VERSION: u32 = 1;
/// `magic + version`: what a reader needs before it knows the rest of the layout.
const PREFIX_LEN: usize = 8 + 4;
/// The whole version-1 file.
const V1_LEN: usize = PREFIX_LEN + 8 + 8 + 8 + 4;
/// A file longer than this is not a `LOG_STATE` of any version; never read further.
const MAX_LEN: u64 = 4096;

/// The durable log state (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogState {
    pub log_start: Lsn,
    pub deletes_compacted_through: Lsn,
}

impl Default for LogState {
    fn default() -> Self {
        LogState {
            log_start: 1,
            deletes_compacted_through: 0,
        }
    }
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8-byte slice"))
}

impl LogState {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(V1_LEN);
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&LOG_STATE_VERSION.to_le_bytes());
        out.extend_from_slice(&self.log_start.to_le_bytes());
        out.extend_from_slice(&self.deletes_compacted_through.to_le_bytes());
        out.extend_from_slice(&0u64.to_le_bytes()); // reserved
        let crc = crc32fast::hash(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decodes and checks the file's bytes: magic, version, then the length, CRC and
    /// fields of that version. `Err` names what is wrong.
    pub fn decode(bytes: &[u8]) -> Result<LogState, String> {
        if bytes.len() < PREFIX_LEN {
            return Err(format!("{} bytes, shorter than its header", bytes.len()));
        }
        if bytes[0..8] != MAGIC {
            return Err("bad magic".to_string());
        }
        let version = u32::from_le_bytes(bytes[8..12].try_into().expect("4-byte slice"));
        if version != LOG_STATE_VERSION {
            return Err(format!(
                "version {version}; this build reads {LOG_STATE_VERSION}"
            ));
        }
        if bytes.len() != V1_LEN {
            return Err(format!(
                "{} bytes, but a version-1 file is {V1_LEN}",
                bytes.len()
            ));
        }
        let (body, crc) = bytes.split_at(V1_LEN - 4);
        let stored = u32::from_le_bytes(crc.try_into().expect("4-byte slice"));
        if crc32fast::hash(body) != stored {
            return Err("checksum mismatch".to_string());
        }
        let reserved = u64_at(body, 28);
        if reserved != 0 {
            return Err(format!(
                "reserved field is {reserved:#x}; this build knows no use of it"
            ));
        }
        let state = LogState {
            log_start: u64_at(body, 12),
            deletes_compacted_through: u64_at(body, 20),
        };
        if state.log_start == 0 {
            return Err("log_start 0: LSNs start at 1".to_string());
        }
        Ok(state)
    }

    /// Reads `dir/LOG_STATE`; the default when it does not exist. A file that exists but
    /// does not decode is `CorruptSegment` naming it: guessing a log start could hide lost
    /// segments, which is what the file exists to prevent.
    pub fn read(vfs: &dyn Vfs, dir: &Path) -> Result<LogState, WalError> {
        let path = dir.join(LOG_STATE_FILE);
        if !vfs.exists(&path)? {
            return Ok(LogState::default());
        }
        let file = vfs.open(&path, OpenMode::Read)?;
        let len = file.len()?.min(MAX_LEN) as usize;
        let mut buf = vec![0u8; len];
        let mut read = 0;
        while read < len {
            let n = file.read_at(read as u64, &mut buf[read..])?;
            if n == 0 {
                break;
            }
            read += n;
        }
        buf.truncate(read);
        LogState::decode(&buf).map_err(|reason| WalError::CorruptSegment {
            path,
            offset: 0,
            reason,
        })
    }

    /// Replaces `dir/LOG_STATE` atomically (see the module docs).
    pub fn write(&self, vfs: &dyn Vfs, dir: &Path) -> Result<(), WalError> {
        let tmp = dir.join(LOG_STATE_TMP_FILE);
        let file = vfs.create(&tmp)?;
        file.write_at(0, &self.encode())?;
        file.sync_data()?;
        drop(file);
        vfs.rename(&tmp, &dir.join(LOG_STATE_FILE))?;
        vfs.sync_dir(dir)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> LogState {
        LogState {
            log_start: 70,
            deletes_compacted_through: 312,
        }
    }

    /// Re-seals `bytes` (everything but the trailing CRC) with a fresh CRC.
    fn reseal(mut bytes: Vec<u8>) -> Vec<u8> {
        bytes.truncate(bytes.len() - 4);
        let crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes
    }

    #[test]
    fn the_layout_is_the_documented_one() {
        let bytes = sample().encode();
        assert_eq!(bytes.len(), V1_LEN);
        assert_eq!(&bytes[0..8], b"PRKDBLGS");
        assert_eq!(&bytes[8..12], &1u32.to_le_bytes());
        assert_eq!(&bytes[12..20], &70u64.to_le_bytes());
        assert_eq!(&bytes[20..28], &312u64.to_le_bytes());
        assert_eq!(&bytes[28..36], &0u64.to_le_bytes(), "reserved is written 0");
    }

    #[test]
    fn round_trips_and_refuses_every_bit_flip_and_truncation() {
        let bytes = sample().encode();
        assert_eq!(LogState::decode(&bytes), Ok(sample()));
        for i in 0..bytes.len() {
            for bit in 0..8 {
                let mut bad = bytes.clone();
                bad[i] ^= 1 << bit;
                assert!(LogState::decode(&bad).is_err(), "byte {i} bit {bit}");
            }
        }
        for len in 0..bytes.len() {
            assert!(LogState::decode(&bytes[..len]).is_err());
        }
    }

    /// A later version's file is refused by its version number, whatever its length.
    #[test]
    fn a_version_2_file_is_refused_by_version() {
        for extra in [0usize, 8, 64] {
            let mut bytes = sample().encode();
            bytes[8..12].copy_from_slice(&2u32.to_le_bytes());
            bytes.splice(V1_LEN - 4..V1_LEN - 4, vec![0u8; extra]);
            let bytes = reseal(bytes);
            assert_eq!(
                LogState::decode(&bytes),
                Err("version 2; this build reads 1".to_string()),
                "{extra} extra bytes"
            );
        }
    }

    /// A well-checksummed file with a non-zero reserved field is refused.
    #[test]
    fn an_unknown_reserved_value_is_refused() {
        let mut bytes = sample().encode();
        bytes[28..36].copy_from_slice(&1u64.to_le_bytes());
        let err = LogState::decode(&reseal(bytes)).unwrap_err();
        assert!(err.contains("reserved"), "{err}");
    }

    #[test]
    fn an_absent_file_is_a_log_never_compacted() {
        let dir = tempfile::tempdir().unwrap();
        let vfs = crate::vfs::StdVfs;
        assert_eq!(
            LogState::read(&vfs, dir.path()).unwrap(),
            LogState::default()
        );
        sample().write(&vfs, dir.path()).unwrap();
        assert_eq!(LogState::read(&vfs, dir.path()).unwrap(), sample());
        assert!(!dir.path().join(LOG_STATE_TMP_FILE).exists());
    }
}
