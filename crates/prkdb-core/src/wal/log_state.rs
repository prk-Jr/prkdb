//! `LOG_STATE`: where the log starts and how far compaction has rewritten it (Task 2.15).
//!
//! Compaction removes fully elided segments from the front of the log, so the log may
//! start after LSN 1. Without a durable record of that start, a first segment deleted by
//! accident would be indistinguishable from one compaction removed, and the open would
//! silently lose its records. `LOG_STATE` records it, together with the compaction floor
//! (`compacted_through`): the highest LSN of any frame a compaction rewrote or removed.
//! A change-stream cursor below the floor may have missed dropped deletes and must
//! resynchronise from a snapshot instead of reading an incomplete stream.
//!
//! An absent file means `log_start = 1, compacted_through = 0` (a log never compacted).
//! It is not part of `FORMAT` or of the index checkpoints.
//!
//! # Layout (little-endian)
//!
//! ```text
//! magic              b"PRKDBLGS"
//! version            u32   1
//! log_start          u64   first LSN of the log; every earlier segment was removed
//! compacted_through  u64   highest LSN of a frame compaction changed or removed; 0 = none
//! crc                u32   CRC-32 (crc32fast) of every preceding byte
//! ```
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
const VERSION: u32 = 1;
const LEN: usize = 8 + 4 + 8 + 8 + 4;

/// The durable log state (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogState {
    pub log_start: Lsn,
    pub compacted_through: Lsn,
}

impl Default for LogState {
    fn default() -> Self {
        LogState {
            log_start: 1,
            compacted_through: 0,
        }
    }
}

impl LogState {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(LEN);
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&self.log_start.to_le_bytes());
        out.extend_from_slice(&self.compacted_through.to_le_bytes());
        let crc = crc32fast::hash(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decodes and checks the file's bytes; `Err` names what is wrong.
    pub fn decode(bytes: &[u8]) -> Result<LogState, String> {
        if bytes.len() != LEN {
            return Err(format!("{} bytes, expected {LEN}", bytes.len()));
        }
        let (body, crc) = bytes.split_at(LEN - 4);
        let stored = u32::from_le_bytes(crc.try_into().expect("4-byte slice"));
        if crc32fast::hash(body) != stored {
            return Err("checksum mismatch".to_string());
        }
        if body[0..8] != MAGIC {
            return Err("bad magic".to_string());
        }
        let version = u32::from_le_bytes(body[8..12].try_into().expect("4-byte slice"));
        if version != VERSION {
            return Err(format!("version {version}; this build reads {VERSION}"));
        }
        let state = LogState {
            log_start: u64::from_le_bytes(body[12..20].try_into().expect("8-byte slice")),
            compacted_through: u64::from_le_bytes(body[20..28].try_into().expect("8-byte slice")),
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
        let len = file.len()?.min(4096) as usize;
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

    #[test]
    fn round_trips_and_refuses_every_bit_flip() {
        let state = LogState {
            log_start: 70,
            compacted_through: 312,
        };
        let bytes = state.encode();
        assert_eq!(LogState::decode(&bytes), Ok(state));
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

    #[test]
    fn an_absent_file_is_a_log_never_compacted() {
        let dir = tempfile::tempdir().unwrap();
        let vfs = crate::vfs::StdVfs;
        assert_eq!(
            LogState::read(&vfs, dir.path()).unwrap(),
            LogState::default()
        );
        let state = LogState {
            log_start: 5,
            compacted_through: 9,
        };
        state.write(&vfs, dir.path()).unwrap();
        assert_eq!(LogState::read(&vfs, dir.path()).unwrap(), state);
        assert!(!dir.path().join(LOG_STATE_TMP_FILE).exists());
    }
}
