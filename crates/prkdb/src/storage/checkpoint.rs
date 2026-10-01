//! Index checkpoints: a snapshot of the key → location index at a known LSN (Task 2.14,
//! spec §7 2d).
//!
//! A checkpoint is an optimisation, never a source of truth: recovery loads the newest
//! valid one and replays every frame after its LSN, and anything wrong with it (bad CRC,
//! wrong magic or format, a torn or truncated file, an LSN beyond the log, a location in a
//! segment that does not exist) makes recovery ignore it with a warning naming the file
//! and fall back to an older checkpoint or a full replay. Full replay is always available,
//! so the spec's "refuse to open rather than misread" rule (§8) does not apply here; the
//! invariant is `recover(checkpoint, wal) == recover(∅, wal)`.
//!
//! The JSON checkpoint that used to live here stored per-segment offsets and let recovery
//! skip records below them, which is what lost every key written before a checkpoint
//! (STO-01). It was deleted in Task 2.8c.
//!
//! # File
//!
//! `<data dir>/checkpoints/index-{covered:020}.ckpt`, written atomically through `Vfs`
//! (`.tmp` create → write → `sync_data` → rename → `sync_dir`), after the log covering
//! every entry in it is durable. Older checkpoints are removed once the new one is in
//! place.
//!
//! # Layout (format 2, D3: frozen with the rest of format 2 at Task 2.24)
//!
//! All integers little-endian:
//!
//! ```text
//! magic   b"PRKDBCKP"
//! format  u32            FORMAT_VERSION
//! covered u64            every frame with lsn <= covered is reflected in the entries
//! count   u64
//! count × entry          klen u32 | key | lsn u64 | segment u64 | offset u64 | payload_len u32
//! crc     u32            CRC-32 (crc32fast) of every preceding byte
//! ```
//!
//! Entries are sorted by key, strictly ascending, so the same index always encodes to the
//! same bytes (the golden directory of Task 2.24 checks byte identity). An entry may carry
//! an LSN above `covered`: the snapshot is fuzzy (taken while writes continue), and replay
//! from `covered + 1` makes the result exact, because replay applies every later put and
//! delete in LSN order and the last one wins.

use prkdb_core::format::FORMAT_VERSION;
use prkdb_core::vfs::{OpenMode, Vfs};
use prkdb_core::wal::frame::MAX_PAYLOAD_LEN;
use prkdb_core::wal::segment::SEGMENT_HEADER_LEN;
use prkdb_core::wal::{Lsn, RecordLoc};
use prkdb_types::error::StorageError;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use tracing::warn;

/// The checkpoint directory's name, under the data directory.
pub const CHECKPOINT_DIR: &str = "checkpoints";

/// First eight bytes of every checkpoint file.
pub const CHECKPOINT_MAGIC: [u8; 8] = *b"PRKDBCKP";

const FILE_PREFIX: &str = "index-";
const FILE_SUFFIX: &str = ".ckpt";
const TMP_SUFFIX: &str = ".tmp";

/// `magic(8) + format(4) + covered(8) + count(8)`.
const HEADER_LEN: usize = 28;
const CRC_LEN: usize = 4;
/// `klen(4) + lsn(8) + segment(8) + offset(8) + payload_len(4)`, with an empty key.
const MIN_ENTRY_LEN: usize = 32;

/// One index entry: a key and the location of the frame holding its latest put.
pub type Entry = (Vec<u8>, RecordLoc);

/// A decoded checkpoint: the LSN it covers and its entries, sorted by key.
pub type Decoded = (Lsn, Vec<Entry>);

/// `<log_dir>/checkpoints`.
pub fn checkpoint_dir(log_dir: &Path) -> PathBuf {
    log_dir.join(CHECKPOINT_DIR)
}

/// `index-{covered:020}.ckpt`: zero-padded, so name order is LSN order.
pub fn checkpoint_file_name(covered: Lsn) -> String {
    format!("{FILE_PREFIX}{covered:020}{FILE_SUFFIX}")
}

/// The `covered` LSN of a checkpoint file name, `None` for anything else (temp files
/// included).
pub fn parse_checkpoint_file_name(name: &str) -> Option<Lsn> {
    let digits = name.strip_prefix(FILE_PREFIX)?.strip_suffix(FILE_SUFFIX)?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn corrupt(reason: impl std::fmt::Display) -> StorageError {
    StorageError::Corruption(format!("checkpoint: {reason}"))
}

fn io_err(path: &Path, e: std::io::Error) -> StorageError {
    StorageError::Internal(format!("{}: {e}", path.display()))
}

/// Encodes a checkpoint. `entries` must be sorted by key, strictly ascending (what
/// [`decode_checkpoint`] accepts).
pub fn encode_checkpoint(covered: Lsn, entries: &[Entry]) -> Vec<u8> {
    let body: usize = entries
        .iter()
        .map(|(key, _)| MIN_ENTRY_LEN + key.len())
        .sum();
    let mut out = Vec::with_capacity(HEADER_LEN + body + CRC_LEN);
    out.extend_from_slice(&CHECKPOINT_MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&covered.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for (key, loc) in entries {
        out.extend_from_slice(&(key.len() as u32).to_le_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&loc.lsn.to_le_bytes());
        out.extend_from_slice(&loc.segment.to_le_bytes());
        out.extend_from_slice(&loc.offset.to_le_bytes());
        out.extend_from_slice(&loc.payload_len.to_le_bytes());
    }
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Bounds-checked little-endian reader over the checkpoint body.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], StorageError> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&end| end <= self.buf.len())
            .ok_or_else(|| corrupt(format!("truncated at byte {}", self.pos)))?;
        let bytes = &self.buf[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }

    fn u32(&mut self) -> Result<u32, StorageError> {
        let mut b = [0u8; 4];
        b.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(b))
    }

    fn u64(&mut self) -> Result<u64, StorageError> {
        let mut b = [0u8; 8];
        b.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(b))
    }
}

/// Decodes and checks a checkpoint file's bytes. Never panics, whatever the input
/// (Task 2.23's `checkpoint_load` fuzz target calls this).
///
/// Checks, in order: length, CRC over every byte before the trailing CRC, magic, format
/// ([`FORMAT_VERSION`]), then every entry: bounds, keys strictly ascending, and the
/// location's own sanity (`1 <= segment <= lsn`, offset past the segment header, payload
/// no larger than a frame can hold). Whether the locations exist in the log is the
/// caller's check ([`validate_against_log`]), since it needs the log.
pub fn decode_checkpoint(bytes: &[u8]) -> Result<Decoded, StorageError> {
    if bytes.len() < HEADER_LEN + CRC_LEN {
        return Err(corrupt(format!(
            "{} bytes, shorter than the {}-byte header and checksum",
            bytes.len(),
            HEADER_LEN + CRC_LEN
        )));
    }
    let (body, crc_bytes) = bytes.split_at(bytes.len() - CRC_LEN);
    let mut stored = [0u8; 4];
    stored.copy_from_slice(crc_bytes);
    let stored = u32::from_le_bytes(stored);
    let computed = crc32fast::hash(body);
    if stored != computed {
        return Err(corrupt(format!(
            "checksum mismatch (stored {stored:#010x}, computed {computed:#010x})"
        )));
    }

    let mut r = Reader { buf: body, pos: 0 };
    if r.take(8)? != CHECKPOINT_MAGIC {
        return Err(corrupt("bad magic"));
    }
    let format = r.u32()?;
    if format != FORMAT_VERSION {
        return Err(corrupt(format!(
            "format {format}; this build reads format {FORMAT_VERSION}"
        )));
    }
    let covered = r.u64()?;
    let count = r.u64()?;
    let room = (body.len() - HEADER_LEN) / MIN_ENTRY_LEN;
    if count > room as u64 {
        return Err(corrupt(format!(
            "{count} entries cannot fit in {} bytes",
            body.len()
        )));
    }

    let mut entries: Vec<Entry> = Vec::with_capacity(count as usize);
    for i in 0..count {
        let klen = r.u32()? as usize;
        let key = r.take(klen)?.to_vec();
        let loc = RecordLoc {
            lsn: r.u64()?,
            segment: r.u64()?,
            offset: r.u64()?,
            payload_len: r.u32()?,
        };
        if let Some((prev, _)) = entries.last() {
            if *prev >= key {
                return Err(corrupt(format!(
                    "entry {i}: keys are not strictly ascending"
                )));
            }
        }
        if loc.segment == 0
            || loc.segment > loc.lsn
            || loc.offset < SEGMENT_HEADER_LEN
            || loc.payload_len as usize > MAX_PAYLOAD_LEN
        {
            return Err(corrupt(format!("entry {i}: impossible location {loc:?}")));
        }
        entries.push((key, loc));
    }
    if r.pos != body.len() {
        return Err(corrupt(format!(
            "{} trailing bytes after {count} entries",
            body.len() - r.pos
        )));
    }
    Ok((covered, entries))
}

/// Checks a decoded checkpoint against the opened log: `covered` and every entry's LSN
/// must be within the log (`<= last_lsn`), and every entry must sit in an existing
/// segment, the one whose LSN range holds its LSN. `segments` are the log's segments'
/// first LSNs.
pub fn validate_against_log(
    covered: Lsn,
    locations: &[RecordLoc],
    last_lsn: Lsn,
    segments: &[Lsn],
) -> Result<(), String> {
    if covered > last_lsn {
        return Err(format!(
            "covers LSN {covered}, beyond the log's last LSN {last_lsn}"
        ));
    }
    let segments: BTreeSet<Lsn> = segments.iter().copied().collect();
    for loc in locations {
        if loc.lsn > last_lsn {
            return Err(format!(
                "an entry points at LSN {}, beyond the log's last LSN {last_lsn}",
                loc.lsn
            ));
        }
        // The segment holding `lsn` is the last one starting at or before it.
        let holder = segments.range(..=loc.lsn).next_back().copied();
        if holder != Some(loc.segment) {
            return Err(format!(
                "an entry points at LSN {} in segment {}, which the log does not hold",
                loc.lsn, loc.segment
            ));
        }
    }
    Ok(())
}

/// Writes a checkpoint covering `covered` atomically, then removes every older one.
///
/// The caller guarantees that every frame an entry references, and every frame up to
/// `covered`, is durable before calling this. `entries` must be sorted by key, strictly
/// ascending.
///
/// Steps: create `checkpoints/` if missing (+ `sync_dir` of the data directory); write
/// `index-{covered}.ckpt.tmp` → `sync_data` → rename to `index-{covered}.ckpt` →
/// `sync_dir`. A crash at any point leaves either the previous checkpoints or the new
/// one complete, and at most a stale temp file, which loading ignores and the next
/// checkpoint removes. Removing older checkpoints afterwards is best-effort: one left
/// behind is never loaded while a newer valid one exists.
pub fn write_checkpoint(
    vfs: &dyn Vfs,
    log_dir: &Path,
    covered: Lsn,
    entries: &[Entry],
) -> Result<PathBuf, StorageError> {
    let dir = checkpoint_dir(log_dir);
    if !vfs.exists(&dir).map_err(|e| io_err(&dir, e))? {
        vfs.create_dir_all(&dir).map_err(|e| io_err(&dir, e))?;
        vfs.sync_dir(log_dir).map_err(|e| io_err(log_dir, e))?;
    }

    let name = checkpoint_file_name(covered);
    let path = dir.join(&name);
    let tmp = dir.join(format!("{name}{TMP_SUFFIX}"));
    let bytes = encode_checkpoint(covered, entries);
    {
        let file = vfs.create(&tmp).map_err(|e| io_err(&tmp, e))?;
        file.write_at(0, &bytes).map_err(|e| io_err(&tmp, e))?;
        file.sync_data().map_err(|e| io_err(&tmp, e))?;
    }
    vfs.rename(&tmp, &path).map_err(|e| io_err(&path, e))?;
    vfs.sync_dir(&dir).map_err(|e| io_err(&dir, e))?;

    remove_older(vfs, &dir, &path, covered);
    Ok(path)
}

/// Removes every checkpoint older than `covered` and every stale temp file, then syncs
/// the directory. Failures are logged, not returned: the new checkpoint is already in
/// place, and a leftover older one is only ever read if the new one turns out invalid.
fn remove_older(vfs: &dyn Vfs, dir: &Path, keep: &Path, covered: Lsn) {
    let listed = match vfs.read_dir(dir) {
        Ok(listed) => listed,
        Err(e) => {
            warn!(dir = %dir.display(), error = %e, "could not list old checkpoints to remove them");
            return;
        }
    };
    let mut removed = false;
    for path in listed {
        if path == keep {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let stale = name.ends_with(TMP_SUFFIX)
            || parse_checkpoint_file_name(name).is_some_and(|lsn| lsn < covered);
        if !stale {
            continue;
        }
        match vfs.remove(&path) {
            Ok(()) => removed = true,
            Err(e) => {
                warn!(path = %path.display(), error = %e, "could not remove an old checkpoint")
            }
        }
    }
    if removed {
        if let Err(e) = vfs.sync_dir(dir) {
            warn!(dir = %dir.display(), error = %e, "could not sync the checkpoint directory after removing old checkpoints");
        }
    }
}

/// The checkpoint files under `log_dir`, newest (highest `covered`) first. A missing
/// directory is no checkpoints. Temp files and unknown names are skipped.
pub fn list_checkpoints(
    vfs: &dyn Vfs,
    log_dir: &Path,
) -> Result<Vec<(Lsn, PathBuf)>, StorageError> {
    let dir = checkpoint_dir(log_dir);
    if !vfs.exists(&dir).map_err(|e| io_err(&dir, e))? {
        return Ok(Vec::new());
    }
    let mut found: Vec<(Lsn, PathBuf)> = vfs
        .read_dir(&dir)
        .map_err(|e| io_err(&dir, e))?
        .into_iter()
        .filter_map(|path| {
            let lsn = parse_checkpoint_file_name(path.file_name()?.to_str()?)?;
            Some((lsn, path))
        })
        .collect();
    found.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    Ok(found)
}

/// Reads and decodes one checkpoint file. Its name's LSN must match the `covered` inside.
pub fn read_checkpoint(vfs: &dyn Vfs, path: &Path, named: Lsn) -> Result<Decoded, StorageError> {
    let file = vfs
        .open(path, OpenMode::Read)
        .map_err(|e| io_err(path, e))?;
    let len = file.len().map_err(|e| io_err(path, e))?;
    let len = usize::try_from(len).map_err(|_| corrupt(format!("{len} bytes is too large")))?;
    let mut buf = vec![0u8; len];
    let mut read = 0;
    while read < len {
        let n = file
            .read_at(read as u64, &mut buf[read..])
            .map_err(|e| io_err(path, e))?;
        if n == 0 {
            break;
        }
        read += n;
    }
    buf.truncate(read);
    let decoded = decode_checkpoint(&buf)?;
    if decoded.0 != named {
        return Err(corrupt(format!(
            "file name says LSN {named}, contents say {}",
            decoded.0
        )));
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prkdb_core::vfs::StdVfs;
    use rand::{Rng, SeedableRng};

    fn loc(lsn: Lsn) -> RecordLoc {
        RecordLoc {
            lsn,
            segment: 1,
            offset: SEGMENT_HEADER_LEN + lsn * 10,
            payload_len: 7,
        }
    }

    fn sample() -> Vec<Entry> {
        vec![
            (b"a".to_vec(), loc(3)),
            (b"b".to_vec(), loc(9)),
            (b"c".to_vec(), loc(4)),
        ]
    }

    #[test]
    fn a_checkpoint_round_trips() {
        let bytes = encode_checkpoint(7, &sample());
        assert_eq!(decode_checkpoint(&bytes).unwrap(), (7, sample()));
        let empty = encode_checkpoint(0, &[]);
        assert_eq!(decode_checkpoint(&empty).unwrap(), (0, Vec::new()));
    }

    #[test]
    fn the_layout_is_the_documented_one() {
        let bytes = encode_checkpoint(5, &[(b"k".to_vec(), loc(2))]);
        assert_eq!(&bytes[..8], b"PRKDBCKP");
        assert_eq!(&bytes[8..12], &FORMAT_VERSION.to_le_bytes());
        assert_eq!(&bytes[12..20], &5u64.to_le_bytes());
        assert_eq!(&bytes[20..28], &1u64.to_le_bytes());
        assert_eq!(&bytes[28..32], &1u32.to_le_bytes());
        assert_eq!(bytes[32], b'k');
        assert_eq!(bytes.len(), HEADER_LEN + MIN_ENTRY_LEN + 1 + CRC_LEN);
        let crc = crc32fast::hash(&bytes[..bytes.len() - 4]);
        assert_eq!(&bytes[bytes.len() - 4..], &crc.to_le_bytes());
    }

    #[test]
    fn every_single_bit_flip_and_every_truncation_is_refused() {
        let bytes = encode_checkpoint(9, &sample());
        for i in 0..bytes.len() {
            for bit in 0..8 {
                let mut bad = bytes.clone();
                bad[i] ^= 1 << bit;
                assert!(decode_checkpoint(&bad).is_err(), "flip byte {i} bit {bit}");
            }
        }
        for len in 0..bytes.len() {
            assert!(
                decode_checkpoint(&bytes[..len]).is_err(),
                "truncated to {len}"
            );
        }
    }

    /// The checksum is valid but the contents are not: each must be refused by its own
    /// check, not trusted because the CRC matched.
    #[test]
    fn well_checksummed_nonsense_is_refused() {
        let reseal = |mut body: Vec<u8>| {
            body.truncate(body.len() - CRC_LEN);
            let crc = crc32fast::hash(&body);
            body.extend_from_slice(&crc.to_le_bytes());
            body
        };
        let good = encode_checkpoint(9, &sample());

        let mut magic = good.clone();
        magic[0] = b'X';
        assert!(decode_checkpoint(&reseal(magic)).is_err());

        let mut format = good.clone();
        format[8..12].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        let e = decode_checkpoint(&reseal(format)).unwrap_err();
        assert!(e.to_string().contains("format"), "{e}");

        let mut count = good.clone();
        count[20..28].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(decode_checkpoint(&reseal(count)).is_err());

        let unsorted = encode_checkpoint(9, &[(b"b".to_vec(), loc(1)), (b"a".to_vec(), loc(2))]);
        assert!(decode_checkpoint(&unsorted).is_err());
        let duplicate = encode_checkpoint(9, &[(b"a".to_vec(), loc(1)), (b"a".to_vec(), loc(2))]);
        assert!(decode_checkpoint(&duplicate).is_err());

        for bad in [
            RecordLoc {
                segment: 0,
                ..loc(3)
            },
            RecordLoc {
                segment: 4,
                ..loc(3)
            },
            RecordLoc {
                offset: 0,
                ..loc(3)
            },
            RecordLoc {
                payload_len: u32::MAX,
                ..loc(3)
            },
        ] {
            let bytes = encode_checkpoint(9, &[(b"a".to_vec(), bad)]);
            assert!(decode_checkpoint(&bytes).is_err(), "{bad:?}");
        }

        let mut trailing = good.clone();
        trailing.truncate(trailing.len() - CRC_LEN);
        trailing.push(0);
        trailing.extend_from_slice(&[0; CRC_LEN]);
        assert!(decode_checkpoint(&reseal(trailing)).is_err());
    }

    /// The fuzz target's contract (Task 2.23): arbitrary bytes never panic.
    #[test]
    fn decoding_random_bytes_never_panics() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(14);
        let good = encode_checkpoint(9, &sample());
        for _ in 0..20_000 {
            let len = rng.gen_range(0..128);
            let mut bytes: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
            let _ = decode_checkpoint(&bytes);
            // Valid prefix with a random tail, resealed so the CRC passes.
            bytes = good[..rng.gen_range(HEADER_LEN..good.len())].to_vec();
            bytes.extend((0..rng.gen_range(0..64)).map(|_| rng.gen::<u8>()));
            let crc = crc32fast::hash(&bytes);
            bytes.extend_from_slice(&crc.to_le_bytes());
            let _ = decode_checkpoint(&bytes);
        }
    }

    #[test]
    fn file_names_sort_by_lsn_and_temp_files_are_not_checkpoints() {
        assert_eq!(checkpoint_file_name(42), "index-00000000000000000042.ckpt");
        assert_eq!(
            parse_checkpoint_file_name(&checkpoint_file_name(42)),
            Some(42)
        );
        assert_eq!(
            parse_checkpoint_file_name("index-00000000000000000042.ckpt.tmp"),
            None
        );
        assert_eq!(parse_checkpoint_file_name("index-42.ckpt"), None);
        assert_eq!(
            parse_checkpoint_file_name("index-+0000000000000000042.ckpt"),
            None
        );
    }

    #[test]
    fn validation_against_the_log() {
        let locs = [
            RecordLoc {
                lsn: 2,
                segment: 1,
                ..loc(2)
            },
            RecordLoc {
                lsn: 12,
                segment: 10,
                ..loc(12)
            },
        ];
        assert_eq!(validate_against_log(11, &locs, 12, &[1, 10]), Ok(()));
        assert!(
            validate_against_log(11, &locs, 10, &[1, 10]).is_err(),
            "covered beyond the log"
        );
        assert!(
            validate_against_log(11, &locs, 11, &[1, 10]).is_err(),
            "entry beyond the log"
        );
        assert!(
            validate_against_log(11, &locs, 12, &[1]).is_err(),
            "segment 10 is gone"
        );
        // LSN 2 is held by a segment starting at 2, not by segment 1.
        assert!(validate_against_log(11, &locs, 12, &[1, 2, 10]).is_err());
    }

    #[test]
    fn writing_replaces_older_checkpoints_and_stale_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let first = write_checkpoint(&StdVfs, dir.path(), 3, &sample()).unwrap();
        let stale = checkpoint_dir(dir.path()).join("index-00000000000000000002.ckpt.tmp");
        std::fs::write(&stale, b"half").unwrap();
        let second = write_checkpoint(&StdVfs, dir.path(), 9, &sample()).unwrap();
        assert!(!first.exists() && !stale.exists());
        assert_eq!(
            list_checkpoints(&StdVfs, dir.path()).unwrap(),
            vec![(9, second.clone())]
        );
        assert_eq!(read_checkpoint(&StdVfs, &second, 9).unwrap(), (9, sample()));
        let e = read_checkpoint(&StdVfs, &second, 8).unwrap_err();
        assert!(e.to_string().contains("file name says LSN 8"), "{e}");
    }

    #[test]
    fn no_checkpoint_directory_is_no_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        assert!(list_checkpoints(&StdVfs, dir.path()).unwrap().is_empty());
    }
}
