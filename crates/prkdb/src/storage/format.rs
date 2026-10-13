//! The data-directory format marker (spec §7 2b, D3).
//!
//! Every data directory holds a `FORMAT` file:
//!
//! ```text
//! format = 2
//! created_by = "0.6.0"
//! ```
//!
//! KV markers keep two `key = value` lines; streams add `kind = "stream"` and then a last
//! line `checksum = "<crc32>"`: the CRC-32 (IEEE, lowercase hex, 8 digits) of every byte
//! before that line. A missing `kind` is KV. Unknown kinds are refused. Unknown keys are
//! ignored so a later version can add fields.
//!
//! # Checksum
//!
//! A `checksum` line, for any kind, must be the last line and must match; otherwise the
//! marker is refused as corrupt. A stream marker without one is refused too. Without it a
//! single flipped bit in the `kind` line (`kinf`, or a newline turned into another
//! character that merges `kind` into the line before) would read an empty stream
//! directory as KV, and a KV open would then write `Batch` frames into it. KV markers
//! carry no checksum, so their bytes are what every format-2 build has written; for them a
//! damaged marker still fails closed on the frames (keyed replay refuses `Records`). The number is [`FORMAT_VERSION`], the same one written into every WAL
//! segment header, so the program has exactly one format version.
//!
//! # Frozen syntax
//!
//! Every future version must keep writing the version as a line `format = <integer>`
//! (decimal `u32`; whitespace around `=` is free; a quoted `"<integer>"` is also read).
//! That one line is how an older build recognises a newer directory and refuses it by
//! number instead of as unreadable, so it is the only part of this file that can never
//! change. `kind` is checked before opening; `created_by` is informational.
//!
//! # Open rules ([`ensure_format`])
//!
//! - absent or empty directory: created as format 2, `FORMAT` written before anything else.
//!   "Empty" ignores what a filesystem or OS puts in a fresh directory on its own:
//!   `lost+found` (the root of an ext4 volume, e.g. a Kubernetes PVC mount) and dotfiles
//!   (`.DS_Store`). Anything else counts as data. This is deliberately an ignore list,
//!   not a list of format-1 file names: a name missing from an ignore list refuses a
//!   directory that could have been opened (fail closed), while a name missing from an
//!   evidence list would open old data as an empty database (the failure this exists
//!   to prevent). `LOCK`, the data-directory lock every open takes first
//!   ([`super::lock`]), is not data either;
//! - `FORMAT` says 2: opens;
//! - no `FORMAT` on a non-empty directory (format 1, which had no marker) or any other
//!   number: refused with [`StorageError::UnsupportedFormat`] before a single byte is
//!   written (the directory may gain a `LOCK` file, taken before the check). A format-1 log must never be shadowed by an empty format-2 log next to it,
//!   which would make the database look wiped.

use prkdb_core::vfs::{OpenMode, StdVfs, Vfs};
use prkdb_types::error::StorageError;
use std::path::Path;

pub use prkdb_core::format::FORMAT_VERSION;

/// The marker's file name, at the data directory's root.
pub const FORMAT_FILE: &str = "FORMAT";

/// Where the marker is written before it is renamed into place. A directory holding only
/// this file was interrupted while being created, and counts as empty.
const FORMAT_TMP_FILE: &str = "FORMAT.tmp";

/// Far more than two short lines; anything larger is not a marker this code wrote.
const MAX_FORMAT_BYTES: u64 = 4096;

/// The data stored in a directory. An absent marker kind means [`Kind::Kv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Kind {
    #[default]
    Kv,
    Stream,
}

impl Kind {
    /// "a key/value store" or "a stream", for messages.
    pub fn description(self) -> &'static str {
        match self {
            Self::Kv => "a key/value store",
            Self::Stream => "a stream",
        }
    }
}

enum MarkerParseError {
    Invalid,
    UnknownKind { kind: String, format: u32 },
    ChecksumMismatch,
    MissingChecksum,
}

/// The key of the marker's integrity line (see the module docs).
const CHECKSUM_KEY: &str = "checksum";

/// The quoted checksum value for the marker bytes `body`.
fn checksum_value(body: &str) -> String {
    format!("\"{:08x}\"", crc32fast::hash(body.as_bytes()))
}

/// Splits `text` at its `checksum` line, if it has one: the bytes before the line, and
/// the line's value. The line must be the last one (only its own newline may follow).
fn split_checksum(text: &str) -> Result<Option<(&str, &str)>, MarkerParseError> {
    let mut start = 0;
    while start < text.len() {
        let end = text[start..]
            .find('\n')
            .map_or(text.len(), |i| start + i + 1);
        if let Some((key, value)) = text[start..end].split_once('=') {
            if key.trim() == CHECKSUM_KEY {
                if end != text.len() {
                    return Err(MarkerParseError::ChecksumMismatch);
                }
                return Ok(Some((&text[..start], value.trim())));
            }
        }
        start = end;
    }
    Ok(None)
}

/// The contents of a data directory's `FORMAT` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatMarker {
    pub format: u32,
    pub kind: Kind,
    /// The `CARGO_PKG_VERSION` of the build that created the directory. Informational.
    pub created_by: String,
}

impl FormatMarker {
    /// The marker this build writes for a key/value directory.
    pub fn current() -> Self {
        Self::current_for(Kind::Kv)
    }

    /// The marker this build writes for the expected directory kind.
    pub fn current_for(kind: Kind) -> Self {
        Self {
            format: FORMAT_VERSION,
            kind,
            created_by: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    fn encode(&self) -> String {
        let mut text = format!(
            "format = {}\ncreated_by = \"{}\"\n",
            self.format, self.created_by
        );
        if self.kind == Kind::Stream {
            text.push_str("kind = \"stream\"\n");
            let checksum = checksum_value(&text);
            text.push_str(&format!("{CHECKSUM_KEY} = {checksum}\n"));
        }
        text
    }

    #[cfg(test)]
    fn parse(text: &str) -> Option<Self> {
        let format = Self::parse_version(text).ok()?;
        Self::parse_checked(text, format).ok()
    }

    fn parse_checked(text: &str, format: u32) -> Result<Self, MarkerParseError> {
        let covered = match split_checksum(text)? {
            Some((body, value)) if value == checksum_value(body) => Some(body),
            Some(_) => return Err(MarkerParseError::ChecksumMismatch),
            None => None,
        };
        let marker = Self::parse_fields(covered.unwrap_or(text), format)?;
        if marker.kind == Kind::Stream && covered.is_none() {
            return Err(MarkerParseError::MissingChecksum);
        }
        Ok(marker)
    }

    fn parse_version(text: &str) -> Result<u32, MarkerParseError> {
        let mut format = None;
        for line in text.lines() {
            if let Some((key, value)) = line.split_once('=') {
                if key.trim() == "format" {
                    if format.is_some() {
                        return Err(MarkerParseError::Invalid);
                    }
                    format = Some(
                        value
                            .trim()
                            .trim_matches('"')
                            .parse::<u32>()
                            .map_err(|_| MarkerParseError::Invalid)?,
                    );
                }
            }
        }
        format.ok_or(MarkerParseError::Invalid)
    }

    fn parse_fields(text: &str, format: u32) -> Result<Self, MarkerParseError> {
        let mut created_by = None;
        let mut kind = None;
        for line in text.lines() {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            match key.trim() {
                "created_by" => {
                    if created_by.is_some() {
                        return Err(MarkerParseError::Invalid);
                    }
                    created_by = Some(value.trim().trim_matches('"').to_string());
                }
                "kind" => {
                    if kind.is_some() {
                        return Err(MarkerParseError::Invalid);
                    }
                    let value = value.trim();
                    let value = if value.starts_with('"') || value.ends_with('"') {
                        value
                            .strip_prefix('"')
                            .and_then(|v| v.strip_suffix('"'))
                            .ok_or(MarkerParseError::Invalid)?
                    } else {
                        value
                    };
                    kind = Some(match value {
                        "kv" => Ok(Kind::Kv),
                        "stream" => Ok(Kind::Stream),
                        unknown => Err(unknown.to_string()),
                    });
                }
                _ => {}
            }
        }
        let kind = kind
            .unwrap_or(Ok(Kind::Kv))
            .map_err(|kind| MarkerParseError::UnknownKind { kind, format })?;
        Ok(Self {
            format,
            kind,
            created_by: created_by.unwrap_or_default(),
        })
    }
}

/// The refusal for a directory at format `found` (spec 2b wording). Format 1 had no
/// marker, so "no `FORMAT` file on a non-empty directory" is reported as format 1.
pub fn unsupported_format(dir: &Path, found: u32) -> StorageError {
    let age = if found < FORMAT_VERSION {
        "older"
    } else {
        "newer"
    };
    StorageError::UnsupportedFormat(format!(
        "data directory {} was created by an {age} PrkDB (format {found}); this version \
         reads format {FORMAT_VERSION}. See docs/guide/upgrade.",
        dir.display()
    ))
}

fn io_err(path: &Path, e: std::io::Error) -> StorageError {
    StorageError::Internal(format!("{}: {e}", path.display()))
}

/// Entries a filesystem or OS creates in a directory on its own, which do not make it a
/// data directory: `lost+found` and dotfiles. See the module docs for why this is an
/// ignore list.
fn is_ignorable(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n == "lost+found" || n.starts_with('.'))
}

/// Whether `dir` holds anything other than ignorable entries, a stale `FORMAT.tmp` and the
/// directory lock's `LOCK` (which every open creates before the format check), i.e.
/// whether a directory without `FORMAT` must be treated as format 1.
pub fn holds_data(vfs: &dyn Vfs, dir: &Path) -> Result<bool, StorageError> {
    let tmp = dir.join(FORMAT_TMP_FILE);
    let lock = dir.join(super::lock::LOCK_FILE);
    // `LOG_STATE` (Task 2.15) records where the log starts; like `LOCK` it is not data.
    let log_state = dir.join(prkdb_core::wal::log_state::LOG_STATE_FILE);
    let log_state_tmp = dir.join(prkdb_core::wal::log_state::LOG_STATE_TMP_FILE);
    Ok(vfs
        .read_dir(dir)
        .map_err(|e| io_err(dir, e))?
        .iter()
        .any(|p| {
            *p != tmp && *p != lock && *p != log_state && *p != log_state_tmp && !is_ignorable(p)
        }))
}

/// Reads `dir/FORMAT` without creating anything. `Ok(None)` if the file does not exist.
/// A future version is refused by its unique frozen version line, before interpreting
/// its checksum or kind schema. Current and older markers still validate their fields.
pub fn read_format(dir: &Path) -> Result<Option<FormatMarker>, StorageError> {
    read_format_with(&StdVfs, dir)
}

/// [`read_format`] on any `Vfs`.
pub fn read_format_with(vfs: &dyn Vfs, dir: &Path) -> Result<Option<FormatMarker>, StorageError> {
    let path = dir.join(FORMAT_FILE);
    if !vfs.exists(&path).map_err(|e| io_err(&path, e))? {
        return Ok(None);
    }
    let file = vfs
        .open(&path, OpenMode::Read)
        .map_err(|e| io_err(&path, e))?;
    let len = file.len().map_err(|e| io_err(&path, e))?;
    if len > MAX_FORMAT_BYTES {
        return Err(StorageError::Corruption(format!(
            "{}: not a format marker ({len} bytes)",
            path.display()
        )));
    }
    let mut buf = vec![0u8; len as usize];
    let mut read = 0;
    while read < buf.len() {
        let n = file
            .read_at(read as u64, &mut buf[read..])
            .map_err(|e| io_err(&path, e))?;
        if n == 0 {
            break;
        }
        read += n;
    }
    buf.truncate(read);
    let unreadable = || {
        StorageError::Corruption(format!(
            "{}: unreadable format marker (expected unique fields and a `format = <number>` line)",
            path.display()
        ))
    };
    let text = std::str::from_utf8(&buf).map_err(|_| unreadable())?;
    // The reader's version policy precedes current schema checks; the pure syntax
    // parser can still recognize a quoted future number without opening a directory.
    let format = FormatMarker::parse_version(text).map_err(|_| unreadable())?;
    if format > FORMAT_VERSION {
        return Err(unsupported_format(dir, format));
    }
    FormatMarker::parse_checked(text, format)
        .map(Some)
        .map_err(|e| match e {
            MarkerParseError::Invalid => unreadable(),
            MarkerParseError::ChecksumMismatch => StorageError::Corruption(format!(
                "{}: format marker checksum does not match its contents, or is not its last line",
                path.display()
            )),
            MarkerParseError::MissingChecksum => StorageError::Corruption(format!(
                "{}: stream format marker has no checksum line",
                path.display()
            )),
            MarkerParseError::UnknownKind { format, .. } if format != FORMAT_VERSION => {
                unsupported_format(dir, format)
            }
            MarkerParseError::UnknownKind { kind, .. } => StorageError::UnsupportedFormat(format!(
                "data directory {} has unsupported kind {kind:?}; supported kinds are \"kv\" and \"stream\"",
                dir.display()
            )),
        })
}

/// The format of an existing directory, read-only: `Some(n)` from its `FORMAT`, `Some(1)`
/// for a directory without one that [`holds_data`], `None` for an empty one (which the
/// open rules would create as format 2). Future markers return `UnsupportedFormat`.
pub fn detect_format(vfs: &dyn Vfs, dir: &Path) -> Result<Option<u32>, StorageError> {
    if let Some(marker) = read_format_with(vfs, dir)? {
        return Ok(Some(marker.format));
    }
    Ok(holds_data(vfs, dir)?.then_some(1))
}

/// The open rules' refusal, without creating or writing anything: `Ok` if `dir` is at
/// format 2 or empty.
pub fn check_format(vfs: &dyn Vfs, dir: &Path) -> Result<(), StorageError> {
    check_format_kind(vfs, dir, Kind::Kv)
}

/// Checks the version and expected kind without modifying the directory.
pub fn check_format_kind(vfs: &dyn Vfs, dir: &Path, expected: Kind) -> Result<(), StorageError> {
    if let Some(marker) = read_format_with(vfs, dir)? {
        return check_marker(dir, &marker, expected);
    }
    if holds_data(vfs, dir)? {
        return Err(unsupported_format(dir, 1));
    }
    Ok(())
}

fn check_marker(dir: &Path, marker: &FormatMarker, expected: Kind) -> Result<(), StorageError> {
    if marker.format != FORMAT_VERSION {
        return Err(unsupported_format(dir, marker.format));
    }
    if marker.kind != expected {
        return Err(StorageError::UnsupportedFormat(format!(
            "data directory {} is {}, not {}",
            dir.display(),
            marker.kind.description(),
            expected.description()
        )));
    }
    Ok(())
}

/// The open rules: an absent or empty directory is created as format 2 (FORMAT written
/// atomically: `FORMAT.tmp` create → write → sync_data → rename → sync_dir); `FORMAT == 2`
/// opens only with the expected kind; anything else is refused before a single byte is written.
///
/// "Empty" means [`holds_data`] is false: nothing but ignorable entries and a stale
/// `FORMAT.tmp` from a crash during creation (removed first). An absent directory is
/// created with its missing ancestors by `create_dir_all_durable`, as `Wal::open` does; an
/// existing empty one has its parent synced. Either way the marker cannot outlive its own
/// directory entry.
pub fn ensure_format(
    vfs: &dyn Vfs,
    dir: &Path,
    expected: Kind,
) -> Result<FormatMarker, StorageError> {
    if !vfs.exists(dir).map_err(|e| io_err(dir, e))? {
        prkdb_core::vfs::create_dir_all_durable(vfs, dir).map_err(|e| io_err(dir, e))?;
        return write_format(vfs, dir, expected, FORMAT_VERSION);
    }

    if let Some(marker) = read_format_with(vfs, dir)? {
        check_marker(dir, &marker, expected)?;
        return Ok(marker);
    }

    if holds_data(vfs, dir)? {
        return Err(unsupported_format(dir, 1));
    }
    let tmp = dir.join(FORMAT_TMP_FILE);
    if vfs.exists(&tmp).map_err(|e| io_err(&tmp, e))? {
        vfs.remove(&tmp).map_err(|e| io_err(&tmp, e))?;
    }
    // The directory may itself be new and unsynced (a caller may have created it with a
    // plain `create_dir_all`): sync its parent so the marker cannot outlive its own
    // directory entry.
    if let Some(parent) = dir.parent() {
        if vfs.exists(parent).map_err(|e| io_err(parent, e))? {
            vfs.sync_dir(parent).map_err(|e| io_err(parent, e))?;
        }
    }
    write_format(vfs, dir, expected, FORMAT_VERSION)
}

/// Rewrites `dir/FORMAT` to a migration step's explicit target version, atomically.
/// Pass `Migration::to()` and the directory's kind; an intermediate step must not
/// advertise this build's final version before later conversions have run.
pub fn rewrite_format(
    vfs: &dyn Vfs,
    dir: &Path,
    kind: Kind,
    target: u32,
) -> Result<FormatMarker, StorageError> {
    write_format(vfs, dir, kind, target)
}

/// `FORMAT.tmp` create → write → sync_data → rename to `FORMAT` → sync_dir. A crash at any
/// point leaves either no `FORMAT` (and at most a stale temp file, which counts as empty)
/// or a complete one.
fn write_format(
    vfs: &dyn Vfs,
    dir: &Path,
    kind: Kind,
    target: u32,
) -> Result<FormatMarker, StorageError> {
    let mut marker = FormatMarker::current_for(kind);
    marker.format = target;
    let tmp = dir.join(FORMAT_TMP_FILE);
    let path = dir.join(FORMAT_FILE);
    let file = vfs.create(&tmp).map_err(|e| io_err(&tmp, e))?;
    file.write_at(0, marker.encode().as_bytes())
        .map_err(|e| io_err(&tmp, e))?;
    file.sync_data().map_err(|e| io_err(&tmp, e))?;
    drop(file);
    vfs.rename(&tmp, &path).map_err(|e| io_err(&path, e))?;
    vfs.sync_dir(dir).map_err(|e| io_err(dir, e))?;
    Ok(marker)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_marker_bytes_and_missing_kind_remain_compatible() {
        let marker = FormatMarker::current();
        let expected = format!(
            "format = {FORMAT_VERSION}\ncreated_by = \"{}\"\n",
            env!("CARGO_PKG_VERSION")
        );
        assert_eq!(marker.encode(), expected);
        assert_eq!(FormatMarker::parse(&expected).unwrap().kind, Kind::Kv);
        assert_eq!(
            FormatMarker::parse("format = 2\nkind = \"kv\"\n")
                .unwrap()
                .kind,
            Kind::Kv
        );
        let dir = tempfile::tempdir().unwrap();
        ensure_format(&StdVfs, dir.path(), Kind::Kv).unwrap();
        assert_eq!(
            std::fs::read(dir.path().join(FORMAT_FILE)).unwrap(),
            expected.as_bytes()
        );
    }

    #[test]
    fn stream_creation_writes_its_kind() {
        let dir = tempfile::tempdir().unwrap();
        let marker = ensure_format(&StdVfs, dir.path(), Kind::Stream).unwrap();
        assert_eq!(marker.kind, Kind::Stream);
        assert!(std::fs::read_to_string(dir.path().join(FORMAT_FILE))
            .unwrap()
            .contains("\nkind = \"stream\"\nchecksum = \""));
        assert_eq!(
            ensure_format(&StdVfs, dir.path(), Kind::Stream).unwrap(),
            marker
        );
    }

    #[test]
    fn kind_mismatches_refuse_without_modifying_any_bytes() {
        for (actual, expected) in [(Kind::Kv, Kind::Stream), (Kind::Stream, Kind::Kv)] {
            let dir = tempfile::tempdir().unwrap();
            let marker = FormatMarker::current_for(actual).encode();
            std::fs::write(dir.path().join(FORMAT_FILE), &marker).unwrap();
            std::fs::write(dir.path().join(FORMAT_TMP_FILE), b"preserve stale temp").unwrap();
            std::fs::write(dir.path().join("00000000000000000001.wal"), b"preserve WAL").unwrap();
            for result in [
                check_format_kind(&StdVfs, dir.path(), expected),
                ensure_format(&StdVfs, dir.path(), expected).map(|_| ()),
            ] {
                let Err(StorageError::UnsupportedFormat(message)) = result else {
                    panic!("expected kind refusal, got {result:?}");
                };
                assert_eq!(
                    message,
                    format!(
                        "data directory {} is {}, not {}",
                        dir.path().display(),
                        actual.description(),
                        expected.description()
                    )
                );
            }
            assert_eq!(
                std::fs::read(dir.path().join(FORMAT_FILE)).unwrap(),
                marker.as_bytes()
            );
            assert_eq!(
                std::fs::read(dir.path().join(FORMAT_TMP_FILE)).unwrap(),
                b"preserve stale temp"
            );
            assert_eq!(
                std::fs::read(dir.path().join("00000000000000000001.wal")).unwrap(),
                b"preserve WAL"
            );
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 3);
            if actual == Kind::Stream {
                assert!(matches!(
                    check_format(&StdVfs, dir.path()),
                    Err(StorageError::UnsupportedFormat(_))
                ));
            }
        }
    }

    #[test]
    fn incomplete_kind_quotes_fail_closed() {
        for text in [
            "format = 2\nkind = \"stream\n",
            "format = 2\nkind = stream\"\n",
        ] {
            assert_eq!(FormatMarker::parse(text), None);
        }
    }

    #[test]
    fn unknown_kind_errors_name_supported_kinds_and_prioritize_versions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FORMAT_FILE);
        std::fs::write(&path, "format = 2\nkind = \"table\"\n").unwrap();
        let error = read_format(dir.path()).unwrap_err().to_string();
        assert!(
            error.contains("kind \"table\"") && error.contains("kv") && error.contains("stream"),
            "{error}"
        );
        std::fs::write(&path, "kind = \"table\"\nformat = 3\n").unwrap();
        let error = read_format(dir.path()).unwrap_err().to_string();
        assert!(error.contains("newer PrkDB (format 3)"), "{error}");
    }

    #[test]
    fn unknown_kind_refuses_without_modifying_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FORMAT_FILE);
        let marker = b"format = 2\ncreated_by = \"future\"\nkind = \"future\"\n";
        std::fs::write(&path, marker).unwrap();
        let result = ensure_format(&StdVfs, dir.path(), Kind::Kv);
        assert!(
            matches!(result, Err(StorageError::UnsupportedFormat(_))),
            "{result:?}"
        );
        assert_eq!(std::fs::read(path).unwrap(), marker);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn duplicate_semantic_fields_are_rejected() {
        for text in [
            "format = 2\nformat = 2\n",
            "format = 2\nkind = \"kv\"\nkind = \"stream\"\n",
            "format = 2\ncreated_by = \"a\"\ncreated_by = \"b\"\n",
        ] {
            assert_eq!(FormatMarker::parse(text), None, "{text}");
        }
    }

    /// Writes `bytes` as the marker of a fresh directory and reads it back.
    fn read_marker_bytes(bytes: &[u8]) -> Result<Option<FormatMarker>, StorageError> {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(FORMAT_FILE), bytes).unwrap();
        read_format(dir.path())
    }

    #[test]
    fn a_stream_marker_ends_with_a_checksum_of_everything_before_it() {
        let text = FormatMarker::current_for(Kind::Stream).encode();
        let (body, last) = text
            .trim_end_matches('\n')
            .rsplit_once('\n')
            .expect("more than one line");
        let body = format!("{body}\n");
        assert_eq!(
            last,
            format!("checksum = \"{:08x}\"", crc32fast::hash(body.as_bytes()))
        );
        assert!(body.ends_with("kind = \"stream\"\n"), "{text}");
        assert_eq!(
            FormatMarker::parse(&text),
            Some(FormatMarker::current_for(Kind::Stream))
        );
    }

    #[test]
    fn no_single_bit_flip_turns_a_stream_marker_into_a_kv_marker() {
        let text = FormatMarker::current_for(Kind::Stream).encode();
        for i in 0..text.len() {
            for bit in 0..8 {
                let mut bytes = text.clone().into_bytes();
                bytes[i] ^= 1 << bit;
                if let Ok(Some(marker)) = read_marker_bytes(&bytes) {
                    assert_eq!(
                        marker.kind,
                        Kind::Stream,
                        "flipping bit {bit} of byte {i} read a stream marker as {:?}: {:?}",
                        marker.kind,
                        String::from_utf8_lossy(&bytes)
                    );
                }
            }
        }
    }

    #[test]
    fn a_stream_marker_without_its_checksum_is_refused() {
        let text = format!(
            "format = 2\ncreated_by = \"{}\"\nkind = \"stream\"\n",
            env!("CARGO_PKG_VERSION")
        );
        let result = read_marker_bytes(text.as_bytes());
        assert!(
            matches!(result, Err(StorageError::Corruption(_))),
            "{result:?}"
        );
    }

    #[test]
    fn a_checksum_that_is_wrong_or_not_last_is_refused() {
        let good = FormatMarker::current_for(Kind::Stream).encode();
        let wrong = good.replace("created_by = \"", "created_by = \"x");
        let (body, last) = good.trim_end_matches('\n').rsplit_once('\n').unwrap();
        let not_last = format!("{last}\n{body}\n");
        let with_trailer = format!("{good}note = \"after\"\n");
        for text in [wrong, not_last, with_trailer] {
            let result = read_marker_bytes(text.as_bytes());
            assert!(
                matches!(result, Err(StorageError::Corruption(_))),
                "{text:?} → {result:?}"
            );
        }
    }

    #[test]
    fn a_kv_marker_with_a_valid_checksum_is_accepted() {
        // A later version may checksum kv markers too; the rule is the same for every kind.
        let body = format!(
            "format = 2\ncreated_by = \"{}\"\n",
            env!("CARGO_PKG_VERSION")
        );
        let text = format!(
            "{body}checksum = \"{:08x}\"\n",
            crc32fast::hash(body.as_bytes())
        );
        assert_eq!(
            read_marker_bytes(text.as_bytes()).unwrap(),
            Some(FormatMarker::current())
        );
    }

    #[test]
    fn the_marker_round_trips_and_ignores_unknown_keys() {
        let m = FormatMarker::current();
        assert_eq!(FormatMarker::parse(&m.encode()), Some(m));
        let later = "format = 2\ncreated_by = \"1.0.0\"\ncompression = \"zstd\"\n";
        assert_eq!(
            FormatMarker::parse(later),
            Some(FormatMarker {
                format: 2,
                kind: Kind::Kv,
                created_by: "1.0.0".into()
            })
        );
    }

    #[test]
    fn a_marker_without_a_numeric_format_does_not_parse() {
        assert_eq!(FormatMarker::parse("created_by = \"1\"\n"), None);
        assert_eq!(FormatMarker::parse("format = two\n"), None);
    }

    #[test]
    fn a_quoted_integer_format_parses() {
        assert_eq!(
            FormatMarker::parse("format = \"3\"\n").map(|m| m.format),
            Some(3)
        );
    }

    #[test]
    fn lost_and_found_and_dotfiles_do_not_count_as_data() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("lost+found")).unwrap();
        std::fs::write(dir.path().join(".DS_Store"), b"x").unwrap();
        assert!(!holds_data(&StdVfs, dir.path()).unwrap());
        std::fs::write(dir.path().join("00000000000000000001.wal"), b"x").unwrap();
        assert!(holds_data(&StdVfs, dir.path()).unwrap());
    }

    #[test]
    fn the_lock_file_does_not_count_as_data() {
        let dir = tempfile::tempdir().unwrap();
        let _held = crate::storage::lock::lock_data_dir(&StdVfs, dir.path()).unwrap();
        assert!(!holds_data(&StdVfs, dir.path()).unwrap());
        assert_eq!(detect_format(&StdVfs, dir.path()).unwrap(), None);
        assert_eq!(
            ensure_format(&StdVfs, dir.path(), Kind::Kv).unwrap(),
            FormatMarker::current()
        );
    }

    #[test]
    fn older_and_newer_refusals_follow_the_spec_wording() {
        let dir = Path::new("/data");
        assert_eq!(
            unsupported_format(dir, 1).to_string(),
            "data directory /data was created by an older PrkDB (format 1); this version \
             reads format 2. See docs/guide/upgrade."
        );
        assert!(unsupported_format(dir, 7)
            .to_string()
            .contains("newer PrkDB (format 7)"));
    }

    #[test]
    fn read_format_on_a_missing_directory_is_none_and_creates_nothing() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("absent");
        assert_eq!(read_format(&dir).unwrap(), None);
        assert!(!dir.exists());
    }

    #[test]
    fn ensure_format_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let first = ensure_format(&StdVfs, dir.path(), Kind::Kv).unwrap();
        let again = ensure_format(&StdVfs, dir.path(), Kind::Kv).unwrap();
        assert_eq!(first, again);
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            1,
            "only FORMAT, no temp file left behind"
        );
    }
}
