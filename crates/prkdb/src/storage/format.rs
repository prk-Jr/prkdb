//! The data-directory format marker (spec §7 2b, D3).
//!
//! Every data directory holds a `FORMAT` file:
//!
//! ```text
//! format = 2
//! created_by = "0.6.0"
//! ```
//!
//! KV markers keep two `key = value` lines; streams add `kind = "stream"`. A missing
//! `kind` is KV. Unknown kinds are refused. Unknown keys are ignored so a later version
//! can add fields. The number is [`FORMAT_VERSION`], the same one written into every WAL
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
    fn description(self) -> &'static str {
        match self {
            Self::Kv => "a key/value store",
            Self::Stream => "a stream",
        }
    }
}

enum MarkerParseError {
    Invalid,
    UnknownKind { kind: String, format: u32 },
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
        }
        text
    }

    #[cfg(test)]
    fn parse(text: &str) -> Option<Self> {
        Self::parse_checked(text).ok()
    }

    fn parse_checked(text: &str) -> Result<Self, MarkerParseError> {
        let mut format = None;
        let mut created_by = None;
        let mut kind = None;
        for line in text.lines() {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            match key.trim() {
                "format" => {
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
        let format = format.ok_or(MarkerParseError::Invalid)?;
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
    FormatMarker::parse_checked(text)
        .map(Some)
        .map_err(|e| match e {
            MarkerParseError::Invalid => unreadable(),
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
/// open rules would create as format 2).
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
        return write_format(vfs, dir, expected);
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
    write_format(vfs, dir, expected)
}

/// `FORMAT.tmp` create → write → sync_data → rename to `FORMAT` → sync_dir. A crash at any
/// point leaves either no `FORMAT` (and at most a stale temp file, which counts as empty)
/// or a complete one.
fn write_format(vfs: &dyn Vfs, dir: &Path, kind: Kind) -> Result<FormatMarker, StorageError> {
    let marker = FormatMarker::current_for(kind);
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
            .ends_with("kind = \"stream\"\n"));
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

    #[test]
    fn the_marker_round_trips_and_ignores_unknown_keys() {
        let m = FormatMarker::current();
        assert_eq!(FormatMarker::parse(&m.encode()), Some(m));
        let later = "format = 2\ncreated_by = \"1.0.0\"\nchecksum = \"abc\"\n";
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
