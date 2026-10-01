//! The data-directory format marker (spec §7 2b, D3).
//!
//! Every data directory holds a `FORMAT` file:
//!
//! ```text
//! format = 2
//! created_by = "0.6.0"
//! ```
//!
//! Two `key = value` lines, parsed by hand. Unknown keys are ignored so a later version
//! can add fields. The number is [`FORMAT_VERSION`], the same one written into every WAL
//! segment header, so the program has exactly one format version.
//!
//! # Open rules ([`ensure_format`])
//!
//! - absent or empty directory: created as format 2, `FORMAT` written before anything else;
//! - `FORMAT` says 2: opens;
//! - no `FORMAT` on a non-empty directory (format 1, which had no marker) or any other
//!   number: refused with [`StorageError::UnsupportedFormat`] before a single byte is
//!   written. A format-1 log must never be shadowed by an empty format-2 log next to it,
//!   which would make the database look wiped.

use prkdb_core::vfs::{OpenMode, StdVfs, Vfs};
use prkdb_types::error::StorageError;
use std::path::{Path, PathBuf};

pub use prkdb_core::format::FORMAT_VERSION;

/// The marker's file name, at the data directory's root.
pub const FORMAT_FILE: &str = "FORMAT";

/// Where the marker is written before it is renamed into place. A directory holding only
/// this file was interrupted while being created, and counts as empty.
const FORMAT_TMP_FILE: &str = "FORMAT.tmp";

/// Far more than two short lines; anything larger is not a marker this code wrote.
const MAX_FORMAT_BYTES: u64 = 4096;

/// The contents of a data directory's `FORMAT` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatMarker {
    pub format: u32,
    /// The `CARGO_PKG_VERSION` of the build that created the directory. Informational.
    pub created_by: String,
}

impl FormatMarker {
    /// The marker this build writes.
    pub fn current() -> Self {
        Self {
            format: FORMAT_VERSION,
            created_by: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    fn encode(&self) -> String {
        format!(
            "format = {}\ncreated_by = \"{}\"\n",
            self.format, self.created_by
        )
    }

    fn parse(text: &str) -> Option<Self> {
        let mut format = None;
        let mut created_by = String::new();
        for line in text.lines() {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            match key.trim() {
                "format" => format = Some(value.trim().parse::<u32>().ok()?),
                "created_by" => created_by = value.trim().trim_matches('"').to_string(),
                _ => {}
            }
        }
        Some(Self {
            format: format?,
            created_by,
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
    std::str::from_utf8(&buf)
        .ok()
        .and_then(FormatMarker::parse)
        .map(Some)
        .ok_or_else(|| {
            StorageError::Corruption(format!(
                "{}: unreadable format marker (expected a `format = <number>` line)",
                path.display()
            ))
        })
}

/// The open rules: an absent or empty directory is created as format 2 (FORMAT written
/// atomically: `FORMAT.tmp` create → write → sync_data → rename → sync_dir); `FORMAT == 2`
/// opens; anything else is refused before a single byte is written.
///
/// "Empty" means `read_dir` returns nothing, or only a stale `FORMAT.tmp` from a crash
/// during creation (removed first). A directory this call creates has its parent synced,
/// as `Wal::open` does, so the marker cannot outlive its own directory entry.
pub fn ensure_format(vfs: &dyn Vfs, dir: &Path) -> Result<FormatMarker, StorageError> {
    if !vfs.exists(dir).map_err(|e| io_err(dir, e))? {
        vfs.create_dir_all(dir).map_err(|e| io_err(dir, e))?;
        if let Some(parent) = dir.parent() {
            if vfs.exists(parent).map_err(|e| io_err(parent, e))? {
                vfs.sync_dir(parent).map_err(|e| io_err(parent, e))?;
            }
        }
        return write_format(vfs, dir);
    }

    if let Some(marker) = read_format_with(vfs, dir)? {
        if marker.format == FORMAT_VERSION {
            return Ok(marker);
        }
        return Err(unsupported_format(dir, marker.format));
    }

    let tmp = dir.join(FORMAT_TMP_FILE);
    let entries: Vec<PathBuf> = vfs.read_dir(dir).map_err(|e| io_err(dir, e))?;
    if entries.iter().any(|p| *p != tmp) {
        return Err(unsupported_format(dir, 1));
    }
    if !entries.is_empty() {
        vfs.remove(&tmp).map_err(|e| io_err(&tmp, e))?;
    }
    write_format(vfs, dir)
}

/// `FORMAT.tmp` create → write → sync_data → rename to `FORMAT` → sync_dir. A crash at any
/// point leaves either no `FORMAT` (and at most a stale temp file, which counts as empty)
/// or a complete one.
fn write_format(vfs: &dyn Vfs, dir: &Path) -> Result<FormatMarker, StorageError> {
    let marker = FormatMarker::current();
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
    fn the_marker_round_trips_and_ignores_unknown_keys() {
        let m = FormatMarker::current();
        assert_eq!(FormatMarker::parse(&m.encode()), Some(m));
        let later = "format = 2\ncreated_by = \"1.0.0\"\nchecksum = \"abc\"\n";
        assert_eq!(
            FormatMarker::parse(later),
            Some(FormatMarker {
                format: 2,
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
        let first = ensure_format(&StdVfs, dir.path()).unwrap();
        let again = ensure_format(&StdVfs, dir.path()).unwrap();
        assert_eq!(first, again);
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            1,
            "only FORMAT, no temp file left behind"
        );
    }
}
