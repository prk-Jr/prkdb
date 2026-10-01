//! The data-directory lock (STO-10).
//!
//! A data directory is used by one open at a time: every open takes an exclusive advisory
//! lock on `<dir>/LOCK` (through [`Vfs::lock_exclusive`]) before anything else touches the
//! directory, and holds it for the adapter's lifetime. A second open, from this process
//! or another, is refused with [`StorageError::Locked`] instead of interleaving its frames
//! into the same WAL segments. `prkdb-cli migrate` takes the same lock, so it cannot run
//! against a live database.
//!
//! The lock is advisory and lives in the OS, not in the file: `LOCK` existing means
//! nothing, and a process that dies (even by `SIGKILL`) releases it. The file's content
//! is the holder's pid, written best effort for the refusal message. `LOCK` is not data:
//! the format emptiness check ignores it.

use prkdb_core::vfs::{LockGuard, OpenMode, Vfs};
use prkdb_types::error::StorageError;
use std::path::Path;

/// The lock file's name, at the data directory's root.
pub const LOCK_FILE: &str = "LOCK";

/// A pid is at most 10 digits; anything longer is not one this code wrote.
const MAX_PID_BYTES: usize = 32;

/// Takes `dir`'s lock. `dir` must exist. Released when the guard drops.
pub fn lock_data_dir(vfs: &dyn Vfs, dir: &Path) -> Result<Box<dyn LockGuard>, StorageError> {
    let path = dir.join(LOCK_FILE);
    match vfs.lock_exclusive(&path) {
        Ok(guard) => Ok(guard),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            let holder = match holder_pid(vfs, &path) {
                Some(pid) => format!("pid {pid}"),
                None => "pid unknown".to_string(),
            };
            Err(StorageError::Locked(format!(
                "data directory {} is in use by another process ({holder}); close it \
                 before opening the directory again",
                dir.display()
            )))
        }
        Err(e) => Err(StorageError::Internal(format!("{}: {e}", path.display()))),
    }
}

/// The pid the holder wrote into `LOCK`, if it can be read (Windows refuses reads of a
/// locked file) and parses.
fn holder_pid(vfs: &dyn Vfs, path: &Path) -> Option<u32> {
    let file = vfs.open(path, OpenMode::Read).ok()?;
    let mut buf = [0u8; MAX_PID_BYTES];
    let n = file.read_at(0, &mut buf).ok()?;
    std::str::from_utf8(&buf[..n]).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use prkdb_core::vfs::StdVfs;

    #[test]
    fn a_held_lock_is_refused_with_the_holders_pid_and_released_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let held = lock_data_dir(&StdVfs, dir.path()).unwrap();
        let err = lock_data_dir(&StdVfs, dir.path()).err().unwrap();
        let StorageError::Locked(msg) = err else {
            panic!("expected Locked, got {err:?}");
        };
        assert!(msg.contains(&dir.path().display().to_string()), "{msg}");
        // Windows refuses reads of a locked file, so there the pid is "unknown".
        if cfg!(unix) {
            assert!(
                msg.contains(&format!("pid {}", std::process::id())),
                "{msg}"
            );
        }
        drop(held);
        drop(lock_data_dir(&StdVfs, dir.path()).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn an_unreadable_pid_is_reported_as_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let _held = lock_data_dir(&StdVfs, dir.path()).unwrap();
        std::fs::write(dir.path().join(LOCK_FILE), b"not a pid").unwrap();
        let err = lock_data_dir(&StdVfs, dir.path()).err().unwrap();
        assert!(err.to_string().contains("pid unknown"), "{err}");
    }

    #[test]
    fn a_missing_directory_is_an_error_not_created() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("absent");
        assert!(lock_data_dir(&StdVfs, &dir).is_err());
        assert!(!dir.exists());
    }
}
