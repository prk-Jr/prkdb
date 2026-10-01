//! Health checks and backups for the single WAL (Task 2.8a).
//!
//! Recovery itself is not here: it is `Wal::open`, which every constructor runs. A torn
//! tail on the last segment is truncated there, and corruption anywhere earlier refuses
//! the open naming the file. Nothing repairs mid-log corruption automatically.

use prkdb_core::vfs::{OpenMode, Vfs};
use prkdb_core::wal::segment::{parse_segment_file_name, scan_segment, SEGMENT_HEADER_LEN};
use prkdb_core::wal::Lsn;
use prkdb_types::error::StorageError;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, instrument, warn};

/// Manages health checks and backups for the storage engine
pub struct RecoveryManager {
    vfs: Arc<dyn Vfs>,
    log_dir: PathBuf,
}

impl RecoveryManager {
    pub fn new(vfs: Arc<dyn Vfs>, log_dir: PathBuf) -> Self {
        Self { vfs, log_dir }
    }

    /// Re-scan every segment and verify every frame (CRC and LSN).
    ///
    /// Returns `Err(StorageError::Corruption)` naming the segment file for any fault in a
    /// sealed segment. The active (highest-numbered) segment is allowed a torn tail or a
    /// missing header, which a write or roll in flight produces; a bad header there is
    /// still corruption.
    #[instrument(skip(self))]
    pub async fn check_health(&self) -> Result<(), StorageError> {
        info!("Starting WAL health check");
        let result = self.scan_all();
        match &result {
            Ok(()) => info!("WAL health check passed"),
            Err(e) => warn!("WAL health check failed: {}", e),
        }
        result
    }

    fn scan_all(&self) -> Result<(), StorageError> {
        let entries = self
            .vfs
            .read_dir(&self.log_dir)
            .map_err(|e| StorageError::Corruption(format!("{}: {e}", self.log_dir.display())))?;
        let mut segments: Vec<(Lsn, PathBuf)> = entries
            .into_iter()
            .filter_map(|path| {
                let first = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(parse_segment_file_name)?;
                Some((first, path))
            })
            .collect();
        segments.sort();

        let last = segments.len().saturating_sub(1);
        for (idx, (first_lsn, path)) in segments.into_iter().enumerate() {
            let active = idx == last;
            let file = self
                .vfs
                .open(&path, OpenMode::Read)
                .map_err(|e| StorageError::Corruption(format!("{}: {e}", path.display())))?;
            // The active segment may be mid-roll (created, header not yet written) or
            // mid-write (a frame partly written at its tail). Neither is corruption on a
            // running database; the open path handles both after a crash.
            let len = file
                .len()
                .map_err(|e| StorageError::Corruption(format!("{}: {e}", path.display())))?;
            if active && len < SEGMENT_HEADER_LEN {
                continue;
            }
            let scan = scan_segment(&*file, &path, first_lsn, &mut |_, _, _| Ok(()))
                .map_err(|e| StorageError::Corruption(e.to_string()))?;
            if let Some((offset, fault)) = scan.stopped.filter(|_| !active) {
                return Err(StorageError::Corruption(format!(
                    "{} at byte {offset}: {fault:?}",
                    path.display()
                )));
            }
        }
        Ok(())
    }

    /// There is no in-place repair.
    ///
    /// The old `repair_segments` truncated each segment at its first bad record, wherever
    /// it was, silently discarding every record after it. The single WAL's rules forbid
    /// that: a torn tail is truncated by the open path, and anything else refuses to open.
    #[instrument(skip(self))]
    pub async fn recover(&self) -> Result<(), StorageError> {
        Err(StorageError::Recovery(
            "run the database open path; torn tails are truncated there and mid-log \
             corruption is not repaired automatically"
                .to_string(),
        ))
    }

    /// Create a backup of the current WAL state
    #[instrument(skip(self), fields(backup_dir = %backup_dir.display()))]
    pub async fn create_backup(&self, backup_dir: PathBuf) -> Result<(), StorageError> {
        info!("Creating WAL backup");

        // Create backup directory if it doesn't exist
        std::fs::create_dir_all(&backup_dir)
            .map_err(|e| StorageError::Internal(format!("Failed to create backup dir: {}", e)))?;

        // Copy all log files to backup directory
        let entries = std::fs::read_dir(&self.log_dir)
            .map_err(|e| StorageError::Internal(format!("Failed to read log dir: {}", e)))?;

        let mut file_count = 0;
        for entry in entries {
            let entry = entry
                .map_err(|e| StorageError::Internal(format!("Failed to read entry: {}", e)))?;
            let path = entry.path();

            if path.is_file() {
                let filename = path
                    .file_name()
                    .ok_or_else(|| StorageError::Internal("Invalid filename".to_string()))?;
                let backup_path = backup_dir.join(filename);

                std::fs::copy(&path, &backup_path)
                    .map_err(|e| StorageError::Internal(format!("Failed to copy file: {}", e)))?;
                file_count += 1;
            }
        }

        info!(
            "WAL backup created successfully. Copied {} files",
            file_count
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prkdb_core::vfs::StdVfs;
    use prkdb_core::wal::segment::{segment_file_name, write_segment_header};

    fn manager(dir: &std::path::Path) -> RecoveryManager {
        RecoveryManager::new(Arc::new(StdVfs), dir.to_path_buf())
    }

    /// A roll in flight leaves the new, highest-numbered segment without a header; a
    /// write in flight leaves a partial frame at its tail. Neither is corruption.
    #[tokio::test]
    async fn the_active_segment_may_be_headerless_or_torn() {
        let dir = tempfile::tempdir().unwrap();
        let sealed = StdVfs
            .create(&dir.path().join(segment_file_name(1)))
            .unwrap();
        write_segment_header(&*sealed, 1).unwrap();
        let active = StdVfs
            .create(&dir.path().join(segment_file_name(2)))
            .unwrap();
        manager(dir.path())
            .check_health()
            .await
            .expect("a headerless active segment is a roll in flight");

        write_segment_header(&*active, 2).unwrap();
        active.write_at(SEGMENT_HEADER_LEN, &[0xAB; 7]).unwrap();
        manager(dir.path())
            .check_health()
            .await
            .expect("a torn tail on the active segment is a write in flight");
    }

    /// The same faults in a sealed segment are corruption, named by file.
    #[tokio::test]
    async fn a_sealed_segment_must_be_whole() {
        let dir = tempfile::tempdir().unwrap();
        let sealed = StdVfs
            .create(&dir.path().join(segment_file_name(1)))
            .unwrap();
        write_segment_header(&*sealed, 1).unwrap();
        sealed.write_at(SEGMENT_HEADER_LEN, &[0xAB; 7]).unwrap();
        let active = StdVfs
            .create(&dir.path().join(segment_file_name(2)))
            .unwrap();
        write_segment_header(&*active, 2).unwrap();

        let error = manager(dir.path())
            .check_health()
            .await
            .expect_err("a torn sealed segment is corruption");
        assert!(
            matches!(error, StorageError::Corruption(ref m) if m.contains(&segment_file_name(1))),
            "{error}"
        );
    }
}
