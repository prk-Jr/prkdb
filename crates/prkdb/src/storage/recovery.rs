//! Health checks and backups for the single WAL (Task 2.8a).
//!
//! Recovery itself is not here: it is `Wal::open`, which every constructor runs. A torn
//! tail on the last segment is truncated there, and corruption anywhere earlier refuses
//! the open naming the file. Nothing repairs mid-log corruption automatically.

use prkdb_core::vfs::{OpenMode, Vfs};
use prkdb_core::wal::segment::{parse_segment_file_name, scan_segment};
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
    /// Returns `Err(StorageError::Corruption)` naming the segment file for any fault,
    /// including one in the last segment: on a running database a fault there means the
    /// file changed under the writer.
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

        for (first_lsn, path) in segments {
            let file = self
                .vfs
                .open(&path, OpenMode::Read)
                .map_err(|e| StorageError::Corruption(format!("{}: {e}", path.display())))?;
            let scan = scan_segment(&*file, &path, first_lsn, &mut |_, _, _| Ok(()))
                .map_err(|e| StorageError::Corruption(e.to_string()))?;
            if let Some((offset, fault)) = scan.stopped {
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
