use prkdb_core::wal::mmap_parallel_wal::MmapParallelWal;
use prkdb_types::error::StorageError;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, instrument, warn};

/// Manages recovery and health checks for the storage engine
pub struct RecoveryManager {
    wal: Arc<MmapParallelWal>,
    log_dir: PathBuf,
}

impl RecoveryManager {
    pub fn new(wal: Arc<MmapParallelWal>, log_dir: PathBuf) -> Self {
        Self { wal, log_dir }
    }

    /// Run a full health check on the WAL
    /// Returns Ok if healthy, Err(StorageError::Corruption) if corrupted
    #[instrument(skip(self))]
    pub async fn check_health(&self) -> Result<(), StorageError> {
        info!("Starting WAL health check");

        // Verify all segments
        let result = self.wal.verify_segments().await;

        match &result {
            Ok(_) => info!("WAL health check passed"),
            Err(e) => warn!("WAL health check failed: {}", e),
        }

        result.map_err(|e| StorageError::Corruption(e.to_string()))
    }

    /// Attempt to recover from corrupted segments
    #[instrument(skip(self))]
    pub async fn recover(&self) -> Result<(), StorageError> {
        info!("Starting WAL recovery");

        let result = self.wal.repair_segments().await;

        match &result {
            Ok(_) => info!("WAL recovery completed successfully"),
            Err(e) => warn!("WAL recovery failed: {}", e),
        }

        result.map_err(|e| StorageError::Recovery(e.to_string()))
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
