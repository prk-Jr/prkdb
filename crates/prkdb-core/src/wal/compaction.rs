use super::mmap_parallel_wal::MmapParallelWal;
use super::WalError;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Configuration for WAL compaction
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Minimum size of WAL before compaction is triggered (bytes)
    pub min_wal_size_bytes: u64,
    /// Minimum time interval between compaction runs
    pub min_interval: Duration,
    /// Number of segments to keep (history)
    pub keep_segments: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            min_wal_size_bytes: 100 * 1024 * 1024,  // 100 MB
            min_interval: Duration::from_secs(300), // 5 minutes
            keep_segments: 2,                       // Keep at least 2 segments per partition
        }
    }
}

/// Manages WAL compaction and cleanup
pub struct Compactor {
    wal: Arc<MmapParallelWal>,
    config: CompactionConfig,
    last_compaction: Mutex<Instant>,
}

impl Compactor {
    pub fn new(wal: Arc<MmapParallelWal>, config: CompactionConfig) -> Self {
        Self {
            wal,
            config,
            last_compaction: Mutex::new(Instant::now()),
        }
    }

    /// Check if compaction is needed and run it if so
    pub async fn run_if_needed(&self, current_offset: u64) -> Result<bool, WalError> {
        let mut last_run = self.last_compaction.lock().await;

        if last_run.elapsed() < self.config.min_interval {
            return Ok(false);
        }

        // Phase 16: Check total WAL size before triggering compaction
        let total_wal_size = self.wal.total_size().await;
        if total_wal_size < self.config.min_wal_size_bytes {
            tracing::debug!(
                "WAL size {}MB is below threshold {}MB, skipping compaction",
                total_wal_size / (1024 * 1024),
                self.config.min_wal_size_bytes / (1024 * 1024)
            );
            return Ok(false);
        }

        tracing::info!(
            "WAL size {}MB exceeds threshold, triggering compaction",
            total_wal_size / (1024 * 1024)
        );

        // Calculate safe truncation offset
        // We want to keep some history, so we don't delete everything up to current_offset
        // This is a simplified logic; in a real DB we'd use the checkpoint offset
        let truncate_offset = current_offset.saturating_sub(1000);

        if truncate_offset > 0 {
            self.wal.truncate_before(truncate_offset).await?;
            *last_run = Instant::now();
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
