//! Checkpoint persistence for fast WAL recovery
//!
//! Checkpoints store the last-known-good offsets for each WAL segment,
//! allowing recovery to skip already-indexed records.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Checkpoint data for WAL recovery optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Version for forward/backward compatibility
    pub version: u32,
    /// Per-segment offsets: segment_id -> last_processed_offset
    pub segment_offsets: HashMap<usize, u64>,
    /// Max global offset at checkpoint time
    pub max_offset: u64,
    /// Timestamp of checkpoint creation (unix millis)
    pub created_at: u64,
}

impl Checkpoint {
    /// Current checkpoint format version
    pub const VERSION: u32 = 1;

    /// Create a new checkpoint from segment offsets
    pub fn new(segment_offsets: HashMap<usize, u64>, max_offset: u64) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            version: Self::VERSION,
            segment_offsets,
            max_offset,
            created_at,
        }
    }

    /// Create checkpoint from WAL state
    pub fn from_segment_count(segment_count: usize, max_offset: u64) -> Self {
        // Each segment gets the max_offset as its starting point
        // This is a simplified approach - in practice, you might track per-segment offsets
        let segment_offsets: HashMap<usize, u64> =
            (0..segment_count).map(|i| (i, max_offset)).collect();

        Self::new(segment_offsets, max_offset)
    }

    /// Get start offset for a specific segment
    pub fn get_segment_offset(&self, segment_id: usize) -> u64 {
        self.segment_offsets.get(&segment_id).copied().unwrap_or(0)
    }

    /// Build start_offsets array for scan_from
    pub fn to_start_offsets(&self, segment_count: usize) -> Vec<u64> {
        (0..segment_count)
            .map(|i| self.get_segment_offset(i))
            .collect()
    }
}

/// Save checkpoint to file
///
/// Uses atomic write (write to temp, then rename) to prevent corruption.
pub fn save_checkpoint(path: &Path, checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
    use std::io::Write;

    // Serialize to JSON
    let json = serde_json::to_string_pretty(checkpoint)
        .map_err(|e| CheckpointError::Serialization(e.to_string()))?;

    // Calculate checksum
    let checksum = crc32fast::hash(json.as_bytes());

    // Write with checksum suffix
    let content = format!("{}\n#{:08x}", json, checksum);

    // Atomic write: temp file then rename
    let temp_path = path.with_extension("checkpoint.tmp");

    let mut file =
        std::fs::File::create(&temp_path).map_err(|e| CheckpointError::Io(e.to_string()))?;

    file.write_all(content.as_bytes())
        .map_err(|e| CheckpointError::Io(e.to_string()))?;

    file.sync_all()
        .map_err(|e| CheckpointError::Io(e.to_string()))?;

    drop(file);

    // Rename to final location (atomic on most filesystems)
    std::fs::rename(&temp_path, path).map_err(|e| CheckpointError::Io(e.to_string()))?;

    tracing::debug!(
        "Saved checkpoint: {} segments, max_offset={}",
        checkpoint.segment_offsets.len(),
        checkpoint.max_offset
    );

    Ok(())
}

/// Load checkpoint from file
///
/// Returns `None` if file doesn't exist.
/// Returns error if file exists but is corrupted.
pub fn load_checkpoint(path: &Path) -> Result<Option<Checkpoint>, CheckpointError> {
    if !path.exists() {
        return Ok(None);
    }

    let content = std::fs::read_to_string(path).map_err(|e| CheckpointError::Io(e.to_string()))?;

    // Split content and checksum
    let (json, checksum_line) = content
        .rsplit_once('\n')
        .ok_or_else(|| CheckpointError::Corrupted("Invalid format".to_string()))?;

    // Verify checksum
    let expected_checksum = checksum_line
        .strip_prefix('#')
        .and_then(|s| u32::from_str_radix(s, 16).ok())
        .ok_or_else(|| CheckpointError::Corrupted("Invalid checksum format".to_string()))?;

    let actual_checksum = crc32fast::hash(json.as_bytes());

    if expected_checksum != actual_checksum {
        return Err(CheckpointError::Corrupted(format!(
            "Checksum mismatch: expected {:08x}, got {:08x}",
            expected_checksum, actual_checksum
        )));
    }

    // Deserialize
    let checkpoint: Checkpoint =
        serde_json::from_str(json).map_err(|e| CheckpointError::Serialization(e.to_string()))?;

    // Version check
    if checkpoint.version > Checkpoint::VERSION {
        return Err(CheckpointError::UnsupportedVersion(checkpoint.version));
    }

    tracing::info!(
        "Loaded checkpoint: {} segments, max_offset={}, age={}ms",
        checkpoint.segment_offsets.len(),
        checkpoint.max_offset,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
            .saturating_sub(checkpoint.created_at)
    );

    Ok(Some(checkpoint))
}

/// Errors that can occur during checkpoint operations
#[derive(Debug, Clone)]
pub enum CheckpointError {
    /// I/O error
    Io(String),
    /// Serialization/deserialization error
    Serialization(String),
    /// Checkpoint file is corrupted
    Corrupted(String),
    /// Unsupported checkpoint version
    UnsupportedVersion(u32),
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckpointError::Io(e) => write!(f, "Checkpoint I/O error: {}", e),
            CheckpointError::Serialization(e) => write!(f, "Checkpoint serialization error: {}", e),
            CheckpointError::Corrupted(e) => write!(f, "Checkpoint corrupted: {}", e),
            CheckpointError::UnsupportedVersion(v) => {
                write!(f, "Unsupported checkpoint version: {}", v)
            }
        }
    }
}

impl std::error::Error for CheckpointError {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_checkpoint_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");

        let mut offsets = HashMap::new();
        offsets.insert(0, 1000);
        offsets.insert(1, 2000);
        offsets.insert(2, 1500);

        let checkpoint = Checkpoint::new(offsets, 2000);

        // Save
        save_checkpoint(&path, &checkpoint).unwrap();

        // Load
        let loaded = load_checkpoint(&path).unwrap().unwrap();

        assert_eq!(loaded.version, Checkpoint::VERSION);
        assert_eq!(loaded.max_offset, 2000);
        assert_eq!(loaded.segment_offsets.len(), 3);
        assert_eq!(loaded.get_segment_offset(0), 1000);
        assert_eq!(loaded.get_segment_offset(1), 2000);
        assert_eq!(loaded.get_segment_offset(2), 1500);
        assert_eq!(loaded.get_segment_offset(99), 0); // Non-existent segment
    }

    #[test]
    fn test_checkpoint_missing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");

        let result = load_checkpoint(&path).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_checkpoint_corrupted() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupted.json");

        // Write corrupted content
        std::fs::write(&path, "{}\n#00000000").unwrap();

        let result = load_checkpoint(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_start_offsets() {
        let mut offsets = HashMap::new();
        offsets.insert(0, 100);
        offsets.insert(2, 300);
        // Segment 1 intentionally missing

        let checkpoint = Checkpoint::new(offsets, 300);
        let start_offsets = checkpoint.to_start_offsets(4);

        assert_eq!(start_offsets, vec![100, 0, 300, 0]);
    }
}
