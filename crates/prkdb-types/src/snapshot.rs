use serde::{Deserialize, Serialize};

/// Format version for compatibility check
pub const SNAPSHOT_VERSION: u32 = 1;

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, bincode::Encode, bincode::Decode,
)]
pub enum CompressionType {
    None,
    Gzip,
}

#[derive(Debug, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct SnapshotHeader {
    pub version: u32,
    pub created_at: u64,
    pub max_offset: u64,
    pub index_entries: u64,
    pub compression: CompressionType,
}

impl SnapshotHeader {
    pub fn new(max_offset: u64, index_entries: u64, compression: CompressionType) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            version: SNAPSHOT_VERSION,
            created_at,
            max_offset,
            index_entries,
            compression,
        }
    }
}
