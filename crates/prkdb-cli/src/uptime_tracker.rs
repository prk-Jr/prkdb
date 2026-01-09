use anyhow::Result;
use prkdb_storage_sled::SledAdapter;
use prkdb_types::storage::StorageAdapter;
use std::time::{SystemTime, UNIX_EPOCH};

const STARTUP_TIMESTAMP_KEY: &[u8] = b"__prkdb_metadata:startup_timestamp";

/// Track database startup time for uptime calculation
pub async fn record_startup_time(storage: &SledAdapter) -> Result<()> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .to_string();

    storage
        .put(STARTUP_TIMESTAMP_KEY, timestamp.as_bytes())
        .await?;
    Ok(())
}

/// Get database uptime in seconds
pub async fn get_uptime_seconds(storage: &SledAdapter) -> Result<u64> {
    match storage.get(STARTUP_TIMESTAMP_KEY).await? {
        Some(timestamp_bytes) => {
            let timestamp_str = String::from_utf8(timestamp_bytes)?;
            let startup_timestamp: u64 = timestamp_str.parse()?;
            let current_timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

            Ok(current_timestamp.saturating_sub(startup_timestamp))
        }
        None => {
            // No startup timestamp recorded, record it now
            record_startup_time(storage).await?;
            Ok(0)
        }
    }
}

/// Format uptime in human-readable format
pub fn format_uptime(seconds: u64) -> String {
    if seconds == 0 {
        return "Just started".to_string();
    }

    let days = seconds / 86400;
    let hours = (seconds % 86400) / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;

    if days > 0 {
        format!("{}d {}h {}m {}s", days, hours, minutes, secs)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, secs)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, secs)
    } else {
        format!("{}s", secs)
    }
}
