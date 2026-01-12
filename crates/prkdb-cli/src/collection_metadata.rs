use anyhow::Result;
use prkdb_storage_sled::SledAdapter;
use prkdb_types::storage::StorageAdapter;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize, Deserialize, Clone)]
pub struct CollectionMetadata {
    pub name: String,
    pub created_at: u64,            // Unix timestamp
    pub first_item_at: Option<u64>, // When first item was added
    pub last_accessed_at: u64,      // Last time collection was accessed
    pub schema_version: Option<String>,
}

impl CollectionMetadata {
    pub fn new(name: String) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            name,
            created_at: now,
            first_item_at: None,
            last_accessed_at: now,
            schema_version: None,
        }
    }

    pub fn format_created_at(&self) -> String {
        self.format_timestamp(self.created_at)
    }

    #[allow(dead_code)]
    pub fn format_first_item_at(&self) -> String {
        match self.first_item_at {
            Some(timestamp) => self.format_timestamp(timestamp),
            None => "No items yet".to_string(),
        }
    }

    fn format_timestamp(&self, timestamp: u64) -> String {
        // Convert to ISO 8601 format for better readability
        let dt = std::time::UNIX_EPOCH + std::time::Duration::from_secs(timestamp);

        // Format as ISO 8601 string
        match dt.duration_since(std::time::UNIX_EPOCH) {
            Ok(duration) => {
                let seconds = duration.as_secs();
                let days_since_epoch = seconds / 86400;
                let days_since_1970 = days_since_epoch;

                // Simple approximation for demo - in production, use chrono crate
                let year = 1970 + (days_since_1970 / 365);
                let remaining_days = days_since_1970 % 365;
                let month = (remaining_days / 30).min(11) + 1;
                let day = (remaining_days % 30).max(1);

                let hour = (seconds % 86400) / 3600;
                let minute = (seconds % 3600) / 60;
                let second = seconds % 60;

                format!(
                    "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                    year, month, day, hour, minute, second
                )
            }
            Err(_) => "Invalid timestamp".to_string(),
        }
    }
}

fn collection_metadata_key(collection_name: &str) -> Vec<u8> {
    format!("__prkdb_metadata:collection:{}", collection_name).into_bytes()
}

/// Get or create collection metadata
pub async fn get_or_create_collection_metadata(
    storage: &SledAdapter,
    collection_name: &str,
) -> Result<CollectionMetadata> {
    let key = collection_metadata_key(collection_name);

    match storage.get(&key).await? {
        Some(metadata_bytes) => {
            // Try to deserialize existing metadata
            match serde_json::from_slice::<CollectionMetadata>(&metadata_bytes) {
                Ok(mut metadata) => {
                    // Update last accessed time
                    metadata.last_accessed_at = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();

                    // Save updated metadata
                    let updated_bytes = serde_json::to_vec(&metadata)?;
                    storage.put(&key, &updated_bytes).await?;

                    Ok(metadata)
                }
                Err(_) => {
                    // Corrupted metadata, create new
                    let metadata = CollectionMetadata::new(collection_name.to_string());
                    let metadata_bytes = serde_json::to_vec(&metadata)?;
                    storage.put(&key, &metadata_bytes).await?;
                    Ok(metadata)
                }
            }
        }
        None => {
            // Create new metadata
            let metadata = CollectionMetadata::new(collection_name.to_string());
            let metadata_bytes = serde_json::to_vec(&metadata)?;
            storage.put(&key, &metadata_bytes).await?;
            Ok(metadata)
        }
    }
}

/// Update collection metadata when first item is added
#[allow(dead_code)]
pub async fn record_first_item_added(storage: &SledAdapter, collection_name: &str) -> Result<()> {
    let key = collection_metadata_key(collection_name);

    if let Some(metadata_bytes) = storage.get(&key).await? {
        if let Ok(mut metadata) = serde_json::from_slice::<CollectionMetadata>(&metadata_bytes) {
            // Only set first_item_at if it hasn't been set before
            if metadata.first_item_at.is_none() {
                metadata.first_item_at = Some(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                );

                let updated_bytes = serde_json::to_vec(&metadata)?;
                storage.put(&key, &updated_bytes).await?;
            }
        }
    }

    Ok(())
}

/// List all collection metadata
#[allow(dead_code)]
pub async fn list_all_collection_metadata(
    storage: &SledAdapter,
) -> Result<Vec<CollectionMetadata>> {
    let prefix = b"__prkdb_metadata:collection:";
    let entries = storage.scan_prefix(prefix).await?;

    let mut metadata_list = Vec::new();

    for (_, value) in entries {
        if let Ok(metadata) = serde_json::from_slice::<CollectionMetadata>(&value) {
            metadata_list.push(metadata);
        }
    }

    // Sort by creation time
    metadata_list.sort_by_key(|m| m.created_at);

    Ok(metadata_list)
}
