use crate::storage::WalStorageAdapter;
use async_trait::async_trait;
use prkdb_types::storage::StorageAdapter;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StateMachineError {
    #[error("Storage error: {0}")]
    Storage(#[from] prkdb_types::error::StorageError),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Interface for applying committed entries to the state machine
#[async_trait]
pub trait StateMachine: Send + Sync {
    /// Apply a committed entry to the state machine
    async fn apply(&self, data: &[u8]) -> Result<(), StateMachineError>;

    /// Create a snapshot of the current state
    /// Returns a serialized snapshot as bytes
    async fn snapshot(&self) -> Result<Vec<u8>, StateMachineError>;

    /// Restore state from a snapshot
    /// Replaces the current state with the snapshot data
    async fn restore(&self, snapshot: &[u8]) -> Result<(), StateMachineError>;
}

/// PrkDB implementation of the State Machine
pub struct PrkDbStateMachine {
    storage: Arc<WalStorageAdapter>,
}

impl PrkDbStateMachine {
    pub fn new(storage: Arc<WalStorageAdapter>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl StateMachine for PrkDbStateMachine {
    async fn apply(&self, data: &[u8]) -> Result<(), StateMachineError> {
        use super::command::Command;

        if let Some(cmd) = Command::deserialize(data) {
            match cmd {
                Command::Put { key, value } => {
                    tracing::debug!(
                        "Applying PUT command: key={:?}",
                        String::from_utf8_lossy(&key)
                    );
                    self.storage
                        .put(&key, &value)
                        .await
                        .map_err(StateMachineError::Storage)?;
                }
                Command::Delete { key } => {
                    tracing::debug!(
                        "Applying DELETE command: key={:?}",
                        String::from_utf8_lossy(&key)
                    );
                    self.storage
                        .delete(&key)
                        .await
                        .map_err(StateMachineError::Storage)?;
                }
            }
        } else {
            tracing::warn!("Failed to deserialize command in state machine");
            return Err(StateMachineError::Serialization(
                "Failed to deserialize".to_string(),
            ));
        }

        Ok(())
    }

    async fn snapshot(&self) -> Result<Vec<u8>, StateMachineError> {
        // Serialize all key-value pairs from the storage
        // Format: [u64: num_entries][repeated: u64 key_len, key bytes, u64 value_len, value bytes]

        let mut snapshot_data = Vec::new();
        let mut count: u64 = 0;

        // Reserve space for count (we'll write it at the end)
        snapshot_data.extend_from_slice(&count.to_le_bytes());

        // Iterate over all keys in the storage
        let keys = self.storage.get_all_keys();
        for key in keys {
            // Read the value from storage
            if let Ok(Some(value)) = self.storage.get(&key).await {
                // Write key length and key
                let key_len = key.len() as u64;
                snapshot_data.extend_from_slice(&key_len.to_le_bytes());
                snapshot_data.extend_from_slice(&key);

                // Write value length and value
                let value_len = value.len() as u64;
                snapshot_data.extend_from_slice(&value_len.to_le_bytes());
                snapshot_data.extend_from_slice(&value);

                count += 1;
            }
        }

        // Write the count at the beginning
        snapshot_data[0..8].copy_from_slice(&count.to_le_bytes());

        tracing::info!(
            "Created snapshot with {} entries, size: {} bytes",
            count,
            snapshot_data.len()
        );
        Ok(snapshot_data)
    }

    async fn restore(&self, snapshot: &[u8]) -> Result<(), StateMachineError> {
        if snapshot.len() < 8 {
            return Err(StateMachineError::Serialization(
                "Snapshot too small".to_string(),
            ));
        }

        // Read number of entries
        let mut offset = 0;
        let count = u64::from_le_bytes(
            snapshot[offset..offset + 8]
                .try_into()
                .map_err(|e| StateMachineError::Serialization(format!("Invalid count: {}", e)))?,
        );
        offset += 8;

        tracing::info!("Restoring snapshot with {} entries", count);

        // Read and restore all key-value pairs
        for i in 0..count {
            // Read key length
            if offset + 8 > snapshot.len() {
                return Err(StateMachineError::Serialization(format!(
                    "Unexpected end of snapshot at entry {}",
                    i
                )));
            }
            let key_len =
                u64::from_le_bytes(snapshot[offset..offset + 8].try_into().map_err(|e| {
                    StateMachineError::Serialization(format!("Invalid key length: {}", e))
                })?) as usize;
            offset += 8;

            // Read key
            if offset + key_len > snapshot.len() {
                return Err(StateMachineError::Serialization(format!(
                    "Unexpected end of snapshot reading key at entry {}",
                    i
                )));
            }
            let key = &snapshot[offset..offset + key_len];
            offset += key_len;

            // Read value length
            if offset + 8 > snapshot.len() {
                return Err(StateMachineError::Serialization(format!(
                    "Unexpected end of snapshot at entry {}",
                    i
                )));
            }
            let value_len =
                u64::from_le_bytes(snapshot[offset..offset + 8].try_into().map_err(|e| {
                    StateMachineError::Serialization(format!("Invalid value length: {}", e))
                })?) as usize;
            offset += 8;

            // Read value
            if offset + value_len > snapshot.len() {
                return Err(StateMachineError::Serialization(format!(
                    "Unexpected end of snapshot reading value at entry {}",
                    i
                )));
            }
            let value = &snapshot[offset..offset + value_len];
            offset += value_len;

            // Write to storage
            self.storage
                .put(key, value)
                .await
                .map_err(StateMachineError::Storage)?;
        }

        tracing::info!("Restored {} entries from snapshot", count);
        Ok(())
    }
}
