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
    /// The node's live authorization cache, when this partition owns the authz keyspace.
    ///
    /// `None` for every other partition, and for embedded use where no server is
    /// authenticating anyone.
    authz: Option<crate::authz::PrincipalStore>,
}

impl PrkDbStateMachine {
    pub fn new(storage: Arc<WalStorageAdapter>) -> Self {
        Self {
            storage,
            authz: None,
        }
    }

    /// Give this partition the authorization cache to keep coherent.
    ///
    /// Set only on the partition that owns the authz keyspace. Authentication reads
    /// `PrincipalStore`'s in-memory map, so replicating the durable write alone would
    /// leave every follower answering from the map it loaded at startup — a revoke on one
    /// node would not revoke anything anywhere else.
    pub fn with_authz(mut self, store: crate::authz::PrincipalStore) -> Self {
        self.authz = Some(store);
        self
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
                Command::CreateCollection {
                    name,
                    num_partitions,
                    replication_factor,
                } => {
                    tracing::info!(
                        "Applying CreateCollection command: name={}, partitions={}, replication={}",
                        name,
                        num_partitions,
                        replication_factor
                    );
                    // Store collection metadata as a special key
                    let metadata_key = format!("meta:col:{}", name).into_bytes();

                    // Store configuration as JSON
                    let metadata = serde_json::json!({
                        "num_partitions": num_partitions,
                        "replication_factor": replication_factor,
                        "created_at": chrono::Utc::now().to_rfc3339()
                    });

                    let metadata_value = metadata.to_string().into_bytes();
                    self.storage
                        .put(&metadata_key, &metadata_value)
                        .await
                        .map_err(StateMachineError::Storage)?;
                }
                Command::UpsertPrincipal { name, encoded } => {
                    tracing::info!("Applying UpsertPrincipal: name={}", name);

                    // Durable copy first, under the same key `PrincipalStore::persist`
                    // uses, so a node that reloads from storage sees exactly this.
                    let key = crate::authz::principal_key(&name);
                    self.storage
                        .put(&key, &encoded)
                        .await
                        .map_err(StateMachineError::Storage)?;

                    // Then the live cache, which is what `resolve` actually reads.
                    if let Some(store) = &self.authz {
                        match serde_json::from_slice::<crate::authz::Principal>(&encoded) {
                            Ok(principal) => store.apply_replicated_upsert(principal),
                            Err(e) => {
                                // Refuse rather than continue: a principal whose grants
                                // cannot be read is one whose authority is unknown, and
                                // applying half of it diverges this node from the log.
                                return Err(StateMachineError::Serialization(format!(
                                    "replicated principal {name} is unreadable: {e}"
                                )));
                            }
                        }
                    }
                }
                Command::RevokePrincipal { name } => {
                    tracing::info!("Applying RevokePrincipal: name={}", name);

                    let key = crate::authz::principal_key(&name);
                    self.storage
                        .delete(&key)
                        .await
                        .map_err(StateMachineError::Storage)?;

                    if let Some(store) = &self.authz {
                        if !store.apply_replicated_revoke(&name) {
                            // Not an error: the log is authoritative and the end state is
                            // the same. Logged because it means this node did not have the
                            // principal the leader did.
                            tracing::warn!(
                                "RevokePrincipal {name} matched nothing in the local cache"
                            );
                        }
                    }
                }
                Command::DropCollection { name } => {
                    tracing::info!("Applying DropCollection command: name={}", name);
                    let metadata_key = format!("meta:col:{}", name).into_bytes();
                    self.storage
                        .delete(&metadata_key)
                        .await
                        .map_err(StateMachineError::Storage)?;
                }
            }
        } else {
            tracing::warn!(
                entry_bytes = data.len(),
                "Raft entry is not a command (malformed, or a declared length over the \
                 record limit); refusing to apply it"
            );
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
        // Parsed whole before anything is written: the bytes come from a peer's
        // InstallSnapshot, and a malformed snapshot must change nothing.
        let entries = parse_snapshot(snapshot)?;
        let count = entries.len();
        tracing::info!("Restoring snapshot with {} entries", count);
        for (key, value) in entries {
            self.storage
                .put(key, value)
                .await
                .map_err(StateMachineError::Storage)?;
        }

        tracing::info!("Restored {} entries from snapshot", count);
        Ok(())
    }
}

/// One snapshot entry, borrowed from the snapshot's bytes.
pub type SnapshotEntry<'a> = (&'a [u8], &'a [u8]);

/// Smallest encoded entry: two `u64` lengths and empty key and value.
const MIN_SNAPSHOT_ENTRY_LEN: usize = 16;

fn malformed(reason: String) -> StateMachineError {
    StateMachineError::Serialization(format!("malformed snapshot: {reason}"))
}

fn take_u64(rest: &mut &[u8], what: &str) -> Result<u64, StateMachineError> {
    let (head, tail) = rest
        .split_first_chunk::<8>()
        .ok_or_else(|| malformed(format!("truncated {what}")))?;
    *rest = tail;
    Ok(u64::from_le_bytes(*head))
}

fn take_prefixed<'a>(rest: &mut &'a [u8], what: &str) -> Result<&'a [u8], StateMachineError> {
    let declared = take_u64(rest, what)?;
    let len = usize::try_from(declared)
        .ok()
        .filter(|&len| len <= rest.len())
        .ok_or_else(|| {
            malformed(format!(
                "{what} declares {declared} bytes, {} remain",
                rest.len()
            ))
        })?;
    let (bytes, tail) = rest.split_at(len);
    *rest = tail;
    Ok(bytes)
}

/// Parses the format [`PrkDbStateMachine::snapshot`] writes:
/// `[u64 count][count x (u64 key_len, key, u64 value_len, value)]`, little-endian, with no
/// trailing bytes. Never panics, and allocates at most one slot per 16 input bytes,
/// whatever the input: the bytes arrive from a peer (`InstallSnapshot`), so every length
/// is checked against what is actually there (RFT-11). Fuzzed by `snapshot_restore`.
pub fn parse_snapshot(snapshot: &[u8]) -> Result<Vec<SnapshotEntry<'_>>, StateMachineError> {
    let mut rest = snapshot;
    let count = take_u64(&mut rest, "entry count")?;
    let room = rest.len() / MIN_SNAPSHOT_ENTRY_LEN;
    if count > room as u64 {
        return Err(malformed(format!(
            "{count} entries cannot fit in {} bytes",
            rest.len()
        )));
    }
    let mut entries = Vec::with_capacity(count as usize);
    for i in 0..count {
        let key = take_prefixed(&mut rest, &format!("entry {i} key"))?;
        let value = take_prefixed(&mut rest, &format!("entry {i} value"))?;
        entries.push((key, value));
    }
    if !rest.is_empty() {
        return Err(malformed(format!(
            "{} trailing bytes after {count} entries",
            rest.len()
        )));
    }
    Ok(entries)
}

#[cfg(test)]
mod snapshot_parse_tests {
    use super::*;

    fn entry(key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut out = (key.len() as u64).to_le_bytes().to_vec();
        out.extend_from_slice(key);
        out.extend_from_slice(&(value.len() as u64).to_le_bytes());
        out.extend_from_slice(value);
        out
    }

    fn snapshot(count: u64, body: &[u8]) -> Vec<u8> {
        let mut out = count.to_le_bytes().to_vec();
        out.extend_from_slice(body);
        out
    }

    #[test]
    fn a_valid_snapshot_parses() {
        let mut body = entry(b"a", b"1");
        body.extend(entry(b"", b""));
        let bytes = snapshot(2, &body);
        let parsed = parse_snapshot(&bytes).unwrap();
        assert_eq!(parsed, vec![(&b"a"[..], &b"1"[..]), (&b""[..], &b""[..])]);
        assert!(parse_snapshot(&snapshot(0, &[])).unwrap().is_empty());
    }

    /// RFT-11: `[count = 1][key_len = u64::MAX]` overflowed `offset + key_len` (a panic
    /// in debug builds; in release a wrapped bounds check, then an out-of-range slice).
    /// The parser does no unchecked arithmetic, so both build profiles take the same path.
    #[test]
    fn rft11_a_snapshot_length_of_u64_max_is_an_error() {
        let mut key_max = u64::MAX.to_le_bytes().to_vec();
        key_max.extend_from_slice(&[0; 8]);
        assert!(parse_snapshot(&snapshot(1, &key_max)).is_err());
        let mut value_max = 0u64.to_le_bytes().to_vec();
        value_max.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(parse_snapshot(&snapshot(1, &value_max)).is_err());
    }

    #[test]
    fn rft11_truncated_and_overcounted_snapshots_are_errors() {
        let whole = entry(b"key", b"value");
        for cut in 0..whole.len() {
            assert!(
                parse_snapshot(&snapshot(1, &whole[..cut])).is_err(),
                "cut {cut}"
            );
        }
        assert!(parse_snapshot(&snapshot(2, &whole)).is_err());
        assert!(parse_snapshot(&snapshot(u64::MAX, &whole)).is_err());
        assert!(parse_snapshot(&[1, 2, 3]).is_err());
        let mut trailing = snapshot(1, &whole);
        trailing.push(0);
        assert!(parse_snapshot(&trailing).is_err());
    }
}
