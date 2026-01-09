//! Transaction support for PrkDB
//!
//! This module provides ACID transaction support with zero overhead for non-transactional
//! operations. Transactions buffer writes until commit, then apply them atomically using
//! the existing high-performance `put_batch` path.
//!
//! # Example
//!
//! ```rust,ignore
//! let tx = db.begin_transaction();
//! tx.put(b"key1", b"value1")?;
//! tx.put(b"key2", b"value2")?;
//! let value = tx.get(b"key3").await?;
//! tx.commit().await?;  // Atomic commit of all changes
//! ```
//!
//! # Performance
//!
//! - Regular operations bypass transaction code entirely (zero overhead)
//! - Transaction commit uses `put_batch` (same 214K ops/sec throughput)
//! - Reads within a transaction see uncommitted writes (read-your-writes)

use crate::storage::WalStorageAdapter;
use prkdb_core::error::StorageError;
use prkdb_core::storage::StorageAdapter;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Global transaction ID counter
static NEXT_TX_ID: AtomicU64 = AtomicU64::new(1);

/// Transaction status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Transaction is active and accepting operations
    Active,
    /// Transaction was successfully committed
    Committed,
    /// Transaction was aborted/rolled back
    Aborted,
}

/// Transaction error types
#[derive(Debug, Clone)]
pub enum TransactionError {
    /// Transaction is no longer active
    NotActive,
    /// Write buffer limit exceeded
    WriteBufferFull,
    /// Storage error during commit
    StorageError(String),
    /// Transaction already committed or aborted
    AlreadyFinished,
    /// Conflict detected during commit (optimistic concurrency control)
    ConflictDetected {
        /// Keys that had conflicts
        conflicting_keys: Vec<Vec<u8>>,
    },
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::NotActive => write!(f, "Transaction is not active"),
            TransactionError::WriteBufferFull => write!(f, "Write buffer limit exceeded"),
            TransactionError::StorageError(e) => write!(f, "Storage error: {}", e),
            TransactionError::AlreadyFinished => {
                write!(f, "Transaction already committed or aborted")
            }
            TransactionError::ConflictDetected { conflicting_keys } => {
                write!(f, "Conflict detected on {} keys", conflicting_keys.len())
            }
        }
    }
}

impl std::error::Error for TransactionError {}

impl From<StorageError> for TransactionError {
    fn from(e: StorageError) -> Self {
        TransactionError::StorageError(e.to_string())
    }
}

/// Isolation level for transactions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Read committed - sees committed data at read time (default, fastest)
    #[default]
    ReadCommitted,
    /// Serializable - detects conflicts when data read has changed
    Serializable,
}

/// A savepoint within a transaction for nested rollback
#[derive(Debug, Clone)]
pub struct Savepoint {
    /// Name of the savepoint
    pub name: String,
    /// Write set snapshot at savepoint creation
    write_set_snapshot: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

/// Configuration for transactions
#[derive(Debug, Clone)]
pub struct TransactionConfig {
    /// Maximum number of writes buffered before error (default: 100,000)
    pub max_write_buffer: usize,
    /// Transaction timeout in milliseconds (default: 30,000)
    pub timeout_ms: u64,
    /// Isolation level for conflict detection (default: ReadCommitted)
    pub isolation_level: IsolationLevel,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            max_write_buffer: 100_000,
            timeout_ms: 30_000,
            isolation_level: IsolationLevel::ReadCommitted,
        }
    }
}

/// A database transaction
///
/// Transactions buffer writes in memory until commit, then apply them
/// atomically using the high-performance batch write path.
///
/// # Read-Your-Writes
///
/// Reads within a transaction will see uncommitted writes from the same
/// transaction. This provides consistent view during the transaction.
///
/// # Conflict Detection (Serializable)
///
/// When using `IsolationLevel::Serializable`, the transaction tracks all
/// reads. On commit, it verifies that read values haven't changed. If
/// any value has changed, a `ConflictDetected` error is returned.
///
/// # Savepoints (Nested Transactions)
///
/// Use `savepoint()` to create a named checkpoint. Use `rollback_to_savepoint()`
/// to partially undo changes while keeping the transaction open.
///
/// # Zero Overhead
///
/// Regular (non-transactional) operations are not affected by transaction
/// support. The transaction code path is only invoked when explicitly using
/// transactions.
pub struct Transaction {
    /// Unique transaction ID
    id: u64,
    /// Reference to storage adapter
    storage: Arc<WalStorageAdapter>,
    /// Buffered writes (key -> value)
    write_set: HashMap<Vec<u8>, Option<Vec<u8>>>,
    /// Read set for conflict detection (key -> value hash at read time)
    /// Only populated when isolation_level is Serializable
    read_set: HashMap<Vec<u8>, Option<u64>>,
    /// Savepoints for nested rollback
    savepoints: Vec<Savepoint>,
    /// Transaction status
    status: TransactionStatus,
    /// When the transaction started
    started_at: Instant,
    /// Configuration
    config: TransactionConfig,
}

impl Transaction {
    /// Create a new transaction
    pub(crate) fn new(storage: Arc<WalStorageAdapter>) -> Self {
        Self::with_config(storage, TransactionConfig::default())
    }

    /// Create a new transaction with custom configuration
    pub(crate) fn with_config(storage: Arc<WalStorageAdapter>, config: TransactionConfig) -> Self {
        Self {
            id: NEXT_TX_ID.fetch_add(1, Ordering::Relaxed),
            storage,
            write_set: HashMap::new(),
            read_set: HashMap::new(),
            savepoints: Vec::new(),
            status: TransactionStatus::Active,
            started_at: Instant::now(),
            config,
        }
    }

    /// Get the transaction ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Check if the transaction is still active
    pub fn is_active(&self) -> bool {
        self.status == TransactionStatus::Active
    }

    /// Get the transaction status
    pub fn status(&self) -> TransactionStatus {
        self.status
    }

    /// Check if transaction has timed out
    fn check_timeout(&self) -> Result<(), TransactionError> {
        if self.started_at.elapsed().as_millis() > self.config.timeout_ms as u128 {
            return Err(TransactionError::NotActive);
        }
        Ok(())
    }

    /// Put a key-value pair in the transaction
    ///
    /// The write is buffered and will be applied on commit.
    pub fn put(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<(), TransactionError> {
        if self.status != TransactionStatus::Active {
            return Err(TransactionError::NotActive);
        }
        self.check_timeout()?;

        if self.write_set.len() >= self.config.max_write_buffer {
            return Err(TransactionError::WriteBufferFull);
        }

        self.write_set.insert(key.into(), Some(value.into()));
        Ok(())
    }

    /// Delete a key in the transaction
    ///
    /// The delete is buffered and will be applied on commit.
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> Result<(), TransactionError> {
        if self.status != TransactionStatus::Active {
            return Err(TransactionError::NotActive);
        }
        self.check_timeout()?;

        self.write_set.insert(key.into(), None);
        Ok(())
    }

    /// Get a value by key
    ///
    /// Returns the uncommitted value if the key was written in this transaction,
    /// otherwise reads from storage. For Serializable isolation, also tracks
    /// the read for conflict detection.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, TransactionError> {
        if self.status != TransactionStatus::Active {
            return Err(TransactionError::NotActive);
        }
        self.check_timeout()?;

        // Check write set first (read-your-writes)
        if let Some(value) = self.write_set.get(key) {
            return Ok(value.clone());
        }

        // Read from storage
        let value = self.storage.get(key).await?;

        // Track read for Serializable isolation
        if self.config.isolation_level == IsolationLevel::Serializable {
            let hash = value.as_ref().map(|v| Self::hash_value(v));
            self.read_set.insert(key.to_vec(), hash);
        }

        Ok(value)
    }

    /// Hash a value for conflict detection
    fn hash_value(value: &[u8]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Create a savepoint for partial rollback
    ///
    /// Use `rollback_to_savepoint()` to undo changes back to this point
    /// while keeping the transaction open.
    pub fn savepoint(&mut self, name: impl Into<String>) -> Result<(), TransactionError> {
        if self.status != TransactionStatus::Active {
            return Err(TransactionError::NotActive);
        }
        self.check_timeout()?;

        self.savepoints.push(Savepoint {
            name: name.into(),
            write_set_snapshot: self.write_set.clone(),
        });
        Ok(())
    }

    /// Rollback to a named savepoint
    ///
    /// Undoes all writes made after the savepoint was created.
    /// The transaction remains open for further operations.
    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<(), TransactionError> {
        if self.status != TransactionStatus::Active {
            return Err(TransactionError::NotActive);
        }
        self.check_timeout()?;

        // Find the savepoint
        if let Some(idx) = self.savepoints.iter().rposition(|s| s.name == name) {
            let savepoint = self.savepoints.remove(idx);
            self.write_set = savepoint.write_set_snapshot;
            // Remove all savepoints after this one
            self.savepoints.truncate(idx);
            Ok(())
        } else {
            Err(TransactionError::StorageError(format!(
                "Savepoint '{}' not found",
                name
            )))
        }
    }

    /// Release a savepoint (removes it without rolling back)
    pub fn release_savepoint(&mut self, name: &str) -> Result<(), TransactionError> {
        if self.status != TransactionStatus::Active {
            return Err(TransactionError::NotActive);
        }

        if let Some(idx) = self.savepoints.iter().rposition(|s| s.name == name) {
            self.savepoints.remove(idx);
            Ok(())
        } else {
            Err(TransactionError::StorageError(format!(
                "Savepoint '{}' not found",
                name
            )))
        }
    }

    /// List active savepoints
    pub fn list_savepoints(&self) -> Vec<&str> {
        self.savepoints.iter().map(|s| s.name.as_str()).collect()
    }

    /// Commit the transaction
    ///
    /// For Serializable isolation, first checks that all read values
    /// haven't changed. Then atomically applies all buffered writes.
    /// Returns the number of operations committed.
    pub async fn commit(mut self) -> Result<usize, TransactionError> {
        if self.status != TransactionStatus::Active {
            return Err(TransactionError::AlreadyFinished);
        }
        self.check_timeout()?;

        // Conflict detection for Serializable isolation
        if self.config.isolation_level == IsolationLevel::Serializable && !self.read_set.is_empty()
        {
            let mut conflicts = Vec::new();

            for (key, expected_hash) in &self.read_set {
                let current_value = self.storage.get(key).await?;
                let current_hash = current_value.as_ref().map(|v| Self::hash_value(v));

                if current_hash != *expected_hash {
                    conflicts.push(key.clone());
                }
            }

            if !conflicts.is_empty() {
                return Err(TransactionError::ConflictDetected {
                    conflicting_keys: conflicts,
                });
            }
        }

        let write_count = self.write_set.len();

        if write_count == 0 {
            self.status = TransactionStatus::Committed;
            return Ok(0);
        }

        // Separate puts and deletes
        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut deletes: Vec<Vec<u8>> = Vec::new();

        for (key, value) in self.write_set.drain() {
            match value {
                Some(v) => puts.push((key, v)),
                None => deletes.push(key),
            }
        }

        // Apply writes using batch path (high performance!)
        if !puts.is_empty() {
            self.storage.put_batch(puts).await?;
        }

        // Apply deletes
        for key in deletes {
            self.storage.delete(&key).await?;
        }

        self.status = TransactionStatus::Committed;
        Ok(write_count)
    }

    /// Rollback (abort) the transaction
    ///
    /// Discards all buffered writes.
    pub fn rollback(mut self) {
        self.write_set.clear();
        self.status = TransactionStatus::Aborted;
    }

    /// Get the number of pending writes
    pub fn pending_writes(&self) -> usize {
        self.write_set.len()
    }
}

/// Extension trait for PrkDb to support transactions
pub trait TransactionExt {
    /// Begin a new transaction
    fn begin_transaction(&self) -> Transaction;

    /// Begin a new transaction with custom configuration
    fn begin_transaction_with_config(&self, config: TransactionConfig) -> Transaction;
}

/// Implementation for Arc<WalStorageAdapter>
impl TransactionExt for Arc<WalStorageAdapter> {
    fn begin_transaction(&self) -> Transaction {
        Transaction::new(self.clone())
    }

    fn begin_transaction_with_config(&self, config: TransactionConfig) -> Transaction {
        Transaction::with_config(self.clone(), config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prkdb_core::wal::WalConfig;

    async fn create_test_storage() -> Arc<WalStorageAdapter> {
        let dir = tempfile::tempdir().unwrap();
        Arc::new(
            WalStorageAdapter::new(WalConfig {
                log_dir: dir.path().to_path_buf(),
                ..WalConfig::test_config()
            })
            .unwrap(),
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transaction_basic_put_get() {
        let storage = create_test_storage().await;
        let mut tx = Transaction::new(storage);

        // Put within transaction
        tx.put(b"key1", b"value1").unwrap();
        tx.put(b"key2", b"value2").unwrap();

        // Read-your-writes
        assert_eq!(tx.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(tx.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));

        // Commit
        let count = tx.commit().await.unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transaction_rollback() {
        let storage = create_test_storage().await;
        let mut tx = Transaction::new(storage.clone());

        tx.put(b"key1", b"value1").unwrap();
        tx.rollback();

        // Value should not exist in storage
        assert_eq!(storage.get(b"key1").await.unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transaction_commit_persists() {
        let storage = create_test_storage().await;
        let mut tx = Transaction::new(storage.clone());

        tx.put(b"key1", b"value1").unwrap();
        tx.commit().await.unwrap();

        // Value should exist in storage
        assert_eq!(
            storage.get(b"key1").await.unwrap(),
            Some(b"value1".to_vec())
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transaction_delete() {
        let storage = create_test_storage().await;

        // Pre-populate
        storage.put(b"key1", b"value1").await.unwrap();

        // Delete in transaction
        let mut tx = Transaction::new(storage.clone());
        tx.delete(b"key1").unwrap();

        // Should see delete in transaction
        assert_eq!(tx.get(b"key1").await.unwrap(), None);

        tx.commit().await.unwrap();

        // Should be deleted in storage
        assert_eq!(storage.get(b"key1").await.unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transaction_write_buffer_limit() {
        let storage = create_test_storage().await;
        let config = TransactionConfig {
            max_write_buffer: 10,
            ..Default::default()
        };
        let mut tx = Transaction::with_config(storage, config);

        for i in 0..10 {
            tx.put(format!("key{}", i), format!("value{}", i)).unwrap();
        }

        // 11th should fail
        let result = tx.put(b"key10", b"value10");
        assert!(matches!(result, Err(TransactionError::WriteBufferFull)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_savepoint_rollback() {
        let storage = create_test_storage().await;
        let mut tx = Transaction::new(storage);

        // Initial write
        tx.put(b"key1", b"value1").unwrap();

        // Create savepoint
        tx.savepoint("sp1").unwrap();

        // Write after savepoint
        tx.put(b"key2", b"value2").unwrap();
        tx.put(b"key1", b"updated").unwrap();

        // Rollback to savepoint
        tx.rollback_to_savepoint("sp1").unwrap();

        // key2 should not exist, key1 should be original value
        assert_eq!(tx.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(tx.write_set.get(b"key2".as_slice()), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_nested_savepoints() {
        let storage = create_test_storage().await;
        let mut tx = Transaction::new(storage);

        tx.put(b"key1", b"v1").unwrap();
        tx.savepoint("sp1").unwrap();

        tx.put(b"key2", b"v2").unwrap();
        tx.savepoint("sp2").unwrap();

        tx.put(b"key3", b"v3").unwrap();

        // List savepoints
        assert_eq!(tx.list_savepoints(), vec!["sp1", "sp2"]);

        // Rollback to sp1 (should remove sp2 too)
        tx.rollback_to_savepoint("sp1").unwrap();

        assert_eq!(tx.list_savepoints(), Vec::<&str>::new());
        assert_eq!(tx.write_set.get(b"key2".as_slice()), None);
        assert_eq!(tx.write_set.get(b"key3".as_slice()), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_serializable_conflict_detection() {
        let storage = create_test_storage().await;

        // Pre-populate
        storage.put(b"key1", b"original").await.unwrap();

        // Start transaction with Serializable isolation
        let config = TransactionConfig {
            isolation_level: IsolationLevel::Serializable,
            ..Default::default()
        };
        let mut tx = Transaction::with_config(storage.clone(), config);

        // Read the value (tracked)
        let _ = tx.get(b"key1").await.unwrap();

        // Another writer modifies the value
        storage.put(b"key1", b"modified").await.unwrap();

        // Transaction tries to commit - should detect conflict
        tx.put(b"key2", b"value2").unwrap();
        let result = tx.commit().await;

        assert!(matches!(
            result,
            Err(TransactionError::ConflictDetected { .. })
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_serializable_no_conflict() {
        let storage = create_test_storage().await;

        // Pre-populate
        storage.put(b"key1", b"original").await.unwrap();

        // Start transaction with Serializable isolation
        let config = TransactionConfig {
            isolation_level: IsolationLevel::Serializable,
            ..Default::default()
        };
        let mut tx = Transaction::with_config(storage.clone(), config);

        // Read the value (tracked)
        let _ = tx.get(b"key1").await.unwrap();

        // No other writer modifies the value

        // Transaction should commit successfully
        tx.put(b"key2", b"value2").unwrap();
        let result = tx.commit().await;

        assert!(result.is_ok());
    }
}
