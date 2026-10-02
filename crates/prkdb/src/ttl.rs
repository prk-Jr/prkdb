//! TTL (Time-To-Live) support for PrkDB
//!
//! This module provides automatic record expiration. Records can be set to
//! expire after a specified duration, and will return None on get() after
//! expiration. A background task periodically cleans up expired records.
//!
//! # Example
//!
//! ```no_run
//! use prkdb::ttl::TtlStorage;
//! use std::time::Duration;
//! # use prkdb::storage::WalStorageAdapter;
//! # use prkdb_core::wal::WalConfig;
//! # use std::sync::Arc;
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! # let storage = Arc::new(WalStorageAdapter::new(WalConfig::default())?);
//! // Wrap storage with TTL support
//! let ttl_storage = TtlStorage::new(storage);
//!
//! // Put with 1 hour TTL
//! ttl_storage.put_with_ttl(b"session:123", b"data", Duration::from_secs(3600)).await?;
//!
//! // Get returns None if expired
//! let value = ttl_storage.get(b"session:123").await?;
//!
//! // Check remaining TTL
//! let remaining = ttl_storage.ttl(b"session:123").await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Performance
//!
//! - Regular put/get operations have zero overhead
//! - put_with_ttl() has minimal overhead (hash map insert)
//! - get() for TTL keys has one additional timestamp check
//! - Background cleanup is async and non-blocking

use crate::storage::WalStorageAdapter;
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// TTL metadata key prefix
const TTL_META_PREFIX: &[u8] = b"__ttl:";

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as u64
}

/// Source of "now" in milliseconds since the epoch.
///
/// Production uses the system clock; tests inject a manual clock so that expiry
/// decisions do not depend on how long the storage writes in between happen to take.
type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

fn system_clock() -> Clock {
    Arc::new(current_timestamp_ms)
}

/// TTL index for tracking expiration times
#[derive(Debug, Default)]
struct TtlIndex {
    /// Key -> expiry timestamp (milliseconds since epoch)
    by_key: HashMap<Vec<u8>, u64>,
    /// Expiry timestamp -> keys (for efficient cleanup)
    by_expiry: BTreeMap<u64, Vec<Vec<u8>>>,
}

impl TtlIndex {
    fn new() -> Self {
        Self::default()
    }

    /// Add or update TTL for a key
    fn set(&mut self, key: Vec<u8>, expires_at: u64) {
        // Remove old expiry if exists
        if let Some(old_expires) = self.by_key.insert(key.clone(), expires_at) {
            if let Some(keys) = self.by_expiry.get_mut(&old_expires) {
                keys.retain(|k| k != &key);
                if keys.is_empty() {
                    self.by_expiry.remove(&old_expires);
                }
            }
        }

        // Add new expiry
        self.by_expiry.entry(expires_at).or_default().push(key);
    }

    /// Remove TTL for a key
    fn remove(&mut self, key: &[u8]) {
        if let Some(expires_at) = self.by_key.remove(key) {
            if let Some(keys) = self.by_expiry.get_mut(&expires_at) {
                keys.retain(|k| k != key);
                if keys.is_empty() {
                    self.by_expiry.remove(&expires_at);
                }
            }
        }
    }

    /// Get expiry timestamp for a key
    fn get(&self, key: &[u8]) -> Option<u64> {
        self.by_key.get(key).copied()
    }

    /// Check if a key is expired as of `now` (ms since epoch)
    fn is_expired(&self, key: &[u8], now: u64) -> bool {
        if let Some(expires_at) = self.by_key.get(key) {
            now >= *expires_at
        } else {
            false
        }
    }

    /// Get all expired keys up to `now` (ms since epoch)
    #[allow(dead_code)]
    fn get_expired(&self, now: u64) -> Vec<Vec<u8>> {
        let mut expired = Vec::new();

        for (&expires_at, keys) in self.by_expiry.iter() {
            if expires_at <= now {
                expired.extend(keys.iter().cloned());
            } else {
                break; // BTreeMap is sorted, no more expired keys
            }
        }

        expired
    }

    /// Remove entries expired as of `now` (ms since epoch) from the index
    fn cleanup_index(&mut self, now: u64) -> Vec<Vec<u8>> {
        let mut expired = Vec::new();

        // Collect expired timestamps
        let expired_times: Vec<u64> = self.by_expiry.range(..=now).map(|(&t, _)| t).collect();

        // Remove expired entries
        for time in expired_times {
            if let Some(keys) = self.by_expiry.remove(&time) {
                for key in keys {
                    self.by_key.remove(&key);
                    expired.push(key);
                }
            }
        }

        expired
    }
}

/// Storage wrapper that adds TTL support
///
/// Wraps any `WalStorageAdapter` to add time-to-live functionality.
/// TTL metadata is stored alongside the data, and expired records
/// are automatically filtered on read.
pub struct TtlStorage {
    /// Underlying storage
    storage: Arc<WalStorageAdapter>,
    /// TTL index (in-memory)
    index: Arc<RwLock<TtlIndex>>,
    /// Cleanup task handle
    cleanup_handle: Option<tokio::task::JoinHandle<()>>,
    /// Source of the current time for every expiry decision
    clock: Clock,
}

impl TtlStorage {
    /// Create a new TTL-aware storage wrapper
    pub fn new(storage: Arc<WalStorageAdapter>) -> Self {
        Self::with_clock(storage, system_clock())
    }

    /// Create a wrapper whose expiry decisions read `clock` instead of the system clock.
    fn with_clock(storage: Arc<WalStorageAdapter>, clock: Clock) -> Self {
        Self {
            storage,
            index: Arc::new(RwLock::new(TtlIndex::new())),
            cleanup_handle: None,
            clock,
        }
    }

    fn now_ms(&self) -> u64 {
        (self.clock)()
    }

    /// Start background cleanup task
    ///
    /// Periodically removes expired records from storage.
    pub fn start_cleanup(&mut self, interval: Duration) {
        let storage = self.storage.clone();
        let index = self.index.clone();
        let clock = self.clock.clone();

        let handle = tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;

                // Get expired keys
                let expired_keys = {
                    let mut idx = index.write().await;
                    idx.cleanup_index(clock())
                };

                // Delete expired keys from storage
                for key in expired_keys {
                    let _ = storage.delete(&key).await;
                    // Also delete TTL metadata
                    let meta_key = Self::ttl_meta_key(&key);
                    let _ = storage.delete(&meta_key).await;
                }
            }
        });

        self.cleanup_handle = Some(handle);
    }

    /// Stop background cleanup task
    pub fn stop_cleanup(&mut self) {
        if let Some(handle) = self.cleanup_handle.take() {
            handle.abort();
        }
    }

    /// Generate TTL metadata key
    fn ttl_meta_key(key: &[u8]) -> Vec<u8> {
        let mut meta_key = TTL_META_PREFIX.to_vec();
        meta_key.extend_from_slice(key);
        meta_key
    }

    /// Put a value with TTL
    pub async fn put_with_ttl(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let expires_at = self.now_ms() + ttl.as_millis() as u64;

        // Store the value
        self.storage.put(key, value).await?;

        // Store TTL metadata (for persistence)
        let meta_key = Self::ttl_meta_key(key);
        self.storage
            .put(&meta_key, &expires_at.to_le_bytes())
            .await?;

        // Update in-memory index
        {
            let mut index = self.index.write().await;
            index.set(key.to_vec(), expires_at);
        }

        Ok(())
    }

    /// Get a value, returning None if expired
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        // Check in-memory index first (fast path)
        {
            let index = self.index.read().await;
            if index.is_expired(key, self.now_ms()) {
                return Ok(None);
            }
        }

        // Get the value
        let value = self.storage.get(key).await?;

        // If value exists and we don't have TTL info in memory, check storage
        if value.is_some() {
            let meta_key = Self::ttl_meta_key(key);
            if let Some(meta) = self.storage.get(&meta_key).await? {
                if meta.len() == 8 {
                    let expires_at = u64::from_le_bytes(meta.try_into().unwrap());
                    if self.now_ms() >= expires_at {
                        // Expired - lazily delete
                        let _ = self.storage.delete(key).await;
                        let _ = self.storage.delete(&meta_key).await;
                        return Ok(None);
                    }

                    // Update in-memory index
                    let mut index = self.index.write().await;
                    index.set(key.to_vec(), expires_at);
                }
            }
        }

        Ok(value)
    }

    /// Get remaining TTL for a key
    pub async fn ttl(&self, key: &[u8]) -> Result<Option<Duration>, StorageError> {
        // Check in-memory index
        {
            let index = self.index.read().await;
            if let Some(expires_at) = index.get(key) {
                let now = self.now_ms();
                if now >= expires_at {
                    return Ok(None); // Expired
                }
                return Ok(Some(Duration::from_millis(expires_at - now)));
            }
        }

        // Check storage for TTL metadata
        let meta_key = Self::ttl_meta_key(key);
        if let Some(meta) = self.storage.get(&meta_key).await? {
            if meta.len() == 8 {
                let expires_at = u64::from_le_bytes(meta.try_into().unwrap());
                let now = self.now_ms();
                if now >= expires_at {
                    return Ok(None); // Expired
                }
                return Ok(Some(Duration::from_millis(expires_at - now)));
            }
        }

        Ok(None) // No TTL set
    }

    /// Immediately expire a key
    pub async fn expire(&self, key: &[u8]) -> Result<(), StorageError> {
        // Remove from storage
        self.storage.delete(key).await?;

        // Remove TTL metadata
        let meta_key = Self::ttl_meta_key(key);
        self.storage.delete(&meta_key).await?;

        // Remove from index
        {
            let mut index = self.index.write().await;
            index.remove(key);
        }

        Ok(())
    }

    /// Remove TTL from a key (persist indefinitely)
    pub async fn persist(&self, key: &[u8]) -> Result<bool, StorageError> {
        // Check if key has TTL
        let meta_key = Self::ttl_meta_key(key);
        if self.storage.get(&meta_key).await?.is_some() {
            // Remove TTL metadata
            self.storage.delete(&meta_key).await?;

            // Remove from index
            {
                let mut index = self.index.write().await;
                index.remove(key);
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Regular put without TTL
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        // Remove any existing TTL
        {
            let mut index = self.index.write().await;
            index.remove(key);
        }

        // Delete TTL metadata if exists
        let meta_key = Self::ttl_meta_key(key);
        let _ = self.storage.delete(&meta_key).await;

        // Store value
        self.storage.put(key, value).await
    }

    /// Regular delete
    pub async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        // Remove from index
        {
            let mut index = self.index.write().await;
            index.remove(key);
        }

        // Delete TTL metadata
        let meta_key = Self::ttl_meta_key(key);
        let _ = self.storage.delete(&meta_key).await;

        // Delete value
        self.storage.delete(key).await
    }

    /// Get underlying storage
    pub fn inner(&self) -> &Arc<WalStorageAdapter> {
        &self.storage
    }
}

impl Drop for TtlStorage {
    fn drop(&mut self) {
        self.stop_cleanup();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prkdb_core::wal::WalConfig;
    use std::sync::atomic::{AtomicU64, Ordering};

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

    /// A clock that only moves when the test moves it, so an expiry decision never
    /// depends on how long the durable writes before it took.
    #[derive(Clone)]
    struct ManualClock(Arc<AtomicU64>);

    impl ManualClock {
        fn starting_at(ms: u64) -> Self {
            Self(Arc::new(AtomicU64::new(ms)))
        }

        fn set(&self, ms: u64) {
            self.0.store(ms, Ordering::SeqCst);
        }

        fn clock(&self) -> Clock {
            let now = self.0.clone();
            Arc::new(move || now.load(Ordering::SeqCst))
        }
    }

    const T0: u64 = 1_700_000_000_000;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_put_with_ttl_and_get() {
        let storage = create_test_storage().await;
        let clock = ManualClock::starting_at(T0);
        let ttl_storage = TtlStorage::with_clock(storage, clock.clock());

        // Put with 1 second TTL
        ttl_storage
            .put_with_ttl(b"key1", b"value1", Duration::from_secs(1))
            .await
            .unwrap();

        // Readable while no time has passed
        let value = ttl_storage.get(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // The full TTL remains, and it counts down with the clock
        assert_eq!(
            ttl_storage.ttl(b"key1").await.unwrap(),
            Some(Duration::from_secs(1))
        );
        clock.set(T0 + 400);
        assert_eq!(
            ttl_storage.ttl(b"key1").await.unwrap(),
            Some(Duration::from_millis(600))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_ttl_expiration() {
        let storage = create_test_storage().await;
        let clock = ManualClock::starting_at(T0);
        let ttl_storage = TtlStorage::with_clock(storage.clone(), clock.clock());

        // Put with very short TTL
        ttl_storage
            .put_with_ttl(b"key1", b"value1", Duration::from_millis(50))
            .await
            .unwrap();

        // Readable immediately, and up to the last millisecond before expiry
        assert!(ttl_storage.get(b"key1").await.unwrap().is_some());
        clock.set(T0 + 49);
        assert!(ttl_storage.get(b"key1").await.unwrap().is_some());

        // Expired exactly at the deadline, and from then on
        clock.set(T0 + 50);
        assert_eq!(ttl_storage.get(b"key1").await.unwrap(), None);
        assert_eq!(ttl_storage.ttl(b"key1").await.unwrap(), None);
        clock.set(T0 + 100);
        assert_eq!(ttl_storage.get(b"key1").await.unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn expiry_survives_a_fresh_wrapper_via_persisted_metadata() {
        let storage = create_test_storage().await;
        let clock = ManualClock::starting_at(T0);
        let writer = TtlStorage::with_clock(storage.clone(), clock.clock());
        writer
            .put_with_ttl(b"key1", b"value1", Duration::from_millis(50))
            .await
            .unwrap();

        // A wrapper with an empty in-memory index reads the expiry from storage.
        let reader = TtlStorage::with_clock(storage.clone(), clock.clock());
        assert_eq!(reader.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));

        let late_reader = TtlStorage::with_clock(storage.clone(), clock.clock());
        clock.set(T0 + 50);
        assert_eq!(late_reader.get(b"key1").await.unwrap(), None);
        // The expired read lazily deleted the record itself.
        assert_eq!(storage.get(b"key1").await.unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_expire_immediately() {
        let storage = create_test_storage().await;
        let ttl_storage = TtlStorage::new(storage);

        // Put with TTL
        ttl_storage
            .put_with_ttl(b"key1", b"value1", Duration::from_secs(3600))
            .await
            .unwrap();

        // Should be readable
        assert!(ttl_storage.get(b"key1").await.unwrap().is_some());

        // Expire immediately
        ttl_storage.expire(b"key1").await.unwrap();

        // Should be gone
        assert!(ttl_storage.get(b"key1").await.unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_persist_removes_ttl() {
        let storage = create_test_storage().await;
        let clock = ManualClock::starting_at(T0);
        let ttl_storage = TtlStorage::with_clock(storage, clock.clock());

        // Put with TTL
        ttl_storage
            .put_with_ttl(b"key1", b"value1", Duration::from_millis(50))
            .await
            .unwrap();

        // Persist (remove TTL)
        let had_ttl = ttl_storage.persist(b"key1").await.unwrap();
        assert!(had_ttl);

        // Move past the original expiry
        clock.set(T0 + 100);

        // Should still be readable (no TTL)
        let value = ttl_storage.get(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // TTL should be None
        assert!(ttl_storage.ttl(b"key1").await.unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_regular_put_has_no_ttl() {
        let storage = create_test_storage().await;
        let ttl_storage = TtlStorage::new(storage);

        // Regular put
        ttl_storage.put(b"key1", b"value1").await.unwrap();

        // Should have no TTL
        assert!(ttl_storage.ttl(b"key1").await.unwrap().is_none());

        // Should be readable
        assert!(ttl_storage.get(b"key1").await.unwrap().is_some());
    }
}
