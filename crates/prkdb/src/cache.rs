//! LRU Cache Layer for PrkDB
//!
//! Provides a simple in-memory LRU cache for hot records.
//!
//! # Example
//!
//! ```no_run
//! use prkdb::cache::LruCache;
//! # use prkdb::storage::InMemoryAdapter;
//! # use prkdb_types::storage::StorageAdapter;
//! # async fn demo(db: &InMemoryAdapter) -> Result<(), Box<dyn std::error::Error>> {
//! # let user_id: u64 = 1;
//! let cache = LruCache::<u64, Vec<u8>>::new(1000);  // Max 1000 entries
//!
//! // Cache hit/miss
//! if let Some(user) = cache.get(&user_id) {
//!     return Ok(());
//! }
//! let user = db.get(b"users:1").await?.unwrap_or_default();
//! cache.put(user_id, user.clone());
//! # Ok(())
//! # }
//! ```

use hashlink::LinkedHashMap;
use std::hash::Hash;
use std::sync::Mutex;

/// LRU cache with configurable capacity.
///
/// Reads and writes both count as use; the least recently used entry is evicted when a new
/// key would exceed the capacity. Every operation is O(1). A capacity of 0 behaves as 1.
pub struct LruCache<K, V> {
    /// Maximum number of entries
    capacity: usize,
    /// Cached items, least recently used first
    cache: Mutex<LinkedHashMap<K, V>>,
}

impl<K: Eq + Hash + Clone, V: Clone> LruCache<K, V> {
    /// Create a new cache with the given capacity
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            cache: Mutex::new(LinkedHashMap::with_capacity(capacity)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LinkedHashMap<K, V>> {
        // A panic while holding the lock leaves the map structurally valid.
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Get a value from the cache, marking it most recently used
    pub fn get(&self, key: &K) -> Option<V> {
        self.lock().to_back(key).map(|value| value.clone())
    }

    /// Check if key is in cache (does not change recency)
    pub fn contains(&self, key: &K) -> bool {
        self.lock().contains_key(key)
    }

    /// Put a value in the cache, marking it most recently used
    pub fn put(&self, key: K, value: V) {
        let mut cache = self.lock();
        let replaced = cache.insert(key, value);
        if replaced.is_none() && cache.len() > self.capacity {
            cache.pop_front();
        }
    }

    /// Remove a value from the cache
    pub fn remove(&self, key: &K) {
        self.lock().remove(key);
    }

    /// Clear the cache
    pub fn clear(&self) {
        self.lock().clear();
    }

    /// Current number of cached items
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Cache capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get cache stats
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            size: self.len(),
            capacity: self.capacity,
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current number of entries
    pub size: usize,
    /// Maximum capacity
    pub capacity: usize,
}

impl CacheStats {
    /// Cache utilization percentage
    pub fn utilization(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            (self.size as f64 / self.capacity as f64) * 100.0
        }
    }
}

/// Write-through cache wrapper
///
/// Automatically caches on get and invalidates on write.
pub struct CachedStorage<S, K, V> {
    storage: S,
    cache: LruCache<K, V>,
}

impl<S, K: Eq + Hash + Clone, V: Clone> CachedStorage<S, K, V> {
    pub fn new(storage: S, capacity: usize) -> Self {
        Self {
            storage,
            cache: LruCache::new(capacity),
        }
    }

    /// Get cache stats
    pub fn cache_stats(&self) -> CacheStats {
        self.cache.stats()
    }

    /// Access underlying storage
    pub fn inner(&self) -> &S {
        &self.storage
    }

    /// Access mutable underlying storage
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.storage
    }

    /// Access the cache directly
    pub fn cache(&self) -> &LruCache<K, V> {
        &self.cache
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_basic() {
        let cache = LruCache::<u64, String>::new(3);

        cache.put(1, "one".to_string());
        cache.put(2, "two".to_string());
        cache.put(3, "three".to_string());

        assert_eq!(cache.get(&1), Some("one".to_string()));
        assert_eq!(cache.get(&2), Some("two".to_string()));
        assert_eq!(cache.get(&3), Some("three".to_string()));
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_lru_cache_eviction() {
        let cache = LruCache::<u64, String>::new(2);

        cache.put(1, "one".to_string());
        cache.put(2, "two".to_string());
        cache.put(3, "three".to_string()); // Should evict 1

        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some("two".to_string()));
        assert_eq!(cache.get(&3), Some("three".to_string()));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_lru_cache_update() {
        let cache = LruCache::<u64, String>::new(2);

        cache.put(1, "one".to_string());
        cache.put(1, "ONE".to_string()); // Update

        assert_eq!(cache.get(&1), Some("ONE".to_string()));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_lru_cache_remove() {
        let cache = LruCache::<u64, String>::new(3);

        cache.put(1, "one".to_string());
        cache.put(2, "two".to_string());
        cache.remove(&1);

        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some("two".to_string()));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn a_zero_capacity_cache_holds_one_entry_instead_of_hanging() {
        let cache = LruCache::<u64, String>::new(0);

        cache.put(1, "one".to_string());
        cache.put(2, "two".to_string());

        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some("two".to_string()));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn a_read_makes_an_entry_most_recently_used() {
        let cache = LruCache::<u64, String>::new(2);

        cache.put(1, "one".to_string());
        cache.put(2, "two".to_string());
        cache.get(&1); // 2 is now the least recently used
        cache.put(3, "three".to_string());

        assert_eq!(cache.get(&1), Some("one".to_string()));
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.get(&3), Some("three".to_string()));
    }

    #[test]
    fn an_overwrite_at_capacity_evicts_nothing() {
        let cache = LruCache::<u64, String>::new(2);

        cache.put(1, "one".to_string());
        cache.put(2, "two".to_string());
        cache.put(1, "ONE".to_string());

        assert_eq!(cache.get(&1), Some("ONE".to_string()));
        assert_eq!(cache.get(&2), Some("two".to_string()));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_lru_cache_stats() {
        let cache = LruCache::<u64, String>::new(10);

        cache.put(1, "one".to_string());
        cache.put(2, "two".to_string());

        let stats = cache.stats();
        assert_eq!(stats.size, 2);
        assert_eq!(stats.capacity, 10);
        assert!((stats.utilization() - 20.0).abs() < 0.001);
    }
}
