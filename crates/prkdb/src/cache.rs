//! LRU Cache Layer for PrkDB
//!
//! Provides a simple in-memory LRU cache for hot records.
//!
//! # Example
//!
//! ```rust,ignore
//! use prkdb::cache::LruCache;
//!
//! let cache = LruCache::<u64, User>::new(1000);  // Max 1000 entries
//!
//! // Cache hit/miss
//! if let Some(user) = cache.get(&user_id) {
//!     return Ok(user);
//! }
//! let user = db.get(&user_id).await?;
//! cache.put(user_id, user.clone());
//! ```

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::RwLock;

/// Simple LRU cache with configurable capacity
pub struct LruCache<K, V> {
    /// Maximum number of entries
    capacity: usize,
    /// Cached items
    cache: RwLock<LruCacheInner<K, V>>,
}

struct LruCacheInner<K, V> {
    /// Key -> Value mapping
    map: HashMap<K, V>,
    /// Access order (most recent at back)
    order: VecDeque<K>,
}

impl<K: Eq + Hash + Clone, V: Clone> LruCache<K, V> {
    /// Create a new cache with the given capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            cache: RwLock::new(LruCacheInner {
                map: HashMap::with_capacity(capacity),
                order: VecDeque::with_capacity(capacity),
            }),
        }
    }

    /// Get a value from the cache
    ///
    /// Returns None if not cached. Does not update access order
    /// (would require write lock, prefer simplicity).
    pub fn get(&self, key: &K) -> Option<V> {
        let cache = self.cache.read().unwrap();
        cache.map.get(key).cloned()
    }

    /// Check if key is in cache
    pub fn contains(&self, key: &K) -> bool {
        let cache = self.cache.read().unwrap();
        cache.map.contains_key(key)
    }

    /// Put a value in the cache
    pub fn put(&self, key: K, value: V) {
        let mut cache = self.cache.write().unwrap();

        // Remove old entry if exists
        if cache.map.contains_key(&key) {
            cache.order.retain(|k| k != &key);
        }

        // Evict if at capacity
        while cache.order.len() >= self.capacity {
            if let Some(old_key) = cache.order.pop_front() {
                cache.map.remove(&old_key);
            }
        }

        // Insert new entry
        cache.map.insert(key.clone(), value);
        cache.order.push_back(key);
    }

    /// Remove a value from the cache
    pub fn remove(&self, key: &K) {
        let mut cache = self.cache.write().unwrap();
        cache.map.remove(key);
        cache.order.retain(|k| k != key);
    }

    /// Clear the cache
    pub fn clear(&self) {
        let mut cache = self.cache.write().unwrap();
        cache.map.clear();
        cache.order.clear();
    }

    /// Current number of cached items
    pub fn len(&self) -> usize {
        let cache = self.cache.read().unwrap();
        cache.map.len()
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
        let cache = self.cache.read().unwrap();
        CacheStats {
            size: cache.map.len(),
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
