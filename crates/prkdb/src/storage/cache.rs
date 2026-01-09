use prkdb_metrics::storage::StorageMetrics;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Weak};
use tokio::sync::RwLock;

/// Sharded LRU cache for high-concurrency access
///
/// Uses 16 independent shards to allow concurrent reads and writes.
/// This eliminates the bottleneck where writes block all reads.
///
/// Performance:
/// - Reads: Lock-free for different shards
/// - Writes: Only blocks the target shard (1/16 of cache)
/// - Expected: 10-100x better read throughput under write load
pub struct ShardedLruCache<K, V> {
    shards: Vec<Arc<RwLock<LruCache<K, V>>>>,
    shard_count: usize,
}

impl<K: Eq + Hash + Clone, V: Clone> ShardedLruCache<K, V> {
    /// Create a new sharded cache with the given total capacity
    /// Capacity is distributed evenly across shards
    pub fn new(total_capacity: usize) -> Self {
        Self::with_shard_count(total_capacity, 16)
    }

    /// Create a sharded cache with custom shard count
    pub fn with_shard_count(total_capacity: usize, shard_count: usize) -> Self {
        let capacity_per_shard = (total_capacity / shard_count).max(1);

        let shards = (0..shard_count)
            .map(|_| Arc::new(RwLock::new(LruCache::new(capacity_per_shard))))
            .collect();

        Self {
            shards,
            shard_count,
        }
    }

    /// Create with metrics tracking
    pub fn with_metrics(total_capacity: usize, metrics: Arc<StorageMetrics>) -> Self {
        let shard_count = 16;
        let capacity_per_shard = (total_capacity / shard_count).max(1);

        let shards = (0..shard_count)
            .map(|_| {
                Arc::new(RwLock::new(LruCache::with_metrics(
                    capacity_per_shard,
                    metrics.clone(),
                )))
            })
            .collect();

        Self {
            shards,
            shard_count,
        }
    }

    /// Get the shard index for a given key
    #[inline]
    fn shard_index(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shard_count
    }

    /// Get a value from the cache
    pub async fn get(&self, key: &K) -> Option<V> {
        let shard_idx = self.shard_index(key);
        let mut shard = self.shards[shard_idx].write().await;
        shard.get(key)
    }

    /// Insert a value into the cache
    pub async fn put(&self, key: K, value: V) {
        let shard_idx = self.shard_index(&key);
        let mut shard = self.shards[shard_idx].write().await;
        shard.put(key, value);
    }

    /// Bulk insert multiple entries (optimized)
    pub async fn put_batch(&self, entries: Vec<(K, V)>) {
        // Group entries by shard - pre-allocating for efficiency
        let mut shard_groups: Vec<Vec<(K, V)>> = vec![Vec::new(); self.shard_count];

        for (key, value) in entries {
            let shard_idx = self.shard_index(&key);
            shard_groups[shard_idx].push((key, value));
        }

        // Update all shards concurrently
        let futures: Vec<_> = shard_groups
            .into_iter()
            .enumerate()
            .filter(|(_, entries)| !entries.is_empty())
            .map(|(shard_idx, entries)| {
                let shard = self.shards[shard_idx].clone();
                async move {
                    let mut guard = shard.write().await;
                    for (key, value) in entries {
                        guard.put(key, value);
                    }
                }
            })
            .collect();

        futures::future::join_all(futures).await;
    }

    /// Bulk remove multiple keys (optimized)
    pub async fn remove_batch(&self, keys: Vec<K>) {
        // Group keys by shard
        let mut shard_groups: Vec<Vec<K>> = vec![Vec::new(); self.shard_count];

        for key in keys {
            let shard_idx = self.shard_index(&key);
            shard_groups[shard_idx].push(key);
        }

        // Remove from all shards concurrently
        let futures: Vec<_> = shard_groups
            .into_iter()
            .enumerate()
            .filter(|(_, keys)| !keys.is_empty())
            .map(|(shard_idx, keys)| {
                let shard = self.shards[shard_idx].clone();
                async move {
                    let mut guard = shard.write().await;
                    for key in keys {
                        guard.remove(&key);
                    }
                }
            })
            .collect();

        futures::future::join_all(futures).await;
    }

    /// Remove a key from the cache
    pub async fn remove(&self, key: &K) {
        let shard_idx = self.shard_index(key);
        let mut shard = self.shards[shard_idx].write().await;
        shard.remove(key);
    }

    /// Get total cache size across all shards
    pub async fn len(&self) -> usize {
        let mut total = 0;
        for shard in &self.shards {
            let guard = shard.read().await;
            total += guard.len();
        }
        total
    }

    /// Check if cache is empty
    pub async fn is_empty(&self) -> bool {
        for shard in &self.shards {
            let guard = shard.read().await;
            if !guard.is_empty() {
                return false;
            }
        }
        true
    }

    /// Clear all shards
    pub async fn clear(&self) {
        let futures: Vec<_> = self
            .shards
            .iter()
            .map(|shard| async move {
                let mut guard = shard.write().await;
                guard.clear();
            })
            .collect();

        futures::future::join_all(futures).await;
    }
}

/// Simple LRU cache with configurable capacity and optional metrics tracking
pub struct LruCache<K, V> {
    capacity: usize,
    map: HashMap<K, (V, usize)>, // (value, last_access_time)
    access_counter: usize,
    metrics: Option<Weak<StorageMetrics>>, // Weak reference to avoid circular dependencies
}

impl<K: Eq + Hash + Clone, V: Clone> LruCache<K, V> {
    /// Create a new LRU cache with the given capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::with_capacity(capacity),
            access_counter: 0,
            metrics: None,
        }
    }

    /// Create a new LRU cache with metrics tracking
    pub fn with_metrics(capacity: usize, metrics: Arc<StorageMetrics>) -> Self {
        Self {
            capacity,
            map: HashMap::with_capacity(capacity),
            access_counter: 0,
            metrics: Some(Arc::downgrade(&metrics)),
        }
    }

    /// Get a value from the cache, updating access time
    pub fn get(&mut self, key: &K) -> Option<V> {
        if let Some((value, access_time)) = self.map.get_mut(key) {
            self.access_counter += 1;
            *access_time = self.access_counter;
            Some(value.clone())
        } else {
            None
        }
    }

    /// Insert a value into the cache
    pub fn put(&mut self, key: K, value: V) {
        self.access_counter += 1;

        // If at capacity, evict LRU item
        if self.map.len() >= self.capacity && !self.map.contains_key(&key) {
            self.evict_lru();
        }

        self.map.insert(key, (value, self.access_counter));
    }

    /// Remove a key from the cache
    pub fn remove(&mut self, key: &K) {
        self.map.remove(key);
    }

    /// Clear the entire cache
    pub fn clear(&mut self) {
        self.map.clear();
        self.access_counter = 0;
    }

    /// Get the current size of the cache
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Evict the least recently used item
    fn evict_lru(&mut self) {
        if let Some((lru_key, _)) = self
            .map
            .iter()
            .min_by_key(|(_, (_, access_time))| access_time)
            .map(|(k, _)| (k.clone(), ()))
        {
            self.map.remove(&lru_key);

            // Record eviction metric
            if let Some(metrics_weak) = &self.metrics {
                if let Some(metrics) = metrics_weak.upgrade() {
                    metrics.record_cache_eviction();
                }
            }
        }
    }

    /// Estimate cache size in bytes (rough approximation)
    /// For Vec<u8> keys and values, this is reasonably accurate
    pub fn estimate_size_bytes(&self) -> u64
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.map
            .iter()
            .map(|(k, (v, _))| (k.as_ref().len() + v.as_ref().len()) as u64)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_basic() {
        let mut cache = LruCache::new(2);

        cache.put(1, "one");
        cache.put(2, "two");

        assert_eq!(cache.get(&1), Some("one"));
        assert_eq!(cache.get(&2), Some("two"));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = LruCache::new(2);

        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three"); // Should evict key 1

        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some("two"));
        assert_eq!(cache.get(&3), Some("three"));
    }

    #[test]
    fn test_lru_access_order() {
        let mut cache = LruCache::new(2);

        cache.put(1, "one");
        cache.put(2, "two");
        cache.get(&1); // Access key 1, making it more recent
        cache.put(3, "three"); // Should evict key 2, not 1

        assert_eq!(cache.get(&1), Some("one"));
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.get(&3), Some("three"));
    }

    #[tokio::test]
    async fn test_sharded_cache_basic() {
        let cache = ShardedLruCache::new(100);

        cache.put(vec![1], vec![10]).await;
        cache.put(vec![2], vec![20]).await;

        assert_eq!(cache.get(&vec![1]).await, Some(vec![10]));
        assert_eq!(cache.get(&vec![2]).await, Some(vec![20]));
    }

    #[tokio::test]
    async fn test_sharded_cache_batch() {
        let cache = ShardedLruCache::new(100);

        let entries = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];

        cache.put_batch(entries).await;

        assert_eq!(cache.get(&vec![1]).await, Some(vec![10]));
        assert_eq!(cache.get(&vec![2]).await, Some(vec![20]));
        assert_eq!(cache.get(&vec![3]).await, Some(vec![30]));
    }
}
