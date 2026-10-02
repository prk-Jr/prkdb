use hashlink::LinkedHashMap;
use prkdb_metrics::storage::StorageMetrics;
use std::collections::hash_map::DefaultHasher;
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
    capacity_per_shard: usize,
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
            capacity_per_shard,
        }
    }

    /// Total entries this cache holds before evicting, across all shards.
    pub fn capacity(&self) -> usize {
        self.capacity_per_shard * self.shards.len()
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
            capacity_per_shard,
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

    /// Estimate cache size in bytes across all shards
    /// For Vec<u8> keys and values, this is reasonably accurate
    pub async fn estimate_size_bytes(&self) -> u64
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut total = 0u64;
        for shard in &self.shards {
            let guard = shard.read().await;
            total += guard.estimate_size_bytes();
        }
        total
    }
}

/// LRU cache with configurable capacity and optional metrics tracking.
///
/// Backed by `hashlink::LinkedHashMap`, a hash map threaded through a doubly linked list
/// in recency order (least recently used at the front), so `get`, `put` and eviction are
/// all O(1). The previous version stamped each entry with an access counter and found the
/// victim by scanning the whole map on every evicting `put`, which made a full cache's
/// write path O(capacity) and dominated the WAL adapter's CPU profile.
pub struct LruCache<K, V> {
    capacity: usize,
    map: LinkedHashMap<K, V>,
    metrics: Option<Weak<StorageMetrics>>, // Weak reference to avoid circular dependencies
}

impl<K: Eq + Hash + Clone, V: Clone> LruCache<K, V> {
    /// Create a new LRU cache with the given capacity. A capacity of 0 still holds the
    /// most recent entry, as it always has.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            map: LinkedHashMap::with_capacity(capacity),
            metrics: None,
        }
    }

    /// Create a new LRU cache with metrics tracking
    pub fn with_metrics(capacity: usize, metrics: Arc<StorageMetrics>) -> Self {
        Self {
            metrics: Some(Arc::downgrade(&metrics)),
            ..Self::new(capacity)
        }
    }

    /// Get a value from the cache, marking it most recently used
    pub fn get(&mut self, key: &K) -> Option<V> {
        self.map.to_back(key).map(|value| value.clone())
    }

    /// Insert a value into the cache, marking it most recently used. Inserting a new key
    /// into a full cache evicts the least recently used entry; overwriting a key never
    /// evicts.
    pub fn put(&mut self, key: K, value: V) {
        let replaced = self.map.insert(key, value);
        if replaced.is_none() && self.map.len() > self.capacity {
            self.evict_lru();
        }
    }

    /// Remove a key from the cache
    pub fn remove(&mut self, key: &K) {
        self.map.remove(key);
    }

    /// Clear the entire cache
    pub fn clear(&mut self) {
        self.map.clear();
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
        if self.map.pop_front().is_some() {
            if let Some(metrics) = self.metrics.as_ref().and_then(Weak::upgrade) {
                metrics.record_cache_eviction();
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
            .map(|(k, v)| (k.as_ref().len() + v.as_ref().len()) as u64)
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

    #[test]
    fn test_lru_evicts_in_least_recently_used_order() {
        let mut cache = LruCache::new(3);
        cache.put(1, 10);
        cache.put(2, 20);
        cache.put(3, 30);
        cache.get(&1); // order, oldest first: 2, 3, 1
        cache.put(4, 40); // evicts 2
        assert_eq!(cache.get(&2), None);
        cache.put(5, 50); // evicts 3
        assert_eq!(cache.get(&3), None);
        // 1, 4, 5 remain, oldest first (a get that misses touches nothing)
        cache.put(6, 60); // evicts 1
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&4), Some(40));
        assert_eq!(cache.get(&5), Some(50));
        assert_eq!(cache.get(&6), Some(60));
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_lru_overwrite_refreshes_recency_without_evicting() {
        let mut cache = LruCache::new(2);
        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(1, "uno"); // overwrite at capacity: no eviction, 1 becomes most recent
        assert_eq!(cache.len(), 2);
        cache.put(3, "three"); // evicts 2, the least recently used
        assert_eq!(cache.get(&1), Some("uno"));
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.get(&3), Some("three"));
    }

    #[test]
    fn test_lru_zero_capacity_holds_one_entry() {
        // The scanning implementation always kept the newest entry, even at capacity 0.
        let mut cache = LruCache::new(0);
        cache.put(1, "one");
        assert_eq!(cache.get(&1), Some("one"));
        cache.put(2, "two");
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some("two"));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_lru_remove_and_clear() {
        let mut cache = LruCache::new(2);
        cache.put(1, "one");
        cache.put(2, "two");
        cache.remove(&1);
        cache.remove(&42); // absent: no-op
        assert_eq!(cache.len(), 1);
        cache.put(3, "three"); // a slot was free: nothing evicted
        assert_eq!(cache.get(&2), Some("two"));
        assert_eq!(cache.get(&3), Some("three"));
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.get(&2), None);
    }

    #[test]
    fn test_lru_records_one_eviction_metric_per_eviction() {
        let metrics = Arc::new(StorageMetrics::new());
        let mut cache = LruCache::with_metrics(2, metrics.clone());
        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(1, 11); // overwrite: not an eviction
        cache.remove(&2); // removal: not an eviction
        cache.put(3, 3);
        assert_eq!(metrics.cache_evictions(), 0);
        cache.put(4, 4);
        cache.put(5, 5);
        assert_eq!(metrics.cache_evictions(), 2);
    }

    #[test]
    fn test_lru_estimate_size_bytes() {
        let mut cache = LruCache::new(4);
        cache.put(vec![1u8, 2], vec![0u8; 10]);
        cache.put(vec![3u8], vec![0u8; 5]);
        assert_eq!(cache.estimate_size_bytes(), 18);
    }

    /// A shard at capacity evicts exactly its least recently used entry on every put, and
    /// does so without scanning: 50k evicting puts into a full 50k-entry shard cost 2.5e9
    /// entry visits with the old min-by-access-time scan, and take milliseconds with a
    /// linked LRU. The time bound is deliberately loose (it only has to tell O(1) from
    /// O(n) per put), so a slow CI machine does not flake it.
    #[test]
    fn test_full_shard_evicts_lru_entry_in_constant_time() {
        const CAP: u32 = 50_000;
        let mut cache = LruCache::new(CAP as usize);
        for k in 0..CAP {
            cache.put(k, k);
        }
        assert_eq!(cache.get(&0), Some(0)); // 0 is now the most recently used

        let start = std::time::Instant::now();
        for k in CAP..(2 * CAP - 1) {
            cache.put(k, k);
        }
        let elapsed = start.elapsed();

        assert_eq!(cache.len(), CAP as usize);
        assert_eq!(
            cache.get(&0),
            Some(0),
            "the touched entry outlived the rest"
        );
        for k in 1..CAP {
            assert_eq!(cache.get(&k), None, "key {k} should have been evicted");
        }
        assert!(
            elapsed < std::time::Duration::from_secs(10),
            "49_999 evicting puts took {elapsed:?}: eviction is not O(1)"
        );
    }

    #[test]
    fn test_sharded_capacity_is_per_shard_floor_times_shards() {
        assert_eq!(ShardedLruCache::<u32, u32>::new(1_600).capacity(), 1_600);
        assert_eq!(ShardedLruCache::<u32, u32>::new(1_610).capacity(), 1_600);
        assert_eq!(ShardedLruCache::<u32, u32>::new(16).capacity(), 16);
        assert_eq!(ShardedLruCache::<u32, u32>::new(5).capacity(), 16); // max(1, 5/16)
        assert_eq!(ShardedLruCache::<u32, u32>::new(0).capacity(), 16);
        let metrics = Arc::new(StorageMetrics::new());
        assert_eq!(
            ShardedLruCache::<u32, u32>::with_metrics(100_000, metrics).capacity(),
            100_000
        );
        assert_eq!(
            ShardedLruCache::<u32, u32>::with_shard_count(10, 4).capacity(),
            8
        );
    }

    #[tokio::test]
    async fn test_sharded_cache_never_exceeds_capacity() {
        let cache = ShardedLruCache::new(64);
        for i in 0..10_000u32 {
            cache.put(i, i).await;
        }
        assert!(cache.len().await <= cache.capacity());
    }

    /// The setup `bench_wal_get_one` (benches/iai_hot_paths.rs) relies on: capacity 16
    /// gives every shard one slot, so 256 further distinct puts evict the first key.
    #[tokio::test]
    async fn test_sharded_capacity_16_with_256_decoys_evicts_key() {
        let cache = ShardedLruCache::new(16);
        cache.put(b"bench-key".to_vec(), vec![1u8]).await;
        for i in 0..256u32 {
            cache
                .put(format!("evict-{i}").into_bytes(), b"x".to_vec())
                .await;
        }
        assert_eq!(cache.get(&b"bench-key".to_vec()).await, None);
        assert!(cache.len().await <= 16);
    }

    #[tokio::test]
    async fn test_sharded_put_batch_last_write_of_a_key_wins() {
        let cache = ShardedLruCache::new(100);
        cache
            .put_batch(vec![
                (vec![1u8], (1u64, vec![10u8])),
                (vec![2u8], (1u64, vec![20u8])),
                (vec![1u8], (1u64, vec![11u8])),
            ])
            .await;
        assert_eq!(cache.get(&vec![1u8]).await, Some((1, vec![11])));
        assert_eq!(cache.get(&vec![2u8]).await, Some((1, vec![20])));
        assert_eq!(cache.len().await, 2);
    }

    #[tokio::test]
    async fn test_sharded_put_batch_keeps_op_order_within_a_shard() {
        // One shard of capacity 2: the batch's later entries are the more recent.
        let cache = ShardedLruCache::with_shard_count(2, 1);
        cache.put_batch(vec![(1u32, 1u32), (2, 2), (3, 3)]).await;
        assert_eq!(cache.get(&1).await, None);
        assert_eq!(cache.get(&2).await, Some(2));
        assert_eq!(cache.get(&3).await, Some(3));
    }

    #[tokio::test]
    async fn test_sharded_remove_and_remove_batch() {
        let cache = ShardedLruCache::new(100);
        for i in 0..10u32 {
            cache.put(i, i).await;
        }
        cache.remove(&0).await;
        cache.remove_batch(vec![1, 2, 3, 99]).await;
        for i in 0..4u32 {
            assert_eq!(cache.get(&i).await, None);
        }
        for i in 4..10u32 {
            assert_eq!(cache.get(&i).await, Some(i));
        }
        assert_eq!(cache.len().await, 6);
        cache.clear().await;
        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_sharded_cache_replaces_stale_lsn_entry() {
        // wal_adapter caches (lsn, value) and only serves an entry whose LSN matches the
        // index; a newer write must replace, never sit beside, the older entry.
        let cache = ShardedLruCache::new(100);
        cache.put(b"k".to_vec(), (1u64, b"old".to_vec())).await;
        cache.put(b"k".to_vec(), (2u64, b"new".to_vec())).await;
        assert_eq!(cache.get(&b"k".to_vec()).await, Some((2, b"new".to_vec())));
        assert_eq!(cache.len().await, 1);
    }

    #[tokio::test]
    async fn test_sharded_estimate_size_bytes() {
        let cache = ShardedLruCache::new(100);
        cache.put(vec![1u8], vec![0u8; 9]).await;
        cache.put(vec![2u8, 3], vec![0u8; 3]).await;
        assert_eq!(cache.estimate_size_bytes().await, 15);
    }
}
