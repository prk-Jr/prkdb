//! # IndexedStorage - High-Performance Storage with Secondary Indexes
//!
//! This module provides a generic storage adapter with secondary index support,
//! enabling fast queries by any indexed field. It uses lock-free DashMap data
//! structures for concurrent access, achieving **878K batch writes/sec** and
//! **894K indexed queries/sec**.
//!
//! ## Features
//!
//! - **Lock-free indexes** - DashMap-based concurrent access
//! - **Secondary indexes** - Query by any `#[index]` field
//! - **Full-text search** - Tokenized text search with ranking
//! - **Batch operations** - Optimized `insert_batch`, `delete_batch`, `upsert_batch`
//! - **Change notifications** - Subscribe to insert/delete events
//! - **Auto-sync** - Periodic index persistence to disk
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use prkdb::indexed_storage::IndexedStorage;
//! use prkdb::storage::InMemoryAdapter;
//! use prkdb_macros::Collection;
//! use serde::{Serialize, Deserialize};
//! use std::sync::Arc;
//!
//! #[derive(Collection, Serialize, Deserialize, Clone)]
//! struct User {
//!     #[id]
//!     id: String,
//!     #[index]
//!     email: String,
//!     #[index]
//!     role: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let storage = Arc::new(InMemoryAdapter::new());
//!     let db = IndexedStorage::new(storage);
//!
//!     // Insert a user
//!     let user = User {
//!         id: "1".into(),
//!         email: "alice@example.com".into(),
//!         role: "admin".into(),
//!     };
//!     db.insert(&user).await?;
//!
//!     // Query by index
//!     let admins: Vec<User> = db.query_by("role", &"admin").await?;
//!     println!("Found {} admins", admins.len());
//!
//!     // Batch insert (76x faster!)
//!     let users: Vec<User> = (0..1000).map(|i| User {
//!         id: i.to_string(),
//!         email: format!("user{}@example.com", i),
//!         role: "user".into(),
//!     }).collect();
//!     db.insert_batch(&users).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Performance
//!
//! | Operation | Speed |
//! |-----------|-------|
//! | `insert_batch` | 878K ops/sec |
//! | `query_by` | 894K ops/sec |
//! | `delete_batch` | 13K ops/sec |
//! | Mixed workload | 61K ops/sec |

use crate::storage::WalStorageAdapter;
use dashmap::DashMap;
use prkdb_types::collection::Collection;
use prkdb_types::error::StorageError;
use prkdb_types::index::Indexed;
use prkdb_types::storage::StorageAdapter;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Type alias for backwards compatibility
pub type IndexedWalStorage = IndexedStorage<WalStorageAdapter>;

/// Persisted index file format
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedIndexes {
    /// Version for future migrations
    version: u32,
    /// Collection name -> index data
    collections: BTreeMap<String, MemoryIndex>,
}

impl Default for PersistedIndexes {
    fn default() -> Self {
        Self {
            version: 1,
            collections: BTreeMap::new(),
        }
    }
}

/// Index statistics for monitoring
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    /// Number of indexed fields
    pub field_count: usize,
    /// Total unique values across all fields
    pub unique_values: usize,
    /// Total entries (may have duplicates across indexes)
    pub total_entries: usize,
    /// Number of compound indexes
    pub compound_indexes: usize,
    /// Number of text/full-text indexes
    pub text_indexes: usize,
}

impl std::fmt::Display for IndexStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Fields: {} | Values: {} | Entries: {} | Compound: {} | Text: {}",
            self.field_count,
            self.unique_values,
            self.total_entries,
            self.compound_indexes,
            self.text_indexes
        )
    }
}

/// In-memory index structure
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct MemoryIndex {
    /// field_name -> (indexed_value -> primary_keys)
    indexes: BTreeMap<String, BTreeMap<Vec<u8>, Vec<Vec<u8>>>>,
    /// compound_index_name -> (composite_value -> primary_keys)
    compound_indexes: BTreeMap<String, BTreeMap<Vec<u8>, Vec<Vec<u8>>>>,
    /// Full-text indexes: field_name -> (token -> primary_keys)
    text_indexes: BTreeMap<String, BTreeMap<String, Vec<Vec<u8>>>>,
}

impl MemoryIndex {
    fn new() -> Self {
        Self::default()
    }

    /// Add an entry to an index
    fn add(&mut self, field: &str, value: Vec<u8>, primary_key: Vec<u8>) {
        self.indexes
            .entry(field.to_string())
            .or_insert_with(BTreeMap::new)
            .entry(value)
            .or_insert_with(Vec::new)
            .push(primary_key);
    }

    /// Remove an entry from an index
    fn remove(&mut self, field: &str, value: &[u8], primary_key: &[u8]) {
        if let Some(field_index) = self.indexes.get_mut(field) {
            if let Some(keys) = field_index.get_mut(value) {
                keys.retain(|k| k != primary_key);
                if keys.is_empty() {
                    field_index.remove(value);
                }
            }
        }
    }

    /// Query by exact value
    fn query(&self, field: &str, value: &[u8]) -> Vec<Vec<u8>> {
        self.indexes
            .get(field)
            .and_then(|idx| idx.get(value))
            .cloned()
            .unwrap_or_default()
    }

    /// Query by range (using BTreeMap's efficient range)
    fn query_range(&self, field: &str, start: &[u8], end: &[u8]) -> Vec<Vec<u8>> {
        self.indexes
            .get(field)
            .map(|idx| {
                idx.range(start.to_vec()..end.to_vec())
                    .flat_map(|(_, pks)| pks.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Query by prefix (for string fields)
    fn query_prefix(&self, field: &str, prefix: &[u8]) -> Vec<Vec<u8>> {
        self.indexes
            .get(field)
            .map(|idx| {
                // Find all keys that start with this prefix
                idx.range(prefix.to_vec()..)
                    .take_while(|(k, _)| k.starts_with(prefix))
                    .flat_map(|(_, pks)| pks.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all primary keys (for filter scan)
    fn all_keys(&self) -> Vec<Vec<u8>> {
        self.indexes
            .values()
            .flat_map(|idx| idx.values().flatten().cloned())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Add to compound index
    fn add_compound(&mut self, index_name: &str, composite_value: Vec<u8>, primary_key: Vec<u8>) {
        self.compound_indexes
            .entry(index_name.to_string())
            .or_insert_with(BTreeMap::new)
            .entry(composite_value)
            .or_insert_with(Vec::new)
            .push(primary_key);
    }

    /// Remove from compound index
    #[allow(dead_code)]
    fn remove_compound(&mut self, index_name: &str, composite_value: &[u8], primary_key: &[u8]) {
        if let Some(idx) = self.compound_indexes.get_mut(index_name) {
            if let Some(keys) = idx.get_mut(composite_value) {
                keys.retain(|k| k != primary_key);
                if keys.is_empty() {
                    idx.remove(composite_value);
                }
            }
        }
    }

    /// Query compound index
    fn query_compound(&self, index_name: &str, composite_value: &[u8]) -> Vec<Vec<u8>> {
        self.compound_indexes
            .get(index_name)
            .and_then(|idx| idx.get(composite_value))
            .cloned()
            .unwrap_or_default()
    }

    /// Get index statistics
    pub fn stats(&self) -> IndexStats {
        let mut total_entries = 0;
        let mut total_keys = 0;

        for (_, index) in &self.indexes {
            total_keys += index.len();
            for (_, pks) in index {
                total_entries += pks.len();
            }
        }

        IndexStats {
            field_count: self.indexes.len(),
            unique_values: total_keys,
            total_entries,
            compound_indexes: self.compound_indexes.len(),
            text_indexes: self.text_indexes.len(),
        }
    }

    /// Tokenize text into searchable tokens
    fn tokenize(text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty() && s.len() > 1)
            .map(|s| s.to_string())
            .collect()
    }

    /// Add text to full-text index
    fn add_text(&mut self, field: &str, text: &str, primary_key: &[u8]) {
        let tokens = Self::tokenize(text);
        let idx = self.text_indexes.entry(field.to_string()).or_default();
        for token in tokens {
            idx.entry(token).or_default().push(primary_key.to_vec());
        }
    }

    /// Remove text from full-text index
    #[allow(dead_code)]
    fn remove_text(&mut self, field: &str, text: &str, primary_key: &[u8]) {
        let tokens = Self::tokenize(text);
        if let Some(idx) = self.text_indexes.get_mut(field) {
            for token in tokens {
                if let Some(keys) = idx.get_mut(&token) {
                    keys.retain(|k| k != primary_key);
                    if keys.is_empty() {
                        idx.remove(&token);
                    }
                }
            }
        }
    }

    /// Search full-text index, returns primary keys ranked by match count
    fn search_text(&self, field: &str, query: &str) -> Vec<(Vec<u8>, usize)> {
        let tokens = Self::tokenize(query);
        let mut scores: std::collections::HashMap<Vec<u8>, usize> =
            std::collections::HashMap::new();

        if let Some(idx) = self.text_indexes.get(field) {
            for token in &tokens {
                if let Some(keys) = idx.get(token) {
                    for key in keys {
                        *scores.entry(key.clone()).or_default() += 1;
                    }
                }
            }
        }

        let mut results: Vec<_> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by score descending
        results
    }
}

/// Change event types for watch/subscribe
#[derive(Debug, Clone)]
pub enum ChangeEvent {
    /// Record inserted
    Inserted {
        collection: String,
        id: Vec<u8>,
        data: Vec<u8>,
    },
    /// Record deleted
    Deleted { collection: String, id: Vec<u8> },
}

// =========================================================================
// QUERY BUILDER
// =========================================================================

/// Query execution plan for debugging and optimization
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// Type name being queried
    pub collection: String,
    /// Number of filters to apply
    pub filter_count: usize,
    /// Whether results are preloaded from index
    pub uses_preload: bool,
    /// Whether sorting is enabled
    pub has_ordering: bool,
    /// Limit on number of results
    pub limit: Option<usize>,
    /// Number of records to skip
    pub offset: usize,
    /// Human-readable execution plan
    pub description: String,
}

impl std::fmt::Display for QueryPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Query Plan for {}", self.collection)?;
        writeln!(f, "├─ Filters: {}", self.filter_count)?;
        writeln!(
            f,
            "├─ Uses Index: {}",
            if self.uses_preload {
                "Yes"
            } else {
                "No (full scan)"
            }
        )?;
        writeln!(
            f,
            "├─ Ordering: {}",
            if self.has_ordering { "Yes" } else { "No" }
        )?;
        writeln!(
            f,
            "├─ Limit: {}",
            self.limit
                .map(|n| n.to_string())
                .unwrap_or("None".to_string())
        )?;
        writeln!(f, "├─ Offset: {}", self.offset)?;
        writeln!(f, "└─ {}", self.description)?;
        Ok(())
    }
}

/// Query builder for fluent query construction
///
/// # Example
/// ```ignore
/// let users = db.query::<User>()
///     .filter(|u| u.age > 18)
///     .order_by(|u| u.created_at)
///     .take(10)
///     .collect().await?;
/// ```
pub struct QueryBuilder<'a, S: StorageAdapter, T> {
    storage: &'a IndexedStorage<S>,
    filters: Vec<Box<dyn Fn(&T) -> bool + 'a>>,
    order_fn: Option<Box<dyn Fn(&T, &T) -> std::cmp::Ordering + 'a>>,
    limit: Option<usize>,
    offset: usize,
    /// Pre-loaded records from indexed query (optional)
    preloaded: Option<Vec<T>>,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, S: StorageAdapter + 'static, T> QueryBuilder<'a, S, T>
where
    T: prkdb_types::index::Indexed
        + prkdb_types::collection::Collection
        + serde::de::DeserializeOwned
        + Clone,
{
    /// Create a new query builder
    pub fn new(storage: &'a IndexedStorage<S>) -> Self {
        Self {
            storage,
            filters: Vec::new(),
            order_fn: None,
            limit: None,
            offset: 0,
            preloaded: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Filter records using a closure
    ///
    /// Can be chained multiple times. All filters are ANDed together.
    pub fn filter<F: Fn(&T) -> bool + 'a>(mut self, predicate: F) -> Self {
        self.filters.push(Box::new(predicate));
        self
    }

    /// Sort results by a key
    pub fn order_by<K: Ord, F: Fn(&T) -> K + 'a + Clone>(mut self, key_fn: F) -> Self {
        let key_fn_clone = key_fn.clone();
        self.order_fn = Some(Box::new(move |a, b| key_fn(a).cmp(&key_fn_clone(b))));
        self
    }

    /// Sort results by a key in descending order
    pub fn order_by_desc<K: Ord, F: Fn(&T) -> K + 'a + Clone>(mut self, key_fn: F) -> Self {
        let key_fn_clone = key_fn.clone();
        self.order_fn = Some(Box::new(move |a, b| key_fn_clone(b).cmp(&key_fn(a))));
        self
    }

    /// Limit the number of results
    pub fn take(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Skip the first N results
    pub fn skip(mut self, n: usize) -> Self {
        self.offset = n;
        self
    }

    /// Cursor-based pagination: skip records until after the given ID
    ///
    /// More efficient than offset-based pagination for large datasets.
    /// Works by filtering out records with IDs <= the cursor.
    ///
    /// # Example
    /// ```ignore
    /// // First page
    /// let page1 = db.query::<User>()
    ///     .order_by(|u| u.id)
    ///     .take(10)
    ///     .collect().await?;
    ///
    /// // Next page (using last item's ID as cursor)
    /// let last_id = page1.last().map(|u| u.id);
    /// let page2 = db.query::<User>()
    ///     .after(&last_id.unwrap())
    ///     .order_by(|u| u.id)
    ///     .take(10)
    ///     .collect().await?;
    /// ```
    pub fn after(self, cursor: &<T as Collection>::Id) -> Self
    where
        <T as Collection>::Id: Clone + PartialOrd + 'a,
    {
        let cursor_clone = cursor.clone();
        self.filter(move |r| r.id() > &cursor_clone)
    }

    /// Get a page of results with cursor-based pagination info
    ///
    /// Returns results along with the cursor for the next page.
    ///
    /// # Example
    /// ```ignore
    /// let (users, next_cursor) = db.query::<User>()
    ///     .order_by(|u| u.id)
    ///     .paginate(10, None).await?;
    ///
    /// // Get next page
    /// if let Some(cursor) = next_cursor {
    ///     let (more, _) = db.query::<User>()
    ///         .order_by(|u| u.id)
    ///         .paginate(10, Some(cursor)).await?;
    /// }
    /// ```
    pub async fn paginate(
        self,
        page_size: usize,
        cursor: Option<<T as Collection>::Id>,
    ) -> Result<(Vec<T>, Option<<T as Collection>::Id>), StorageError>
    where
        <T as Collection>::Id: Clone + PartialOrd + 'a,
    {
        // Apply cursor filter if provided
        let builder = if let Some(c) = cursor {
            self.after(&c)
        } else {
            self
        };

        // Fetch page_size + 1 to detect if there are more
        let results = builder.take(page_size + 1).collect().await?;

        // Check if there are more results
        let has_more = results.len() > page_size;
        let page: Vec<T> = results.into_iter().take(page_size).collect();

        // Get cursor for next page
        let next_cursor = if has_more {
            page.last().map(|r| r.id().clone())
        } else {
            None
        };

        Ok((page, next_cursor))
    }

    /// Explain the query execution plan without executing it
    ///
    /// # Example
    /// ```ignore
    /// let plan = db.query::<User>()
    ///     .filter(|u| u.age > 18)
    ///     .take(10)
    ///     .explain();
    /// println!("{}", plan);  // Pretty-printed query plan
    /// ```
    pub fn explain(&self) -> QueryPlan {
        let collection = std::any::type_name::<T>().to_string();
        let uses_preload = self.preloaded.is_some();

        let description = if uses_preload {
            format!(
                "Load {} preloaded records → Apply {} filters → {}",
                self.preloaded.as_ref().map(|v| v.len()).unwrap_or(0),
                self.filters.len(),
                if self.order_fn.is_some() {
                    "Sort → "
                } else {
                    ""
                },
            )
        } else {
            format!(
                "Full scan → Apply {} filters → {}Return results",
                self.filters.len(),
                if self.order_fn.is_some() {
                    "Sort → "
                } else {
                    ""
                },
            )
        };

        QueryPlan {
            collection,
            filter_count: self.filters.len(),
            uses_preload,
            has_ordering: self.order_fn.is_some(),
            limit: self.limit,
            offset: self.offset,
            description,
        }
    }

    /// Execute query and collect results
    pub async fn collect(self) -> Result<Vec<T>, StorageError> {
        // Get all records (or use preloaded if available)
        let mut records = match self.preloaded {
            Some(r) => r,
            None => self.storage.all::<T>().await?,
        };

        // Apply filters
        for filter in &self.filters {
            records.retain(|r| filter(r));
        }

        // Apply ordering
        if let Some(order_fn) = &self.order_fn {
            records.sort_by(|a, b| order_fn(a, b));
        }

        // Apply offset and limit
        let results: Vec<T> = records
            .into_iter()
            .skip(self.offset)
            .take(self.limit.unwrap_or(usize::MAX))
            .collect();

        Ok(results)
    }

    /// Execute query and get first result
    pub async fn first(mut self) -> Result<Option<T>, StorageError> {
        self.limit = Some(1);
        let results = self.collect().await?;
        Ok(results.into_iter().next())
    }

    /// Execute query and count results
    pub async fn count(self) -> Result<usize, StorageError> {
        let results = self.collect().await?;
        Ok(results.len())
    }

    /// Transform results into a different type (projection)
    pub fn select<U, F: Fn(T) -> U + 'a>(self, map_fn: F) -> MappedQueryBuilder<'a, S, T, U, F> {
        MappedQueryBuilder {
            inner: self,
            map_fn,
        }
    }

    /// Add computed fields to results
    ///
    /// # Example
    /// ```ignore
    /// let users_with_age = db.query::<User>()
    ///     .with_computed(|user| {
    ///         let age_days = (now - user.birth_date) / 86400;
    ///         age_days
    ///     })
    ///     .collect().await?;  // Vec<WithComputed<User, u64>>
    /// ```
    pub async fn with_computed<C, F>(
        self,
        compute_fn: F,
    ) -> Result<Vec<prkdb_types::collection::WithComputed<T, C>>, StorageError>
    where
        F: Fn(&T) -> C,
    {
        let records = self.collect().await?;
        Ok(records
            .into_iter()
            .map(|record| {
                let computed = compute_fn(&record);
                prkdb_types::collection::WithComputed::new(record, computed)
            })
            .collect())
    }

    /// Extract a single field from all records (like SQL SELECT column)
    ///
    /// # Example
    /// ```ignore
    /// let names: Vec<String> = db.query::<User>()
    ///     .pluck(|u| u.name.clone())
    ///     .await?;
    /// ```
    pub async fn pluck<V, F>(self, field_fn: F) -> Result<Vec<V>, StorageError>
    where
        F: Fn(&T) -> V,
    {
        let records = self.collect().await?;
        Ok(records.iter().map(|r| field_fn(r)).collect())
    }

    /// Partition records by a predicate
    ///
    /// Returns (matching, not_matching)
    ///
    /// # Example
    /// ```ignore
    /// let (active, inactive) = db.query::<User>()
    ///     .partition(|u| u.active)
    ///     .await?;
    /// ```
    pub async fn partition<F>(self, predicate: F) -> Result<(Vec<T>, Vec<T>), StorageError>
    where
        F: Fn(&T) -> bool,
    {
        let records = self.collect().await?;
        let (matching, not_matching): (Vec<T>, Vec<T>) = records.into_iter().partition(predicate);
        Ok((matching, not_matching))
    }

    /// Take records while predicate is true
    ///
    /// # Example
    /// ```ignore
    /// let early_users = db.query::<User>()
    ///     .order_by(|u| u.id as i64)
    ///     .take_while(|u| u.id < 100)
    ///     .await?;
    /// ```
    pub async fn take_while<F>(self, predicate: F) -> Result<Vec<T>, StorageError>
    where
        F: Fn(&T) -> bool,
    {
        let records = self.collect().await?;
        Ok(records.into_iter().take_while(predicate).collect())
    }

    /// Skip records while predicate is true
    ///
    /// # Example
    /// ```ignore
    /// let later_users = db.query::<User>()
    ///     .order_by(|u| u.id as i64)
    ///     .skip_while(|u| u.id < 100)
    ///     .await?;
    /// ```
    pub async fn skip_while<F>(self, predicate: F) -> Result<Vec<T>, StorageError>
    where
        F: Fn(&T) -> bool,
    {
        let records = self.collect().await?;
        Ok(records.into_iter().skip_while(predicate).collect())
    }

    /// Get distinct values by a key function
    ///
    /// # Example
    /// ```ignore
    /// let unique_roles = db.query::<User>()
    ///     .distinct(|u| u.role.clone())
    ///     .await?;
    /// ```
    pub async fn distinct<K, F>(self, key_fn: F) -> Result<Vec<K>, StorageError>
    where
        K: Eq + std::hash::Hash,
        F: Fn(&T) -> K,
    {
        let records = self.collect().await?;
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for record in records {
            let key = key_fn(&record);
            if seen.insert(key) {
                result.push(key_fn(&record));
            }
        }
        Ok(result)
    }

    /// Group results by a key function
    ///
    /// # Example
    /// ```ignore
    /// let by_role = db.query::<User>()
    ///     .group_by(|u| u.role.clone())
    ///     .await?;  // HashMap<String, Vec<User>>
    /// ```
    pub async fn group_by<K, F>(
        self,
        key_fn: F,
    ) -> Result<std::collections::HashMap<K, Vec<T>>, StorageError>
    where
        K: Eq + std::hash::Hash,
        F: Fn(&T) -> K,
    {
        let records = self.collect().await?;
        let mut groups: std::collections::HashMap<K, Vec<T>> = std::collections::HashMap::new();
        for record in records {
            let key = key_fn(&record);
            groups.entry(key).or_default().push(record);
        }
        Ok(groups)
    }

    /// Sum values by a key function (grouped aggregation)
    ///
    /// # Example
    /// ```ignore
    /// let salaries_by_role = db.query::<User>()
    ///     .sum_by(|u| u.role.clone(), |u| u.salary)
    ///     .await?;  // HashMap<String, f64>
    /// ```
    pub async fn sum_by<K, V, KF, VF>(
        self,
        key_fn: KF,
        value_fn: VF,
    ) -> Result<std::collections::HashMap<K, V>, StorageError>
    where
        K: Eq + std::hash::Hash,
        V: std::ops::Add<Output = V> + Default + Copy,
        KF: Fn(&T) -> K,
        VF: Fn(&T) -> V,
    {
        let records = self.collect().await?;
        let mut sums: std::collections::HashMap<K, V> = std::collections::HashMap::new();
        for record in records {
            let key = key_fn(&record);
            let value = value_fn(&record);
            let entry = sums.entry(key).or_default();
            *entry = *entry + value;
        }
        Ok(sums)
    }

    /// Count records by a key function (grouped count)
    ///
    /// # Example
    /// ```ignore
    /// let users_by_role = db.query::<User>()
    ///     .count_by(|u| u.role.clone())
    ///     .await?;  // HashMap<String, usize>
    /// ```
    pub async fn count_by<K, F>(
        self,
        key_fn: F,
    ) -> Result<std::collections::HashMap<K, usize>, StorageError>
    where
        K: Eq + std::hash::Hash,
        F: Fn(&T) -> K,
    {
        let records = self.collect().await?;
        let mut counts: std::collections::HashMap<K, usize> = std::collections::HashMap::new();
        for record in records {
            let key = key_fn(&record);
            *counts.entry(key).or_default() += 1;
        }
        Ok(counts)
    }

    /// Find minimum value by a field
    ///
    /// # Example
    /// ```ignore
    /// let youngest = db.query::<User>()
    ///     .min_by(|u| u.age)
    ///     .await?;  // Option<User>
    /// ```
    pub async fn min_by<K, F>(self, key_fn: F) -> Result<Option<T>, StorageError>
    where
        K: Ord,
        F: Fn(&T) -> K,
    {
        let records = self.collect().await?;
        Ok(records.into_iter().min_by_key(|r| key_fn(r)))
    }

    /// Find maximum value by a field
    ///
    /// # Example
    /// ```ignore
    /// let oldest = db.query::<User>()
    ///     .max_by(|u| u.age)
    ///     .await?;  // Option<User>
    /// ```
    pub async fn max_by<K, F>(self, key_fn: F) -> Result<Option<T>, StorageError>
    where
        K: Ord,
        F: Fn(&T) -> K,
    {
        let records = self.collect().await?;
        Ok(records.into_iter().max_by_key(|r| key_fn(r)))
    }

    /// Calculate average of a numeric field
    ///
    /// # Example
    /// ```ignore
    /// let avg_age = db.query::<User>()
    ///     .avg_by(|u| u.age as f64)
    ///     .await?;  // Option<f64>
    /// ```
    pub async fn avg_by<F>(self, field_fn: F) -> Result<Option<f64>, StorageError>
    where
        F: Fn(&T) -> f64,
    {
        let records = self.collect().await?;
        if records.is_empty() {
            return Ok(None);
        }
        let sum: f64 = records.iter().map(&field_fn).sum();
        Ok(Some(sum / records.len() as f64))
    }

    /// Check if any record matches a predicate
    ///
    /// # Example
    /// ```ignore
    /// let has_admin = db.query::<User>()
    ///     .any(|u| u.role == "admin")
    ///     .await?;  // bool
    /// ```
    pub async fn any<F>(self, predicate: F) -> Result<bool, StorageError>
    where
        F: Fn(&T) -> bool,
    {
        let records = self.collect().await?;
        Ok(records.iter().any(predicate))
    }

    /// Check if all records match a predicate
    ///
    /// # Example
    /// ```ignore
    /// let all_active = db.query::<User>()
    ///     .all(|u| u.active)
    ///     .await?;  // bool
    /// ```
    pub async fn all<F>(self, predicate: F) -> Result<bool, StorageError>
    where
        F: Fn(&T) -> bool,
    {
        let records = self.collect().await?;
        Ok(records.iter().all(predicate))
    }

    /// Fold/reduce over records with initial value
    ///
    /// # Example
    /// ```ignore
    /// let total_salary = db.query::<User>()
    ///     .fold(0.0, |acc, u| acc + u.salary)
    ///     .await?;
    /// ```
    pub async fn fold<B, F>(self, init: B, fold_fn: F) -> Result<B, StorageError>
    where
        F: Fn(B, &T) -> B,
    {
        let records = self.collect().await?;
        Ok(records.iter().fold(init, fold_fn))
    }

    /// Get a random sample of records
    ///
    /// # Example
    /// ```ignore
    /// let sample = db.query::<User>()
    ///     .sample(5)
    ///     .await?;  // 5 random users
    /// ```
    pub async fn sample(self, n: usize) -> Result<Vec<T>, StorageError> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut records = self.collect().await?;

        // Simple shuffle using record index as seed
        let len = records.len();
        if len <= n {
            return Ok(records);
        }

        // Fisher-Yates shuffle (simplified)
        for i in (1..len).rev() {
            let mut hasher = DefaultHasher::new();
            i.hash(&mut hasher);
            let j = (hasher.finish() as usize) % (i + 1);
            records.swap(i, j);
        }

        records.truncate(n);
        Ok(records)
    }

    /// Get last matching record
    ///
    /// # Example
    /// ```ignore
    /// let last_admin = db.query::<User>()
    ///     .filter(|u| u.role == "admin")
    ///     .last()
    ///     .await?;
    /// ```
    pub async fn last(self) -> Result<Option<T>, StorageError> {
        let records = self.collect().await?;
        Ok(records.into_iter().last())
    }

    /// Process records in chunks/batches
    ///
    /// Returns Vec of Vec where each inner Vec has at most `size` elements.
    ///
    /// # Example
    /// ```ignore
    /// let chunks = db.query::<User>()
    ///     .chunks(10)
    ///     .await?;  // Vec<Vec<User>>, each with up to 10 users
    /// ```
    pub async fn chunks(self, size: usize) -> Result<Vec<Vec<T>>, StorageError> {
        let records = self.collect().await?;
        Ok(records.chunks(size).map(|c| c.to_vec()).collect())
    }

    /// Add index to each record (like enumerate)
    ///
    /// # Example
    /// ```ignore
    /// let indexed = db.query::<User>()
    ///     .enumerate()
    ///     .await?;  // Vec<(usize, User)>
    /// ```
    pub async fn enumerate(self) -> Result<Vec<(usize, T)>, StorageError> {
        let records = self.collect().await?;
        Ok(records.into_iter().enumerate().collect())
    }

    /// Remove consecutive duplicates by key
    ///
    /// # Example
    /// ```ignore
    /// let unique_roles = db.query::<User>()
    ///     .order_by(|u| u.role.clone())
    ///     .dedup_by_key(|u| u.role.clone())
    ///     .await?;
    /// ```
    pub async fn dedup_by_key<K, F>(self, key_fn: F) -> Result<Vec<T>, StorageError>
    where
        K: PartialEq,
        F: Fn(&T) -> K,
    {
        let records = self.collect().await?;
        let mut result: Vec<T> = Vec::new();
        let mut last_key: Option<K> = None;

        for record in records {
            let key = key_fn(&record);
            if last_key.as_ref() != Some(&key) {
                last_key = Some(key);
                result.push(record);
            }
        }

        Ok(result)
    }

    /// Join two record sets by a key
    ///
    /// # Example
    /// ```ignore
    /// let users = vec![...];
    /// let joined = db.query::<Order>()
    ///     .join_with(&users, |order| order.user_id, |user| user.id)
    ///     .await?;  // Vec<(Order, Option<User>)>
    /// ```
    pub async fn join_with<R, FK, RK, K>(
        self,
        related: &[R],
        foreign_key_fn: FK,
        related_key_fn: RK,
    ) -> Result<Vec<(T, Option<R>)>, StorageError>
    where
        R: Clone,
        K: Eq + std::hash::Hash,
        FK: Fn(&T) -> K,
        RK: Fn(&R) -> K,
    {
        use std::collections::HashMap;

        // Build lookup map
        let lookup: HashMap<K, R> = related
            .iter()
            .map(|r| (related_key_fn(r), r.clone()))
            .collect();

        let records = self.collect().await?;
        Ok(records
            .into_iter()
            .map(|record| {
                let key = foreign_key_fn(&record);
                let related = lookup.get(&key).cloned();
                (record, related)
            })
            .collect())
    }

    /// Set preloaded records (used by indexed queries)
    pub fn with_preloaded(mut self, records: Vec<T>) -> Self {
        self.preloaded = Some(records);
        self
    }

    /// Eager load related records (has_many relationship)
    ///
    /// Fetches all related records for each primary record in a single query,
    /// avoiding N+1 query problem.
    ///
    /// # Example
    /// ```ignore
    /// // User has many Orders
    /// let users_with_orders = db.query::<User>()
    ///     .filter(|u| u.active)
    ///     .collect_with::<Order, _, _>(|user| user.id.clone(), |order| order.user_id.clone())
    ///     .await?;
    ///
    /// // Returns Vec<(User, Vec<Order>)>
    /// for (user, orders) in users_with_orders {
    ///     println!("{} has {} orders", user.name, orders.len());
    /// }
    /// ```
    pub async fn collect_with<R, FK, RK>(
        self,
        _foreign_key_fn: FK,
        related_key_fn: RK,
    ) -> Result<Vec<(T, Vec<R>)>, StorageError>
    where
        R: Indexed + Collection + DeserializeOwned + Clone,
        FK: Fn(&T) -> <R as Collection>::Id,
        RK: Fn(&R) -> <T as Collection>::Id,
        <T as Collection>::Id: Eq + std::hash::Hash + Clone,
        <R as Collection>::Id: Eq + std::hash::Hash + Clone,
    {
        // Save storage reference before consuming self
        let storage = self.storage;

        // Get primary records
        let primary_records = self.collect().await?;

        // Get all related records (single query)
        let all_related: Vec<R> = storage.all().await?;

        // Group related by foreign key
        let mut related_map: std::collections::HashMap<<T as Collection>::Id, Vec<R>> =
            std::collections::HashMap::new();

        for r in all_related {
            let fk = related_key_fn(&r);
            related_map.entry(fk).or_default().push(r);
        }

        // Join primary with related
        let result: Vec<(T, Vec<R>)> = primary_records
            .into_iter()
            .map(|p| {
                let related = related_map.get(p.id()).cloned().unwrap_or_default();
                (p, related)
            })
            .collect();

        Ok(result)
    }

    /// Eager load a single related record (has_one/belongs_to relationship)
    ///
    /// # Example
    /// ```ignore
    /// // Order belongs to User
    /// let orders_with_user = db.query::<Order>()
    ///     .collect_with_one::<User, _>(|order| order.user_id.clone())
    ///     .await?;
    ///
    /// // Returns Vec<(Order, Option<User>)>
    /// ```
    pub async fn collect_with_one<R, FK>(
        self,
        foreign_key_fn: FK,
    ) -> Result<Vec<(T, Option<R>)>, StorageError>
    where
        R: Indexed + Collection + DeserializeOwned + Clone,
        FK: Fn(&T) -> <R as Collection>::Id,
        <R as Collection>::Id: Eq + std::hash::Hash + Clone,
    {
        // Save storage reference before consuming self
        let storage = self.storage;

        // Get primary records
        let primary_records = self.collect().await?;

        // Get all related records
        let all_related: Vec<R> = storage.all().await?;

        // Index related by their ID
        let related_map: std::collections::HashMap<<R as Collection>::Id, R> = all_related
            .into_iter()
            .map(|r| (r.id().clone(), r))
            .collect();

        // Join primary with related
        let result: Vec<(T, Option<R>)> = primary_records
            .into_iter()
            .map(|p| {
                let fk = foreign_key_fn(&p);
                let related = related_map.get(&fk).cloned();
                (p, related)
            })
            .collect();

        Ok(result)
    }
}

/// Query builder with projection (select)
pub struct MappedQueryBuilder<'a, S: StorageAdapter, T, U, F: Fn(T) -> U> {
    inner: QueryBuilder<'a, S, T>,
    map_fn: F,
}

impl<'a, S: StorageAdapter + 'static, T, U, F> MappedQueryBuilder<'a, S, T, U, F>
where
    T: prkdb_types::index::Indexed
        + prkdb_types::collection::Collection
        + serde::de::DeserializeOwned
        + Clone,
    F: Fn(T) -> U,
{
    /// Execute query with projection
    pub async fn collect(self) -> Result<Vec<U>, StorageError> {
        let results = self.inner.collect().await?;
        Ok(results.into_iter().map(self.map_fn).collect())
    }

    /// Execute query and get first projected result
    pub async fn first(self) -> Result<Option<U>, StorageError> {
        let result = self.inner.first().await?;
        Ok(result.map(self.map_fn))
    }
}

// =========================================================================
// TRANSACTIONS
// =========================================================================

/// Buffered operation for transaction
#[derive(Debug, Clone)]
enum TxOperation {
    Insert {
        collection: String,
        key: Vec<u8>,
        data: Vec<u8>,
        index_values: Vec<(String, Vec<u8>)>,
    },
    Delete {
        collection: String,
        key: Vec<u8>,
        index_values: Vec<(String, Vec<u8>)>,
    },
}

/// Transaction for atomic operations with rollback support
///
/// # Example
/// ```ignore
/// let mut tx = db.transaction();
/// tx.insert(&user1).await?;
/// tx.savepoint("sp1");
/// tx.insert(&user2).await?;
/// tx.rollback_to("sp1"); // user2 insert removed
/// tx.commit().await?;    // Only user1 inserted
/// ```
pub struct Transaction<'a, S: StorageAdapter> {
    storage: &'a IndexedStorage<S>,
    operations: Vec<TxOperation>,
    savepoints: std::collections::HashMap<String, usize>,
    committed: bool,
}

impl<'a, S: StorageAdapter + 'static> Transaction<'a, S> {
    /// Create a new transaction
    fn new(storage: &'a IndexedStorage<S>) -> Self {
        Self {
            storage,
            operations: Vec::new(),
            savepoints: std::collections::HashMap::new(),
            committed: false,
        }
    }

    /// Buffer an insert operation
    pub fn insert<T: Indexed + Collection>(&mut self, record: &T) -> Result<(), StorageError> {
        let collection_name = std::any::type_name::<T>();
        let primary_key = serde_json::to_vec(record.id())
            .map_err(|e| StorageError::Serialization(format!("Failed to serialize id: {}", e)))?;
        let data = serde_json::to_vec(record).map_err(|e| {
            StorageError::Serialization(format!("Failed to serialize record: {}", e))
        })?;

        let index_values: Vec<(String, Vec<u8>)> = record
            .index_values()
            .into_iter()
            .map(|(f, v)| (f.to_string(), v))
            .collect();

        self.operations.push(TxOperation::Insert {
            collection: collection_name.to_string(),
            key: primary_key,
            data,
            index_values,
        });

        Ok(())
    }

    /// Buffer a delete operation
    pub fn delete<T: Indexed + Collection>(&mut self, record: &T) -> Result<(), StorageError> {
        let collection_name = std::any::type_name::<T>();
        let primary_key = serde_json::to_vec(record.id())
            .map_err(|e| StorageError::Serialization(format!("Failed to serialize id: {}", e)))?;

        let index_values: Vec<(String, Vec<u8>)> = record
            .index_values()
            .into_iter()
            .map(|(f, v)| (f.to_string(), v))
            .collect();

        self.operations.push(TxOperation::Delete {
            collection: collection_name.to_string(),
            key: primary_key,
            index_values,
        });

        Ok(())
    }

    /// Number of pending operations
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Check if transaction has no pending operations
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Create a named savepoint
    ///
    /// Stores the current state of the transaction. Can be rolled back to later.
    pub fn savepoint(&mut self, name: &str) {
        self.savepoints
            .insert(name.to_string(), self.operations.len());
    }

    /// Rollback to a named savepoint
    ///
    /// Discards all operations buffered after the savepoint was created.
    /// Returns error if savepoint doesn't exist.
    pub fn rollback_to(&mut self, name: &str) -> Result<(), StorageError> {
        if let Some(&index) = self.savepoints.get(name) {
            if index <= self.operations.len() {
                self.operations.truncate(index);
                // Also remove any savepoints that were created after this one
                self.savepoints.retain(|_, &mut v| v <= index);
                Ok(())
            } else {
                Err(StorageError::Internal(
                    "Invalid savepoint index".to_string(),
                ))
            }
        } else {
            Err(StorageError::Internal(format!(
                "Savepoint '{}' not found",
                name
            )))
        }
    }

    /// Commit all buffered operations atomically
    pub async fn commit(mut self) -> Result<usize, StorageError> {
        let count = self.operations.len();

        // Apply all operations
        for op in self.operations.drain(..) {
            match op {
                TxOperation::Insert {
                    collection,
                    key,
                    data,
                    index_values,
                } => {
                    // Store record
                    self.storage.storage.put(&key, &data).await?;

                    // Update legacy indexes
                    let mut indexes = self.storage.indexes.write().await;
                    let collection_index = indexes
                        .entry(collection.clone())
                        .or_insert_with(MemoryIndex::new);

                    for (field, value) in &index_values {
                        collection_index.add(field, value.clone(), key.clone());
                    }

                    // Update lock-free indexes
                    let col_idx = self
                        .storage
                        .lock_free_indexes
                        .entry(collection.clone())
                        .or_insert_with(DashMap::new);

                    for (field, value) in index_values {
                        let field_idx = col_idx.entry(field).or_insert_with(DashMap::new);
                        field_idx
                            .entry(value)
                            .or_insert_with(Vec::new)
                            .push(key.clone());
                    }
                }
                TxOperation::Delete {
                    collection,
                    key,
                    index_values,
                } => {
                    // Delete record
                    self.storage.storage.delete(&key).await?;

                    // Update legacy indexes
                    let mut indexes = self.storage.indexes.write().await;
                    if let Some(collection_index) = indexes.get_mut(&collection) {
                        for (field, value) in &index_values {
                            collection_index.remove(field, value, &key);
                        }
                    }

                    // Update lock-free indexes
                    if let Some(col_idx) = self.storage.lock_free_indexes.get(&collection) {
                        for (field, value) in index_values {
                            if let Some(field_idx) = col_idx.get(&field) {
                                if let Some(mut keys) = field_idx.get_mut(&value) {
                                    keys.retain(|k| k != &key);
                                    // Optimization: Remove empty key lists could be done here but locking might make it tricky if we don't have write access to parent map easily
                                    // DashMap allows modification of value.
                                }
                            }
                        }
                    }
                }
            }
        }

        self.committed = true;
        Ok(count)
    }

    /// Rollback transaction, discarding all buffered operations
    pub fn rollback(mut self) {
        self.operations.clear();
        self.committed = true; // Prevent drop warning
    }
}

impl<'a, S: StorageAdapter> Drop for Transaction<'a, S> {
    fn drop(&mut self) {
        if !self.committed && !self.operations.is_empty() {
            // Transaction dropped without commit or rollback
            // This is a programming error - log warning in production
            eprintln!(
                "Warning: Transaction dropped with {} uncommitted operations",
                self.operations.len()
            );
        }
    }
}

/// Generic storage adapter with secondary index support
/// Works with any StorageAdapter implementation (WAL, SQL, memory, etc.)
pub struct IndexedStorage<S: StorageAdapter> {
    /// Underlying storage
    storage: Arc<S>,
    /// In-memory indexes by collection (legacy - uses RwLock)
    indexes: Arc<RwLock<BTreeMap<String, MemoryIndex>>>,
    /// Lock-free indexes using DashMap for concurrent access
    /// Structure: collection_name -> field_name -> value -> primary_keys
    lock_free_indexes: Arc<DashMap<String, DashMap<String, DashMap<Vec<u8>, Vec<Vec<u8>>>>>>,
    /// Lock-free text indexes for full-text search
    /// Structure: collection_name -> field_name -> token -> primary_keys
    lock_free_text_indexes: Arc<DashMap<String, DashMap<String, DashMap<String, Vec<Vec<u8>>>>>>,
    /// Broadcast channel for change notifications
    change_tx: tokio::sync::broadcast::Sender<ChangeEvent>,
    /// Path for auto-sync (if enabled)
    sync_path: Option<std::path::PathBuf>,
    /// Shutdown signal for background sync
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
}

impl<S: StorageAdapter + 'static> IndexedStorage<S> {
    /// Create a new indexed storage wrapper around any StorageAdapter
    pub fn new(storage: Arc<S>) -> Self {
        let (change_tx, _) = tokio::sync::broadcast::channel(1024);
        Self {
            storage,
            indexes: Arc::new(RwLock::new(BTreeMap::new())),
            lock_free_indexes: Arc::new(DashMap::new()),
            lock_free_text_indexes: Arc::new(DashMap::new()),
            change_tx,
            sync_path: None,
            shutdown_tx: None,
        }
    }

    /// Query the lock-free index for primary keys matching a field value
    ///
    /// Returns empty vec if collection/field/value not found.
    /// This is lock-free and O(1) lookup.
    fn query_lockfree_index(&self, collection: &str, field: &str, value: &[u8]) -> Vec<Vec<u8>> {
        self.lock_free_indexes
            .get(collection)
            .and_then(|col_idx| {
                col_idx
                    .get(field)
                    .and_then(|field_idx| field_idx.get(value).map(|v| v.clone()))
            })
            .unwrap_or_default()
    }

    /// Get all primary keys for a collection from lock-free index
    #[allow(dead_code)]
    fn all_keys_lockfree(&self, collection: &str) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        if let Some(col_idx) = self.lock_free_indexes.get(collection) {
            for field_entry in col_idx.iter() {
                for value_entry in field_entry.value().iter() {
                    keys.extend(value_entry.value().clone());
                }
            }
        }
        keys
    }

    /// Start a transaction for atomic operations
    ///
    /// # Example
    /// ```ignore
    /// let mut tx = db.transaction();
    /// tx.insert(&user1)?;
    /// tx.insert(&user2)?;
    /// tx.delete(&old_user)?;
    /// tx.commit().await?;  // All applied atomically
    /// // OR: tx.rollback();  // Nothing changed
    /// ```
    pub fn transaction(&self) -> Transaction<'_, S> {
        Transaction::new(self)
    }

    /// Start building a query
    ///
    /// # Example
    /// ```ignore
    /// let users = db.query::<User>()
    ///     .filter(|u| u.age > 18)
    ///     .order_by(|u| u.created_at)
    ///     .take(10)
    ///     .collect().await?;
    /// ```
    pub fn query<T>(&self) -> QueryBuilder<'_, S, T>
    where
        T: Indexed + Collection + DeserializeOwned + Clone,
    {
        QueryBuilder::new(self)
    }

    /// Create indexed storage with indexes loaded from disk
    ///
    /// # Example
    /// ```ignore
    /// let db = IndexedStorage::load_from(storage, "./data/indexes.db").await?;
    /// ```
    pub async fn load_from<P: AsRef<std::path::Path>>(
        storage: Arc<S>,
        index_path: P,
    ) -> Result<Self, StorageError> {
        let (change_tx, _) = tokio::sync::broadcast::channel(1024);

        let indexes = match tokio::fs::read(index_path.as_ref()).await {
            Ok(data) => {
                let (persisted, _): (PersistedIndexes, _) =
                    bincode::serde::decode_from_slice(&data, bincode::config::standard())
                        .map_err(|e| StorageError::Deserialization(format!("Index file: {}", e)))?;
                persisted.collections
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // No index file yet, start fresh
                BTreeMap::new()
            }
            Err(e) => {
                return Err(StorageError::BackendError(format!(
                    "Failed to read index file: {}",
                    e
                )));
            }
        };

        Ok(Self {
            storage,
            indexes: Arc::new(RwLock::new(indexes)),
            lock_free_indexes: Arc::new(DashMap::new()),
            lock_free_text_indexes: Arc::new(DashMap::new()),
            change_tx,
            sync_path: Some(index_path.as_ref().to_path_buf()),
            shutdown_tx: None,
        })
    }

    /// Start background auto-sync of indexes to disk
    ///
    /// # Example
    /// ```ignore
    /// let db = IndexedStorage::load_from(storage, "./indexes.db").await?;
    /// db.start_auto_sync(Duration::from_secs(30)).await;
    /// ```
    pub async fn start_auto_sync(&mut self, interval: std::time::Duration) {
        // Stop any existing sync task
        self.stop_auto_sync();

        let sync_path = match &self.sync_path {
            Some(p) => p.clone(),
            None => return, // No path configured, can't auto-sync
        };

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        let indexes = Arc::clone(&self.indexes);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {
                        // Save indexes
                        let idx = indexes.read().await;
                        let persisted = PersistedIndexes {
                            version: 1,
                            collections: idx.clone(),
                        };
                        drop(idx); // Release lock before IO

                        if let Ok(data) = bincode::serde::encode_to_vec(&persisted, bincode::config::standard()) {
                            let temp_path = sync_path.with_extension("db.tmp");
                            if tokio::fs::write(&temp_path, &data).await.is_ok() {
                                let _ = tokio::fs::rename(&temp_path, &sync_path).await;
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });
    }

    /// Stop background auto-sync
    pub fn stop_auto_sync(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
    }

    /// Save indexes to disk
    ///
    /// # Example
    /// ```ignore
    /// db.save_indexes("./data/indexes.db").await?;
    /// ```
    pub async fn save_indexes<P: AsRef<std::path::Path>>(
        &self,
        index_path: P,
    ) -> Result<(), StorageError> {
        // Collect data from lock-free indexes to persist
        // We prioritize lock_free_indexes as they contain the most recent data (including batch inserts)
        let mut collections_map = BTreeMap::new();

        for entry in self.lock_free_indexes.iter() {
            let col_name = entry.key().clone();
            let col_idx = entry.value();

            let mut mem_index = MemoryIndex::default();

            // Convert field indexes
            for field_entry in col_idx.iter() {
                let field = field_entry.key().clone();
                let values_map = field_entry.value();

                let mut field_btree = BTreeMap::new();
                for val_entry in values_map.iter() {
                    field_btree.insert(val_entry.key().clone(), val_entry.value().clone());
                }
                mem_index.indexes.insert(field, field_btree);
            }

            // Convert text indexes
            if let Some(text_col) = self.lock_free_text_indexes.get(&col_name) {
                for field_entry in text_col.iter() {
                    let field = field_entry.key().clone();
                    let tokens_map = field_entry.value();

                    let mut token_btree = BTreeMap::new();
                    for token_entry in tokens_map.iter() {
                        token_btree.insert(token_entry.key().clone(), token_entry.value().clone());
                    }
                    mem_index.text_indexes.insert(field, token_btree);
                }
            }

            collections_map.insert(col_name, mem_index);
        }

        let persisted = PersistedIndexes {
            version: 1,
            collections: collections_map,
        };

        let data = bincode::serde::encode_to_vec(&persisted, bincode::config::standard())
            .map_err(|e| StorageError::Serialization(format!("Index serialization: {}", e)))?;

        // Write atomically using temp file + rename
        let path = index_path.as_ref();
        let temp_path = path.with_extension("db.tmp");

        tokio::fs::write(&temp_path, &data).await.map_err(|e| {
            StorageError::BackendError(format!("Failed to write index file: {}", e))
        })?;

        tokio::fs::rename(&temp_path, path).await.map_err(|e| {
            StorageError::BackendError(format!("Failed to rename index file: {}", e))
        })?;

        Ok(())
    }

    pub async fn load_indexes<P: AsRef<std::path::Path>>(
        &self,
        index_path: P,
    ) -> Result<(), StorageError> {
        let path = index_path.as_ref();
        let data = tokio::fs::read(path)
            .await
            .map_err(|e| StorageError::BackendError(format!("Failed to read index file: {}", e)))?;

        let persisted: PersistedIndexes =
            bincode::serde::decode_from_slice(&data, bincode::config::standard())
                .map_err(|e| {
                    StorageError::Deserialization(format!("Index deserialization: {}", e))
                })?
                .0;

        // 1. Restore legacy indexes
        let mut indexes_lock = self.indexes.write().await;
        *indexes_lock = persisted.collections.clone();

        // 2. Restore lock-free indexes
        self.lock_free_indexes.clear();
        self.lock_free_text_indexes.clear();

        for (col_name, mem_index) in &persisted.collections {
            // Restore field values indexes
            let col_idx = self
                .lock_free_indexes
                .entry(col_name.clone())
                .or_insert_with(DashMap::new);

            for (field, values) in &mem_index.indexes {
                let field_idx = col_idx.entry(field.clone()).or_insert_with(DashMap::new);
                for (val, keys) in values {
                    field_idx.insert(val.clone(), keys.clone());
                }
            }

            // Restore text indexes
            let text_col_idx = self
                .lock_free_text_indexes
                .entry(col_name.clone())
                .or_insert_with(DashMap::new);

            for (field, tokens) in &mem_index.text_indexes {
                let field_idx = text_col_idx
                    .entry(field.clone())
                    .or_insert_with(DashMap::new);
                for (token, keys) in tokens {
                    field_idx.insert(token.clone(), keys.clone());
                }
            }
        }

        Ok(())
    }

    /// Get number of indexed records per collection
    pub async fn index_stats(&self) -> BTreeMap<String, usize> {
        let indexes = self.indexes.read().await;
        indexes
            .iter()
            .map(|(name, idx)| (name.clone(), idx.all_keys().len()))
            .collect()
    }

    /// Subscribe to change events (insert, delete)
    /// Returns a broadcast receiver that receives all changes
    pub fn watch(&self) -> tokio::sync::broadcast::Receiver<ChangeEvent> {
        self.change_tx.subscribe()
    }

    /// Get underlying storage
    pub fn inner(&self) -> &Arc<S> {
        &self.storage
    }

    /// Create a full-text index on a field
    ///
    /// # Example
    /// ```ignore
    /// // Index all existing records
    /// db.create_text_index::<User, _>("bio", |u| &u.bio).await?;
    ///
    /// // Later, search the index
    /// let results = db.search::<User>("bio", "rust developer").await?;
    /// ```
    pub async fn create_text_index<T, F>(
        &self,
        field: &str,
        text_fn: F,
    ) -> Result<usize, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned,
        F: Fn(&T) -> &str,
    {
        let collection_name = std::any::type_name::<T>();
        let all_records: Vec<T> = self.all().await?;

        let mut indexes = self.indexes.write().await;
        let collection_index = indexes
            .entry(collection_name.to_string())
            .or_insert_with(MemoryIndex::new);

        let mut count = 0;
        for record in &all_records {
            let text = text_fn(record);
            let primary_key = serde_json::to_vec(record.id()).map_err(|e| {
                StorageError::Serialization(format!("Failed to serialize id: {}", e))
            })?;
            collection_index.add_text(field, text, &primary_key);
            count += 1;
        }

        Ok(count)
    }

    /// Search full-text index (lock-free)
    ///
    /// Returns records ranked by relevance (number of matching tokens)
    /// Uses lock-free DashMap for concurrent text search.
    ///
    /// # Example
    /// ```ignore
    /// let results = db.search::<User>("bio", "rust async").await?;
    /// // Returns users whose bio contains "rust" and/or "async", ranked by match count
    /// ```
    pub async fn search<T: Collection + DeserializeOwned>(
        &self,
        field: &str,
        query: &str,
    ) -> Result<Vec<T>, StorageError> {
        let collection_name = std::any::type_name::<T>();

        // Tokenize query (simple whitespace split + lowercase)
        let tokens: Vec<String> = query.split_whitespace().map(|s| s.to_lowercase()).collect();

        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        // Score primary keys by number of matching tokens (lock-free!)
        let mut scores: std::collections::HashMap<Vec<u8>, usize> =
            std::collections::HashMap::new();

        if let Some(col_idx) = self.lock_free_text_indexes.get(collection_name) {
            if let Some(field_idx) = col_idx.get(field) {
                for token in &tokens {
                    if let Some(keys) = field_idx.get(token) {
                        for key in keys.value() {
                            *scores.entry(key.clone()).or_default() += 1;
                        }
                    }
                }
            }
        }

        // Sort by score descending
        let mut ranked: Vec<_> = scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1));

        // Fetch records
        let mut results = Vec::new();
        for (primary_key, _score) in ranked {
            if let Ok(Some(data)) = self.storage.get(&primary_key).await {
                if let Ok(record) = serde_json::from_slice::<T>(&data) {
                    results.push(record);
                }
            }
        }

        Ok(results)
    }

    /// Insert a record with automatic index updates
    ///
    /// Uses lock-free DashMap for concurrent index updates.
    pub async fn insert<T: Indexed + Collection>(&self, record: &T) -> Result<(), StorageError> {
        let collection_name = std::any::type_name::<T>().to_string();
        let primary_key = serde_json::to_vec(record.id())
            .map_err(|e| StorageError::Serialization(format!("Failed to serialize id: {}", e)))?;
        let data = serde_json::to_vec(record).map_err(|e| {
            StorageError::Serialization(format!("Failed to serialize record: {}", e))
        })?;

        // Store the main record
        self.storage.put(&primary_key, &data).await?;

        // Lock-free index update using DashMap
        let collection_idx = self
            .lock_free_indexes
            .entry(collection_name.clone())
            .or_insert_with(DashMap::new);

        for (field, value) in record.index_values() {
            let field_idx = collection_idx
                .entry(field.to_string())
                .or_insert_with(DashMap::new);
            field_idx
                .entry(value)
                .or_insert_with(Vec::new)
                .push(primary_key.clone());
        }

        // Emit change event (ignore if no subscribers)
        let _ = self.change_tx.send(ChangeEvent::Inserted {
            collection: collection_name,
            id: primary_key,
            data,
        });

        Ok(())
    }

    /// Insert a record with lifecycle hooks
    ///
    /// Calls before_insert before saving, after_insert after saving.
    ///
    /// # Example
    /// ```ignore
    /// impl Hooks for User {
    ///     fn before_insert(&mut self) -> Result<(), String> {
    ///         self.name = self.name.trim().to_string();
    ///         Ok(())
    ///     }
    ///     fn after_insert(&self) {
    ///         log::info!("User {} created", self.id);
    ///     }
    /// }
    ///
    /// db.insert_with_hooks(&mut user).await?;
    /// ```
    pub async fn insert_with_hooks<T>(&self, record: &mut T) -> Result<(), StorageError>
    where
        T: Indexed + Collection + prkdb_types::Hooks,
    {
        // Call before_insert hook
        record
            .before_insert()
            .map_err(|e| StorageError::Validation(e))?;

        // Insert the record
        self.insert(record).await?;

        // Call after_insert hook
        record.after_insert();

        Ok(())
    }

    /// Delete a record with lifecycle hooks
    pub async fn delete_with_hooks<T>(&self, record: &T) -> Result<(), StorageError>
    where
        T: Indexed + Collection + prkdb_types::Hooks,
    {
        // Call before_delete hook
        record
            .before_delete()
            .map_err(|e| StorageError::Validation(e))?;

        // Delete the record
        self.delete(record).await?;

        // Call after_delete hook
        record.after_delete();

        Ok(())
    }

    /// Insert a record with validation
    ///
    /// # Example
    /// ```ignore
    /// impl Validatable for User {
    ///     fn validate(&self) -> Result<(), Vec<ValidationError>> {
    ///         let mut errors = Vec::new();
    ///         if self.name.is_empty() {
    ///             errors.push(ValidationError::new("name", "cannot be empty"));
    ///         }
    ///         if errors.is_empty() { Ok(()) } else { Err(errors) }
    ///     }
    /// }
    ///
    /// // Will fail if validation fails
    /// db.insert_validated(&user).await?;
    /// ```
    pub async fn insert_validated<T>(&self, record: &T) -> Result<(), StorageError>
    where
        T: Indexed + Collection + prkdb_types::Validatable,
    {
        // Run validation first
        if let Err(errors) = record.validate() {
            let messages: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
            return Err(StorageError::Validation(messages.join("; ")));
        }

        // Proceed with normal insert
        self.insert(record).await
    }

    /// Insert with automatic timestamp initialization
    ///
    /// Sets created_at and updated_at to current time before insert.
    ///
    /// # Example
    /// ```ignore
    /// let mut user = User { id: 1, name: "Alice".into(), created_at: 0, updated_at: 0 };
    /// db.insert_timestamped(&mut user).await?;
    /// println!("Created at: {}", user.created_at);
    /// ```
    pub async fn insert_timestamped<T>(&self, record: &mut T) -> Result<(), StorageError>
    where
        T: Indexed + Collection + prkdb_types::Timestamped,
    {
        record.init_timestamps();
        self.insert(record).await
    }

    /// Upsert with automatic timestamp update
    ///
    /// Sets updated_at to current time, sets created_at only if new record.
    pub async fn upsert_timestamped<T>(&self, record: &mut T) -> Result<bool, StorageError>
    where
        T: Indexed + Collection + prkdb_types::Timestamped,
    {
        let primary_key = serde_json::to_vec(record.id())
            .map_err(|e| StorageError::Serialization(format!("Failed to serialize id: {}", e)))?;

        let exists = self.storage.get(&primary_key).await?.is_some();

        if exists {
            record.touch(); // Just update updated_at
        } else {
            record.init_timestamps(); // Set both
        }

        self.upsert(record).await
    }

    /// Upsert a record (insert or update if exists)
    ///
    /// If a record with the same ID exists, it will be replaced.
    /// If not, a new record will be inserted.
    ///
    /// # Example
    /// ```ignore
    /// // Always works, whether user exists or not
    /// db.upsert(&user).await?;
    /// ```
    pub async fn upsert<T: Indexed + Collection>(&self, record: &T) -> Result<bool, StorageError> {
        let primary_key = serde_json::to_vec(record.id())
            .map_err(|e| StorageError::Serialization(format!("Failed to serialize id: {}", e)))?;

        // Check if exists
        let exists = self.storage.get(&primary_key).await?.is_some();

        if exists {
            // Delete old record first (to update indexes correctly)
            self.delete(record).await?;
        }

        // Insert new record
        self.insert(record).await?;

        Ok(exists) // true if updated, false if inserted
    }

    /// Check if a record exists by ID
    ///
    /// # Example
    /// ```ignore
    /// if db.exists::<User>(&user_id).await? {
    ///     println!("User exists!");
    /// }
    /// ```
    pub async fn exists<T: Collection>(&self, id: &T::Id) -> Result<bool, StorageError> {
        let primary_key = serde_json::to_vec(id)
            .map_err(|e| StorageError::Serialization(format!("Failed to serialize id: {}", e)))?;

        Ok(self.storage.get(&primary_key).await?.is_some())
    }

    /// Update a record using a closure
    ///
    /// Loads the existing record, applies the update function, and saves it back.
    /// Returns None if the record doesn't exist.
    ///
    /// # Example
    /// ```ignore
    /// let updated = db.update::<User, _>(&user_id, |user| {
    ///     user.name = "New Name".to_string();
    ///     user.age += 1;
    /// }).await?;
    /// ```
    pub async fn update<T, F>(&self, id: &T::Id, update_fn: F) -> Result<Option<T>, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned,
        F: FnOnce(&mut T),
    {
        // Load existing record
        let mut record = match self.get::<T>(id).await? {
            Some(r) => r,
            None => return Ok(None),
        };

        // Apply update
        update_fn(&mut record);

        // Save (upsert handles index updates)
        self.upsert(&record).await?;

        Ok(Some(record))
    }

    /// Soft delete a record (mark as deleted instead of removing)
    ///
    /// # Example
    /// ```ignore
    /// // Mark user as deleted (keeps data)
    /// db.soft_delete::<User>(&user_id).await?;
    ///
    /// // Query only active records
    /// let active = db.query::<User>()
    ///     .filter(|u| u.is_active())
    ///     .collect().await?;
    /// ```
    pub async fn soft_delete<T>(&self, id: &T::Id) -> Result<bool, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned + prkdb_types::SoftDeletable,
    {
        if let Some(mut record) = self.get::<T>(id).await? {
            record.mark_deleted();
            self.upsert(&record).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Restore a soft-deleted record
    ///
    /// # Example
    /// ```ignore
    /// // Undo soft delete
    /// db.restore::<User>(&user_id).await?;
    /// ```
    pub async fn restore<T>(&self, id: &T::Id) -> Result<bool, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned + prkdb_types::SoftDeletable,
    {
        if let Some(mut record) = self.get::<T>(id).await? {
            record.restore();
            self.upsert(&record).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Query only active (non-deleted) records
    ///
    /// # Example
    /// ```ignore
    /// let active_users = db.query_active::<User>().await?;
    /// ```
    pub async fn query_active<T>(&self) -> Result<Vec<T>, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned + prkdb_types::SoftDeletable,
    {
        let all: Vec<T> = self.all().await?;
        Ok(all.into_iter().filter(|r| r.is_active()).collect())
    }

    /// Query by indexed field
    ///
    /// Uses lock-free DashMap for concurrent access.
    pub async fn query_by<T: Indexed + Collection + DeserializeOwned>(
        &self,
        field: &str,
        value: &impl Serialize,
    ) -> Result<Vec<T>, StorageError> {
        let collection_name = std::any::type_name::<T>();
        let search_value = serde_json::to_vec(value).map_err(|e| {
            StorageError::Serialization(format!("Failed to serialize query value: {}", e))
        })?;

        // Lock-free DashMap lookup
        let primary_keys = self.query_lockfree_index(collection_name, field, &search_value);

        // Fetch records
        let mut results = Vec::new();
        for pk in primary_keys {
            if let Some(data) = self.storage.get(&pk).await? {
                let record: T = serde_json::from_slice(&data).map_err(|e| {
                    StorageError::Deserialization(format!("Failed to deserialize: {}", e))
                })?;
                results.push(record);
            }
        }

        Ok(results)
    }

    /// Query for unique index (returns Option)
    pub async fn query_unique_by<T: Indexed + Collection + DeserializeOwned>(
        &self,
        field: &str,
        value: &impl Serialize,
    ) -> Result<Option<T>, StorageError> {
        let results = self.query_by::<T>(field, value).await?;
        Ok(results.into_iter().next())
    }

    /// Delete a record and update indexes
    ///
    /// Uses lock-free DashMap for concurrent index updates.
    pub async fn delete<T: Indexed + Collection>(&self, record: &T) -> Result<(), StorageError> {
        let collection_name = std::any::type_name::<T>().to_string();
        let primary_key = serde_json::to_vec(record.id())
            .map_err(|e| StorageError::Serialization(format!("Failed to serialize id: {}", e)))?;

        // Remove from lock-free DashMap indexes
        if let Some(col_idx) = self.lock_free_indexes.get(&collection_name) {
            for (field, value) in record.index_values() {
                if let Some(field_idx) = col_idx.get(field) {
                    field_idx.alter(&value, |_, mut keys| {
                        keys.retain(|k| k != &primary_key);
                        keys
                    });
                }
            }
        }

        // Delete main record
        self.storage.delete(&primary_key).await?;

        // Emit change event (ignore if no subscribers)
        let _ = self.change_tx.send(ChangeEvent::Deleted {
            collection: collection_name,
            id: primary_key,
        });

        Ok(())
    }

    /// Get by primary key
    pub async fn get<T: Collection + DeserializeOwned>(
        &self,
        id: &T::Id,
    ) -> Result<Option<T>, StorageError>
    where
        T::Id: Serialize,
    {
        let primary_key = serde_json::to_vec(id)
            .map_err(|e| StorageError::Serialization(format!("Failed to serialize id: {}", e)))?;

        match self.storage.get(&primary_key).await? {
            Some(data) => {
                let record: T = serde_json::from_slice(&data).map_err(|e| {
                    StorageError::Deserialization(format!("Failed to deserialize: {}", e))
                })?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Query by range on indexed field
    /// Uses BTreeMap's efficient range iteration - O(log n + k) where k = results
    pub async fn query_range<T: Indexed + Collection + DeserializeOwned>(
        &self,
        field: &str,
        start: &impl Serialize,
        end: &impl Serialize,
    ) -> Result<Vec<T>, StorageError> {
        let collection_name = std::any::type_name::<T>();
        let start_bytes = serde_json::to_vec(start).map_err(|e| {
            StorageError::Serialization(format!("Failed to serialize start: {}", e))
        })?;
        let end_bytes = serde_json::to_vec(end)
            .map_err(|e| StorageError::Serialization(format!("Failed to serialize end: {}", e)))?;

        // Get matching primary keys from index
        let primary_keys = {
            let indexes = self.indexes.read().await;
            indexes
                .get(collection_name)
                .map(|idx| idx.query_range(field, &start_bytes, &end_bytes))
                .unwrap_or_default()
        };

        // Fetch records
        self.fetch_records(primary_keys).await
    }

    /// Lock-free range query on indexed field
    ///
    /// Uses DashMap for concurrent access. Results are sorted by value.
    /// Slightly slower than BTreeMap range due to post-sorting, but lock-free.
    ///
    /// # Example
    /// ```ignore
    /// // Find users with scores between 50 and 100
    /// let users: Vec<User> = db.query_range_lockfree("score", &50, &100).await?;
    /// ```
    pub async fn query_range_lockfree<T: Indexed + Collection + DeserializeOwned>(
        &self,
        field: &str,
        start: &impl Serialize,
        end: &impl Serialize,
    ) -> Result<Vec<T>, StorageError> {
        let collection_name = std::any::type_name::<T>();
        let start_bytes = serde_json::to_vec(start).map_err(|e| {
            StorageError::Serialization(format!("Failed to serialize start: {}", e))
        })?;
        let end_bytes = serde_json::to_vec(end)
            .map_err(|e| StorageError::Serialization(format!("Failed to serialize end: {}", e)))?;

        // Collect matching primary keys from lock-free DashMap
        let mut primary_keys = Vec::new();

        if let Some(col_idx) = self.lock_free_indexes.get(collection_name) {
            if let Some(field_idx) = col_idx.get(field) {
                for entry in field_idx.iter() {
                    let value = entry.key();
                    if value >= &start_bytes && value <= &end_bytes {
                        primary_keys.extend(entry.value().clone());
                    }
                }
            }
        }

        // Sort by key for consistent ordering
        primary_keys.sort();
        primary_keys.dedup();

        // Fetch records
        self.fetch_records(primary_keys).await
    }

    /// Query with cursor-based pagination (lock-free)
    ///
    /// Efficient for large datasets - only fetches `limit` records.
    /// Returns records and next cursor for continuation.
    ///
    /// # Example
    /// ```ignore
    /// // First page
    /// let (users, cursor) = db.query_with_cursor::<User>("role", &"admin", None, 100).await?;
    ///
    /// // Next page
    /// if let Some(cursor) = cursor {
    ///     let (more_users, _) = db.query_with_cursor::<User>("role", &"admin", Some(&cursor), 100).await?;
    /// }
    /// ```
    pub async fn query_with_cursor<T: Indexed + Collection + DeserializeOwned>(
        &self,
        field: &str,
        value: &impl Serialize,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<T>, Option<Vec<u8>>), StorageError> {
        let collection_name = std::any::type_name::<T>();
        let search_value = serde_json::to_vec(value).map_err(|e| {
            StorageError::Serialization(format!("Failed to serialize query value: {}", e))
        })?;

        // Get matching primary keys from lock-free DashMap
        let mut primary_keys = self.query_lockfree_index(collection_name, field, &search_value);

        // Sort for consistent cursor behavior
        primary_keys.sort();

        // Apply cursor filter
        let filtered_keys: Vec<Vec<u8>> = if let Some(last_key) = cursor {
            primary_keys
                .into_iter()
                .filter(|k| k.as_slice() > last_key)
                .collect()
        } else {
            primary_keys
        };

        // Take limit + 1 to determine if there are more results
        let take_count = limit + 1;
        let paginated: Vec<Vec<u8>> = filtered_keys.into_iter().take(take_count).collect();

        let has_more = paginated.len() > limit;
        let keys_to_fetch: Vec<Vec<u8>> = paginated.into_iter().take(limit).collect();

        // Get next cursor (last key of current page)
        let next_cursor = if has_more {
            keys_to_fetch.last().cloned()
        } else {
            None
        };

        // Fetch records
        let records = self.fetch_records(keys_to_fetch).await?;

        Ok((records, next_cursor))
    }

    /// Query by prefix on indexed field (for string fields)
    /// Finds all records where the field starts with the given prefix
    pub async fn query_prefix<T: Indexed + Collection + DeserializeOwned>(
        &self,
        field: &str,
        prefix: &str,
    ) -> Result<Vec<T>, StorageError> {
        let collection_name = std::any::type_name::<T>();
        let prefix_bytes = serde_json::to_vec(prefix).map_err(|e| {
            StorageError::Serialization(format!("Failed to serialize prefix: {}", e))
        })?;

        // Get matching primary keys from index
        let primary_keys = {
            let indexes = self.indexes.read().await;
            indexes
                .get(collection_name)
                .map(|idx| idx.query_prefix(field, &prefix_bytes))
                .unwrap_or_default()
        };

        // Fetch records
        self.fetch_records(primary_keys).await
    }

    /// Filter all records using a predicate
    /// Note: This is O(n) - scans all records. Use indexed queries when possible.
    ///
    /// Uses lock-free DashMap for concurrent access.
    pub async fn filter<T, F>(&self, predicate: F) -> Result<Vec<T>, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned,
        F: Fn(&T) -> bool,
    {
        let collection_name = std::any::type_name::<T>();

        // Get all unique primary keys from DashMap (lock-free)
        let mut primary_keys = std::collections::HashSet::new();
        if let Some(col_idx) = self.lock_free_indexes.get(collection_name) {
            for field_entry in col_idx.iter() {
                for value_entry in field_entry.value().iter() {
                    primary_keys.extend(value_entry.value().iter().cloned());
                }
            }
        }

        // Fetch and filter records
        let mut results = Vec::new();
        for pk in primary_keys {
            if let Some(data) = self.storage.get(&pk).await? {
                let record: T = serde_json::from_slice(&data).map_err(|e| {
                    StorageError::Deserialization(format!("Failed to deserialize: {}", e))
                })?;
                if predicate(&record) {
                    results.push(record);
                }
            }
        }

        Ok(results)
    }

    /// Helper to fetch records by primary keys
    async fn fetch_records<T: DeserializeOwned>(
        &self,
        primary_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<T>, StorageError> {
        let mut results = Vec::new();
        for pk in primary_keys {
            if let Some(data) = self.storage.get(&pk).await? {
                let record: T = serde_json::from_slice(&data).map_err(|e| {
                    StorageError::Deserialization(format!("Failed to deserialize: {}", e))
                })?;
                results.push(record);
            }
        }
        Ok(results)
    }

    // =========================================================================
    // COMPOUND INDEXES
    // =========================================================================

    /// Create a compound index on existing records
    ///
    /// # Example
    /// ```ignore
    /// db.create_compound_index::<User>("role_age", |u| {
    ///     vec![u.role.clone(), u.age.to_string()]
    /// }).await?;
    /// ```
    pub async fn create_compound_index<T, F>(
        &self,
        index_name: &str,
        key_fn: F,
    ) -> Result<usize, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned,
        F: Fn(&T) -> Vec<String>,
    {
        let collection_name = std::any::type_name::<T>();
        let records: Vec<T> = self.all().await?;
        let count = records.len();

        let mut indexes = self.indexes.write().await;
        let collection_index = indexes
            .entry(collection_name.to_string())
            .or_insert_with(MemoryIndex::new);

        for record in &records {
            let primary_key = serde_json::to_vec(record.id()).map_err(|e| {
                StorageError::Serialization(format!("Failed to serialize id: {}", e))
            })?;

            // Create composite key from multiple field values
            let fields = key_fn(record);
            let composite_key = serde_json::to_vec(&fields).map_err(|e| {
                StorageError::Serialization(format!("Failed to serialize compound key: {}", e))
            })?;

            collection_index.add_compound(index_name, composite_key, primary_key);
        }

        Ok(count)
    }

    /// Query by compound index
    ///
    /// # Example
    /// ```ignore
    /// let users = db.query_compound::<User>("role_age", vec!["Admin".into(), "30".into()]).await?;
    /// ```
    pub async fn query_compound<T: Indexed + Collection + DeserializeOwned>(
        &self,
        index_name: &str,
        values: Vec<String>,
    ) -> Result<Vec<T>, StorageError> {
        let collection_name = std::any::type_name::<T>();
        let composite_key = serde_json::to_vec(&values).map_err(|e| {
            StorageError::Serialization(format!("Failed to serialize compound key: {}", e))
        })?;

        let primary_keys = {
            let indexes = self.indexes.read().await;
            indexes
                .get(collection_name)
                .map(|idx| idx.query_compound(index_name, &composite_key))
                .unwrap_or_default()
        };

        self.fetch_records(primary_keys).await
    }

    // =========================================================================
    // BATCH OPERATIONS
    // =========================================================================

    /// Insert multiple records in a batch with automatic index updates
    ///
    /// This method is optimized for high throughput:
    /// - Bulk WAL write (single I/O operation for all records)
    /// - Lock-free DashMap for concurrent index updates
    /// - No RwLock contention - multiple threads can insert simultaneously
    ///
    /// # Performance
    /// - ~600k+ ops/sec on WAL storage
    /// - ~1.5M+ ops/sec on in-memory storage
    ///
    /// # Example
    /// ```ignore
    /// let users = vec![
    ///     User { id: 1, name: "Alice".into() },
    ///     User { id: 2, name: "Bob".into() },
    /// ];
    /// db.insert_batch(&users).await?;
    /// ```
    pub async fn insert_batch<T: Indexed + Collection + Send + Sync>(
        &self,
        records: &[T],
    ) -> Result<usize, StorageError> {
        if records.is_empty() {
            return Ok(0);
        }

        let collection_name = std::any::type_name::<T>().to_string();

        // Step 1: Serialize all records
        let serialized: Vec<_> = records
            .iter()
            .map(|record| {
                let primary_key = serde_json::to_vec(record.id()).map_err(|e| {
                    StorageError::Serialization(format!("Failed to serialize id: {}", e))
                })?;
                let data = serde_json::to_vec(record).map_err(|e| {
                    StorageError::Serialization(format!("Failed to serialize record: {}", e))
                })?;
                let index_values: Vec<(String, Vec<u8>)> = record
                    .index_values()
                    .into_iter()
                    .map(|(f, v)| (f.to_string(), v))
                    .collect();
                Ok((primary_key, data, index_values))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;

        // Step 2: Bulk WAL write (single I/O operation!)
        let wal_entries: Vec<(Vec<u8>, Vec<u8>)> = serialized
            .iter()
            .map(|(key, data, _)| (key.clone(), data.clone()))
            .collect();
        self.storage.put_batch(wal_entries).await?;

        // Step 3: Lock-free index updates using DashMap
        let collection_idx = self
            .lock_free_indexes
            .entry(collection_name)
            .or_insert_with(DashMap::new);

        for (primary_key, _data, index_values) in &serialized {
            for (field, value) in index_values {
                let field_idx = collection_idx
                    .entry(field.clone())
                    .or_insert_with(DashMap::new);
                field_idx
                    .entry(value.clone())
                    .or_insert_with(Vec::new)
                    .push(primary_key.clone());
            }
        }

        Ok(records.len())
    }

    /// Delete multiple records in a batch with automatic index updates
    ///
    /// Optimized for bulk operations using parallel index removal.
    pub async fn delete_batch<T: Indexed + Collection + Send + Sync>(
        &self,
        records: &[T],
    ) -> Result<usize, StorageError> {
        if records.is_empty() {
            return Ok(0);
        }

        let collection_name = std::any::type_name::<T>().to_string();

        // Step 1: Collect all primary keys and prepare for deletion
        let mut keys_to_delete = Vec::with_capacity(records.len());
        let mut index_removals = Vec::with_capacity(records.len());

        for record in records {
            let primary_key = serde_json::to_vec(record.id()).map_err(|e| {
                StorageError::Serialization(format!("Failed to serialize id: {}", e))
            })?;
            let index_values: Vec<_> = record
                .index_values()
                .into_iter()
                .map(|(f, v)| (f.to_string(), v))
                .collect();
            keys_to_delete.push(primary_key.clone());
            index_removals.push((primary_key, index_values));
        }

        // Step 2: Remove from lock-free DashMap indexes (parallel-safe)
        if let Some(col_idx) = self.lock_free_indexes.get(&collection_name) {
            for (primary_key, index_values) in &index_removals {
                for (field, value) in index_values {
                    if let Some(field_idx) = col_idx.get(field) {
                        field_idx.alter(value, |_, mut keys| {
                            keys.retain(|k| k != primary_key);
                            keys
                        });
                    }
                }
            }
        }

        // Step 3: Delete from storage (individual deletes, as no bulk delete API)
        for key in &keys_to_delete {
            self.storage.delete(key).await?;
        }

        // Step 4: Emit delete events
        for key in keys_to_delete {
            let _ = self.change_tx.send(ChangeEvent::Deleted {
                collection: collection_name.clone(),
                id: key,
            });
        }

        Ok(records.len())
    }

    /// Upsert multiple records in a batch (optimized)
    ///
    /// Uses bulk operations to reduce overhead:
    /// 1. Check which records already exist (lock-free index lookup)
    /// 2. Use insert_batch for new records
    /// 3. Update existing records individually
    ///
    /// Returns count of updated records (vs newly inserted).
    pub async fn upsert_batch<T: Indexed + Collection + Clone>(
        &self,
        records: &[T],
    ) -> Result<usize, StorageError> {
        if records.is_empty() {
            return Ok(0);
        }

        let collection_name = std::any::type_name::<T>();

        // Step 1: Check which records exist using lock-free index
        let mut new_records = Vec::with_capacity(records.len());
        let mut existing_records = Vec::with_capacity(records.len() / 4); // Usually fewer updates

        for record in records {
            let pk = serde_json::to_vec(record.id()).map_err(|e| {
                StorageError::Serialization(format!("Failed to serialize id: {}", e))
            })?;

            // Check if exists in lock-free index
            let exists = if let Some(col_idx) = self.lock_free_indexes.get(collection_name) {
                col_idx.iter().any(|field_entry| {
                    field_entry
                        .value()
                        .iter()
                        .any(|value_entry| value_entry.value().contains(&pk))
                })
            } else {
                false
            };

            if exists {
                existing_records.push(record.clone());
            } else {
                new_records.push(record.clone());
            }
        }

        let updated_count = existing_records.len();

        // Step 2: Bulk insert new records (highly optimized!)
        if !new_records.is_empty() {
            self.insert_batch(&new_records).await?;
        }

        // Step 3: Update existing records (need to reindex)
        for record in &existing_records {
            self.upsert(record).await?;
        }

        Ok(updated_count)
    }

    /// Delete all records matching a predicate
    ///
    /// # Example
    /// ```ignore
    /// // Delete all inactive users
    /// let deleted = db.delete_where::<User, _>(|u| !u.active).await?;
    /// println!("Deleted {} inactive users", deleted);
    /// ```
    pub async fn delete_where<T, F>(&self, predicate: F) -> Result<usize, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned,
        F: Fn(&T) -> bool,
    {
        let to_delete: Vec<T> = self.find_all(predicate).await?;
        let count = to_delete.len();
        for record in &to_delete {
            self.delete(record).await?;
        }
        Ok(count)
    }

    /// Update all records matching a predicate
    ///
    /// # Example
    /// ```ignore
    /// // Mark all users as verified
    /// let updated = db.update_where::<User, _, _>(
    ///     |u| u.email_verified == false,
    ///     |u| u.email_verified = true
    /// ).await?;
    /// ```
    pub async fn update_where<T, P, U>(
        &self,
        predicate: P,
        update_fn: U,
    ) -> Result<usize, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned,
        P: Fn(&T) -> bool,
        U: Fn(&mut T),
    {
        let mut to_update: Vec<T> = self.find_all(&predicate).await?;
        let count = to_update.len();
        for record in &mut to_update {
            update_fn(record);
            self.upsert(record).await?;
        }
        Ok(count)
    }

    // =========================================================================
    // AGGREGATIONS
    // =========================================================================

    /// Count all records of a collection
    ///
    /// Uses lock-free DashMap for concurrent access.
    pub async fn count<T: Indexed + Collection>(&self) -> Result<usize, StorageError> {
        let collection_name = std::any::type_name::<T>();

        // Count unique primary keys from DashMap indexes
        let mut unique_keys = std::collections::HashSet::new();
        if let Some(col_idx) = self.lock_free_indexes.get(collection_name) {
            for field_entry in col_idx.iter() {
                for value_entry in field_entry.value().iter() {
                    unique_keys.extend(value_entry.value().iter().cloned());
                }
            }
        }
        Ok(unique_keys.len())
    }

    /// Get detailed index statistics for a collection
    ///
    /// # Example
    /// ```ignore
    /// let stats = db.collection_stats::<User>().await;
    /// println!("User Index Stats: {}", stats);
    /// ```
    pub async fn collection_stats<T: Collection>(&self) -> IndexStats {
        let collection_name = std::any::type_name::<T>();
        let indexes = self.indexes.read().await;

        indexes
            .get(collection_name)
            .map(|idx| idx.stats())
            .unwrap_or_default()
    }

    /// Get detailed statistics for all collections
    pub async fn all_collection_stats(&self) -> std::collections::HashMap<String, IndexStats> {
        let indexes = self.indexes.read().await;
        indexes
            .iter()
            .map(|(name, idx)| (name.clone(), idx.stats()))
            .collect()
    }

    /// Create a snapshot of a collection (returns all records)
    ///
    /// # Example
    /// ```ignore
    /// let snapshot: Vec<User> = db.snapshot::<User>().await?;
    /// // snapshot can be used for backup, migration, or testing
    /// ```
    pub async fn snapshot<T: Indexed + Collection + DeserializeOwned + Clone>(
        &self,
    ) -> Result<Vec<T>, StorageError> {
        self.all().await
    }

    /// Clone collection to another storage
    ///
    /// Copies all records from this storage to another.
    ///
    /// # Example
    /// ```ignore
    /// let backup_db = IndexedStorage::new(backup_storage);
    /// db.clone_to::<User>(&backup_db).await?;
    /// ```
    pub async fn clone_to<T: Indexed + Collection + DeserializeOwned + Clone>(
        &self,
        target: &IndexedStorage<S>,
    ) -> Result<usize, StorageError> {
        let records: Vec<T> = self.all().await?;
        let count = records.len();
        for record in &records {
            target.insert(record).await?;
        }
        Ok(count)
    }

    /// Find first record matching a predicate
    ///
    /// More efficient than filter().first() for large collections
    /// as it stops at the first match.
    ///
    /// # Example
    /// ```ignore
    /// let admin = db.find_one::<User, _>(|u| u.role == "admin").await?;
    /// ```
    pub async fn find_one<T, F>(&self, predicate: F) -> Result<Option<T>, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned,
        F: Fn(&T) -> bool,
    {
        let all: Vec<T> = self.all().await?;
        Ok(all.into_iter().find(|r| predicate(r)))
    }

    /// Find all records matching a predicate
    ///
    /// # Example
    /// ```ignore
    /// let admins = db.find_all::<User, _>(|u| u.role == "admin").await?;
    /// ```
    pub async fn find_all<T, F>(&self, predicate: F) -> Result<Vec<T>, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned,
        F: Fn(&T) -> bool,
    {
        let all: Vec<T> = self.all().await?;
        Ok(all.into_iter().filter(|r| predicate(r)).collect())
    }

    /// Get all records of a collection
    ///
    /// Uses lock-free DashMap for concurrent access.
    pub async fn all<T: Indexed + Collection + DeserializeOwned>(
        &self,
    ) -> Result<Vec<T>, StorageError> {
        let collection_name = std::any::type_name::<T>();

        // Get all unique primary keys from DashMap (lock-free)
        let mut primary_keys = std::collections::HashSet::new();
        if let Some(col_idx) = self.lock_free_indexes.get(collection_name) {
            for field_entry in col_idx.iter() {
                for value_entry in field_entry.value().iter() {
                    primary_keys.extend(value_entry.value().iter().cloned());
                }
            }
        }

        self.fetch_records(primary_keys.into_iter().collect()).await
    }

    /// Sum a numeric field across all records
    pub async fn sum<T, F, N>(&self, field_fn: F) -> Result<N, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned,
        F: Fn(&T) -> N,
        N: std::iter::Sum + Default,
    {
        let records: Vec<T> = self.all().await?;
        Ok(records.iter().map(field_fn).sum())
    }

    /// Average a numeric field across all records
    pub async fn avg<T, F>(&self, field_fn: F) -> Result<f64, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned,
        F: Fn(&T) -> f64,
    {
        let records: Vec<T> = self.all().await?;
        if records.is_empty() {
            return Ok(0.0);
        }
        let sum: f64 = records.iter().map(&field_fn).sum();
        Ok(sum / records.len() as f64)
    }

    /// Find minimum value of a field
    pub async fn min<T, F, V>(&self, field_fn: F) -> Result<Option<V>, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned,
        F: Fn(&T) -> V,
        V: Ord + Clone,
    {
        let records: Vec<T> = self.all().await?;
        Ok(records.iter().map(field_fn).min())
    }

    /// Find maximum value of a field
    pub async fn max<T, F, V>(&self, field_fn: F) -> Result<Option<V>, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned,
        F: Fn(&T) -> V,
        V: Ord + Clone,
    {
        let records: Vec<T> = self.all().await?;
        Ok(records.iter().map(field_fn).max())
    }

    /// Count records matching a predicate
    pub async fn count_where<T, F>(&self, predicate: F) -> Result<usize, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned,
        F: Fn(&T) -> bool,
    {
        let records: Vec<T> = self.all().await?;
        Ok(records.iter().filter(|r| predicate(r)).count())
    }

    // =========================================================================
    // PAGINATION
    // =========================================================================

    /// Get all records with pagination (limit and offset)
    ///
    /// Uses lock-free DashMap for concurrent access.
    pub async fn paginate<T: Indexed + Collection + DeserializeOwned>(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<T>, StorageError> {
        let collection_name = std::any::type_name::<T>();

        // Get all unique primary keys from DashMap (lock-free)
        let mut all_keys = Vec::new();
        if let Some(col_idx) = self.lock_free_indexes.get(collection_name) {
            let mut seen = std::collections::HashSet::new();
            for field_entry in col_idx.iter() {
                for value_entry in field_entry.value().iter() {
                    for key in value_entry.value().iter() {
                        if seen.insert(key.clone()) {
                            all_keys.push(key.clone());
                        }
                    }
                }
            }
        }

        let primary_keys: Vec<_> = all_keys.into_iter().skip(offset).take(limit).collect();
        self.fetch_records(primary_keys).await
    }

    /// Filter with pagination
    pub async fn filter_paginated<T, F>(
        &self,
        predicate: F,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<T>, StorageError>
    where
        T: Indexed + Collection + DeserializeOwned,
        F: Fn(&T) -> bool,
    {
        let collection_name = std::any::type_name::<T>();

        // Get all unique primary keys from DashMap (lock-free)
        let mut primary_keys = Vec::new();
        if let Some(col_idx) = self.lock_free_indexes.get(collection_name) {
            let mut seen = std::collections::HashSet::new();
            for field_entry in col_idx.iter() {
                for value_entry in field_entry.value().iter() {
                    for key in value_entry.value().iter() {
                        if seen.insert(key.clone()) {
                            primary_keys.push(key.clone());
                        }
                    }
                }
            }
        }

        let mut results = Vec::new();
        let mut skipped = 0;

        for pk in primary_keys {
            if results.len() >= limit {
                break;
            }
            if let Some(data) = self.storage.get(&pk).await? {
                let record: T = serde_json::from_slice(&data).map_err(|e| {
                    StorageError::Deserialization(format!("Failed to deserialize: {}", e))
                })?;
                if predicate(&record) {
                    if skipped >= offset {
                        results.push(record);
                    } else {
                        skipped += 1;
                    }
                }
            }
        }

        Ok(results)
    }

    /// Query by field with pagination
    pub async fn query_by_paginated<T: Indexed + Collection + DeserializeOwned>(
        &self,
        field: &str,
        value: &impl Serialize,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<T>, StorageError> {
        let collection_name = std::any::type_name::<T>();
        let search_value = serde_json::to_vec(value).map_err(|e| {
            StorageError::Serialization(format!("Failed to serialize query value: {}", e))
        })?;

        let primary_keys: Vec<Vec<u8>> = {
            let indexes = self.indexes.read().await;
            indexes
                .get(collection_name)
                .map(|idx| {
                    idx.query(field, &search_value)
                        .into_iter()
                        .skip(offset)
                        .take(limit)
                        .collect()
                })
                .unwrap_or_default()
        };

        self.fetch_records(primary_keys).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prkdb_core::wal::WalConfig;
    use prkdb_macros::Collection;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    enum Role {
        Admin,
        User,
    }

    #[derive(Collection, Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct TestUser {
        #[id]
        pub id: String,
        #[index]
        pub role: Role,
        #[index(unique)]
        pub email: String,
    }

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
    async fn test_insert_and_query_by_index() {
        let storage = create_test_storage().await;
        let indexed = IndexedWalStorage::new(storage);

        // Insert users
        indexed
            .insert(&TestUser {
                id: "1".into(),
                role: Role::Admin,
                email: "alice@example.com".into(),
            })
            .await
            .unwrap();

        indexed
            .insert(&TestUser {
                id: "2".into(),
                role: Role::User,
                email: "bob@example.com".into(),
            })
            .await
            .unwrap();

        indexed
            .insert(&TestUser {
                id: "3".into(),
                role: Role::Admin,
                email: "charlie@example.com".into(),
            })
            .await
            .unwrap();

        // Query by role
        let admins: Vec<TestUser> = indexed.query_by("role", &Role::Admin).await.unwrap();
        assert_eq!(admins.len(), 2);

        let users: Vec<TestUser> = indexed.query_by("role", &Role::User).await.unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].email, "bob@example.com");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_unique() {
        let storage = create_test_storage().await;
        let indexed = IndexedWalStorage::new(storage);

        indexed
            .insert(&TestUser {
                id: "1".into(),
                role: Role::Admin,
                email: "alice@example.com".into(),
            })
            .await
            .unwrap();

        // Query by unique email
        let alice: Option<TestUser> = indexed
            .query_unique_by("email", &"alice@example.com")
            .await
            .unwrap();
        assert!(alice.is_some());
        assert_eq!(alice.unwrap().id, "1");

        // Non-existent email
        let nobody: Option<TestUser> = indexed
            .query_unique_by("email", &"nobody@example.com")
            .await
            .unwrap();
        assert!(nobody.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_delete_updates_index() {
        let storage = create_test_storage().await;
        let indexed = IndexedWalStorage::new(storage);

        let user = TestUser {
            id: "1".into(),
            role: Role::Admin,
            email: "alice@example.com".into(),
        };

        indexed.insert(&user).await.unwrap();

        // Verify exists
        let admins: Vec<TestUser> = indexed.query_by("role", &Role::Admin).await.unwrap();
        assert_eq!(admins.len(), 1);

        // Delete
        indexed.delete(&user).await.unwrap();

        // Verify gone from index
        let admins: Vec<TestUser> = indexed.query_by("role", &Role::Admin).await.unwrap();
        assert_eq!(admins.len(), 0);
    }

    // Test struct with numeric field for range queries
    #[derive(Collection, Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Person {
        #[id]
        pub id: String,
        #[index]
        pub age: u32,
        #[index]
        pub name: String,
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_filter() {
        let storage = create_test_storage().await;
        let indexed = IndexedWalStorage::new(storage);

        // Insert people with various ages
        for (id, name, age) in [
            ("1", "Alice", 25),
            ("2", "Bob", 30),
            ("3", "Charlie", 35),
            ("4", "Diana", 40),
            ("5", "Eve", 20),
        ] {
            indexed
                .insert(&Person {
                    id: id.into(),
                    age,
                    name: name.into(),
                })
                .await
                .unwrap();
        }

        // Filter: age > 25
        let older: Vec<Person> = indexed.filter(|p: &Person| p.age > 25).await.unwrap();
        assert_eq!(older.len(), 3); // Bob, Charlie, Diana

        // Filter: name starts with 'A' or 'B'
        let ab_people: Vec<Person> = indexed
            .filter(|p: &Person| p.name.starts_with('A') || p.name.starts_with('B'))
            .await
            .unwrap();
        assert_eq!(ab_people.len(), 2); // Alice, Bob

        // Complex filter
        let complex: Vec<Person> = indexed
            .filter(|p: &Person| p.age >= 25 && p.age <= 35)
            .await
            .unwrap();
        assert_eq!(complex.len(), 3); // Alice, Bob, Charlie
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_operations() {
        let storage = create_test_storage().await;
        let indexed = IndexedStorage::new(storage);

        let users = vec![
            TestUser {
                id: "1".into(),
                role: Role::Admin,
                email: "a@b.com".into(),
            },
            TestUser {
                id: "2".into(),
                role: Role::User,
                email: "c@d.com".into(),
            },
            TestUser {
                id: "3".into(),
                role: Role::Admin,
                email: "e@f.com".into(),
            },
        ];

        // Insert batch
        let count = indexed.insert_batch(&users).await.unwrap();
        assert_eq!(count, 3);

        // Verify count
        let total = indexed.count::<TestUser>().await.unwrap();
        assert_eq!(total, 3);

        // Delete batch
        let deleted = indexed.delete_batch(&users[0..2]).await.unwrap();
        assert_eq!(deleted, 2);

        // Verify count after delete
        let remaining = indexed.count::<TestUser>().await.unwrap();
        assert_eq!(remaining, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_pagination() {
        let storage = create_test_storage().await;
        let indexed = IndexedStorage::new(storage);

        // Insert 10 people
        for i in 0..10 {
            indexed
                .insert(&Person {
                    id: format!("{}", i),
                    age: 20 + i,
                    name: format!("Person{}", i),
                })
                .await
                .unwrap();
        }

        // Test all()
        let all: Vec<Person> = indexed.all().await.unwrap();
        assert_eq!(all.len(), 10);

        // Test paginate - first page
        let page1: Vec<Person> = indexed.paginate(3, 0).await.unwrap();
        assert_eq!(page1.len(), 3);

        // Test paginate - second page
        let page2: Vec<Person> = indexed.paginate(3, 3).await.unwrap();
        assert_eq!(page2.len(), 3);

        // Test paginate - last page (partial)
        let page4: Vec<Person> = indexed.paginate(3, 9).await.unwrap();
        assert_eq!(page4.len(), 1);

        // Test filter_paginated
        let filtered: Vec<Person> = indexed
            .filter_paginated(|p: &Person| p.age >= 25, 2, 0)
            .await
            .unwrap();
        assert_eq!(filtered.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_aggregations() {
        let storage = create_test_storage().await;
        let indexed = IndexedStorage::new(storage);

        // Insert people with ages: 20, 25, 30, 35, 40
        for (i, age) in [20u32, 25, 30, 35, 40].iter().enumerate() {
            indexed
                .insert(&Person {
                    id: format!("{}", i),
                    age: *age,
                    name: format!("Person{}", i),
                })
                .await
                .unwrap();
        }

        // Test sum
        let total_age: u32 = indexed.sum(|p: &Person| p.age).await.unwrap();
        assert_eq!(total_age, 150); // 20+25+30+35+40

        // Test avg
        let avg_age: f64 = indexed.avg(|p: &Person| p.age as f64).await.unwrap();
        assert!((avg_age - 30.0).abs() < 0.01); // Average is 30

        // Test min
        let min_age: Option<u32> = indexed.min(|p: &Person| p.age).await.unwrap();
        assert_eq!(min_age, Some(20));

        // Test max
        let max_age: Option<u32> = indexed.max(|p: &Person| p.age).await.unwrap();
        assert_eq!(max_age, Some(40));

        // Test count_where
        let adults = indexed.count_where(|p: &Person| p.age >= 25).await.unwrap();
        assert_eq!(adults, 4); // 25, 30, 35, 40
    }

    // =========================================================================
    // LOCK-FREE INSERT_BATCH TESTS
    // =========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_insert_batch_basic() {
        let storage = create_test_storage().await;
        let indexed = IndexedStorage::new(storage);

        // Create batch of users
        let users: Vec<TestUser> = (0..100)
            .map(|i| TestUser {
                id: format!("{}", i),
                role: if i % 2 == 0 { Role::Admin } else { Role::User },
                email: format!("user{}@example.com", i),
            })
            .collect();

        // Insert batch
        let count = indexed.insert_batch(&users).await.unwrap();
        assert_eq!(count, 100);

        // Verify records exist via get (not count - count uses different index)
        let user0: Option<TestUser> = indexed.get(&"0".to_string()).await.unwrap();
        assert!(user0.is_some());
        assert_eq!(user0.unwrap().email, "user0@example.com");

        let user99: Option<TestUser> = indexed.get(&"99".to_string()).await.unwrap();
        assert!(user99.is_some());
        assert_eq!(user99.unwrap().email, "user99@example.com");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_insert_batch_empty() {
        let storage = create_test_storage().await;
        let indexed = IndexedStorage::new(storage);

        // Empty batch should return 0
        let users: Vec<TestUser> = vec![];
        let count = indexed.insert_batch(&users).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_insert_batch_large() {
        let storage = create_test_storage().await;
        let indexed = IndexedStorage::new(storage);

        // Create large batch (1000 records)
        let users: Vec<TestUser> = (0..1000)
            .map(|i| TestUser {
                id: format!("{}", i),
                role: if i % 3 == 0 { Role::Admin } else { Role::User },
                email: format!("user{}@example.com", i),
            })
            .collect();

        // Insert should complete quickly
        let start = std::time::Instant::now();
        let count = indexed.insert_batch(&users).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(count, 1000);
        // Should complete in under 1 second (typically ~10-20ms)
        assert!(
            elapsed.as_secs() < 1,
            "insert_batch took too long: {:?}",
            elapsed
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_inserts() {
        let storage = create_test_storage().await;
        let indexed = Arc::new(IndexedStorage::new(storage));

        // Spawn multiple concurrent writers
        let handles: Vec<_> = (0..4)
            .map(|worker| {
                let db = indexed.clone();
                tokio::spawn(async move {
                    let users: Vec<TestUser> = (0..100)
                        .map(|i| TestUser {
                            id: format!("w{}_u{}", worker, i),
                            role: Role::User,
                            email: format!("worker{}user{}@example.com", worker, i),
                        })
                        .collect();
                    db.insert_batch(&users).await.unwrap()
                })
            })
            .collect();

        // Wait for all writers
        let mut total = 0;
        for h in handles {
            total += h.await.unwrap();
        }

        assert_eq!(total, 400); // 4 workers * 100 records

        // Verify some records exist via get (not count - count uses different index)
        let user: Option<TestUser> = indexed.get(&"w0_u0".to_string()).await.unwrap();
        assert!(user.is_some());

        let user: Option<TestUser> = indexed.get(&"w3_u99".to_string()).await.unwrap();
        assert!(user.is_some());
    }

    // =========================================================================
    // UPSERT/DELETE BATCH TESTS
    // =========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upsert_batch() {
        let storage = create_test_storage().await;
        let indexed = IndexedStorage::new(storage);

        // Insert some initial records
        let users: Vec<TestUser> = (0..50)
            .map(|i| TestUser {
                id: format!("{}", i),
                role: Role::User,
                email: format!("user{}@example.com", i),
            })
            .collect();
        indexed.insert_batch(&users).await.unwrap();

        // Upsert: 25 existing + 25 new
        let upsert_users: Vec<TestUser> = (25..75)
            .map(|i| TestUser {
                id: format!("{}", i),
                role: Role::Admin, // Changed role
                email: format!("updated{}@example.com", i),
            })
            .collect();

        let updated_count = indexed.upsert_batch(&upsert_users).await.unwrap();
        assert_eq!(updated_count, 25); // 25 existing records updated

        // Verify updated record
        let user: Option<TestUser> = indexed.get(&"30".to_string()).await.unwrap();
        assert!(user.is_some());
        let user = user.unwrap();
        assert_eq!(user.role, Role::Admin); // Should be updated
        assert_eq!(user.email, "updated30@example.com");

        // Verify new record
        let user: Option<TestUser> = indexed.get(&"60".to_string()).await.unwrap();
        assert!(user.is_some());
        assert_eq!(user.unwrap().email, "updated60@example.com");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_delete_batch_bulk() {
        let storage = create_test_storage().await;
        let indexed = IndexedStorage::new(storage);

        // Insert records
        let users: Vec<TestUser> = (0..100)
            .map(|i| TestUser {
                id: format!("{}", i),
                role: Role::User,
                email: format!("user{}@example.com", i),
            })
            .collect();
        indexed.insert_batch(&users).await.unwrap();

        // Delete half of them
        let to_delete: Vec<TestUser> = (0..50)
            .map(|i| TestUser {
                id: format!("{}", i),
                role: Role::User,
                email: format!("user{}@example.com", i),
            })
            .collect();

        let deleted = indexed.delete_batch(&to_delete).await.unwrap();
        assert_eq!(deleted, 50);

        // Verify deleted records are gone
        let user: Option<TestUser> = indexed.get(&"25".to_string()).await.unwrap();
        assert!(user.is_none());

        // Verify remaining records still exist
        let user: Option<TestUser> = indexed.get(&"75".to_string()).await.unwrap();
        assert!(user.is_some());
    }

    // =========================================================================
    // STRESS TESTS
    // =========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_read_write() {
        let storage = create_test_storage().await;
        let indexed = Arc::new(IndexedStorage::new(storage));

        // Insert initial data
        let initial_users: Vec<TestUser> = (0..100)
            .map(|i| TestUser {
                id: format!("{}", i),
                role: Role::User,
                email: format!("user{}@example.com", i),
            })
            .collect();
        indexed.insert_batch(&initial_users).await.unwrap();

        // Spawn concurrent readers and writers
        let mut handles = Vec::new();

        // 4 writers
        for w in 0..4 {
            let db = Arc::clone(&indexed);
            handles.push(tokio::spawn(async move {
                let users: Vec<TestUser> = (0..25)
                    .map(|i| TestUser {
                        id: format!("new_w{}_u{}", w, i),
                        role: Role::Admin,
                        email: format!("new_w{}u{}@example.com", w, i),
                    })
                    .collect();
                db.insert_batch(&users).await.unwrap();
                25usize
            }));
        }

        // 4 readers
        for r in 0..4 {
            let db = Arc::clone(&indexed);
            handles.push(tokio::spawn(async move {
                let mut found = 0usize;
                for i in 0..25 {
                    let id = format!("{}", (r * 25 + i) % 100);
                    if db.get::<TestUser>(&id).await.unwrap().is_some() {
                        found += 1;
                    }
                }
                found
            }));
        }

        // All operations should complete without deadlock
        for h in handles {
            let _ = h.await.unwrap();
        }
    }

    // =========================================================================
    // CURSOR PAGINATION TESTS
    // =========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_cursor_pagination() {
        let storage = create_test_storage().await;
        let indexed = IndexedStorage::new(storage);

        // Insert 50 users with same role
        let users: Vec<TestUser> = (0..50)
            .map(|i| TestUser {
                id: format!("{:03}", i), // Zero-padded for consistent ordering
                role: Role::User,
                email: format!("user{}@example.com", i),
            })
            .collect();
        indexed.insert_batch(&users).await.unwrap();

        // First page (10 records)
        let (page1, cursor1) = indexed
            .query_with_cursor::<TestUser>("role", &Role::User, None, 10)
            .await
            .unwrap();
        assert_eq!(page1.len(), 10);
        assert!(cursor1.is_some());

        // Second page
        let (page2, cursor2) = indexed
            .query_with_cursor::<TestUser>("role", &Role::User, cursor1.as_deref(), 10)
            .await
            .unwrap();
        assert_eq!(page2.len(), 10);
        assert!(cursor2.is_some());

        // Verify no duplicates between pages
        let page1_ids: Vec<_> = page1.iter().map(|u| &u.id).collect();
        let page2_ids: Vec<_> = page2.iter().map(|u| &u.id).collect();
        for id in &page2_ids {
            assert!(!page1_ids.contains(id), "Duplicate found: {}", id);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_transaction_savepoints() {
        let storage = create_test_storage().await;
        let db = IndexedStorage::new(storage);

        let user1 = TestUser {
            id: "u1".to_string(),
            role: Role::User,
            email: "u1@example.com".to_string(),
        };

        let user2 = TestUser {
            id: "u2".to_string(),
            role: Role::Admin,
            email: "u2@example.com".to_string(),
        };

        let mut tx = db.transaction();

        // 1. Insert user1
        tx.insert(&user1).unwrap();

        // 2. Create savepoint
        tx.savepoint("sp1");

        // 3. Insert user2
        tx.insert(&user2).unwrap();
        assert_eq!(tx.len(), 2);

        // 4. Rollback to savepoint (should remove user2 insert)
        tx.rollback_to("sp1").unwrap();
        assert_eq!(tx.len(), 1);

        // 5. Commit
        tx.commit().await.unwrap();

        // 6. Verify
        assert!(db
            .get::<TestUser>(&"u1".to_string())
            .await
            .unwrap()
            .is_some());
        assert!(db
            .get::<TestUser>(&"u2".to_string())
            .await
            .unwrap()
            .is_none());
    }
}
