//! Secondary Index Support for PrkDB
//!
//! This module provides the core types for secondary indexes:
//! - `IndexDef` - Definition of an index (field name, unique constraint)
//! - `Indexed` - Trait for types that have secondary indexes
//! - `IndexedStorage` - Trait for storage backends that support indexed queries

use crate::error::StorageError;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

/// Definition of a secondary index
#[derive(Debug, Clone)]
pub struct IndexDef {
    /// Field name being indexed
    pub field: &'static str,
    /// Whether this is a unique index
    pub unique: bool,
}

/// Trait for types that have secondary indexes
///
/// This trait is automatically implemented by the `#[derive(Collection)]` macro
/// when fields are marked with `#[index]` or `#[index(unique)]`.
pub trait Indexed: Serialize + DeserializeOwned + Send + Sync {
    /// Get the index definitions for this type
    fn indexes() -> &'static [IndexDef];

    /// Extract index values from this instance
    /// Returns: Vec<(field_name, serialized_value)>
    fn index_values(&self) -> Vec<(&'static str, Vec<u8>)>;
}

/// Trait for storage backends that support indexed queries
#[async_trait]
pub trait IndexedStorage: Send + Sync {
    /// Query by index field, returning all matching records
    async fn query_index<T: Indexed>(
        &self,
        field: &str,
        value: &impl Serialize,
    ) -> Result<Vec<T>, StorageError>;

    /// Query by unique index field, returning at most one record
    async fn query_unique<T: Indexed>(
        &self,
        field: &str,
        value: &impl Serialize,
    ) -> Result<Option<T>, StorageError>;

    /// Insert a record, updating all indexes
    async fn insert_indexed<T: Indexed>(&self, record: &T) -> Result<(), StorageError>;

    /// Delete a record, removing from all indexes
    async fn delete_indexed<T: Indexed>(&self, record: &T) -> Result<(), StorageError>;
}

/// Generate index key from collection name, field name, and value
pub fn index_key(collection: &str, field: &str, value: &[u8]) -> Vec<u8> {
    // Format: __idx:{collection}:{field}:{value}
    let mut key = Vec::with_capacity(10 + collection.len() + field.len() + value.len());
    key.extend_from_slice(b"__idx:");
    key.extend_from_slice(collection.as_bytes());
    key.extend_from_slice(b":");
    key.extend_from_slice(field.as_bytes());
    key.extend_from_slice(b":");
    key.extend_from_slice(value);
    key
}

/// Generate reverse index key (for cleanup on delete)
pub fn reverse_index_key(collection: &str, primary_key: &[u8]) -> Vec<u8> {
    // Format: __ridx:{collection}:{primary_key}
    let mut key = Vec::with_capacity(12 + collection.len() + primary_key.len());
    key.extend_from_slice(b"__ridx:");
    key.extend_from_slice(collection.as_bytes());
    key.extend_from_slice(b":");
    key.extend_from_slice(primary_key);
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_key_generation() {
        let key = index_key("users", "role", b"admin");
        assert_eq!(key, b"__idx:users:role:admin");
    }

    #[test]
    fn test_reverse_index_key_generation() {
        let key = reverse_index_key("users", b"user-123");
        assert_eq!(key, b"__ridx:users:user-123");
    }

    #[test]
    fn test_index_def() {
        let def = IndexDef {
            field: "email",
            unique: true,
        };
        assert_eq!(def.field, "email");
        assert!(def.unique);
    }
}
