//! The collection catalog (spec §7 2c, KEY-01): a persisted map from a collection's
//! persisted name to the [`CollectionId`] its keys carry (see [`crate::keys`]).
//!
//! # Layout
//!
//! All entries live under [`SYSTEM_COLLECTION`] in the catalog's namespace:
//!
//! - `catalog/name/{name}` → id, `u32` big-endian (the forward entry; it is what makes a
//!   name allocated);
//! - `catalog/id/{id BE}` → name (the reverse entry);
//! - `catalog/next` → the next id to hand out, `u32` big-endian, starting at 1.
//!
//! # Allocation
//!
//! First use of a name takes the storage's allocation lock
//! ([`StorageAdapter::allocation_lock`]), **re-reads the forward entry** (another `Catalog`
//! over the same storage may have allocated it while this one waited), then writes the
//! bumped counter, the reverse entry and the forward entry, in that order, as plain `put`s.
//! A crash between them wastes an id and never reuses one; a reverse entry without its
//! forward entry is ignored. An adapter without an allocation lock gets a per-`Catalog`
//! lock instead, so it must be used through a single `Catalog`.

use crate::keys::{encode_key, CollectionId, SYSTEM_COLLECTION};
use dashmap::DashMap;
use prkdb_types::collection::Collection;
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;
use std::sync::Arc;

const NAME_ENTRY: &[u8] = b"catalog/name/";
const ID_ENTRY: &[u8] = b"catalog/id/";
const NEXT_ENTRY: &[u8] = b"catalog/next";

/// Longest persisted name.
pub const MAX_NAME_LEN: usize = 64;

/// Maps collection names to ids, persisting each allocation in the storage it describes.
pub struct Catalog {
    storage: Arc<dyn StorageAdapter>,
    ns: Vec<u8>,
    cache: DashMap<String, CollectionId>,
    names: DashMap<CollectionId, String>,
    /// Used only when `storage.allocation_lock()` is `None`.
    fallback: Arc<tokio::sync::Mutex<()>>,
}

impl Catalog {
    /// A catalog for namespace `ns` over `storage`. Reads nothing until first use.
    pub fn new(storage: Arc<dyn StorageAdapter>, ns: Vec<u8>) -> Self {
        Self {
            storage,
            ns,
            cache: DashMap::new(),
            names: DashMap::new(),
            fallback: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// A persisted name must match `^[a-z][a-z0-9_]{0,63}$`.
    pub fn validate_name(name: &str) -> Result<(), StorageError> {
        let mut bytes = name.bytes();
        let valid = name.len() <= MAX_NAME_LEN
            && bytes.next().is_some_and(|b| b.is_ascii_lowercase())
            && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        if valid {
            Ok(())
        } else {
            Err(StorageError::Validation(format!(
                "invalid collection name {name:?}: a persisted name must match \
                 ^[a-z][a-z0-9_]{{0,63}}$ (pin one with #[collection(name = \"...\")])"
            )))
        }
    }

    /// The id for `name`, allocating and persisting one on first use.
    pub async fn id_for_name(&self, name: &str) -> Result<CollectionId, StorageError> {
        Self::validate_name(name)?;
        if let Some(id) = self.cache.get(name) {
            return Ok(*id);
        }
        if let Some(id) = self.read_forward(name).await? {
            self.remember(name, id);
            return Ok(id);
        }

        let lock = self
            .storage
            .allocation_lock()
            .unwrap_or_else(|| self.fallback.clone());
        let _guard = lock.lock().await;

        // Another `Catalog` over this storage may have allocated it while this one waited.
        if let Some(id) = self.read_forward(name).await? {
            self.remember(name, id);
            return Ok(id);
        }

        let next_key = self.key(NEXT_ENTRY)?;
        let next = match self.storage.get(&next_key).await? {
            Some(bytes) => decode_u32(&bytes, "catalog/next")?,
            None => 1,
        };
        if next == 0 {
            return Err(StorageError::Corruption(
                "catalog/next is 0, an id reserved for the catalog".to_string(),
            ));
        }
        let after = next.checked_add(1).ok_or_else(|| {
            StorageError::Internal("collection ids exhausted (u32::MAX allocated)".to_string())
        })?;
        let id = CollectionId(next);

        // Counter first: a crash after it wastes `id`, never hands it out twice.
        self.storage.put(&next_key, &after.to_be_bytes()).await?;
        self.storage
            .put(&self.reverse_key(id)?, name.as_bytes())
            .await?;
        self.storage
            .put(&self.forward_key(name)?, &id.0.to_be_bytes())
            .await?;
        self.remember(name, id);
        Ok(id)
    }

    /// The id of collection `C`, by its persisted name.
    pub async fn id_for<C: Collection>(&self) -> Result<CollectionId, StorageError> {
        self.id_for_name(&C::persisted_name()).await
    }

    /// The id for `name` if one was ever allocated; never allocates (admin and read
    /// paths). A name that is not a valid persisted name was never allocated.
    pub async fn lookup(&self, name: &str) -> Result<Option<CollectionId>, StorageError> {
        if Self::validate_name(name).is_err() {
            return Ok(None);
        }
        if let Some(id) = self.cache.get(name) {
            return Ok(Some(*id));
        }
        let found = self.read_forward(name).await?;
        if let Some(id) = found {
            self.remember(name, id);
        }
        Ok(found)
    }

    /// Reverse lookup for ids this storage has allocated (cached; reads the entry on a
    /// miss). A reverse entry whose forward entry does not point back is ignored.
    pub async fn name_for(&self, id: CollectionId) -> Result<Option<String>, StorageError> {
        if let Some(name) = self.names.get(&id) {
            return Ok(Some(name.clone()));
        }
        let Some(bytes) = self.storage.get(&self.reverse_key(id)?).await? else {
            return Ok(None);
        };
        let name = String::from_utf8(bytes)
            .map_err(|_| StorageError::Corruption(format!("catalog/id/{} is not UTF-8", id.0)))?;
        if Self::validate_name(&name).is_err() || self.read_forward(&name).await? != Some(id) {
            return Ok(None);
        }
        self.remember(&name, id);
        Ok(Some(name))
    }

    /// Every allocated name with its id, sorted by name.
    pub async fn list(&self) -> Result<Vec<(String, CollectionId)>, StorageError> {
        let prefix = self.key(NAME_ENTRY)?;
        let mut out = Vec::new();
        for (key, value) in self.storage.scan_prefix(&prefix).await? {
            let Ok(name) = String::from_utf8(key[prefix.len()..].to_vec()) else {
                continue;
            };
            let id = CollectionId(decode_u32(&value, "catalog/name/")?);
            self.remember(&name, id);
            out.push((name, id));
        }
        out.sort();
        Ok(out)
    }

    fn remember(&self, name: &str, id: CollectionId) {
        self.cache.insert(name.to_string(), id);
        self.names.insert(id, name.to_string());
    }

    async fn read_forward(&self, name: &str) -> Result<Option<CollectionId>, StorageError> {
        match self.storage.get(&self.forward_key(name)?).await? {
            Some(bytes) => Ok(Some(CollectionId(decode_u32(&bytes, "catalog/name/")?))),
            None => Ok(None),
        }
    }

    fn key(&self, suffix: &[u8]) -> Result<Vec<u8>, StorageError> {
        encode_key(&self.ns, SYSTEM_COLLECTION, suffix)
    }

    fn forward_key(&self, name: &str) -> Result<Vec<u8>, StorageError> {
        self.key(&[NAME_ENTRY, name.as_bytes()].concat())
    }

    fn reverse_key(&self, id: CollectionId) -> Result<Vec<u8>, StorageError> {
        self.key(&[ID_ENTRY, &id.0.to_be_bytes()[..]].concat())
    }
}

fn decode_u32(bytes: &[u8], what: &str) -> Result<u32, StorageError> {
    let array: [u8; 4] = bytes.try_into().map_err(|_| {
        StorageError::Corruption(format!(
            "catalog entry {what} holds {} bytes, expected 4",
            bytes.len()
        ))
    })?;
    Ok(u32::from_be_bytes(array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::InMemoryAdapter;

    #[test]
    fn names_are_validated() {
        for ok in ["a", "users", "user_profile", "v2_items", &"a".repeat(64)] {
            assert!(Catalog::validate_name(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "Users",
            "1users",
            "_users",
            "user-profile",
            "a b",
            &"a".repeat(65),
        ] {
            assert!(Catalog::validate_name(bad).is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn lookup_and_name_for_never_allocate() {
        let cat = Catalog::new(Arc::new(InMemoryAdapter::new()), b"ns".to_vec());
        assert_eq!(cat.lookup("users").await.unwrap(), None);
        assert_eq!(cat.lookup("Not Valid").await.unwrap(), None);
        assert_eq!(cat.name_for(CollectionId(1)).await.unwrap(), None);
        assert!(cat.list().await.unwrap().is_empty());

        let id = cat.id_for_name("users").await.unwrap();
        assert_eq!(id, CollectionId(1), "ids start at 1; 0 is the catalog's");
        assert_eq!(cat.lookup("users").await.unwrap(), Some(id));
        assert_eq!(cat.list().await.unwrap(), vec![("users".to_string(), id)]);
    }

    #[tokio::test]
    async fn a_reverse_entry_without_its_forward_entry_is_ignored() {
        let storage = Arc::new(InMemoryAdapter::new());
        let cat = Catalog::new(storage.clone(), Vec::new());
        // What a crash between the reverse and the forward write leaves.
        storage
            .put(&cat.reverse_key(CollectionId(1)).unwrap(), b"ghost")
            .await
            .unwrap();
        storage
            .put(&cat.key(NEXT_ENTRY).unwrap(), &2u32.to_be_bytes())
            .await
            .unwrap();

        assert_eq!(cat.name_for(CollectionId(1)).await.unwrap(), None);
        assert!(cat.list().await.unwrap().is_empty());
        assert_eq!(
            cat.id_for_name("ghost").await.unwrap(),
            CollectionId(2),
            "the wasted id is not reused"
        );
    }

    #[tokio::test]
    async fn catalogs_in_different_namespaces_are_independent() {
        let storage: Arc<dyn StorageAdapter> = Arc::new(InMemoryAdapter::new());
        let a = Catalog::new(storage.clone(), b"a".to_vec());
        let b = Catalog::new(storage, b"b".to_vec());
        a.id_for_name("x").await.unwrap();
        assert_eq!(b.lookup("x").await.unwrap(), None);
        assert_eq!(b.id_for_name("y").await.unwrap(), CollectionId(1));
    }
}
