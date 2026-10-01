//! The collection catalog (spec §7 2c, KEY-01): a persisted map from a collection's
//! persisted name to the [`CollectionId`] its keys carry (see [`crate::keys`]).
//!
//! # Layout
//!
//! All entries live under [`SYSTEM_COLLECTION`] in the catalog's namespace:
//!
//! - `catalog/name/{name}` → id, `u32` big-endian, then one byte naming the value
//!   encoding of the API that allocated it ([`ValueEncoding`]), then the Rust type name
//!   that allocated it (empty when a name-based API did) — the forward entry; it is what
//!   makes a name allocated;
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
//!
//! # One type per name (review H1)
//!
//! A persisted name is the collection's identity, so two Rust types resolving the same
//! name in one namespace would silently share records. Every typed resolution
//! ([`Catalog::id_for`], [`Catalog::lookup_for`]) claims the name for its `TypeId` in a
//! registry shared by every catalog over the same storage (keyed by the storage's
//! allocation lock), and a second, different type is refused with
//! [`StorageError::Validation`]. Across restarts the forward entry's recorded type name
//! is compared instead, and a mismatch is only **logged**: a type moved to another module
//! keeps its name and its data.

use crate::keys::{encode_key, CollectionId, SYSTEM_COLLECTION};
use dashmap::DashMap;
use prkdb_types::collection::Collection;
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;
use std::any::TypeId;
use std::sync::{Arc, OnceLock, Weak};

const NAME_ENTRY: &[u8] = b"catalog/name/";
const ID_ENTRY: &[u8] = b"catalog/id/";
const NEXT_ENTRY: &[u8] = b"catalog/next";

/// Longest persisted name.
pub const MAX_NAME_LEN: usize = 64;

type AllocationLock = Arc<tokio::sync::Mutex<()>>;

/// `(namespace, persisted name)` → the type that claimed it, for one storage.
type TypeRegistry = DashMap<(Vec<u8>, String), (TypeId, &'static str)>;

/// How the API that allocated a collection encodes its values, recorded in the forward
/// entry so another API can refuse to write values the owner cannot read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueEncoding {
    /// Allocated by an API that does not say (the routing API, `Catalog::id_for`).
    Unspecified = 0,
    /// JSON: `IndexedStorage`, the HTTP API and the CLI.
    Json = 1,
    /// bincode: `CollectionHandle`.
    Bincode = 2,
}

impl ValueEncoding {
    fn from_byte(byte: u8) -> Result<Self, StorageError> {
        match byte {
            0 => Ok(Self::Unspecified),
            1 => Ok(Self::Json),
            2 => Ok(Self::Bincode),
            other => Err(StorageError::Corruption(format!(
                "catalog/name/ value encoding {other} is unknown"
            ))),
        }
    }
}

/// What a forward entry records about a collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionRecord {
    pub id: CollectionId,
    pub encoding: ValueEncoding,
    /// The Rust type name that allocated it; empty for a name-based API.
    pub type_name: String,
}

/// The Rust type a typed API resolves a collection for, and its value encoding.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CollectionType {
    id: TypeId,
    name: &'static str,
    encoding: ValueEncoding,
}

impl CollectionType {
    pub(crate) fn of<C: Collection>() -> Self {
        Self {
            id: TypeId::of::<C>(),
            name: std::any::type_name::<C>(),
            encoding: ValueEncoding::Unspecified,
        }
    }

    pub(crate) fn with_encoding(self, encoding: ValueEncoding) -> Self {
        Self { encoding, ..self }
    }
}

/// Every live storage's registry, with a weak handle on the storage's allocation lock.
type Registries = std::sync::Mutex<Vec<(Weak<tokio::sync::Mutex<()>>, Arc<TypeRegistry>)>>;

/// The registry for the storage whose allocation lock is `lock`: one per storage, shared
/// by every catalog over it, dropped with it (entries for dropped storages are pruned, so
/// a new storage at a reused address starts empty).
fn registry_for(lock: &AllocationLock) -> Arc<TypeRegistry> {
    static REGISTRIES: OnceLock<Registries> = OnceLock::new();
    let mut all = REGISTRIES
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    all.retain(|(owner, _)| owner.strong_count() > 0);
    if let Some((_, registry)) = all
        .iter()
        .find(|(owner, _)| owner.upgrade().is_some_and(|l| Arc::ptr_eq(&l, lock)))
    {
        return registry.clone();
    }
    let registry = Arc::new(TypeRegistry::new());
    all.push((Arc::downgrade(lock), registry.clone()));
    registry
}

/// Maps collection names to ids, persisting each allocation in the storage it describes.
pub struct Catalog {
    storage: Arc<dyn StorageAdapter>,
    ns: Vec<u8>,
    cache: DashMap<String, CollectionId>,
    names: DashMap<CollectionId, String>,
    /// Ids already resolved for a type in this catalog: the typed hot path (one
    /// `TypeId` hash, no name hashing, no registry check after the first use).
    typed: DashMap<TypeId, CollectionId>,
    /// Forward entries already read (they never change once written).
    records: DashMap<String, CollectionRecord>,
    /// `storage.allocation_lock()`, or a per-`Catalog` lock when it has none.
    lock: AllocationLock,
    types: Arc<TypeRegistry>,
}

impl Catalog {
    /// A catalog for namespace `ns` over `storage`. Reads nothing until first use.
    pub fn new(storage: Arc<dyn StorageAdapter>, ns: Vec<u8>) -> Self {
        let lock = storage
            .allocation_lock()
            .unwrap_or_else(|| Arc::new(tokio::sync::Mutex::new(())));
        let types = registry_for(&lock);
        Self {
            storage,
            ns,
            cache: DashMap::new(),
            names: DashMap::new(),
            typed: DashMap::new(),
            records: DashMap::new(),
            lock,
            types,
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

    /// The id for `name`, allocating and persisting one on first use. For name-based
    /// callers (the routing API, the CLI and HTTP server); typed callers use
    /// [`Self::id_for`], which also checks the type.
    pub async fn id_for_name(&self, name: &str) -> Result<CollectionId, StorageError> {
        self.allocate(name, "", ValueEncoding::Unspecified).await
    }

    /// As [`Self::id_for_name`], recording `encoding` if it allocates.
    pub(crate) async fn id_for_name_with(
        &self,
        name: &str,
        encoding: ValueEncoding,
    ) -> Result<CollectionId, StorageError> {
        self.allocate(name, "", encoding).await
    }

    /// As [`Self::id_for`], recording `encoding` if it allocates.
    pub(crate) async fn id_for_encoded<C: Collection>(
        &self,
        encoding: ValueEncoding,
    ) -> Result<CollectionId, StorageError> {
        if let Some(id) = self.typed.get(&TypeId::of::<C>()) {
            return Ok(*id);
        }
        self.id_for_type(
            CollectionType::of::<C>().with_encoding(encoding),
            &C::persisted_name(),
        )
        .await
    }

    /// The id of collection `C`, by its persisted name, allocated on first use. Refuses a
    /// name another type already resolved over this storage (module docs).
    pub async fn id_for<C: Collection>(&self) -> Result<CollectionId, StorageError> {
        // Hot path first: no persisted-name computation (an allocation for manual impls).
        if let Some(id) = self.typed.get(&TypeId::of::<C>()) {
            return Ok(*id);
        }
        self.id_for_type(CollectionType::of::<C>(), &C::persisted_name())
            .await
    }

    /// As [`Self::id_for`], without allocating: `None` if `C` was never written.
    pub async fn lookup_for<C: Collection>(&self) -> Result<Option<CollectionId>, StorageError> {
        if let Some(id) = self.typed.get(&TypeId::of::<C>()) {
            return Ok(Some(*id));
        }
        self.lookup_type(CollectionType::of::<C>(), &C::persisted_name())
            .await
    }

    pub(crate) async fn id_for_type(
        &self,
        ty: CollectionType,
        name: &str,
    ) -> Result<CollectionId, StorageError> {
        if let Some(id) = self.typed.get(&ty.id) {
            return Ok(*id);
        }
        self.claim(ty, name)?;
        let id = self.allocate(name, ty.name, ty.encoding).await?;
        self.check_recorded_type(ty, name).await?;
        self.typed.insert(ty.id, id);
        Ok(id)
    }

    pub(crate) async fn lookup_type(
        &self,
        ty: CollectionType,
        name: &str,
    ) -> Result<Option<CollectionId>, StorageError> {
        if let Some(id) = self.typed.get(&ty.id) {
            return Ok(Some(*id));
        }
        self.claim(ty, name)?;
        let Some(id) = self.lookup(name).await? else {
            return Ok(None);
        };
        self.check_recorded_type(ty, name).await?;
        self.typed.insert(ty.id, id);
        Ok(Some(id))
    }

    /// Claims `name` (in this namespace, over this storage) for `ty`, or refuses if a
    /// different type holds it.
    fn claim(&self, ty: CollectionType, name: &str) -> Result<(), StorageError> {
        let entry = self
            .types
            .entry((self.ns.clone(), name.to_string()))
            .or_insert((ty.id, ty.name));
        let (held_id, held_name) = *entry;
        if held_id == ty.id {
            return Ok(());
        }
        Err(StorageError::Validation(format!(
            "types {held_name} and {} share persisted name {name}; pin one with \
             #[collection(name = \"...\")]",
            ty.name
        )))
    }

    /// Logs (never refuses) a forward entry recorded by a different type name: after a
    /// restart the registry is empty, and a type moved to another module must keep its
    /// data.
    async fn check_recorded_type(
        &self,
        ty: CollectionType,
        name: &str,
    ) -> Result<(), StorageError> {
        if let Some(entry) = self.read_forward_entry(name).await? {
            let recorded = entry.type_name;
            if !recorded.is_empty() && recorded != ty.name {
                tracing::warn!(
                    collection = name,
                    recorded = %recorded,
                    current = ty.name,
                    "collection {name} was created by type {recorded} and is now used by \
                     {}; if these are different types they share records (pin a name with \
                     #[collection(name = \"...\")])",
                    ty.name
                );
            }
        }
        Ok(())
    }

    /// The id for `name`, allocating it (recording `type_name` and `encoding`) on first use.
    async fn allocate(
        &self,
        name: &str,
        type_name: &str,
        encoding: ValueEncoding,
    ) -> Result<CollectionId, StorageError> {
        Self::validate_name(name)?;
        if let Some(id) = self.cache.get(name) {
            return Ok(*id);
        }
        if let Some(id) = self.read_forward(name).await? {
            self.remember(name, id);
            return Ok(id);
        }

        let _guard = self.lock.lock().await;

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
        let forward = [
            &id.0.to_be_bytes()[..],
            &[encoding as u8],
            type_name.as_bytes(),
        ]
        .concat();
        self.storage.put(&self.forward_key(name)?, &forward).await?;
        self.remember(name, id);
        Ok(id)
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

    /// The Rust type name recorded when `name` was allocated (empty if a name-based API
    /// allocated it); `None` if it never was.
    pub async fn recorded_type(&self, name: &str) -> Result<Option<String>, StorageError> {
        Ok(self.recorded(name).await?.map(|entry| entry.type_name))
    }

    /// Everything the forward entry records about `name`; `None` if it was never
    /// allocated. Cached after the first read: an entry never changes once written.
    pub async fn recorded(&self, name: &str) -> Result<Option<CollectionRecord>, StorageError> {
        if Self::validate_name(name).is_err() {
            return Ok(None);
        }
        if let Some(entry) = self.records.get(name) {
            return Ok(Some(entry.clone()));
        }
        self.read_forward_entry(name).await
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
            let id = decode_forward(&value)?.id;
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
        Ok(self.read_forward_entry(name).await?.map(|entry| entry.id))
    }

    async fn read_forward_entry(
        &self,
        name: &str,
    ) -> Result<Option<CollectionRecord>, StorageError> {
        match self.storage.get(&self.forward_key(name)?).await? {
            Some(bytes) => {
                let entry = decode_forward(&bytes)?;
                self.records.insert(name.to_string(), entry.clone());
                Ok(Some(entry))
            }
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

/// A forward entry: the id, the value-encoding byte, then the allocating type's name
/// (possibly empty).
fn decode_forward(bytes: &[u8]) -> Result<CollectionRecord, StorageError> {
    if bytes.len() < 5 {
        return Err(StorageError::Corruption(format!(
            "catalog entry catalog/name/ holds {} bytes, expected at least 5",
            bytes.len()
        )));
    }
    let (id, rest) = bytes.split_at(4);
    let type_name = String::from_utf8(rest[1..].to_vec()).map_err(|_| {
        StorageError::Corruption("catalog/name/ type name is not UTF-8".to_string())
    })?;
    Ok(CollectionRecord {
        id: CollectionId(decode_u32(id, "catalog/name/")?),
        encoding: ValueEncoding::from_byte(rest[0])?,
        type_name,
    })
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
