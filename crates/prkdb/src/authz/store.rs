//! The principal store.
//!
//! Authorization is consulted on every request, so resolution has to be an in-memory map
//! lookup rather than a read-through to storage. The store below is that cache; the
//! durable copy belongs in the Raft state machine so it survives restart and agrees
//! across the cluster (spec R12, "Where the model lives").
//!
//! # Durability
//!
//! [`PrincipalStore::load`] and [`PrincipalStore::persist`] read and write principals
//! through the storage adapter, under the reserved `__prkdb_metadata:` prefix that the
//! data plane already treats as internal. Going through the adapter rather than a side
//! file means principals inherit everything the database already guarantees: they are
//! written to the WAL, replicated by Raft when the node is clustered, captured by
//! `take_snapshot`, and restored by `restore`.
//!
//! Only the SHA-256 of a credential is stored — see [`Principal`].
//!
//! # Bootstrap
//!
//! A cold cluster has no principals and can therefore authenticate nobody — including the
//! operator. `PRKDB_BOOTSTRAP_TOKEN` creates a single admin principal on first start and
//! is refused once any principal exists, so it cannot be used to mint a second back door
//! later.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;

use super::model::{Grant, Permission, Principal};

/// Where principals live in the keyspace.
///
/// Sits under `__prkdb_metadata:`, which `parse_storage_key` already classifies as
/// internal and the HTTP collection listing already filters out — so principals do not
/// appear as user data.
pub const PRINCIPAL_KEY_PREFIX: &str = "__prkdb_metadata:authz:principal:";

fn principal_key(name: &str) -> Vec<u8> {
    format!("{PRINCIPAL_KEY_PREFIX}{name}").into_bytes()
}

/// Why bootstrapping was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BootstrapError {
    #[error(
        "refusing to bootstrap: {existing} principal(s) already exist. \
         PRKDB_BOOTSTRAP_TOKEN creates the first admin only; use an existing admin \
         credential to create further principals."
    )]
    AlreadyInitialised { existing: usize },

    #[error("refusing to bootstrap with an empty credential")]
    EmptyCredential,
}

/// In-memory principal store, cheap to clone and safe to share.
#[derive(Clone, Default)]
pub struct PrincipalStore {
    inner: Arc<RwLock<HashMap<String, Principal>>>,
}

impl PrincipalStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a store from principals already durable in the state machine.
    pub fn from_principals(principals: impl IntoIterator<Item = Principal>) -> Self {
        let map = principals
            .into_iter()
            .map(|p| (p.name().to_string(), p))
            .collect();
        Self {
            inner: Arc::new(RwLock::new(map)),
        }
    }

    /// Resolve a presented credential to a principal.
    ///
    /// Compares against every principal rather than short-circuiting on the first
    /// mismatch, so the work done does not depend on how far down the list the match sits.
    pub fn resolve(&self, credential: &str) -> Option<Principal> {
        let guard = self
            .inner
            .read()
            .expect("the principal store lock is only poisoned if a holder panicked");
        let mut found = None;
        for principal in guard.values() {
            if principal.credential_matches(credential) {
                found = Some(principal.clone());
            }
        }
        found
    }

    /// Whether a presented credential may perform `required` on `collection`.
    pub fn permits(&self, credential: &str, collection: &str, required: Permission) -> bool {
        self.resolve(credential)
            .is_some_and(|p| p.permits(collection, required))
    }

    /// Load every persisted principal, replacing whatever is cached.
    ///
    /// Called at startup. A store that is not loaded is not empty-but-harmless: it
    /// authenticates nobody, so an operator who restarted a configured node would find
    /// their credentials rejected. That was the behaviour before principals were
    /// persisted at all.
    pub async fn load<S: StorageAdapter + ?Sized>(
        &self,
        storage: &S,
    ) -> Result<usize, StorageError> {
        let entries = storage.scan_prefix(PRINCIPAL_KEY_PREFIX.as_bytes()).await?;

        let mut loaded = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            match serde_json::from_slice::<Principal>(&value) {
                Ok(principal) => loaded.push(principal),
                Err(e) => {
                    // Refuse rather than silently drop: a principal that fails to
                    // deserialize is one whose grants are now unknown, and continuing
                    // would quietly reduce someone's authority — or remove it entirely.
                    return Err(StorageError::Internal(format!(
                        "principal at key {} is unreadable ({e}); refusing to start with                          an incomplete authorization store",
                        String::from_utf8_lossy(&key)
                    )));
                }
            }
        }

        let count = loaded.len();
        self.replace_all(loaded);
        Ok(count)
    }

    /// Write one principal through the storage layer, then cache it.
    ///
    /// Storage first: if the write fails, the cache must not claim a principal exists
    /// that would vanish on the next restart.
    pub async fn persist<S: StorageAdapter + ?Sized>(
        &self,
        storage: &S,
        principal: Principal,
    ) -> Result<(), StorageError> {
        let encoded = serde_json::to_vec(&principal).map_err(|e| {
            StorageError::Serialization(format!("encoding principal {}: {e}", principal.name()))
        })?;
        storage
            .put(&principal_key(principal.name()), &encoded)
            .await?;
        self.insert(principal);
        Ok(())
    }

    /// Remove a principal from storage and cache.
    pub async fn forget<S: StorageAdapter + ?Sized>(
        &self,
        storage: &S,
        name: &str,
    ) -> Result<(), StorageError> {
        storage.delete(&principal_key(name)).await?;
        self.remove(name);
        Ok(())
    }

    pub fn insert(&self, principal: Principal) {
        self.inner
            .write()
            .expect("the principal store lock is only poisoned if a holder panicked")
            .insert(principal.name().to_string(), principal);
    }

    /// Look a principal up by name rather than by credential.
    ///
    /// Administration works in names; only authentication works in credentials.
    pub fn resolve_by_name(&self, name: &str) -> Option<Principal> {
        self.inner
            .read()
            .expect("the principal store lock is only poisoned if a holder panicked")
            .get(name)
            .cloned()
    }

    /// How many principals hold `Admin` on `*`.
    ///
    /// Used to refuse removing the last one: a cluster with no admin cannot be
    /// administered again without stopping it and editing storage by hand, and whoever
    /// does that will be doing it during an incident.
    pub fn admin_count(&self) -> usize {
        self.inner
            .read()
            .expect("the principal store lock is only poisoned if a holder panicked")
            .values()
            .filter(|p| p.permits("*", Permission::Admin))
            .count()
    }

    /// Whether the named principal holds `Admin` on `*`.
    pub fn is_admin(&self, name: &str) -> bool {
        self.resolve_by_name(name)
            .is_some_and(|p| p.permits("*", Permission::Admin))
    }

    pub fn remove(&self, name: &str) -> Option<Principal> {
        self.inner
            .write()
            .expect("the principal store lock is only poisoned if a holder panicked")
            .remove(name)
    }

    pub fn len(&self) -> usize {
        self.inner
            .read()
            .expect("the principal store lock is only poisoned if a holder panicked")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn names(&self) -> Vec<String> {
        self.inner
            .read()
            .expect("the principal store lock is only poisoned if a holder panicked")
            .keys()
            .cloned()
            .collect()
    }

    /// Replace the whole set, for applying a Raft snapshot.
    pub fn replace_all(&self, principals: impl IntoIterator<Item = Principal>) {
        let mut guard = self
            .inner
            .write()
            .expect("the principal store lock is only poisoned if a holder panicked");
        guard.clear();
        for p in principals {
            guard.insert(p.name().to_string(), p);
        }
    }

    /// Every principal, for writing a Raft snapshot.
    pub fn snapshot(&self) -> Vec<Principal> {
        self.inner
            .read()
            .expect("the principal store lock is only poisoned if a holder panicked")
            .values()
            .cloned()
            .collect()
    }

    /// Create the first admin principal. Refused once any principal exists.
    pub fn bootstrap_admin(&self, credential: &str) -> Result<Principal, BootstrapError> {
        if credential.is_empty() {
            return Err(BootstrapError::EmptyCredential);
        }
        let mut guard = self
            .inner
            .write()
            .expect("the principal store lock is only poisoned if a holder panicked");
        if !guard.is_empty() {
            return Err(BootstrapError::AlreadyInitialised {
                existing: guard.len(),
            });
        }
        let principal = Principal::new(
            "bootstrap-admin",
            credential,
            vec![Grant::new("*", Permission::Admin)],
        );
        guard.insert(principal.name().to_string(), principal.clone());
        Ok(principal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_creates_one_admin_then_refuses() {
        let store = PrincipalStore::new();
        assert!(store.is_empty());

        let admin = store.bootstrap_admin("first-secret").expect("cold store");
        assert!(admin.permits("anything", Permission::Admin));
        assert_eq!(store.len(), 1);

        // The whole point: a second call cannot mint another back door.
        assert_eq!(
            store.bootstrap_admin("second-secret"),
            Err(BootstrapError::AlreadyInitialised { existing: 1 })
        );
        assert!(store.resolve("second-secret").is_none());
    }

    #[test]
    fn refuses_an_empty_bootstrap_credential() {
        let store = PrincipalStore::new();
        assert_eq!(
            store.bootstrap_admin(""),
            Err(BootstrapError::EmptyCredential)
        );
        assert!(store.is_empty());
    }

    #[test]
    fn resolves_only_the_matching_credential() {
        let store = PrincipalStore::new();
        store.insert(Principal::new(
            "reader",
            "cred-a",
            vec![Grant::new("users", Permission::Read)],
        ));
        store.insert(Principal::new(
            "writer",
            "cred-b",
            vec![Grant::new("users", Permission::Write)],
        ));

        assert_eq!(store.resolve("cred-a").unwrap().name(), "reader");
        assert_eq!(store.resolve("cred-b").unwrap().name(), "writer");
        assert!(store.resolve("cred-c").is_none());

        assert!(store.permits("cred-b", "users", Permission::Write));
        assert!(!store.permits("cred-a", "users", Permission::Write));
        assert!(store.permits("cred-a", "users", Permission::Read));
    }

    /// An authorization store that does not survive install_snapshot is a cluster that
    /// loses its own credentials during recovery — and the only way back in is an
    /// --allow-anonymous restart. Spec section 10, implementation question 1.
    #[test]
    fn principals_round_trip_through_a_snapshot() {
        let store = PrincipalStore::new();
        store.insert(Principal::admin("root", "root-cred"));
        store.insert(Principal::new(
            "app",
            "app-cred",
            vec![Grant::new("logs/*", Permission::Write)],
        ));

        let snapshot = store.snapshot();
        let encoded = serde_json::to_vec(&snapshot).expect("principals are serializable");
        let decoded: Vec<Principal> =
            serde_json::from_slice(&encoded).expect("principals round-trip");

        let restored = PrincipalStore::new();
        restored.replace_all(decoded);

        assert_eq!(restored.len(), 2);
        assert!(restored.permits("root-cred", "anything", Permission::Admin));
        assert!(restored.permits("app-cred", "logs/app", Permission::Write));
        assert!(!restored.permits("app-cred", "users", Permission::Read));
    }

    #[test]
    fn replace_all_drops_principals_absent_from_the_snapshot() {
        let store = PrincipalStore::new();
        store.insert(Principal::admin("old", "old-cred"));

        store.replace_all(vec![Principal::admin("new", "new-cred")]);

        assert!(store.resolve("old-cred").is_none());
        assert!(store.resolve("new-cred").is_some());
    }
}
