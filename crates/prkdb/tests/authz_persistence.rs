//! Principals survive a restart, and credentials never reach disk in the clear.
//!
//! # What this closes
//!
//! `PrincipalStore` was an `Arc<RwLock<HashMap>>` and nothing else. Every credential
//! vanished when the process exited, so a restarted node authenticated nobody — including
//! the operator — and `PRKDB_BOOTSTRAP_TOKEN` had to stay set forever, which turns the
//! one-time bootstrap into a permanent back door.
//!
//! Principals now go through the storage adapter under the reserved `__prkdb_metadata:`
//! prefix, so they inherit the WAL, Raft replication, `take_snapshot` and `restore`
//! rather than needing a parallel mechanism for each.

use prkdb::authz::{Grant, Permission, Principal, PrincipalStore, PRINCIPAL_KEY_PREFIX};
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::sync::Arc;

fn adapter(dir: &std::path::Path) -> Arc<WalStorageAdapter> {
    Arc::new(
        WalStorageAdapter::new(WalConfig {
            log_dir: dir.to_path_buf(),
            ..WalConfig::test_config()
        })
        .expect("adapter opens"),
    )
}

/// The property the file exists for.
#[tokio::test(flavor = "multi_thread")]
async fn principals_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let storage = adapter(dir.path());
        let store = PrincipalStore::new();
        store
            .persist(
                storage.as_ref(),
                Principal::new(
                    "alice",
                    "alice-token",
                    vec![Grant::new("users", Permission::Write)],
                ),
            )
            .await
            .expect("persist");
        store
            .persist(storage.as_ref(), Principal::admin("root", "root-token"))
            .await
            .expect("persist admin");
    }

    // Reopen the data directory exactly as a restart would.
    let storage = adapter(dir.path());
    let store = PrincipalStore::new();
    let loaded = store.load(storage.as_ref()).await.expect("load");

    assert_eq!(loaded, 2, "both principals must come back");

    let alice = store
        .resolve("alice-token")
        .expect("alice's credential still authenticates after a restart");
    assert_eq!(alice.name(), "alice");
    assert!(
        alice.permits("users", Permission::Write),
        "grants must survive the round trip, not just the identity"
    );
    assert!(
        !alice.permits("orders", Permission::Write),
        "a restart must not widen a grant"
    );

    assert!(
        store
            .resolve("root-token")
            .is_some_and(|p| p.name() == "root"),
        "the admin must come back too"
    );
    assert!(
        store.resolve("not-a-token").is_none(),
        "loading must not admit credentials that were never issued"
    );
}

/// A credential must not be recoverable from storage.
///
/// Principals are written to the WAL, replicated, captured in backups and streamed by
/// `fetch_segment`. Storing the bearer token itself would make every one of those a
/// credential dump.
#[tokio::test(flavor = "multi_thread")]
async fn stored_principals_do_not_contain_the_credential() {
    let dir = tempfile::tempdir().unwrap();
    let storage = adapter(dir.path());
    let store = PrincipalStore::new();

    let secret = "super-secret-bearer-token";
    store
        .persist(storage.as_ref(), Principal::admin("root", secret))
        .await
        .expect("persist");

    let entries = storage
        .scan_prefix(PRINCIPAL_KEY_PREFIX.as_bytes())
        .await
        .expect("scan");
    assert_eq!(entries.len(), 1);

    let raw = String::from_utf8_lossy(&entries[0].1).to_string();
    assert!(
        !raw.contains(secret),
        "the stored principal contains its credential in the clear: {raw}"
    );
    // The digest is what should be there.
    assert!(
        raw.contains("credential_hash"),
        "expected a credential digest in the stored record, got: {raw}"
    );

    // And the digest still authenticates the real credential.
    assert!(store.resolve(secret).is_some());
}

/// Bootstrap mints exactly one admin and refuses afterwards, across a restart.
///
/// The second half is the part that matters: if the refusal only held within one process,
/// leaving `PRKDB_BOOTSTRAP_TOKEN` set — which every deployment does, because it lives in
/// the unit file — would re-mint an admin on every restart.
#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_creates_one_admin_then_refuses() {
    use prkdb::authz::BootstrapError;

    let dir = tempfile::tempdir().unwrap();

    {
        let storage = adapter(dir.path());
        let store = PrincipalStore::new();
        store.load(storage.as_ref()).await.expect("load empty");

        let admin = store
            .bootstrap_admin("first-token")
            .expect("first bootstrap");
        store
            .persist(storage.as_ref(), admin)
            .await
            .expect("persist");

        assert!(
            matches!(
                store.bootstrap_admin("second-token"),
                Err(BootstrapError::AlreadyInitialised { existing: 1 })
            ),
            "a second bootstrap in the same process must be refused"
        );
    }

    // Restart with the variable still set.
    let storage = adapter(dir.path());
    let store = PrincipalStore::new();
    store.load(storage.as_ref()).await.expect("load");

    assert!(
        matches!(
            store.bootstrap_admin("second-token"),
            Err(BootstrapError::AlreadyInitialised { existing: 1 })
        ),
        "bootstrap must stay refused after a restart, or the token is a permanent back door"
    );
    assert!(
        store.resolve("second-token").is_none(),
        "the refused credential must not authenticate"
    );
    assert!(
        store.resolve("first-token").is_some(),
        "the original admin must still work"
    );
}

/// Removing a principal removes it durably, not just from the cache.
#[tokio::test(flavor = "multi_thread")]
async fn forgetting_a_principal_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let storage = adapter(dir.path());
        let store = PrincipalStore::new();
        store
            .persist(storage.as_ref(), Principal::admin("root", "root-token"))
            .await
            .unwrap();
        store
            .persist(storage.as_ref(), Principal::admin("temp", "temp-token"))
            .await
            .unwrap();
        store
            .forget(storage.as_ref(), "temp")
            .await
            .expect("forget");
    }

    let storage = adapter(dir.path());
    let store = PrincipalStore::new();
    store.load(storage.as_ref()).await.expect("load");

    assert!(
        store.resolve("temp-token").is_none(),
        "a revoked credential came back after a restart"
    );
    assert!(store.resolve("root-token").is_some());
}

/// A principal record that cannot be read must stop startup rather than be skipped.
///
/// Dropping it would silently reduce someone's authority — or, for the only admin, lock
/// the operator out while reporting a clean start.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_principal_refuses_to_load() {
    let dir = tempfile::tempdir().unwrap();
    let storage = adapter(dir.path());

    storage
        .put(
            format!("{PRINCIPAL_KEY_PREFIX}broken").as_bytes(),
            b"this is not a principal",
        )
        .await
        .unwrap();

    let store = PrincipalStore::new();
    let err = store
        .load(storage.as_ref())
        .await
        .expect_err("an unreadable principal must not be skipped");
    assert!(
        err.to_string().contains("unreadable"),
        "the error should say what happened, got: {err}"
    );
}
