//! Authorization changes reach every node, not just the one that served the request.
//!
//! # The gap this closes
//!
//! Principals were written with a plain `put` on `db.storage()` — the standalone `meta`
//! adapter that `db.rs` builds outside every Raft group, with a comment conceding it was
//! provisional ("or we can just use the first partition's storage? For now..."). The state
//! machine contained no mention of `principal` or `authz`.
//!
//! So on a cluster: a principal created through `PUT /admin/principals` existed only on the
//! node that served the request, and **a revoke on one node left the credential live on the
//! others**. Meanwhile `authz/store.rs` claimed principals were "replicated by Raft when the
//! node is clustered", and `principals_round_trip_through_a_snapshot` — a serde round-trip
//! over a `Vec` — read as coverage of exactly that.
//!
//! # Why the in-memory half matters as much as the durable half
//!
//! `PrincipalStore::resolve` walks an in-memory map loaded once at startup. Replicating only
//! the storage write would leave every follower authenticating from a stale map until it
//! restarted — which is a revoke that reports success and revokes nothing.
//!
//! These tests drive the state machine directly rather than through a cluster: the property
//! under test is that applying the command updates *both* copies, and a cluster test would
//! add election timing without adding evidence about that.

use prkdb::authz::{Grant, Permission, Principal, PrincipalStore};
use prkdb::raft::command::Command;
use prkdb::raft::{PrkDbStateMachine, StateMachine};
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::sync::Arc;

fn machine(
    dir: &std::path::Path,
    store: &PrincipalStore,
) -> (PrkDbStateMachine, Arc<WalStorageAdapter>) {
    let config = WalConfig {
        log_dir: dir.to_path_buf(),
        ..WalConfig::test_config()
    };
    let storage = Arc::new(WalStorageAdapter::new(config).expect("open storage"));
    (
        PrkDbStateMachine::new(storage.clone()).with_authz(store.clone()),
        storage,
    )
}

/// Applying an upsert updates the durable copy *and* the cache authentication reads.
#[tokio::test(flavor = "multi_thread")]
async fn a_replicated_upsert_reaches_storage_and_the_live_cache() {
    let dir = tempfile::tempdir().unwrap();
    let store = PrincipalStore::new();
    let (sm, storage) = machine(dir.path(), &store);

    let principal = Principal::new(
        "alice",
        "alice-credential",
        vec![Grant::new("*", Permission::Write)],
    );
    let encoded = serde_json::to_vec(&principal).unwrap();

    assert!(
        store.resolve("alice-credential").is_none(),
        "precondition: the cache must not already know this credential"
    );

    sm.apply(
        &Command::UpsertPrincipal {
            name: "alice".into(),
            encoded,
        }
        .serialize(),
    )
    .await
    .expect("applying the upsert succeeds");

    // The half that authentication actually reads.
    let resolved = store
        .resolve("alice-credential")
        .expect("a replicated principal must be resolvable on this node");
    assert_eq!(resolved.name(), "alice");
    assert!(resolved.permits("anything", Permission::Write));

    // And the half that survives a restart.
    let key = prkdb::authz::principal_key("alice");
    let stored = storage
        .get(&key)
        .await
        .unwrap()
        .expect("a replicated principal must be durable, not cache-only");
    let decoded: Principal = serde_json::from_slice(&stored).unwrap();
    assert_eq!(decoded.name(), "alice");
    assert_eq!(
        decoded.credential_hash(),
        principal.credential_hash(),
        "the durable copy must be the principal that was proposed"
    );
}

/// A revoke removes the credential from both copies. This is the one that matters:
/// reporting a revoke that did not take effect tells an operator a credential is dead
/// while it still works.
#[tokio::test(flavor = "multi_thread")]
async fn a_replicated_revoke_removes_the_credential_everywhere() {
    let dir = tempfile::tempdir().unwrap();
    let store = PrincipalStore::new();
    let (sm, storage) = machine(dir.path(), &store);

    let principal = Principal::admin("root", "root-credential");
    sm.apply(
        &Command::UpsertPrincipal {
            name: "root".into(),
            encoded: serde_json::to_vec(&principal).unwrap(),
        }
        .serialize(),
    )
    .await
    .unwrap();
    assert!(store.resolve("root-credential").is_some());

    sm.apply(
        &Command::RevokePrincipal {
            name: "root".into(),
        }
        .serialize(),
    )
    .await
    .expect("applying the revoke succeeds");

    assert!(
        store.resolve("root-credential").is_none(),
        "a revoked credential still authenticates from the in-memory cache; replicating \
         only the storage write leaves every follower admitting it until a restart"
    );
    assert_eq!(
        storage
            .get(&prkdb::authz::principal_key("root"))
            .await
            .unwrap(),
        None,
        "a revoked principal is still durable, so a restart would resurrect it"
    );
}

/// A second node applying the same log arrives at the same authorization state.
///
/// This is the cross-node property stated as the acceptance criterion: create on one,
/// authenticate on the other; revoke on one, refused on the other.
#[tokio::test(flavor = "multi_thread")]
async fn two_nodes_applying_the_same_log_agree_on_who_may_act() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let store1 = PrincipalStore::new();
    let store2 = PrincipalStore::new();
    let (node1, _s1) = machine(dir1.path(), &store1);
    let (node2, _s2) = machine(dir2.path(), &store2);

    let principal = Principal::new(
        "app",
        "app-credential",
        vec![Grant::new("logs/*", Permission::Write)],
    );
    let upsert = Command::UpsertPrincipal {
        name: "app".into(),
        encoded: serde_json::to_vec(&principal).unwrap(),
    }
    .serialize();

    // The leader applies, then the follower applies the same entry.
    node1.apply(&upsert).await.unwrap();
    node2.apply(&upsert).await.unwrap();

    for (label, store) in [("node1", &store1), ("node2", &store2)] {
        let p = store
            .resolve("app-credential")
            .unwrap_or_else(|| panic!("{label} did not learn the replicated principal"));
        assert!(p.permits("logs/today", Permission::Write));
        assert!(
            !p.permits("secrets", Permission::Write),
            "grants must replicate exactly"
        );
    }

    // Revoked on node 1, applied on node 2: refused on both.
    let revoke = Command::RevokePrincipal { name: "app".into() }.serialize();
    node1.apply(&revoke).await.unwrap();
    node2.apply(&revoke).await.unwrap();

    for (label, store) in [("node1", &store1), ("node2", &store2)] {
        assert!(
            store.resolve("app-credential").is_none(),
            "{label} still authenticates a revoked credential"
        );
    }
}

/// An unreadable principal is refused rather than half-applied.
///
/// Applying the storage write and then failing to decode would leave this node's durable
/// copy ahead of its cache — divergence from the log, which is worse than refusing.
#[tokio::test(flavor = "multi_thread")]
async fn an_undecodable_principal_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let store = PrincipalStore::new();
    let (sm, _storage) = machine(dir.path(), &store);

    let outcome = sm
        .apply(
            &Command::UpsertPrincipal {
                name: "broken".into(),
                encoded: b"not json".to_vec(),
            }
            .serialize(),
        )
        .await;

    assert!(
        outcome.is_err(),
        "a principal whose grants cannot be read must not be applied silently"
    );
    assert!(store.resolve("anything").is_none());
}
