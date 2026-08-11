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

// ═══════════════════════════════════════════════════════════════════════════
// The routing predicates
//
// Diff-scoped mutation on this pull request found six survivors in the code it adds — the
// check working on its author, which is what it was turned on for. Three were the routing
// predicates, which the tests above never touch because they drive the state machine
// directly:
//
//   replicates_authz -> true     every instance proposes, including embedded ones with no
//                                Raft to propose to
//   replicates_authz -> false    no instance proposes; every write is local again, which
//                                is the divergence this change removes
//   propose_authz    -> Ok(())   a proposal that proposes nothing and reports success
//
// The last is the same shape as `delete_many -> Ok(())`: an operation that does nothing
// and tells the caller it worked. On that path an operator is told a credential was
// revoked and it was revoked nowhere.
// ═══════════════════════════════════════════════════════════════════════════

/// An embedded instance has no Raft, so it must not claim to replicate.
#[tokio::test(flavor = "multi_thread")]
async fn an_embedded_instance_does_not_claim_to_replicate() {
    use prkdb::storage::InMemoryAdapter;
    use prkdb::PrkDb;

    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .build()
        .expect("an embedded database builds");

    assert!(
        !db.replicates_authz(),
        "an instance with no partition manager has no Raft to propose to; claiming \
         otherwise makes every principal write fail"
    );
}

/// A clustered instance must claim to replicate — the other direction.
///
/// Asserting only the embedded case leaves `replicates_authz -> false` alive, because the
/// mutant agrees there. Both directions are needed: forced `false`, a cluster silently
/// returns to per-node principal writes, and no other test would notice.
///
/// Constructing the instance is enough; Raft is not started, because the predicate asks
/// whether a partition manager exists, not whether it has a leader.
#[tokio::test(flavor = "multi_thread")]
async fn a_clustered_instance_claims_to_replicate() {
    use prkdb::raft::ClusterConfig;
    use prkdb::PrkDb;

    let dir = tempfile::tempdir().unwrap();
    let config = ClusterConfig {
        local_node_id: 1,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        nodes: vec![(1, "127.0.0.1:0".parse().unwrap())],
        ..Default::default()
    };

    let db = PrkDb::new_multi_raft_with_authz(
        1,
        config,
        dir.path().to_path_buf(),
        Some(PrincipalStore::new()),
    )
    .expect("a clustered instance builds");

    assert!(
        db.replicates_authz(),
        "an instance with a partition manager must route principal writes through Raft; \
         claiming otherwise silently restores per-node writes"
    );
}

/// Proposing on an instance with no Raft is an error, not a silent success.
#[tokio::test(flavor = "multi_thread")]
async fn proposing_without_a_partition_manager_is_refused() {
    use prkdb::storage::InMemoryAdapter;
    use prkdb::PrkDb;

    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .build()
        .unwrap();

    let outcome = db
        .propose_authz(Command::RevokePrincipal {
            name: "anyone".into(),
        })
        .await;

    assert!(
        outcome.is_err(),
        "propose_authz reported success with no Raft to propose to; a revoke that \
         replicated nowhere must not be reported as done"
    );
}

/// Partition 0 is the one that gets the authorization cache.
///
/// # What survived without this
///
/// `delete match arm (0, Some(store))` in `new_multi_raft_with_authz`. With that arm gone
/// no partition receives the cache, so every replicated principal change reaches storage
/// on each node and none of their `resolve` calls — the exact half-replication this whole
/// change exists to prevent, and the earlier tests could not see it because they construct
/// the state machine directly with `.with_authz(..)` rather than through the constructor.
///
/// Applying through the partition's state machine rather than proposing: whether partition
/// 0 was handed the cache is a property of construction, so requiring a live election to
/// observe it would make this slower and flakier without making it stronger.
#[tokio::test(flavor = "multi_thread")]
async fn the_constructor_hands_partition_zero_the_authorization_cache() {
    use prkdb::raft::ClusterConfig;
    use prkdb::PrkDb;

    let dir = tempfile::tempdir().unwrap();
    let config = ClusterConfig {
        local_node_id: 1,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        nodes: vec![(1, "127.0.0.1:0".parse().unwrap())],
        ..Default::default()
    };

    let store = PrincipalStore::new();
    let db =
        PrkDb::new_multi_raft_with_authz(2, config, dir.path().to_path_buf(), Some(store.clone()))
            .expect("a clustered instance builds");

    let pm = db
        .partition_manager
        .as_ref()
        .expect("a multi-raft instance has a partition manager");

    let principal = Principal::new(
        "wired",
        "wired-credential",
        vec![Grant::new("*", Permission::Read)],
    );
    let upsert = Command::UpsertPrincipal {
        name: "wired".into(),
        encoded: serde_json::to_vec(&principal).unwrap(),
    }
    .serialize();

    pm.get_state_machine(0)
        .expect("partition 0 exists")
        .apply(&upsert)
        .await
        .expect("applying through partition 0 succeeds");

    assert!(
        store.resolve("wired-credential").is_some(),
        "partition 0's state machine did not receive the authorization cache, so a \
         replicated principal reached storage and no node's resolve"
    );

    // And only partition 0: another partition applying the same command must not also
    // touch the cache, or every upsert would be applied N times.
    let store2 = PrincipalStore::new();
    let dir2 = tempfile::tempdir().unwrap();
    let config2 = ClusterConfig {
        local_node_id: 1,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        nodes: vec![(1, "127.0.0.1:0".parse().unwrap())],
        ..Default::default()
    };
    let db2 = PrkDb::new_multi_raft_with_authz(
        2,
        config2,
        dir2.path().to_path_buf(),
        Some(store2.clone()),
    )
    .unwrap();
    db2.partition_manager
        .as_ref()
        .unwrap()
        .get_state_machine(1)
        .expect("partition 1 exists")
        .apply(&upsert)
        .await
        .expect("applying through partition 1 succeeds");
    assert!(
        store2.resolve("wired-credential").is_none(),
        "a partition other than 0 updated the authorization cache; only partition 0 owns \
         the authz keyspace"
    );
}

/// The revoke warning fires when a follower did not have the principal, and not otherwise.
///
/// # Why a log line is worth a test
///
/// `delete !` on `if !store.apply_replicated_revoke(&name)` survived. Inverted, the warning
/// fires on every *successful* revoke and stays silent on the one case it exists to report:
/// a node revoking a principal it never had, which means it had diverged from the leader.
///
/// That is a diagnostic which is wrong exactly when it matters. An operator reading these
/// logs during an incident would see noise on healthy revokes and nothing on the divergent
/// one — worse than no warning, because it is trusted.
#[tokio::test(flavor = "multi_thread")]
async fn the_revoke_warning_reports_divergence_and_not_success() {
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<String>>>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() != tracing::Level::WARN {
                return;
            }
            struct V<'a>(&'a mut String);
            impl tracing::field::Visit for V<'_> {
                fn record_debug(&mut self, _: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                    self.0.push_str(&format!("{value:?} "));
                }
            }
            let mut line = String::new();
            event.record(&mut V(&mut line));
            self.0.lock().unwrap().push(line);
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let store = PrincipalStore::new();
    let (sm, _storage) = machine(dir.path(), &store);

    let capture = Capture::default();
    let warnings = capture.0.clone();
    let subscriber = tracing_subscriber::registry().with(capture);
    let _guard = tracing::subscriber::set_default(subscriber);

    // A principal this node has: revoking it is a success, and must be silent.
    sm.apply(
        &Command::UpsertPrincipal {
            name: "present".into(),
            encoded: serde_json::to_vec(&Principal::admin("present", "present-cred")).unwrap(),
        }
        .serialize(),
    )
    .await
    .unwrap();
    sm.apply(
        &Command::RevokePrincipal {
            name: "present".into(),
        }
        .serialize(),
    )
    .await
    .unwrap();

    assert!(
        warnings.lock().unwrap().is_empty(),
        "a successful revoke warned; inverted, this diagnostic is noise on every healthy \
         revoke: {:?}",
        warnings.lock().unwrap()
    );

    // A principal this node never had: divergence, and it must say so.
    sm.apply(
        &Command::RevokePrincipal {
            name: "absent".into(),
        }
        .serialize(),
    )
    .await
    .unwrap();

    let captured = warnings.lock().unwrap().join("\n");
    assert!(
        captured.contains("absent"),
        "revoking a principal this node never had did not warn; that is the one case the \
         warning exists for, because it means this node had diverged from the leader"
    );
}
