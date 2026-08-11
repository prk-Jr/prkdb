//! Distributed writes across a real multi-node Raft cluster.
//!
//! # These assertions used to be vacuous
//!
//! `test_raft_leader_election` computed `nodeN_is_leader` as
//! `nodeN.get_leader().await.is_some()` and asserted that at least one was true.
//! `get_leader()` returns `Some(local_id)` when the node leads **and the known leader's id
//! otherwise**, so it is `Some` on every follower as well. The assertion therefore held as
//! soon as any node learned of any leader — and would have held against an implementation
//! that elected three leaders at once.
//!
//! `test_raft_propose` was weaker still: it matched on `propose()` and printed the error on
//! failure without failing the test, so a cluster that accepted nothing still passed.
//!
//! Both now use [`InProcessCluster`], which asks each node whether *it* leads and pairs the
//! answer with the node's term.

mod helpers;

use helpers::in_process_cluster::InProcessCluster;
use std::time::Duration;

/// Exactly one leader, in exactly one term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_three_node_cluster_elects_exactly_one_leader() {
    let cluster = InProcessCluster::new(3).await.expect("cluster starts");

    let leader = cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("a leader is elected");

    let leaders = cluster.leaders_in_current_term().await;
    assert_eq!(
        leaders.len(),
        1,
        "expected exactly one leader in the current term, found {leaders:?}"
    );
    assert_eq!(
        leaders[0], leader,
        "the node reporting itself leader must be the one await_leader found"
    );
}

/// A proposal commits and its effect is visible on every node.
///
/// The previous version treated a failed `propose` as acceptable and printed it. Commit is
/// the property that matters: a proposal that is appended locally and never replicated is
/// indistinguishable from a lost write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_proposal_commits_and_replicates() {
    let cluster = InProcessCluster::new(3).await.expect("cluster starts");
    cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("a leader is elected");

    cluster
        .put(b"users:proposed", b"value")
        .await
        .expect("the leader must accept and commit a proposal");

    // Commit acknowledges a majority; the last node applies shortly after.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if cluster.all_nodes_have(b"users:proposed", b"value").await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("a committed proposal did not reach every node within 10s");
}

/// Writes to different keys all commit and are all visible. Guards against a routing bug
/// that would silently drop everything but the first key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_keys_all_commit() {
    let cluster = InProcessCluster::new(3).await.expect("cluster starts");
    cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("a leader is elected");

    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..10)
        .map(|i| {
            (
                format!("users:k{i}").into_bytes(),
                format!("v{i}").into_bytes(),
            )
        })
        .collect();

    for (key, value) in &pairs {
        cluster
            .put(key, value)
            .await
            .unwrap_or_else(|e| panic!("committing {}: {e}", String::from_utf8_lossy(key)));
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut all = true;
        for (key, value) in &pairs {
            if !cluster.all_nodes_have(key, value).await {
                all = false;
                break;
            }
        }
        if all {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "not every committed key reached every node within 15s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
