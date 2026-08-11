//! Tests for the in-process cluster harness itself.
//!
//! The harness exists so Raft-level tests stop depending on a prebuilt `prkdb-server`
//! binary — a dependency that produced `#[ignore] // Requires server binary` on the tests
//! of a consensus implementation. If any test here needs `cargo build --bin prkdb-server`
//! first, the harness has not solved the problem it was written for.
//!
//! Partition tests require the `chaos` feature, because that is what compiles the
//! fault-injection check into `RpcClientPool`. Without it the rules are written and never
//! read, so a partition test would pass while partitioning nothing. They are gated rather
//! than left to silently no-op.

mod helpers;

use helpers::in_process_cluster::{InProcessCluster, ReadConsistency};
use std::time::Duration;

/// The whole point: a working cluster with no child processes and no prebuilt binary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_process_cluster_elects_a_leader() {
    let cluster = InProcessCluster::new(3).await.expect("cluster starts");

    let leader = cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("a leader is elected");

    assert!(
        cluster.node_ids().contains(&leader),
        "the elected leader {leader} must be a node of this cluster {:?}",
        cluster.node_ids()
    );

    assert_eq!(
        cluster.leaders_in_current_term().await.len(),
        1,
        "exactly one leader in the current term"
    );
}

/// A committed write reaches every node. Without this the election test alone would pass
/// against a cluster that elects a leader and then replicates nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_write_replicates_to_every_node() {
    let cluster = InProcessCluster::new(3).await.expect("cluster starts");
    cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("a leader is elected");

    cluster
        .put(b"users:alpha", b"one")
        .await
        .expect("the leader accepts a write");

    // Commit acknowledges a majority; the remaining node applies shortly after.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if cluster.all_nodes_have(b"users:alpha", b"one").await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("a committed write did not reach every node within 10s");
}

/// Two clusters in one process must not interfere. This is the property that makes the
/// per-cluster node-id blocks worth their complexity: the shared `CHAOS_CONFIG_PATH` would
/// otherwise let one cluster's partition rules sever the other's links.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_clusters_coexist_in_one_process() {
    let a = InProcessCluster::new(3)
        .await
        .expect("first cluster starts");
    let b = InProcessCluster::new(3)
        .await
        .expect("second cluster starts");

    let leader_a = a
        .await_leader(Duration::from_secs(15))
        .await
        .expect("first cluster elects");
    let leader_b = b
        .await_leader(Duration::from_secs(15))
        .await
        .expect("second cluster elects");

    assert!(
        a.node_ids().iter().all(|id| !b.node_ids().contains(id)),
        "clusters must not share node ids: {:?} vs {:?}",
        a.node_ids(),
        b.node_ids()
    );
    assert!(a.node_ids().contains(&leader_a));
    assert!(b.node_ids().contains(&leader_b));
}

/// A stale read is served without coordination, so it must work with no leader at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_read_needs_no_leader() {
    let cluster = InProcessCluster::new(3).await.expect("cluster starts");
    cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("a leader is elected");

    cluster.put(b"users:k", b"v").await.expect("write commits");

    let value = cluster
        .get(b"users:k", ReadConsistency::Stale)
        .await
        .expect("a stale read succeeds");
    assert!(
        value.is_none() || value.as_deref() == Some(b"v".as_slice()),
        "a stale read may lag, but must not invent a value: {value:?}"
    );
}

#[cfg(feature = "chaos")]
mod partitions {
    use super::*;

    /// The majority side elects a new leader after the old one is isolated.
    ///
    /// **The old leader is deliberately placed in the minority.** Partitioning off an
    /// arbitrary node leaves the leader on the majority side about two thirds of the time,
    /// and then `leader_among(majority)` is already satisfied before any re-election
    /// happens — the test passes in milliseconds having exercised nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_partition_leaves_the_majority_with_a_leader() {
        let cluster = InProcessCluster::new(3).await.expect("cluster starts");
        let old_leader = cluster
            .await_leader(Duration::from_secs(15))
            .await
            .expect("initial leader");

        let minority = vec![old_leader];
        let majority: Vec<u64> = cluster
            .node_ids()
            .iter()
            .copied()
            .filter(|id| *id != old_leader)
            .collect();
        assert_eq!(majority.len(), 2, "a 3-node cluster minus its leader");

        cluster.partition(minority.clone(), majority.clone()).await;

        // A *new* leader must appear on the majority side, so require both that one
        // exists and that it is not the node we just isolated.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            match cluster.leader_among(&majority).await {
                Some(id) => {
                    assert_ne!(
                        id, old_leader,
                        "the isolated node cannot be the majority's leader"
                    );
                    break;
                }
                None => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "the majority side did not elect a new leader within 20s after \
                         the previous leader {old_leader} was isolated"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }

        cluster.heal_partitions().await;

        // After healing, the higher term wins and exactly one leader remains.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if cluster.leaders_in_current_term().await.len() == 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the cluster did not reconverge on a single leader within 20s"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
