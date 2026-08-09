//! The read consistency modes PrkDB ships as public API actually differ.
//!
//! `ReadConsistency::Linearizable` is offered to users through the Rust client
//! (`prkdb-client/src/client.rs`), the CLI (`prkdb-cli/src/commands/data.rs`), and the
//! gRPC `ReadMode` enum. It is a correctness guarantee, and before this file the only
//! tests that touched `ReadMode::Linearizable` were inside `#[ignore]`d tests — so the
//! guarantee had no enforced coverage at all.
//!
//! # The two halves, and why both are needed
//!
//! 1. **Linearizable reads observe every acknowledged write.** Checked with the Wing & Gong
//!    search from `helpers::wgl`, not by eyeballing values.
//! 2. **Stale reads are allowed to lag, and demonstrably do.** This half is what makes the
//!    first meaningful. A "linearizable" mode that is merely the same code path as the
//!    stale one passes any test that only checks the strong mode. Nothing here would
//!    detect that, so the weaker mode is required to actually produce a stale read.
//!
//! The second assertion is deliberately shaped as "the modes differ" rather than "stale
//! reads are always stale", which would be false — a stale read usually returns current
//! data. The difference is only observable when a node is cut off, which is why the
//! partition is not incidental to these tests.

mod helpers;

use helpers::in_process_cluster::{InProcessCluster, ReadConsistency};
use helpers::jepsen_checker::{
    LinearizabilityResult, OpKind, OpResult, Operation, OperationHistory,
};
use std::time::{Duration, Instant};

fn record_write(history: &OperationHistory, key: &[u8], value: &[u8], start: Instant, ok: bool) {
    history.record(Operation {
        kind: OpKind::Write,
        key: key.to_vec(),
        write_value: Some(value.to_vec()),
        read_value: None,
        start_time: start,
        end_time: Instant::now(),
        result: if ok {
            OpResult::Ok(None)
        } else {
            OpResult::Err("write failed".into())
        },
        client_id: 1,
    });
}

fn record_read(history: &OperationHistory, key: &[u8], value: Option<Vec<u8>>, start: Instant) {
    history.record(Operation {
        kind: OpKind::Read,
        key: key.to_vec(),
        write_value: None,
        read_value: value.clone(),
        start_time: start,
        end_time: Instant::now(),
        result: OpResult::Ok(value),
        client_id: 2,
    });
}

/// Acceptance 1: a history of linearizable reads interleaved with writes passes the
/// checker.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn linearizable_reads_produce_a_linearizable_history() {
    let cluster = InProcessCluster::new(3).await.expect("cluster starts");
    cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("a leader is elected");

    let history = OperationHistory::new();
    let key = b"users:register";

    // Kept short on purpose: the WGL search is exponential in the worst case and refuses
    // histories above MAX_CHECKABLE_OPS rather than guessing.
    for i in 0..8u32 {
        let value = format!("v{i}").into_bytes();

        let start = Instant::now();
        let wrote = cluster.put(key, &value).await;
        record_write(&history, key, &value, start, wrote.is_ok());
        wrote.expect("the leader accepts a write");

        let start = Instant::now();
        let read = cluster
            .get(key, ReadConsistency::Linearizable)
            .await
            .expect("a linearizable read succeeds");
        record_read(&history, key, read, start);
    }

    match history.is_linearizable() {
        LinearizabilityResult::Linearizable => {}
        LinearizabilityResult::NotLinearizable { reason } => {
            panic!("linearizable reads produced a non-linearizable history: {reason}")
        }
    }
}

/// A linearizable read must never return a value older than a write that has already been
/// acknowledged to the client. This is read-your-writes, stated directly rather than via
/// the checker, so a failure names the values involved.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_linearizable_read_never_precedes_an_acknowledged_write() {
    let cluster = InProcessCluster::new(3).await.expect("cluster starts");
    cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("a leader is elected");

    let key = b"users:counter";
    for i in 0..10u32 {
        let value = format!("{i}").into_bytes();
        cluster.put(key, &value).await.expect("write commits");

        let read = cluster
            .get(key, ReadConsistency::Linearizable)
            .await
            .expect("linearizable read");

        assert_eq!(
            read.as_deref(),
            Some(value.as_slice()),
            "after committing {:?}, a linearizable read returned {:?}",
            String::from_utf8_lossy(&value),
            read.as_deref().map(String::from_utf8_lossy)
        );
    }
}

/// Acceptance 2: the modes are not the same code path.
///
/// A follower is cut off from the rest of the cluster, then a write is committed by the
/// majority. Reading that follower's local state must return the **old** value, while a
/// linearizable read returns the new one. If both return the same thing, either the
/// partition did nothing or the two modes are identical — and the assertions distinguish
/// which, because a stale read of the *pre-write* value is required, not merely a
/// difference.
#[cfg(feature = "chaos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_stale_read_can_lag_a_linearizable_one() {
    let cluster = InProcessCluster::new(3).await.expect("cluster starts");
    let leader = cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("a leader is elected");

    let key = b"users:mode";
    cluster.put(key, b"first").await.expect("initial write");

    // Wait for the initial value to reach every node, so the follower we isolate starts
    // from a known state rather than from "not yet replicated".
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !cluster.all_nodes_have(key, b"first").await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the initial write never reached every node"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Isolate a follower. The remaining two still form a majority and can commit.
    let follower = *cluster
        .node_ids()
        .iter()
        .find(|id| **id != leader)
        .expect("a 3-node cluster has a follower");
    let rest: Vec<u64> = cluster
        .node_ids()
        .iter()
        .copied()
        .filter(|id| *id != follower)
        .collect();
    cluster.partition(vec![follower], rest).await;

    cluster
        .put(key, b"second")
        .await
        .expect("the majority commits without the isolated follower");

    // The isolated node cannot have learned of the second write.
    let stale = cluster
        .read_local(follower, key)
        .await
        .expect("a local read on the isolated node");
    assert_eq!(
        stale.as_deref(),
        Some(b"first".as_slice()),
        "an isolated node served {:?}; it cannot have seen the write the majority \
         committed while it was cut off",
        stale.as_deref().map(String::from_utf8_lossy)
    );

    // A linearizable read, served by the leader, sees the new value.
    let fresh = cluster
        .get(key, ReadConsistency::Linearizable)
        .await
        .expect("linearizable read");
    assert_eq!(
        fresh.as_deref(),
        Some(b"second".as_slice()),
        "a linearizable read must reflect the committed write"
    );

    // Stated explicitly: this is the assertion that a weaker mode which silently behaved
    // like the stronger one would fail.
    assert_ne!(
        stale, fresh,
        "the stale and linearizable modes returned the same value under a partition, so \
         nothing here distinguishes them"
    );

    cluster.heal_partitions().await;
}

/// Once healed, the previously isolated node catches up. Without this the test above
/// leaves the impression that a stale read is permanently wrong rather than temporarily
/// behind.
#[cfg(feature = "chaos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_isolated_node_catches_up_after_healing() {
    let cluster = InProcessCluster::new(3).await.expect("cluster starts");
    let leader = cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("a leader is elected");

    let key = b"users:catchup";
    let follower = *cluster
        .node_ids()
        .iter()
        .find(|id| **id != leader)
        .expect("a follower");
    let rest: Vec<u64> = cluster
        .node_ids()
        .iter()
        .copied()
        .filter(|id| *id != follower)
        .collect();

    cluster.partition(vec![follower], rest).await;
    cluster
        .put(key, b"written-while-split")
        .await
        .expect("commit");
    cluster.heal_partitions().await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if cluster
            .read_local(follower, key)
            .await
            .ok()
            .flatten()
            .as_deref()
            == Some(b"written-while-split".as_slice())
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the previously isolated node never caught up after the partition healed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
