//! Election Safety: **at most one leader can be elected in a given term.**
//!
//! This is the first of the five safety properties in the Raft paper (§5.2, Figure 3) and
//! the one the rest depend on. Violating it means two nodes can both commit in the same
//! term, and every guarantee above it collapses.
//!
//! # Why the property is stated per term
//!
//! "Only one node thinks it is the leader" is **not** the invariant, and asserting it
//! produces a flaky test that is wrong about what it is testing. During a partition the
//! isolated old leader keeps believing it leads at term N while the majority elects a new
//! leader at term N+1. Two leaders exist simultaneously and Raft is behaving correctly:
//! the old one cannot commit anything, because it cannot reach a majority.
//!
//! What must never happen is two leaders **in the same term**.
//!
//! # Sampling, and what it can and cannot show
//!
//! These tests poll each node for `(term, state)` while the cluster is disrupted. Sampling
//! cannot prove a violation never occurred — a violation lasting less than the polling
//! interval is invisible. It is a monitor, not a proof, and is written to say so rather
//! than to imply a stronger guarantee than it delivers.
//!
//! Two things make it worth having anyway: the sampler runs across exactly the events that
//! provoke double elections (partition, heal, leader loss), and a violation of election
//! safety is not a momentary glitch — a second leader in the same term persists until it
//! learns of a higher term, which is many polling intervals.

mod helpers;

use helpers::in_process_cluster::InProcessCluster;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Every `(term, leader)` pair observed, so a violation can name the term and both nodes
/// rather than only reporting that one occurred.
#[derive(Default)]
struct Observations {
    leaders_by_term: HashMap<u64, Vec<u64>>,
    samples: u64,
}

impl Observations {
    fn record(&mut self, leaders: Vec<(u64, u64)>) {
        self.samples += 1;
        for (node, term) in leaders {
            let entry = self.leaders_by_term.entry(term).or_default();
            if !entry.contains(&node) {
                entry.push(node);
            }
        }
    }

    /// Terms in which more than one distinct node claimed leadership.
    fn violations(&self) -> Vec<(u64, Vec<u64>)> {
        let mut out: Vec<(u64, Vec<u64>)> = self
            .leaders_by_term
            .iter()
            .filter(|(_, nodes)| nodes.len() > 1)
            .map(|(term, nodes)| (*term, nodes.clone()))
            .collect();
        out.sort_by_key(|(term, _)| *term);
        out
    }
}

/// Polls the cluster until told to stop, accumulating `(term, leader)` observations.
fn watch(
    cluster: Arc<InProcessCluster>,
    stop: Arc<AtomicBool>,
    seen: Arc<Mutex<Observations>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while !stop.load(Ordering::Relaxed) {
            let leaders = cluster.leaders_by_term().await;
            seen.lock().expect("observations mutex").record(leaders);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
}

fn assert_no_violation(seen: &Observations) {
    assert!(
        seen.samples > 0,
        "the watcher never sampled; the assertion below would be vacuous"
    );
    let violations = seen.violations();
    assert!(
        violations.is_empty(),
        "election safety violated after {} samples: {}",
        seen.samples,
        violations
            .iter()
            .map(|(term, nodes)| format!("term {term} had leaders {nodes:?}"))
            .collect::<Vec<_>>()
            .join("; ")
    );
}

/// Baseline: a quiescent cluster settles on one leader and stays there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn election_safety_holds_in_a_quiescent_cluster() {
    let cluster = Arc::new(InProcessCluster::new(3).await.expect("cluster starts"));
    cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("a leader is elected");

    let stop = Arc::new(AtomicBool::new(false));
    let seen = Arc::new(Mutex::new(Observations::default()));
    let watcher = watch(cluster.clone(), stop.clone(), seen.clone());

    tokio::time::sleep(Duration::from_secs(2)).await;

    stop.store(true, Ordering::Relaxed);
    watcher.await.expect("watcher joins");
    assert_no_violation(&seen.lock().expect("observations mutex"));
}

/// Election safety survives losing the leader outright.
///
/// Stopping the leader forces a new election under time pressure, which is where a
/// split-vote bug would show up as two nodes winning the same term.
///
/// Requires `chaos`: `stop_node` simulates a crash by cutting the node off in both
/// directions, and the fault injection that does so is compiled only under that feature.
#[cfg(feature = "chaos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn election_safety_holds_when_the_leader_is_lost() {
    let cluster = Arc::new(InProcessCluster::new(3).await.expect("cluster starts"));
    let first = cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("initial leader");

    let stop = Arc::new(AtomicBool::new(false));
    let seen = Arc::new(Mutex::new(Observations::default()));
    let watcher = watch(cluster.clone(), stop.clone(), seen.clone());

    cluster.stop_node(first).await;

    // The two survivors must elect among themselves.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut replacement = None;
    while tokio::time::Instant::now() < deadline {
        if let Some(id) = cluster.leader().await {
            if id != first {
                replacement = Some(id);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    stop.store(true, Ordering::Relaxed);
    watcher.await.expect("watcher joins");

    let seen = seen.lock().expect("observations mutex");
    assert!(
        replacement.is_some(),
        "no replacement leader was elected within 20s after node {first} stopped; \
         the safety assertion below would then be trivially satisfied by having no \
         leaders at all"
    );
    assert_no_violation(&seen);
}

/// The case the property exists for: a partition heals and two leaders briefly coexist.
///
/// This must **not** assert that only one leader exists at a time — that is legal and
/// expected here. It asserts the terms differ.
#[cfg(feature = "chaos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn election_safety_holds_across_a_partition_and_heal() {
    let cluster = Arc::new(InProcessCluster::new(3).await.expect("cluster starts"));
    let old_leader = cluster
        .await_leader(Duration::from_secs(15))
        .await
        .expect("initial leader");

    let stop = Arc::new(AtomicBool::new(false));
    let seen = Arc::new(Mutex::new(Observations::default()));
    let watcher = watch(cluster.clone(), stop.clone(), seen.clone());

    // Isolate the leader, so the majority is forced into a new term.
    let majority: Vec<u64> = cluster
        .node_ids()
        .iter()
        .copied()
        .filter(|id| *id != old_leader)
        .collect();
    cluster.partition(vec![old_leader], majority.clone()).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut elected = false;
    while tokio::time::Instant::now() < deadline {
        if let Some(id) = cluster.leader_among(&majority).await {
            if id != old_leader {
                elected = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        elected,
        "the majority side never elected a new leader, so the heal below exercises nothing"
    );

    cluster.heal_partitions().await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    stop.store(true, Ordering::Relaxed);
    watcher.await.expect("watcher joins");

    let seen = seen.lock().expect("observations mutex");

    // Two leaders across *different* terms is the expected shape of this run; seeing
    // none would mean the partition never split leadership and the test is not
    // measuring what it claims.
    assert!(
        seen.leaders_by_term.len() >= 2,
        "expected leadership in at least two terms after isolating the leader, saw {:?}",
        seen.leaders_by_term
    );
    assert_no_violation(&seen);
}
