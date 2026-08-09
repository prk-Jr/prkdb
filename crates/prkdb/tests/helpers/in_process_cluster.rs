//! A Raft cluster running inside the test process.
//!
//! # Which harness to reach for
//!
//! - **`InProcessCluster` (this file)** — Raft behaviour: elections, replication,
//!   partitions, read consistency. Needs no prebuilt binary and no `cargo build` step.
//! - **[`TestCluster`](super::test_cluster::TestCluster)** — behaviour that only exists at
//!   the binary boundary: startup, environment parsing, signal handling, graceful
//!   shutdown. It spawns real `prkdb-server` processes and is the right tool for those.
//!
//! # Why this exists
//!
//! `TestCluster` panics with *"Binary not found … Run 'cargo build --bin prkdb-server
//! --release' first"* when the binary is absent. Every Raft-level test therefore either
//! carried a build step or, more often, `#[ignore] // Requires server binary` — which is
//! how a consensus implementation ends up with its consensus tests switched off.
//!
//! `distributed_writes.rs` already proved in-process nodes work; it just built them inline
//! rather than behind a reusable type.
//!
//! # Node identifiers are unique per cluster, deliberately
//!
//! Partitioning is injected through the `chaos` feature, which reads its rules from the
//! file named by `CHAOS_CONFIG_PATH` — one path for the whole process. Test binaries run
//! their tests concurrently, so two clusters both numbering their nodes 1..3 would read
//! each other's partition rules and fail in ways that look like Raft bugs.
//!
//! Each cluster therefore takes a disjoint block of node ids from a process-wide counter,
//! and all clusters share one rules file that each filters by its own ids. `RpcClientPool`
//! matches rules against its own `local_node_id`, so the sharing is safe.

#![allow(dead_code)]

use prkdb::raft::command::Command;
use prkdb::raft::{
    ClusterConfig, PartitionManager, PrkDbStateMachine, RaftNode, RaftState, RpcClientPool,
};
use prkdb_types::storage::StorageAdapter;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tempfile::TempDir;

/// How a read is served. Mirrors the three modes the database actually offers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadConsistency {
    /// Served by the leader, reflecting every acknowledged write.
    Linearizable,
    /// Served from local state with no coordination. May be arbitrarily stale.
    Stale,
    /// Served locally after a ReadIndex round-trip confirms the leader's commit index.
    Follower,
}

// ── Chaos rule plumbing ──────────────────────────────────────────────────────

/// One shared rules file for the whole process, because `CHAOS_CONFIG_PATH` is one
/// environment variable. Clusters coexist in it by owning disjoint node ids.
struct ChaosControl {
    _dir: TempDir,
    path: std::path::PathBuf,
    rules: Mutex<HashMap<u64, Vec<serde_json::Value>>>,
}

static CHAOS: OnceLock<ChaosControl> = OnceLock::new();
static NEXT_ID_BLOCK: AtomicU64 = AtomicU64::new(0);

fn chaos() -> &'static ChaosControl {
    CHAOS.get_or_init(|| {
        let dir = TempDir::new().expect("tempdir for chaos rules");
        let path = dir.path().join("chaos.json");
        std::fs::write(&path, "[]").expect("seed the chaos rules file");
        // Safe here: set once, before any cluster starts issuing RPCs.
        std::env::set_var("CHAOS_CONFIG_PATH", &path);
        ChaosControl {
            _dir: dir,
            path,
            rules: Mutex::new(HashMap::new()),
        }
    })
}

impl ChaosControl {
    /// Replace the rules owned by one cluster, leaving other clusters' rules alone.
    fn replace_for(&self, cluster: u64, rules: Vec<serde_json::Value>) {
        let mut guard = self.rules.lock().expect("chaos rules mutex");
        if rules.is_empty() {
            guard.remove(&cluster);
        } else {
            guard.insert(cluster, rules);
        }
        let flat: Vec<&serde_json::Value> = guard.values().flatten().collect();
        let json = serde_json::to_vec(&flat).expect("chaos rules serialize");
        std::fs::write(&self.path, json).expect("write chaos rules");
    }
}

// ── The harness ──────────────────────────────────────────────────────────────

struct Node {
    id: u64,
    addr: SocketAddr,
    manager: Arc<PartitionManager>,
    raft: Arc<RaftNode>,
    /// Held so the data directory outlives the node.
    _dir: TempDir,
    /// Aborting this closes the node's gRPC listener. That alone stops only inbound
    /// traffic — see `stop_node` for why that is not enough to simulate a crash.
    server: tokio::task::JoinHandle<()>,
}

pub struct InProcessCluster {
    cluster_id: u64,
    nodes: Mutex<HashMap<u64, Node>>,
    /// Every id in the cluster, including stopped ones, in ascending order.
    ids: Vec<u64>,
    peers: Vec<(u64, SocketAddr)>,
}

impl InProcessCluster {
    /// Start an `n`-node cluster. Ports are OS-assigned; ids come from a private block.
    pub async fn new(n: usize) -> anyhow::Result<Self> {
        assert!(n > 0, "a cluster needs at least one node");

        let cluster_id = NEXT_ID_BLOCK.fetch_add(1, Ordering::SeqCst);
        let base = cluster_id * 1_000 + 1;
        let ids: Vec<u64> = (0..n as u64).map(|i| base + i).collect();

        let ports = super::free_ports(n).await;
        let peers: Vec<(u64, SocketAddr)> = ids
            .iter()
            .zip(&ports)
            .map(|(id, port)| {
                (
                    *id,
                    format!("127.0.0.1:{port}")
                        .parse()
                        .expect("loopback address with a valid port"),
                )
            })
            .collect();

        let mut nodes = HashMap::new();
        for (id, addr) in &peers {
            nodes.insert(*id, Self::spawn_node(*id, *addr, peers.clone())?);
        }

        Ok(Self {
            cluster_id,
            nodes: Mutex::new(nodes),
            ids,
            peers,
        })
    }

    fn spawn_node(
        id: u64,
        addr: SocketAddr,
        peers: Vec<(u64, SocketAddr)>,
    ) -> anyhow::Result<Node> {
        let dir = TempDir::new()?;
        let config = ClusterConfig {
            local_node_id: id,
            listen_addr: addr,
            nodes: peers,
            // Short timeouts keep tests quick. The spread must stay wide enough that
            // candidates do not repeatedly split the vote.
            election_timeout_min_ms: 200,
            election_timeout_max_ms: 400,
            heartbeat_interval_ms: 50,
            partition_id: 0,
        };

        let manager = Arc::new(
            PartitionManager::new(1, config, dir.path().to_path_buf(), |_part, storage| {
                Arc::new(PrkDbStateMachine::new(storage))
            })
            .map_err(|e| anyhow::anyhow!("building partition manager for node {id}: {e}"))?,
        );

        manager.start_all(Arc::new(RpcClientPool::new(id)), &[]);

        let serving = manager.clone();
        let server = tokio::spawn(async move {
            let _ = prkdb::raft::server::start_raft_server(serving, addr).await;
        });

        let raft = manager
            .get_partition(0)
            .ok_or_else(|| anyhow::anyhow!("partition 0 missing on node {id}"))?;

        Ok(Node {
            id,
            addr,
            manager,
            raft,
            _dir: dir,
            server,
        })
    }

    /// Node ids in this cluster.
    pub fn node_ids(&self) -> &[u64] {
        &self.ids
    }

    fn raft_of(&self, id: u64) -> Option<Arc<RaftNode>> {
        self.nodes
            .lock()
            .expect("nodes mutex")
            .get(&id)
            .map(|n| n.raft.clone())
    }

    fn running(&self) -> Vec<(u64, Arc<RaftNode>)> {
        let guard = self.nodes.lock().expect("nodes mutex");
        let mut out: Vec<(u64, Arc<RaftNode>)> =
            guard.iter().map(|(id, n)| (*id, n.raft.clone())).collect();
        out.sort_by_key(|(id, _)| *id);
        out
    }

    /// The id of a node that currently believes it is the leader, if any.
    ///
    /// Asks each node whether *it* leads rather than who it thinks leads: a follower's
    /// `leader_id` can name a node that has already been deposed.
    pub async fn leader(&self) -> Option<u64> {
        for (id, raft) in self.running() {
            if raft.get_state().await == RaftState::Leader {
                return Some(id);
            }
        }
        None
    }

    /// As [`leader`](Self::leader), restricted to `ids`. Used to assert that the majority
    /// side of a partition, specifically, has a leader.
    pub async fn leader_among(&self, ids: &[u64]) -> Option<u64> {
        for (id, raft) in self.running() {
            if ids.contains(&id) && raft.get_state().await == RaftState::Leader {
                return Some(id);
            }
        }
        None
    }

    /// Leaders in the highest term any node currently reports.
    ///
    /// Election safety is a per-term property: two nodes both claiming leadership is
    /// legal while a partition heals, provided their terms differ. Comparing states
    /// without terms therefore produces an assertion that is either wrong or vacuous.
    pub async fn leaders_in_current_term(&self) -> Vec<u64> {
        let mut observations = Vec::new();
        for (id, raft) in self.running() {
            observations.push((id, raft.current_term().await, raft.get_state().await));
        }

        let Some(max_term) = observations.iter().map(|(_, t, _)| *t).max() else {
            return Vec::new();
        };

        observations
            .into_iter()
            .filter(|(_, term, state)| *term == max_term && *state == RaftState::Leader)
            .map(|(id, _, _)| id)
            .collect()
    }

    /// Every `(node, term)` pair where that node believes it leads, across all terms.
    pub async fn leaders_by_term(&self) -> Vec<(u64, u64)> {
        let mut out = Vec::new();
        for (id, raft) in self.running() {
            if raft.get_state().await == RaftState::Leader {
                out.push((id, raft.current_term().await));
            }
        }
        out
    }

    /// Sever traffic between the two groups in both directions.
    ///
    /// Requires the `chaos` feature; without it `RpcClientPool` never consults the rules
    /// and this call has no effect, so tests using it must be gated on that feature.
    pub async fn partition(&self, group1: Vec<u64>, group2: Vec<u64>) {
        self.isolate(&group1, &group2).await;
    }

    /// Remove only this cluster's rules.
    pub async fn heal_partitions(&self) {
        chaos().replace_for(self.cluster_id, Vec::new());
    }

    /// Take a node out of the cluster, as a crash would.
    ///
    /// # This cuts the network rather than halting the node
    ///
    /// Aborting the node's gRPC server closes its listener, which stops *inbound* RPCs
    /// only. `RaftNode::start` spawns its election and heartbeat loops detached, with no
    /// handle and no shutdown channel, so those keep running — a "stopped" leader goes on
    /// heartbeating its peers successfully and never loses leadership. The first version
    /// of this method did exactly that, and the survivors sat as followers of a node the
    /// test believed was gone.
    ///
    /// So the node is also isolated in both directions through the chaos rules. From
    /// every other node's perspective that is indistinguishable from a crash, which is
    /// what these tests need.
    ///
    /// **Requires the `chaos` feature.** Without it only the listener closes, and the
    /// node keeps leading. Tests that stop a node must be gated accordingly.
    pub async fn stop_node(&self, id: u64) {
        let others: Vec<u64> = self.ids.iter().copied().filter(|n| *n != id).collect();
        self.isolate(&[id], &others).await;

        if let Some(node) = self.nodes.lock().expect("nodes mutex").remove(&id) {
            node.server.abort();
        }
    }

    /// Add partition rules between two sets without discarding this cluster's existing
    /// rules, so a stop and a partition can be in force at once.
    async fn isolate(&self, group1: &[u64], group2: &[u64]) {
        let mut rules = self.current_rules();
        for a in group1 {
            for b in group2 {
                let rule = serde_json::json!({ "Partition": { "node1": a, "node2": b } });
                if !rules.contains(&rule) {
                    rules.push(rule);
                }
            }
        }
        chaos().replace_for(self.cluster_id, rules);
    }

    fn current_rules(&self) -> Vec<serde_json::Value> {
        chaos()
            .rules
            .lock()
            .expect("chaos rules mutex")
            .get(&self.cluster_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Bring a stopped node back on its original address.
    pub async fn restart_node(&self, id: u64) -> anyhow::Result<()> {
        let addr = self
            .peers
            .iter()
            .find(|(peer, _)| *peer == id)
            .map(|(_, addr)| *addr)
            .ok_or_else(|| anyhow::anyhow!("node {id} is not part of this cluster"))?;

        let node = Self::spawn_node(id, addr, self.peers.clone())?;
        self.nodes.lock().expect("nodes mutex").insert(id, node);
        Ok(())
    }

    /// Propose a write through the current leader and wait for it to commit.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        let leader_id = self
            .leader()
            .await
            .ok_or_else(|| anyhow::anyhow!("no leader to accept a write"))?;
        let raft = self
            .raft_of(leader_id)
            .ok_or_else(|| anyhow::anyhow!("leader {leader_id} vanished"))?;

        let command = Command::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        raft.propose(command.serialize())
            .await
            .map_err(|e| anyhow::anyhow!("propose failed: {e}"))?
            .wait_commit()
            .await
            .map_err(|e| anyhow::anyhow!("commit failed: {e}"))?;
        Ok(())
    }

    /// Read a key under the requested consistency.
    pub async fn get(
        &self,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        match consistency {
            ReadConsistency::Linearizable => {
                let leader_id = self
                    .leader()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("no leader to serve a linearizable read"))?;
                // A ReadIndex round-trip confirms this node still leads and that its
                // commit index is applied; without it the "leader" may be deposed and
                // serving stale state, which is precisely what R14 asks us to prove.
                let raft = self
                    .raft_of(leader_id)
                    .ok_or_else(|| anyhow::anyhow!("leader {leader_id} vanished"))?;
                let index = raft
                    .read_index()
                    .await
                    .map_err(|e| anyhow::anyhow!("read_index failed: {e}"))?;
                raft.wait_for_apply(index)
                    .await
                    .map_err(|e| anyhow::anyhow!("wait_for_apply failed: {e}"))?;
                self.read_local(leader_id, key).await
            }
            ReadConsistency::Stale => {
                let (id, _) = self
                    .running()
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("no running node"))?;
                self.read_local(id, key).await
            }
            ReadConsistency::Follower => {
                let leader = self.leader().await;
                let follower = self
                    .running()
                    .into_iter()
                    .map(|(id, _)| id)
                    .find(|id| Some(*id) != leader)
                    .ok_or_else(|| anyhow::anyhow!("no follower available"))?;

                // ReadIndex from the leader, then read locally once applied.
                if let Some(leader_id) = leader {
                    let leader_raft = self
                        .raft_of(leader_id)
                        .ok_or_else(|| anyhow::anyhow!("leader {leader_id} vanished"))?;
                    let index = leader_raft
                        .read_index()
                        .await
                        .map_err(|e| anyhow::anyhow!("read_index failed: {e}"))?;
                    if let Some(raft) = self.raft_of(follower) {
                        raft.wait_for_apply(index)
                            .await
                            .map_err(|e| anyhow::anyhow!("follower apply failed: {e}"))?;
                    }
                }
                self.read_local(follower, key).await
            }
        }
    }

    /// Read straight from one node's storage, with no coordination at all.
    pub async fn read_local(&self, id: u64, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let storage = {
            let guard = self.nodes.lock().expect("nodes mutex");
            guard
                .get(&id)
                .and_then(|n| n.manager.get_partition_storage(0))
                .ok_or_else(|| anyhow::anyhow!("node {id} has no storage for partition 0"))?
        };
        storage
            .get(key)
            .await
            .map_err(|e| anyhow::anyhow!("local read on node {id}: {e}"))
    }

    /// Whether every running node has applied `key = value`.
    pub async fn all_nodes_have(&self, key: &[u8], value: &[u8]) -> bool {
        for (id, _) in self.running() {
            match self.read_local(id, key).await {
                Ok(Some(found)) if found == value => {}
                _ => return false,
            }
        }
        true
    }

    /// Block until a leader exists, or fail with a message naming what was waited for.
    pub async fn await_leader(&self, within: Duration) -> anyhow::Result<u64> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if let Some(id) = self.leader().await {
                return Ok(id);
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("no leader elected within {within:?}");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

impl Drop for InProcessCluster {
    fn drop(&mut self) {
        // Leave no rules behind for the next cluster that reuses this process.
        chaos().replace_for(self.cluster_id, Vec::new());
        if let Ok(guard) = self.nodes.lock() {
            for node in guard.values() {
                node.server.abort();
            }
        }
    }
}
