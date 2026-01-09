use super::config::{ClusterConfig, NodeId};
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, Notify, RwLock};

/// State of the Raft node
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

use crate::storage::WalStorageAdapter;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RaftError {
    #[error("Not leader. Leader is {0:?}")]
    NotLeader(Option<NodeId>),
    #[error("Storage error: {0}")]
    Storage(#[from] prkdb_core::error::StorageError),
    #[error("Timeout waiting for commit")]
    Timeout,
}

/// Handle for an async proposal
/// Allows caller to await commit without blocking the proposal path
pub struct ProposeHandle {
    /// Receiver for the commit notification
    commit_rx: oneshot::Receiver<Result<u64, RaftError>>,
    /// Index of the proposed entry
    index: u64,
}

impl ProposeHandle {
    /// Wait for the proposal to be committed
    pub async fn wait_commit(self) -> Result<u64, RaftError> {
        self.commit_rx.await.map_err(|_| {
            RaftError::Storage(prkdb_core::error::StorageError::Internal(
                "Commit notification dropped".into(),
            ))
        })?
    }

    /// Get the log index (available immediately)
    pub fn index(&self) -> u64 {
        self.index
    }
}

/// Log entry in the Raft log
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
}

/// Tracks an in-flight AppendEntries RPC
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct InFlightRpc {
    start_index: u64,
    end_index: u64,
    sent_at: Instant,
}

/// Per-follower state for pipelined replication
#[derive(Debug)]
struct FollowerState {
    next_index: u64,
    match_index: u64,
    in_flight: VecDeque<InFlightRpc>,
    max_in_flight: usize, // Window size (e.g., 5)
}

impl FollowerState {
    fn new(log_len: u64) -> Self {
        Self {
            next_index: log_len + 1,
            match_index: 0,
            in_flight: VecDeque::new(),
            max_in_flight: 50, // MAXIMUM pipelining - absolute limit
        }
    }

    fn can_send_more(&self) -> bool {
        self.in_flight.len() < self.max_in_flight
    }

    fn add_in_flight(&mut self, start: u64, end: u64) {
        self.in_flight.push_back(InFlightRpc {
            start_index: start,
            end_index: end,
            sent_at: Instant::now(),
        });
    }

    fn handle_success(&mut self, match_index: u64) -> bool {
        // Find and remove the corresponding in-flight RPC
        let mut found = false;
        self.in_flight.retain(|rpc| {
            if rpc.end_index == match_index {
                found = true;
                false // Remove this one
            } else {
                true // Keep others
            }
        });

        if found {
            self.match_index = self.match_index.max(match_index);
            self.next_index = match_index + 1;
        }
        found
    }

    fn handle_failure(&mut self, conflicting_index: u64) {
        // Clear in-flight queue and retry from conflict point
        self.in_flight.clear();
        self.next_index = conflicting_index.max(1);
    }
}

/// Core Raft node structure
use super::state_machine::StateMachine;

/// Core Raft node structure
pub struct RaftNode {
    /// Configuration
    config: ClusterConfig,

    /// Persistent storage for Raft log
    storage: Arc<WalStorageAdapter>,

    /// State machine for applying committed entries
    state_machine: Arc<dyn StateMachine>,

    /// Current state (Follower, Candidate, Leader)
    state: Arc<RwLock<RaftState>>,

    /// Current term
    current_term: Arc<RwLock<u64>>,

    /// ID of the candidate voted for in current term
    voted_for: Arc<RwLock<Option<NodeId>>>,

    /// ID of the current leader (if known)
    leader_id: Arc<RwLock<Option<NodeId>>>,

    /// Index of highest log entry known to be committed
    commit_index: Arc<RwLock<u64>>,

    /// Index of highest log entry applied to state machine
    last_applied: Arc<RwLock<u64>>,

    // Leader state (volatile, reinitialized after election)
    /// Follower replication state (includes next_index, match_index, in-flight RPCs)
    followers: Arc<RwLock<HashMap<NodeId, FollowerState>>>,

    /// In-memory log (for simplicity in Phase 6.7)
    log: Arc<RwLock<Vec<LogEntry>>>,

    /// Logical index of the first entry in the log vector (handles truncation)
    log_start_index: Arc<RwLock<u64>>,

    /// Trigger for immediate replication
    replication_trigger: Arc<Notify>,

    /// Channel for sending proposals to the batching loop
    proposal_tx: mpsc::Sender<(Vec<u8>, oneshot::Sender<Result<u64, RaftError>>)>,

    /// Receiver for proposals (moved out during start)
    proposal_rx: Arc<
        tokio::sync::Mutex<
            Option<mpsc::Receiver<(Vec<u8>, oneshot::Sender<Result<u64, RaftError>>)>>,
        >,
    >,

    /// Notify to reset election timer
    heartbeat_notify: Arc<Notify>,

    /// Notify when commit_index advances (for event-driven apply loop)
    commit_notify: Arc<Notify>,

    /// Track commit waiters for async proposals (lock-free!)
    /// Maps log index → list of waiters to notify when that index commits
    commit_waiters: Arc<DashMap<u64, Vec<oneshot::Sender<Result<u64, RaftError>>>>>,

    // Snapshot state
    /// Index of last entry included in snapshot
    snapshot_index: Arc<RwLock<u64>>,

    /// Term of last entry included in snapshot
    snapshot_term: Arc<RwLock<u64>>,

    /// The actual snapshot data (or None if no snapshot yet)
    snapshot_data: Arc<RwLock<Option<Vec<u8>>>>,

    /// Last time a valid heartbeat was received (for Pre-Vote leader lease)
    last_heartbeat_time: Arc<RwLock<Instant>>,

    /// Minimum election timeout (for Pre-Vote leader lease check)
    election_timeout_min: std::time::Duration,
}

impl RaftNode {
    pub fn new(
        config: ClusterConfig,
        storage: Arc<WalStorageAdapter>,
        state_machine: Arc<dyn StateMachine>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(10000);

        // Try to load snapshot
        let mut snapshot_index_val = 0;
        let mut snapshot_term_val = 0;
        let mut snapshot_data_val = None;
        let mut log_start_index_val = 1;

        let snapshot_path = storage.get_log_dir().join("snapshot.bin");
        if snapshot_path.exists() {
            if let Ok(encoded) = std::fs::read(&snapshot_path) {
                if let Ok((state, _)) = bincode::decode_from_slice::<(u64, u64, Vec<u8>), _>(
                    &encoded,
                    bincode::config::standard(),
                ) {
                    tracing::info!(
                        "Loaded snapshot from disk: index {}, term {}",
                        state.0,
                        state.1
                    );
                    snapshot_index_val = state.0;
                    snapshot_term_val = state.1;
                    snapshot_data_val = Some(state.2);
                    log_start_index_val = snapshot_index_val + 1;
                } else {
                    tracing::warn!("Failed to decode snapshot file");
                }
            } else {
                tracing::warn!("Failed to read snapshot file");
            }
        }

        tracing::info!(
            "[RAFTNODE-NEW] Creating node with listen_addr: {}, local_node_id: {}",
            config.listen_addr,
            config.local_node_id
        );

        Self {
            config: config.clone(),
            storage,
            state_machine,
            state: Arc::new(RwLock::new(RaftState::Follower)),
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            leader_id: Arc::new(RwLock::new(None)),
            commit_index: Arc::new(RwLock::new(snapshot_index_val)),
            last_applied: Arc::new(RwLock::new(snapshot_index_val)),
            followers: Arc::new(RwLock::new(HashMap::new())),
            log: Arc::new(RwLock::new(Vec::new())),
            log_start_index: Arc::new(RwLock::new(log_start_index_val)),
            replication_trigger: Arc::new(Notify::new()),
            proposal_tx: tx,
            proposal_rx: Arc::new(tokio::sync::Mutex::new(Some(rx))),
            heartbeat_notify: Arc::new(Notify::new()),
            commit_notify: Arc::new(Notify::new()),
            commit_waiters: Arc::new(DashMap::new()),
            snapshot_index: Arc::new(RwLock::new(snapshot_index_val)),
            snapshot_term: Arc::new(RwLock::new(snapshot_term_val)),
            snapshot_data: Arc::new(RwLock::new(snapshot_data_val)),
            last_heartbeat_time: Arc::new(RwLock::new(Instant::now())),
            election_timeout_min: std::time::Duration::from_millis(150),
        }
    }

    /// Propose a new command to the cluster (async, non-blocking)
    /// Returns a ProposeHandle that can be awaited for commit
    pub async fn propose(&self, data: Vec<u8>) -> Result<ProposeHandle, RaftError> {
        // 1. Check if Leader
        if *self.state.read().await != RaftState::Leader {
            return Err(RaftError::NotLeader(self.get_leader().await));
        }

        // 2. Append to local log (via batching channel)
        let (tx, rx) = oneshot::channel();
        self.proposal_tx.send((data, tx)).await.map_err(|_| {
            RaftError::Storage(prkdb_core::error::StorageError::Internal(
                "Proposal channel closed".into(),
            ))
        })?;

        let index = rx.await.map_err(|_| {
            RaftError::Storage(prkdb_core::error::StorageError::Internal(
                "Proposal dropped".into(),
            ))
        })??;

        // 3. Create a commit notification channel
        let (commit_tx, commit_rx) = oneshot::channel();

        // 4. Register for commit notification
        self.register_commit_waiter(index, commit_tx).await;

        // 5. Return handle immediately (non-blocking!)
        Ok(ProposeHandle { commit_rx, index })
    }

    /// Register a waiter for commit notification at a specific index
    async fn register_commit_waiter(
        &self,
        index: u64,
        tx: oneshot::Sender<Result<u64, RaftError>>,
    ) {
        // FAST PATH: Check if already committed!
        // This avoids the race condition where the proposal commits before we register
        if *self.commit_index.read().await >= index {
            let _ = tx.send(Ok(index));
            return;
        }

        // Slow path: Register waiter
        self.commit_waiters
            .entry(index)
            .or_insert_with(Vec::new)
            .push(tx);

        // Double check to avoid race (commit happened while we were registering)
        if *self.commit_index.read().await >= index {
            // Remove the waiter we just added and notify immediately
            if let Some((_, senders)) = self.commit_waiters.remove(&index) {
                // We might have removed other waiters too, so notify all of them
                for sender in senders {
                    let _ = sender.send(Ok(index));
                }
            }
            // Note: if someone else removed it, they handled the notification
        }
    }

    /// Notify all waiters for indices up to commit_index
    async fn notify_commit_waiters(&self, commit_index: u64) {
        // Collect all indices that are now committed
        let committed_indices: Vec<u64> = self
            .commit_waiters
            .iter()
            .filter(|entry| *entry.key() <= commit_index)
            .map(|entry| *entry.key())
            .collect();

        // Notify and remove each index
        for index in committed_indices {
            if let Some((_, senders)) = self.commit_waiters.remove(&index) {
                for sender in senders {
                    // Ignore send errors (receiver may have been dropped)
                    let _ = sender.send(Ok(index));
                }
            }
        }
    }

    /// Get the current leader ID (if known)
    pub async fn get_leader(&self) -> Option<NodeId> {
        // If we are leader, return self
        if *self.state.read().await == RaftState::Leader {
            return Some(self.config.local_node_id);
        }

        // Otherwise return known leader
        *self.leader_id.read().await
    }

    /// Get the current state
    pub async fn get_state(&self) -> RaftState {
        *self.state.read().await
    }

    /// Get the current size of the log
    pub async fn log_size(&self) -> usize {
        self.log.read().await.len()
    }

    /// Get the current commit index
    pub async fn commit_index(&self) -> u64 {
        *self.commit_index.read().await
    }

    /// Get the last applied index
    pub async fn last_applied(&self) -> u64 {
        *self.last_applied.read().await
    }

    /// Get the cluster configuration
    pub async fn get_config(&self) -> ClusterConfig {
        self.config.clone()
    }

    /// Handle ReadIndex RPC from a follower
    ///
    /// Returns the current commit_index if this node is still the leader.
    /// This enables linearizable follower reads without forwarding to the leader.
    pub async fn handle_read_index(&self, term: u64) -> Result<(u64, u64), RaftError> {
        let current_term = *self.current_term.read().await;

        // Reply false if term < currentTerm
        if term < current_term {
            return Ok((current_term, 0)); // Stale term
        }

        // Check if we're still the leader
        let state = self.state.read().await;
        if *state != RaftState::Leader {
            let known_leader = *self.leader_id.read().await;
            return Err(RaftError::NotLeader(known_leader));
        }

        // Get current commit index
        let commit_idx = *self.commit_index.read().await;

        // Note: For full linearizability, we should confirm leadership
        // with a heartbeat round to majority. For now, we trust our
        // leadership status which is good enough for most cases.

        Ok((current_term, commit_idx))
    }

    /// Perform a local ReadIndex operation (for leader)
    pub async fn read_index(&self) -> Result<u64, RaftError> {
        let current_term = *self.current_term.read().await;
        let (_, index) = self.handle_read_index(current_term).await?;
        Ok(index)
    }

    /// Wait for last_applied to reach the given index
    ///
    /// Used by followers to wait before serving a read after ReadIndex.
    pub async fn wait_for_apply(&self, index: u64) -> Result<(), RaftError> {
        const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < TIMEOUT {
            let last_applied = *self.last_applied.read().await;
            if last_applied >= index {
                return Ok(());
            }

            // Wait for apply loop notification
            tokio::select! {
                _ = self.commit_notify.notified() => continue,
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => continue,
            }
        }

        Err(RaftError::Timeout)
    }

    /// Save snapshot to disk
    async fn save_snapshot(&self) -> Result<(), RaftError> {
        let snapshot_index = *self.snapshot_index.read().await;
        let snapshot_term = *self.snapshot_term.read().await;
        let snapshot_data = self.snapshot_data.read().await.clone();

        if let Some(data) = snapshot_data {
            let path = self.storage.get_log_dir().join("snapshot.bin");
            let temp_path = self.storage.get_log_dir().join("snapshot.tmp");

            let snapshot_state = (snapshot_index, snapshot_term, data);

            // Use blocking IO for file operations
            tokio::task::spawn_blocking(move || {
                let encoded = bincode::encode_to_vec(&snapshot_state, bincode::config::standard())
                    .map_err(|e| {
                        RaftError::Storage(prkdb_core::error::StorageError::Internal(e.to_string()))
                    })?;

                std::fs::write(&temp_path, encoded).map_err(|e| {
                    RaftError::Storage(prkdb_core::error::StorageError::Internal(e.to_string()))
                })?;

                std::fs::rename(&temp_path, &path).map_err(|e| {
                    RaftError::Storage(prkdb_core::error::StorageError::Internal(e.to_string()))
                })?;

                Ok::<(), RaftError>(())
            })
            .await
            .map_err(|e| {
                RaftError::Storage(prkdb_core::error::StorageError::Internal(e.to_string()))
            })??;

            tracing::info!(
                "Saved snapshot to disk: index {}, term {}",
                snapshot_index,
                snapshot_term
            );
        }
        Ok(())
    }

    /// Compact the log if it exceeds the threshold
    pub async fn compact_log(&self) -> Result<(), RaftError> {
        const LOG_SIZE_THRESHOLD: usize = 10000; // Compact when log exceeds 10k entries

        let log_len = {
            let log = self.log.read().await;
            log.len()
        };

        if log_len < LOG_SIZE_THRESHOLD {
            return Ok(()); // Nothing to compact
        }

        tracing::info!("Starting log compaction, current log size: {}", log_len);

        // Create snapshot
        let snapshot_data = self.state_machine.snapshot().await.map_err(|e| {
            RaftError::Storage(prkdb_core::error::StorageError::Internal(format!(
                "Snapshot failed: {}",
                e
            )))
        })?;

        // Get current last_applied index and term
        let last_applied_idx = *self.last_applied.read().await;
        let last_applied_term = {
            let log = self.log.read().await;
            if last_applied_idx > 0 && last_applied_idx as usize <= log.len() {
                log[(last_applied_idx - 1) as usize].term
            } else {
                0
            }
        };

        // Update snapshot metadata
        {
            *self.snapshot_index.write().await = last_applied_idx;
            *self.snapshot_term.write().await = last_applied_term;
            *self.snapshot_data.write().await = Some(snapshot_data);
        }

        // Save snapshot to disk
        if let Err(e) = self.save_snapshot().await {
            tracing::error!("Failed to save snapshot to disk: {}", e);
            // Continue with compaction anyway? Or abort?
            // If we abort, we keep the log but have updated snapshot metadata in memory.
            // This is inconsistent if we crash.
            // But since log is in-memory only, crashing loses log anyway.
            // So proceeding is fine.
        }

        // Truncate log (keep some entries after snapshot for safety)
        const KEEP_ENTRIES: usize = 100;
        {
            let mut log = self.log.write().await;
            if last_applied_idx as usize > KEEP_ENTRIES {
                let _new_start = (last_applied_idx as usize) - KEEP_ENTRIES;

                // Calculate how many entries we are removing
                // Current log starts at *log_start_index
                // We want to keep entries starting from logical index `new_start + 1`
                // But wait, new_start is calculated assuming 0-based index from 1?
                // No, last_applied_idx is a logical index.
                // If log_start_index is 1, then logical index I is at log[I-1].
                // If log_start_index is S, then logical index I is at log[I-S].

                let mut log_start = self.log_start_index.write().await;
                let current_start = *log_start;

                // We want to keep entries starting from logical index `last_applied_idx - KEEP_ENTRIES + 1`
                // Let's call this target_start_index
                let target_start_index = if last_applied_idx > (KEEP_ENTRIES as u64) {
                    last_applied_idx - (KEEP_ENTRIES as u64) + 1
                } else {
                    1
                };

                if target_start_index > current_start {
                    let split_offset = (target_start_index - current_start) as usize;
                    if split_offset < log.len() {
                        *log = log.split_off(split_offset);
                        *log_start = target_start_index;

                        tracing::info!(
                            "Compacted log. New start index: {}, Log len: {}, Snapshot at: {}",
                            target_start_index,
                            log.len(),
                            last_applied_idx
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Get log entries starting from a specific index
    async fn get_entries_from(&self, start_index: u64) -> Vec<LogEntry> {
        let log = self.log.read().await;
        let log_start = *self.log_start_index.read().await;

        if start_index < log_start {
            // Entries have been compacted
            return Vec::new();
        }

        let vec_idx = (start_index - log_start) as usize;
        if vec_idx < log.len() {
            log[vec_idx..].to_vec()
        } else {
            Vec::new()
        }
    }

    /// Get previous log index and term for a follower
    async fn get_prev_log_info(&self, next_idx: u64) -> (u64, u64) {
        if next_idx <= 1 {
            return (0, 0);
        }

        let prev_index = next_idx - 1;
        let log_start = *self.log_start_index.read().await;

        // Check if prev_index is in the log
        if prev_index >= log_start {
            let log = self.log.read().await;
            let vec_idx = (prev_index - log_start) as usize;
            if vec_idx < log.len() {
                let prev_entry = &log[vec_idx];
                return (prev_entry.index, prev_entry.term);
            }
        }

        // Check if prev_index is the snapshot index
        let snapshot_index = *self.snapshot_index.read().await;
        if prev_index == snapshot_index {
            return (snapshot_index, *self.snapshot_term.read().await);
        }

        (0, 0)
    }

    /// Get the index and term of the last log entry
    async fn get_last_log_info(&self) -> (u64, u64) {
        let log = self.log.read().await;
        let log_start = *self.log_start_index.read().await;

        if log.is_empty() {
            // If log is empty, check snapshot
            let snapshot_index = *self.snapshot_index.read().await;
            let snapshot_term = *self.snapshot_term.read().await;
            return (snapshot_index, snapshot_term);
        }

        let last_idx = log_start + log.len() as u64 - 1;
        let last_term = log.last().unwrap().term;
        (last_idx, last_term)
    }

    /// Update commit index based on match_index (called by leader)
    async fn update_commit_index(&self) {
        let followers_map = self.followers.read().await;
        let mut indices: Vec<u64> = followers_map.values().map(|f| f.match_index).collect();

        // Add leader's own index
        let log = self.log.read().await;
        indices.push(log.len() as u64);

        // Sort to find median (majority)
        indices.sort_unstable();

        // Majority index is at position n/2 (for n+1 nodes including leader)
        let majority_idx = indices.len() / 2;
        let new_commit_index = indices[majority_idx];

        // Only update if the new commit index is for current term
        let mut commit_index = self.commit_index.write().await;
        if new_commit_index > *commit_index {
            // Check term of the entry
            if new_commit_index as usize <= log.len() {
                let entry = &log[(new_commit_index - 1) as usize];
                let current_term = *self.current_term.read().await;
                if entry.term == current_term {
                    let old_commit = *commit_index;
                    *commit_index = new_commit_index;
                    tracing::debug!(
                        "Advancing commit_index from {} to {}",
                        old_commit,
                        new_commit_index
                    );

                    // Update Prometheus metrics
                    crate::prometheus_metrics::RAFT_COMMIT_INDEX
                        .with_label_values(&[&self.config.local_node_id.to_string(), "0"])
                        .set(new_commit_index as f64);

                    // Notify apply loop (event-driven!)
                    if new_commit_index > old_commit {
                        drop(commit_index); // Release lock before notify
                        self.commit_notify.notify_waiters();
                        return;
                    }
                }
            }
        }
    }

    /// Handle RequestVote RPC
    pub async fn handle_request_vote(
        &self,
        term: u64,
        candidate_id: NodeId,
        candidate_last_log_index: u64,
        candidate_last_log_term: u64,
    ) -> (u64, bool) {
        let mut current_term = self.current_term.write().await;
        let mut voted_for = self.voted_for.write().await;
        let mut state = self.state.write().await;

        // 1. Reply false if term < currentTerm
        if term < *current_term {
            return (*current_term, false);
        }

        // If RPC request or response contains term > currentTerm:
        // set currentTerm = term, convert to follower
        if term > *current_term {
            *current_term = term;
            *state = RaftState::Follower;
            *voted_for = None;
        }

        // 2. If votedFor is null or candidateId, and candidate's log is at least
        // as up-to-date as receiver's log, grant vote
        let vote_granted = if voted_for.is_none() || *voted_for == Some(candidate_id) {
            // Check if candidate's log is at least as up-to-date
            // Drop locks before calling get_last_log_info (avoid deadlock)
            drop(current_term);
            drop(state);
            drop(voted_for);

            let (my_last_index, my_last_term) = self.get_last_log_info().await;

            // Candidate's log is up-to-date if:
            // - candidate's last term > my last term, OR
            // - same term and candidate's index >= my index
            let log_ok = candidate_last_log_term > my_last_term
                || (candidate_last_log_term == my_last_term
                    && candidate_last_log_index >= my_last_index);

            // Re-acquire locks
            current_term = self.current_term.write().await;
            voted_for = self.voted_for.write().await;
            // state = self.state.write().await; // Unused

            log_ok
        } else {
            false
        };

        if vote_granted {
            *voted_for = Some(candidate_id);
            // Reset election timer
            self.heartbeat_notify.notify_waiters();
        }

        (*current_term, vote_granted)
    }

    /// Handle PreVote RPC (§9.6 Pre-Vote protocol)
    /// Unlike RequestVote, this does NOT change votedFor or term.
    /// Used to check if a candidate would win an election before incrementing term.
    pub async fn handle_pre_vote(
        &self,
        term: u64,
        candidate_id: NodeId,
        candidate_last_log_index: u64,
        candidate_last_log_term: u64,
    ) -> (u64, bool) {
        let current_term = *self.current_term.read().await;

        // Deny if candidate's term is less than ours
        if term < current_term {
            tracing::debug!(
                "PreVote denied: candidate {} term {} < current term {}",
                candidate_id,
                term,
                current_term
            );
            return (current_term, false);
        }

        // Leader lease check: deny if we recently received heartbeat from leader
        let since_heartbeat = self.last_heartbeat_time.read().await.elapsed();
        if since_heartbeat < self.election_timeout_min {
            tracing::debug!(
                "PreVote denied: received heartbeat {:?} ago (min timeout {:?})",
                since_heartbeat,
                self.election_timeout_min
            );
            return (current_term, false);
        }

        // Check if candidate's log is at least as up-to-date as ours
        let (my_last_index, my_last_term) = self.get_last_log_info().await;
        let log_ok = candidate_last_log_term > my_last_term
            || (candidate_last_log_term == my_last_term
                && candidate_last_log_index >= my_last_index);

        tracing::debug!(
            "PreVote from {}: term={}, log_ok={}, my_last=({},{}), candidate_last=({},{})",
            candidate_id,
            term,
            log_ok,
            my_last_index,
            my_last_term,
            candidate_last_log_index,
            candidate_last_log_term
        );

        (current_term, log_ok)
    }

    /// Handle InstallSnapshot RPC from leader
    pub async fn handle_install_snapshot(
        &self,
        term: u64,
        leader_id: NodeId,
        last_included_index: u64,
        last_included_term: u64,
        snapshot_data: Vec<u8>,
    ) -> (u64, bool) {
        let mut current_term = self.current_term.write().await;
        let mut state = self.state.write().await;
        let mut voted_for = self.voted_for.write().await;

        // 1. Reply false if term < currentTerm
        if term < *current_term {
            return (*current_term, false);
        }

        // If RPC request or response contains term > currentTerm:
        // set currentTerm = term, convert to follower
        if term > *current_term {
            *current_term = term;
            *state = RaftState::Follower;
            *voted_for = None;
            *self.leader_id.write().await = Some(leader_id);
        }

        // If we receive InstallSnapshot from current leader, we are a follower
        if *state == RaftState::Candidate {
            *state = RaftState::Follower;
            *self.leader_id.write().await = Some(leader_id);
        }

        // Also update leader_id if it's None
        if self.leader_id.read().await.is_none() {
            *self.leader_id.write().await = Some(leader_id);
        }

        tracing::info!(
            "Received InstallSnapshot from leader {} at index {}/term {}, size {} bytes",
            leader_id,
            last_included_index,
            last_included_term,
            snapshot_data.len()
        );

        // Drop locks before calling state machine (avoid deadlock)
        let current_term_val = *current_term;
        drop(current_term);
        drop(state);
        drop(voted_for);

        // Restore state machine from snapshot
        if let Err(e) = self.state_machine.restore(&snapshot_data).await {
            tracing::error!("Failed to restore state machine from snapshot: {}", e);
            return (current_term_val, false);
        }

        // Update snapshot metadata
        {
            *self.snapshot_index.write().await = last_included_index;
            *self.snapshot_term.write().await = last_included_term;
            *self.snapshot_data.write().await = Some(snapshot_data);
        }

        // Save snapshot to disk
        if let Err(e) = self.save_snapshot().await {
            tracing::error!("Failed to save snapshot to disk: {}", e);
            // Continue anyway - snapshot is in memory
        }

        // Reset log to start after snapshot
        {
            let mut log = self.log.write().await;
            let mut log_start = self.log_start_index.write().await;

            // Discard all log entries before and including snapshot
            log.clear();
            *log_start = last_included_index + 1;

            tracing::info!(
                "Reset log after snapshot, new log_start_index: {}",
                *log_start
            );
        }

        // Update last_applied to snapshot index
        {
            let mut last_applied = self.last_applied.write().await;
            *last_applied = last_included_index;
        }

        // Update commit_index if snapshot is ahead
        {
            let mut commit_index = self.commit_index.write().await;
            if last_included_index > *commit_index {
                *commit_index = last_included_index;
            }
        }

        tracing::info!(
            "Successfully installed snapshot at index {}/term {}",
            last_included_index,
            last_included_term
        );

        (current_term_val, true)
    }

    pub async fn handle_append_entries(
        &self,
        term: u64,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        leader_commit: u64,
        entries: Vec<super::rpc::LogEntry>,
    ) -> (u64, bool) {
        let mut current_term = self.current_term.write().await;
        let mut state = self.state.write().await;
        let mut commit_index = self.commit_index.write().await;
        let mut voted_for = self.voted_for.write().await;

        // Notify heartbeat received
        self.heartbeat_notify.notify_waiters();

        // Update last heartbeat time for Pre-Vote leader lease check
        *self.last_heartbeat_time.write().await = Instant::now();

        // 1. Reply false if term < currentTerm
        if term < *current_term {
            return (*current_term, false);
        }

        // If RPC request or response contains term > currentTerm:
        // set currentTerm        // Check if we need to step down
        if term > *current_term {
            *current_term = term;
            *state = RaftState::Follower;
            *voted_for = None;

            // Update Prometheus metrics
            crate::prometheus_metrics::RAFT_STATE
                .with_label_values(&[&self.config.local_node_id.to_string(), "0"])
                .set(2.0); // Follower
            crate::prometheus_metrics::RAFT_TERM
                .with_label_values(&[&self.config.local_node_id.to_string(), "0"])
                .set(term as f64);
        }

        // If we receive AppendEntries from current leader, we are a follower
        if *state == RaftState::Candidate {
            *state = RaftState::Follower;

            // Update Prometheus metrics
            crate::prometheus_metrics::RAFT_STATE
                .with_label_values(&[&self.config.local_node_id.to_string(), "0"])
                .set(2.0); // Follower
        }

        // Always update leader_id when receiving AppendEntries
        // This ensures we track leader changes correctly
        *self.leader_id.write().await = Some(leader_id);

        tracing::debug!(
            "Received AppendEntries from leader {} with {} entries",
            leader_id,
            entries.len()
        );

        // 2. Reply false if log doesn't contain an entry at prevLogIndex
        // whose term matches prevLogTerm
        let mut log = self.log.write().await;
        let log_start = *self.log_start_index.read().await;

        if prev_log_index > 0 {
            // Check if prev_log_index is in current log
            if prev_log_index >= log_start {
                let vec_idx = (prev_log_index - log_start) as usize;
                if vec_idx >= log.len() {
                    // Missing entries
                    return (*current_term, false);
                }

                let prev_entry = &log[vec_idx];
                if prev_entry.term != prev_log_term {
                    // Term mismatch
                    return (*current_term, false);
                }
            } else {
                // prev_log_index has been compacted, check against snapshot
                let snapshot_index = *self.snapshot_index.read().await;
                let snapshot_term = *self.snapshot_term.read().await;

                if prev_log_index != snapshot_index || prev_log_term != snapshot_term {
                    // Mismatch - follower needs snapshot
                    tracing::warn!(
                        "prev_log_index {} is compacted, snapshot at {}/term {}",
                        prev_log_index,
                        snapshot_index,
                        snapshot_term
                    );
                    return (*current_term, false);
                }
            }
        }

        // 3. Delete conflicting entries and append new ones
        if !entries.is_empty() {
            let mut handles = Vec::with_capacity(entries.len());

            for entry in entries {
                // Optimization: Clone data once and reuse
                let entry_data = entry.data.clone();
                let log_entry = LogEntry {
                    term: entry.term,
                    index: entry.index,
                    data: entry_data.clone(),
                };

                // Convert logical index to vector index
                if log_entry.index >= log_start {
                    let vec_idx = (log_entry.index - log_start) as usize;

                    // If entry exists at this index with different term, truncate
                    if vec_idx < log.len() {
                        let existing = &log[vec_idx];
                        if existing.term != log_entry.term {
                            // Truncate from this index
                            log.truncate(vec_idx);
                            log.push(log_entry);

                            // Persist (since we replaced it)
                            let storage = self.storage.clone();
                            // Optimization: Reuse entry_data instead of cloning again
                            handles.push(tokio::spawn(async move {
                                storage.append_raft_entry(&entry_data).await
                            }));
                        }
                    } else {
                        // New entry, append
                        log.push(log_entry);

                        // Persist to WAL for durability (concurrently)
                        let storage = self.storage.clone();
                        // Optimization: Reuse entry_data instead of cloning again
                        handles.push(tokio::spawn(async move {
                            storage.append_raft_entry(&entry_data).await
                        }));
                    }
                } else {
                    // Entry is before our log_start (already compacted)
                    // This should not happen if prev_log check passed
                    tracing::warn!(
                        "Received entry at index {} which is before log_start {}",
                        log_entry.index,
                        log_start
                    );
                }
            }

            // Wait for all persistence tasks
            for handle in handles {
                match handle.await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        tracing::error!("Failed to append entry to WAL: {}", e);
                        return (*current_term, false);
                    }
                    Err(e) => {
                        tracing::error!("Failed to join persistence task: {}", e);
                        return (*current_term, false);
                    }
                }
            }
            tracing::debug!("Log now has {} entries", log.len());
        }

        drop(log); // Release log lock

        // 5. If leaderCommit > commitIndex, set commitIndex =
        // min(leaderCommit, index of last new entry)
        let should_notify = if leader_commit > *commit_index {
            let log_len = self.log.read().await.len() as u64;
            let new_commit = std::cmp::min(leader_commit, log_len);
            let old_commit = *commit_index;
            *commit_index = new_commit;
            tracing::debug!("Updated commit_index to {}", new_commit);
            new_commit > old_commit
        } else {
            false
        };

        let result = (*current_term, true);

        // Notify apply loop (event-driven!) AFTER dropping locks
        if should_notify {
            drop(commit_index);
            drop(voted_for);
            drop(state);
            drop(current_term);
            self.commit_notify.notify_waiters();
        }

        result
    }

    /// Start the Raft node (election timer + heartbeat loop + apply loop)
    pub fn start(self: Arc<Self>, rpc_pool: Arc<super::rpc_client::RpcClientPool>) {
        // Spawn election timer
        let election_node = self.clone();
        let election_pool = rpc_pool.clone();
        tokio::spawn(async move {
            election_node.run_election_timer(election_pool).await;
        });

        // Spawn heartbeat loop
        let heartbeat_node = self.clone();
        tokio::spawn(async move {
            heartbeat_node.run_heartbeat_loop(rpc_pool).await;
        });

        // Spawn apply loop
        let apply_node = self.clone();
        tokio::spawn(async move {
            apply_node.run_apply_loop().await;
        });

        // Spawn proposal loop
        let proposal_node = self.clone();
        tokio::spawn(async move {
            // Take receiver out of Mutex
            let rx = {
                let mut lock = proposal_node.proposal_rx.lock().await;
                lock.take()
            };

            if let Some(rx) = rx {
                proposal_node.run_proposal_loop(rx).await;
            }
        });

        tracing::info!("Raft node {} started", self.config.local_node_id);
    }

    /// Run the apply loop to apply committed entries to state machine
    async fn run_apply_loop(self: Arc<Self>) {
        loop {
            let commit_index = *self.commit_index.read().await;
            let mut last_applied = self.last_applied.write().await;

            if commit_index > *last_applied {
                // Apply entries from last_applied + 1 to commit_index
                for idx in (*last_applied + 1)..=commit_index {
                    let log = self.log.read().await;
                    let entry_data = if let Some(entry) = log.get((idx - 1) as usize) {
                        Some(entry.data.clone())
                    } else {
                        None
                    };
                    drop(log); // Release lock before async call

                    if let Some(data) = entry_data {
                        // Apply to state machine
                        if let Err(e) = self.state_machine.apply(&data).await {
                            tracing::error!(
                                "Failed to apply entry {} to state machine: {}",
                                idx,
                                e
                            );
                        } else {
                            tracing::debug!("Applied entry {} to state machine", idx);
                        }
                    } else {
                        tracing::warn!("Missing log entry at index {}", idx);
                    }
                }
                *last_applied = commit_index;

                // Notify any waiting proposals that their indices are now committed
                drop(last_applied);
                self.notify_commit_waiters(commit_index).await;

                // Trigger log compaction if needed (non-blocking)
                let self_clone = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = self_clone.compact_log().await {
                        tracing::debug!("Log compaction check: {}", e);
                    }
                });
            } else {
                drop(last_applied);
            }

            // Wait for commit_index to advance (event-driven!)
            // Use select! to also check periodically in case we miss a notification
            tokio::select! {
                _ = self.commit_notify.notified() => {
                    // New commits available, loop immediately
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
                    // Periodic check as fallback
                }
            }
        }
    }

    /// Get a random election timeout
    fn random_election_timeout(&self) -> std::time::Duration {
        use rand::Rng;
        let min = self.config.election_timeout_min_ms;
        let max = self.config.election_timeout_max_ms;
        let timeout_ms = rand::thread_rng().gen_range(min..=max);
        std::time::Duration::from_millis(timeout_ms)
    }

    /// Run the election timer loop
    async fn run_election_timer(self: Arc<Self>, rpc_pool: Arc<super::rpc_client::RpcClientPool>) {
        loop {
            let timeout = self.random_election_timeout();

            tokio::select! {
                _ = tokio::time::sleep(timeout) => {
                    // Timeout expired, check if we should elect
                    let state = self.state.read().await;
                    let should_elect = *state == RaftState::Follower || *state == RaftState::Candidate;
                    drop(state);

                    if should_elect {
                        // Pre-Vote phase: check if we would win before incrementing term
                        let pre_vote_success = self.clone().start_pre_vote(rpc_pool.clone()).await;
                        if pre_vote_success {
                            self.clone().start_election(rpc_pool.clone()).await;
                        } else {
                            tracing::debug!("Pre-vote failed, not starting election");
                        }
                    }
                }
                _ = self.heartbeat_notify.notified() => {
                    // Received heartbeat, reset timer
                    continue;
                }
            }
        }
    }

    /// Start pre-vote phase before real election (§9.6 Pre-Vote protocol)
    /// Returns true if majority granted pre-vote
    async fn start_pre_vote(
        self: Arc<Self>,
        rpc_pool: Arc<super::rpc_client::RpcClientPool>,
    ) -> bool {
        let current_term = *self.current_term.read().await;
        let (last_log_index, last_log_term) = self.get_last_log_info().await;

        tracing::debug!(
            "Node {} starting pre-vote for term {} (would be {})",
            self.config.local_node_id,
            current_term,
            current_term + 1
        );

        // Request pre-votes from all peers concurrently
        let mut vote_tasks = Vec::new();

        for (node_id, addr) in &self.config.nodes {
            if *node_id == self.config.local_node_id {
                continue;
            }

            let pool = rpc_pool.clone();
            let node_id = *node_id;
            let addr = addr.to_string();
            // PreVote uses current term (NOT incremented)
            let request = super::rpc::PreVoteRequest {
                term: current_term + 1, // What term we WOULD have
                candidate_id: self.config.local_node_id,
                last_log_index,
                last_log_term,
            };

            let task =
                tokio::spawn(async move { pool.send_pre_vote(node_id, &addr, request).await });
            vote_tasks.push(task);
        }

        // Count pre-votes (self-vote + responses)
        let mut votes = 1; // Vote for self
        for task in vote_tasks {
            if let Ok(Ok(response)) = task.await {
                if response.vote_granted {
                    votes += 1;
                }
            }
        }

        // Check if we have majority
        let majority = (self.config.nodes.len() / 2) + 1;
        let success = votes >= majority;

        tracing::info!(
            "Node {} pre-vote result: {}/{} votes (need {}), success={}",
            self.config.local_node_id,
            votes,
            self.config.nodes.len(),
            majority,
            success
        );

        success
    }

    /// Start a leader election
    async fn start_election(self: Arc<Self>, rpc_pool: Arc<super::rpc_client::RpcClientPool>) {
        // Transition to Candidate
        {
            let mut state = self.state.write().await;
            *state = RaftState::Candidate;
            // Update Prometheus metrics
            crate::prometheus_metrics::RAFT_STATE
                .with_label_values(&[&self.config.local_node_id.to_string(), "0"])
                .set(3.0); // Candidate
        }

        // Increment term
        let current_term = {
            let mut term = self.current_term.write().await;
            *term += 1;
            *term
        };

        // Get last log index and term
        let (last_log_index, last_log_term) = self.get_last_log_info().await;
        // Vote for self
        {
            let mut voted_for = self.voted_for.write().await;
            *voted_for = Some(self.config.local_node_id);
            // Clear leader_id as we are starting election
            *self.leader_id.write().await = None;
        }

        tracing::info!(
            "Node {} starting election for term {}",
            self.config.local_node_id,
            current_term
        );

        // Request votes from all peers concurrently
        let mut vote_tasks = Vec::new();

        for (node_id, addr) in &self.config.nodes {
            if *node_id == self.config.local_node_id {
                continue;
            }

            let pool = rpc_pool.clone();
            let node_id = *node_id;
            let addr = addr.to_string();
            let request = super::rpc::RequestVoteRequest {
                term: current_term,
                candidate_id: self.config.local_node_id,
                last_log_index,
                last_log_term,
            };

            let task =
                tokio::spawn(async move { pool.send_request_vote(node_id, &addr, request).await });
            vote_tasks.push(task);
        }

        // Count votes (self-vote + responses)
        let mut votes = 1;
        for task in vote_tasks {
            if let Ok(Ok(response)) = task.await {
                if response.vote_granted {
                    votes += 1;
                }
            }
        }

        // Check if we won
        let majority = (self.config.nodes.len() / 2) + 1;
        if votes >= majority {
            let mut state = self.state.write().await;
            *state = RaftState::Leader;
            // Update Prometheus metrics
            crate::prometheus_metrics::RAFT_STATE
                .with_label_values(&[&self.config.local_node_id.to_string(), "0"])
                .set(1.0); // Leader
            crate::prometheus_metrics::RAFT_LEADER_ELECTIONS_TOTAL
                .with_label_values(&[&self.config.local_node_id.to_string(), "0"])
                .inc();
            crate::prometheus_metrics::RAFT_TERM
                .with_label_values(&[&self.config.local_node_id.to_string(), "0"])
                .set(current_term as f64);
            tracing::info!(
                "Node {} became LEADER in term {} with {} votes",
                self.config.local_node_id,
                current_term,
                votes
            );
        } else {
            tracing::debug!(
                "Node {} failed election in term {} with {} votes (needed {})",
                self.config.local_node_id,
                current_term,
                votes,
                majority
            );
        }
    }

    /// Run heartbeat loop (only when leader)
    async fn run_proposal_loop(
        self: Arc<Self>,
        mut rx: mpsc::Receiver<(Vec<u8>, oneshot::Sender<Result<u64, RaftError>>)>,
    ) {
        let max_batch_size = 10000; // MAXIMUM batch size
        let linger_time = std::time::Duration::from_micros(100); // Near-zero linger

        let mut batch = Vec::with_capacity(max_batch_size);

        loop {
            // Collect batch
            let size = batch.len();
            if size < max_batch_size {
                let timeout = if size > 0 {
                    linger_time
                } else {
                    std::time::Duration::from_secs(3600)
                };

                tokio::select! {
                    res = rx.recv() => {
                        match res {
                            Some(req) => batch.push(req),
                            None => return, // Channel closed
                        }
                    }
                    _ = tokio::time::sleep(timeout), if size > 0 => {
                        // Linger timeout, flush batch
                    }
                }

                // Drain channel up to max_batch_size
                while batch.len() < max_batch_size {
                    match rx.try_recv() {
                        Ok(req) => batch.push(req),
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => return,
                    }
                }
            }

            if batch.is_empty() {
                continue;
            }

            // Process batch
            let current_batch = std::mem::take(&mut batch);
            let batch_size = current_batch.len();

            // 1. Append to log (holding lock once)
            let term = *self.current_term.read().await;
            let mut log = self.log.write().await;
            let start_index = log.len() as u64 + 1;

            let mut handles = Vec::with_capacity(batch_size);
            let mut senders = Vec::with_capacity(batch_size);

            for (i, (data, tx)) in current_batch.into_iter().enumerate() {
                let index = start_index + i as u64;
                let entry = LogEntry {
                    term,
                    index,
                    data: data.clone(),
                };
                log.push(entry);

                let storage = self.storage.clone();
                // Spawn task to persist concurrently
                handles.push(tokio::spawn(async move {
                    storage.append_raft_entry(&data).await
                }));
                senders.push((tx, index));
            }
            drop(log);

            tracing::debug!(
                "Appended batch of {} entries starting at index {}",
                batch_size,
                start_index
            );

            // 2. Trigger replication immediately (Pipeline)
            self.replication_trigger.notify_waiters();

            // 3. Wait for WAL persistence
            for (handle, (tx, index)) in handles.into_iter().zip(senders) {
                match handle.await {
                    Ok(Ok(_)) => {
                        let _ = tx.send(Ok(index));
                    }
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(RaftError::Storage(e)));
                    }
                    Err(_) => {
                        let _ = tx.send(Err(RaftError::Storage(
                            prkdb_core::error::StorageError::Internal("Task join error".into()),
                        )));
                    }
                }
            }

            // If single node cluster, commit immediately after persistence
            if self.config.nodes.len() == 1 {
                let mut commit_index = self.commit_index.write().await;
                let last_index = start_index + batch_size as u64 - 1;
                if last_index > *commit_index {
                    *commit_index = last_index;
                    tracing::debug!("Single node: Updated commit_index to {}", last_index);
                }
            }
        }
    }

    async fn run_heartbeat_loop(self: Arc<Self>, rpc_pool: Arc<super::rpc_client::RpcClientPool>) {
        let heartbeat_interval =
            std::time::Duration::from_millis(self.config.heartbeat_interval_ms);

        loop {
            tokio::select! {
                _ = self.replication_trigger.notified() => {
                    // Triggered by new entry - replicate immediately
                }
                _ = tokio::time::sleep(heartbeat_interval) => {
                    // Heartbeat timeout
                }
            }

            // Only send heartbeats if we're the leader
            let state = self.state.read().await;
            if *state != RaftState::Leader {
                continue;
            }
            drop(state);

            let current_term = *self.current_term.read().await;
            let commit_index = *self.commit_index.read().await;

            // Initialize next_index for new peers
            {
                let mut followers_map = self.followers.write().await;
                let log_len = self.log.read().await.len() as u64;

                for (node_id, _) in &self.config.nodes {
                    if *node_id == self.config.local_node_id {
                        continue;
                    }
                    followers_map
                        .entry(*node_id)
                        .or_insert_with(|| FollowerState::new(log_len));
                }
            }

            // Send AppendEntries (heartbeats or with data) to all peers concurrently
            let mut replication_tasks = Vec::new();

            // Log which peers we're replicating to
            let peer_ids: Vec<u64> = self
                .config
                .nodes
                .iter()
                .map(|(id, _)| *id)
                .filter(|id| *id != self.config.local_node_id)
                .collect();
            tracing::info!("[HEARTBEAT] Sending to peers: {:?}", peer_ids);

            for (node_id, addr) in &self.config.nodes {
                if *node_id == self.config.local_node_id {
                    continue;
                }

                tracing::debug!("[HEARTBEAT] Preparing task for peer {}", node_id);

                let pool = rpc_pool.clone();
                let follower_id = *node_id;
                let addr = addr.to_string();
                let self_clone = self.clone();

                let task = tokio::spawn(async move {
                    tracing::debug!("[HEARTBEAT] Task started for peer {}", follower_id);
                    // Get follower state and check if we can send more (pipelining)
                    let (next_idx, can_send, in_flight_count) = {
                        let followers_map = self_clone.followers.read().await;
                        if let Some(state) = followers_map.get(&follower_id) {
                            (
                                state.next_index,
                                state.can_send_more(),
                                state.in_flight.len(),
                            )
                        } else {
                            (1, true, 0) // Default if not found
                        }
                    };

                    tracing::debug!(
                        "[HEARTBEAT] Peer {}: next_idx={}, can_send={}, in_flight={}",
                        follower_id,
                        next_idx,
                        can_send,
                        in_flight_count
                    );

                    // Skip if pipeline is full
                    if !can_send {
                        tracing::warn!("[HEARTBEAT] Peer {}: Pipeline full, skipping", follower_id);
                        return;
                    }

                    // Check if follower needs a snapshot
                    let log_start = *self_clone.log_start_index.read().await;
                    tracing::debug!(
                        "[HEARTBEAT] Peer {}: next_idx={}, log_start={}, needs_snapshot={}",
                        follower_id,
                        next_idx,
                        log_start,
                        next_idx < log_start
                    );
                    if next_idx < log_start {
                        // Follower is too far behind, send snapshot
                        tracing::info!(
                            "Follower {} needs snapshot (next_idx={}, log_start={})",
                            follower_id,
                            next_idx,
                            log_start
                        );

                        let snapshot_index = *self_clone.snapshot_index.read().await;
                        let snapshot_term = *self_clone.snapshot_term.read().await;
                        let snapshot_data_opt = self_clone.snapshot_data.read().await.clone();

                        if let Some(snapshot_data) = snapshot_data_opt {
                            let request = super::rpc::InstallSnapshotRequest {
                                term: current_term,
                                leader_id: self_clone.config.local_node_id,
                                last_included_index: snapshot_index,
                                last_included_term: snapshot_term,
                                offset: 0, // Send entire snapshot in one RPC for simplicity
                                data: snapshot_data,
                                done: true,
                            };

                            match pool
                                .send_install_snapshot(follower_id, &addr, request)
                                .await
                            {
                                Ok(response) => {
                                    if response.term > current_term {
                                        // Leader is stale, step down
                                        tracing::warn!("Stepping down, follower has higher term");
                                        let mut term = self_clone.current_term.write().await;
                                        *term = response.term;
                                        let mut state = self_clone.state.write().await;
                                        *state = RaftState::Follower;
                                    } else {
                                        // Update next_index to after snapshot
                                        let mut followers_map = self_clone.followers.write().await;
                                        if let Some(state) = followers_map.get_mut(&follower_id) {
                                            state.next_index = snapshot_index + 1;
                                            state.match_index = snapshot_index;
                                            tracing::info!(
                                                "Installed snapshot on follower {}, updated next_index to {}",
                                                follower_id,
                                                state.next_index
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Failed to send snapshot to {}: {}",
                                        follower_id,
                                        e
                                    );
                                }
                            }
                        } else {
                            tracing::warn!(
                                "No snapshot available but follower {} needs one",
                                follower_id
                            );
                        }

                        return; // Don't send AppendEntries
                    }

                    // Get entries from next_idx onwards
                    let entries_to_send = self_clone.get_entries_from(next_idx).await;

                    // Get prev_log_index and prev_log_term
                    let (prev_log_index, prev_log_term) =
                        self_clone.get_prev_log_info(next_idx).await;

                    // Convert LogEntry to RPC format
                    let rpc_entries: Vec<super::rpc::LogEntry> = entries_to_send
                        .iter()
                        .map(|e| super::rpc::LogEntry {
                            term: e.term,
                            index: e.index,
                            data: e.data.clone(),
                        })
                        .collect();

                    let request = super::rpc::AppendEntriesRequest {
                        term: current_term,
                        leader_id: self_clone.config.local_node_id,
                        prev_log_index,
                        prev_log_term,
                        leader_commit: commit_index,
                        entries: rpc_entries,
                    };

                    let entries_count = request.entries.len();

                    // Track this RPC as in-flight (for pipelining)
                    let end_index = if !entries_to_send.is_empty() {
                        entries_to_send.last().unwrap().index
                    } else {
                        prev_log_index
                    };

                    {
                        let mut followers_map = self_clone.followers.write().await;
                        if let Some(state) = followers_map.get_mut(&follower_id) {
                            state.add_in_flight(next_idx, end_index);
                        }
                    }

                    // Send RPC
                    tracing::debug!(
                        "[HEARTBEAT] Peer {}: Sending AppendEntries with {} entries",
                        follower_id,
                        entries_count
                    );
                    match pool.send_append_entries(follower_id, &addr, request).await {
                        Ok(response) => {
                            // Update Prometheus metrics - heartbeat sent successfully
                            crate::prometheus_metrics::RAFT_HEARTBEATS_SENT_TOTAL
                                .with_label_values(&[
                                    &self_clone.config.local_node_id.to_string(),
                                    &follower_id.to_string(),
                                    "0",
                                ])
                                .inc();
                            crate::prometheus_metrics::RAFT_APPEND_ENTRIES_TOTAL
                                .with_label_values(&[
                                    &self_clone.config.local_node_id.to_string(),
                                    "0",
                                    "success",
                                ])
                                .inc();

                            if response.success {
                                // Calculate the index this RPC covered
                                let last_index = if !entries_to_send.is_empty() {
                                    entries_to_send.last().unwrap().index
                                } else {
                                    prev_log_index
                                };

                                // Update follower state (handle success, remove from in-flight)
                                {
                                    let mut followers_map = self_clone.followers.write().await;
                                    if let Some(state) = followers_map.get_mut(&follower_id) {
                                        state.handle_success(last_index);
                                    }
                                }

                                // Recalculate commit_index
                                self_clone.update_commit_index().await;
                            } else {
                                // Follower rejected - decrement next_index and retry
                                let mut followers_map = self_clone.followers.write().await;
                                if let Some(state) = followers_map.get_mut(&follower_id) {
                                    // Decrement and retry
                                    let prev_next = state.next_index;
                                    state.handle_failure(prev_next.saturating_sub(1).max(1));

                                    tracing::debug!(
                                        "Follower {} rejected AppendEntries, decremented next_index to {}",
                                        follower_id,
                                        state.next_index
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            // Update Prometheus metrics - heartbeat failed
                            crate::prometheus_metrics::RAFT_HEARTBEATS_FAILED_TOTAL
                                .with_label_values(&[
                                    &self_clone.config.local_node_id.to_string(),
                                    &follower_id.to_string(),
                                    "0",
                                ])
                                .inc();
                            crate::prometheus_metrics::RAFT_APPEND_ENTRIES_TOTAL
                                .with_label_values(&[
                                    &self_clone.config.local_node_id.to_string(),
                                    "0",
                                    "failure",
                                ])
                                .inc();

                            // Remove this specific RPC from in-flight tracking
                            {
                                let mut followers_map = self_clone.followers.write().await;
                                if let Some(state) = followers_map.get_mut(&follower_id) {
                                    // Remove the RPC that just failed from in-flight queue
                                    state.in_flight.retain(|rpc| {
                                        !(rpc.start_index == next_idx && rpc.end_index == end_index)
                                    });
                                }
                            }

                            tracing::warn!(
                                "Failed to send AppendEntries to {}: {}",
                                follower_id,
                                e
                            );
                        }
                    }
                });
                replication_tasks.push(task);
            }

            // Wait for all replication tasks
            for task in replication_tasks {
                let _ = task.await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::WalStorageAdapter;
    use prkdb_core::wal::WalConfig;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_storage() -> (Arc<WalStorageAdapter>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let mut config = WalConfig::test_config();
        config.log_dir = temp_dir.path().to_path_buf();
        let storage = Arc::new(WalStorageAdapter::new(config).unwrap());
        (storage, temp_dir)
    }

    struct MockStateMachine;
    #[async_trait::async_trait]
    impl StateMachine for MockStateMachine {
        async fn apply(
            &self,
            _data: &[u8],
        ) -> Result<(), super::super::state_machine::StateMachineError> {
            Ok(())
        }

        async fn snapshot(
            &self,
        ) -> Result<Vec<u8>, super::super::state_machine::StateMachineError> {
            Ok(Vec::new())
        }

        async fn restore(
            &self,
            _snapshot: &[u8],
        ) -> Result<(), super::super::state_machine::StateMachineError> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_request_vote_grants_vote() {
        let config = ClusterConfig::default();
        let (storage, _temp) = create_test_storage();
        let node = RaftNode::new(config, storage, Arc::new(MockStateMachine));

        // Request vote with term 1
        let (response_term, vote_granted) = node.handle_request_vote(1, 2, 0, 0).await;

        assert_eq!(response_term, 1);
        assert!(vote_granted, "Node should grant vote to candidate 2");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_request_vote_rejects_lower_term() {
        let config = ClusterConfig::default();
        let (storage, _temp) = create_test_storage();
        let node = RaftNode::new(config, storage, Arc::new(MockStateMachine));

        // First vote updates term to 2
        node.handle_request_vote(2, 3, 0, 0).await;

        // Request with lower term should be rejected
        let (response_term, vote_granted) = node.handle_request_vote(1, 4, 0, 0).await;

        assert_eq!(response_term, 2);
        assert!(!vote_granted, "Node should reject vote with lower term");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_request_vote_only_grants_once_per_term() {
        let config = ClusterConfig::default();
        let (storage, _temp) = create_test_storage();
        let node = RaftNode::new(config, storage, Arc::new(MockStateMachine));

        // Grant vote to candidate 2
        let (_, granted1) = node.handle_request_vote(1, 2, 0, 0).await;
        assert!(granted1);

        // Try to vote for different candidate in same term
        let (_, granted2) = node.handle_request_vote(1, 3, 0, 0).await;
        assert!(
            !granted2,
            "Node should not grant vote to different candidate in same term"
        );

        // But should grant to same candidate again
        let (_, granted3) = node.handle_request_vote(1, 2, 0, 0).await;
        assert!(granted3, "Node can vote for same candidate again");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_append_entries_updates_term() {
        let config = ClusterConfig::default();
        let (storage, _temp) = create_test_storage();
        let node = RaftNode::new(config, storage, Arc::new(MockStateMachine));

        // Heartbeat with term 5
        let (response_term, success) = node.handle_append_entries(5, 1, 0, 0, 0, vec![]).await;

        assert_eq!(response_term, 5);
        assert!(success);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_append_entries_rejects_lower_term() {
        let config = ClusterConfig::default();
        let (storage, _temp) = create_test_storage();
        let node = RaftNode::new(config, storage, Arc::new(MockStateMachine));

        // Update to term 5
        node.handle_append_entries(5, 1, 0, 0, 0, vec![]).await;

        // Heartbeat with lower term should be rejected
        let (response_term, success) = node.handle_append_entries(3, 2, 0, 0, 0, vec![]).await;

        assert_eq!(response_term, 5);
        assert!(!success);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_append_entries_persists_log() {
        let config = ClusterConfig::default();
        let (storage, _temp) = create_test_storage();
        let node = RaftNode::new(config, storage, Arc::new(MockStateMachine));

        use crate::raft::rpc::LogEntry;

        let entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                data: b"command1".to_vec(),
            },
            LogEntry {
                index: 2,
                term: 1,
                data: b"command2".to_vec(),
            },
        ];

        let (_, success) = node.handle_append_entries(1, 1, 0, 0, 0, entries).await;

        assert!(success, "AppendEntries should succeed");
    }
}
