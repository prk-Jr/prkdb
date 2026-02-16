use super::config::ClusterConfig;
use super::node::RaftNode;
use super::state_machine::StateMachine;
use crate::storage::wal_adapter::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Manages multiple independent Raft partitions for horizontal scaling
pub struct PartitionManager {
    /// Number of partitions
    pub num_partitions: usize, // Made public for gRPC service access

    /// Partition ID → RaftNode mapping
    raft_groups: HashMap<u64, Arc<RaftNode>>,

    /// Storage adapters per partition
    #[allow(dead_code)]
    storage_adapters: HashMap<u64, Arc<WalStorageAdapter>>,

    /// State machine per partition
    #[allow(dead_code)]
    state_machines: HashMap<u64, Arc<dyn StateMachine>>,

    /// Leader status per partition
    #[allow(dead_code)]
    partition_status: Arc<RwLock<HashMap<u64, PartitionStatus>>>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct PartitionStatus {
    has_leader: bool,
    is_leader: bool,
    last_heartbeat: Instant,
}

impl PartitionManager {
    /// Create N independent Raft groups for horizontal partitioning
    ///
    /// Each partition is a fully independent Raft cluster with its own:
    /// - WAL storage (isolated directory)
    /// - State machine (isolated state)
    /// - Network ports (offset by partition_id * 100)
    ///
    /// Keys are routed to partitions via consistent hashing
    pub fn new(
        num_partitions: usize,
        base_config: ClusterConfig,
        base_storage_path: PathBuf,
        create_state_machine: impl Fn(u64, Arc<WalStorageAdapter>) -> Arc<dyn StateMachine>,
    ) -> anyhow::Result<Self> {
        tracing::info!(
            "Creating PartitionManager with {} partitions",
            num_partitions
        );

        // Optimization: Pre-size HashMaps for known partition count
        let mut raft_groups = HashMap::with_capacity(num_partitions);
        let mut storage_adapters = HashMap::with_capacity(num_partitions);
        let mut state_machines = HashMap::with_capacity(num_partitions);
        let mut partition_status = HashMap::with_capacity(num_partitions);

        for partition_id in 0..num_partitions {
            tracing::info!("Initializing partition {}", partition_id);

            // Create isolated storage directory for this partition
            let partition_path = base_storage_path.join(format!("partition_{}", partition_id));
            std::fs::create_dir_all(&partition_path)?;

            let wal_config = WalConfig {
                log_dir: partition_path,
                ..WalConfig::default()
            };
            let storage = Arc::new(WalStorageAdapter::new(wal_config)?);

            // Create state machine for this partition
            let state_machine = create_state_machine(partition_id as u64, storage.clone());

            // Adjust config for this partition (different ports to avoid conflicts)
            tracing::info!(
                "[BEFORE-ADJUST] About to adjust config for partition {}",
                partition_id
            );
            let partition_config = Self::adjust_config_for_partition(&base_config, partition_id);
            tracing::info!(
                "[AFTER-ADJUST] Config adjusted, listen_addr is now: {}",
                partition_config.listen_addr
            );

            // Create RaftNode for this partition
            let raft_node = Arc::new(RaftNode::new(
                partition_config,
                storage.clone(),
                state_machine.clone(),
            ));

            raft_groups.insert(partition_id as u64, raft_node);
            storage_adapters.insert(partition_id as u64, storage);
            state_machines.insert(partition_id as u64, state_machine);
            partition_status.insert(
                partition_id as u64,
                PartitionStatus {
                    has_leader: false,
                    is_leader: false,
                    last_heartbeat: Instant::now(),
                },
            );
        }

        Ok(Self {
            num_partitions,
            raft_groups,
            storage_adapters,
            state_machines,
            partition_status: Arc::new(RwLock::new(partition_status)),
        })
    }

    /// Adjust cluster configuration for a specific partition
    ///
    /// Each partition needs isolated network ports to avoid conflicts.
    /// Strategy: Base port + (partition_id * 100)
    fn adjust_config_for_partition(
        base_config: &ClusterConfig,
        partition_id: usize,
    ) -> ClusterConfig {
        let mut config = base_config.clone();

        // In Multiplexed mode, we DO NOT adjust ports.
        // All partitions share the same main gRPC port and use x-prkdb-partition-id header.

        let base_listen_port = config.listen_addr.port();
        config.partition_id = partition_id as u64;

        tracing::info!(
            "[PARTITION-CONFIG] Partition {}: Using main port {} (Multiplexed)",
            partition_id,
            base_listen_port
        );

        config
    }

    /// Start all Raft partitions
    pub fn start_all(
        &self,
        rpc_pool: Arc<super::rpc_client::RpcClientPool>,
        skip_server_partitions: &[u64],
    ) {
        tracing::info!("Starting all {} partitions", self.num_partitions);

        for (partition_id, raft_node) in &self.raft_groups {
            tracing::info!("Starting Raft partition {}", partition_id);

            let raft_node_clone = raft_node.clone();
            let rpc_pool_clone = rpc_pool.clone();

            // Start Raft background tasks (election, heartbeat, apply loop)
            tokio::spawn(async move {
                raft_node_clone.start(rpc_pool_clone);
            });

            // In Multiplexed mode, we DO NOT start separate servers for partitions.
            // The main server handles all traffic via RaftServiceImpl routing.
            tracing::info!(
                "Partition {} initialized (Multiplexed on main server)",
                partition_id
            );
        }
    }

    /// Wait for all partitions to have leaders elected
    /// - Become leader themselves, OR
    /// - Detect a leader in their cluster
    pub async fn wait_for_leaders(&self, timeout: Duration) -> anyhow::Result<()> {
        let start = Instant::now();
        let check_interval = Duration::from_millis(100);

        loop {
            let ready_count = self.count_ready_partitions().await;

            if ready_count == self.num_partitions {
                tracing::info!("All {} partitions have leaders!", self.num_partitions);
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for partition leaders. Only {}/{} partitions ready",
                    ready_count,
                    self.num_partitions
                ));
            }

            tracing::debug!(
                "Waiting for partition leaders... {}/{} ready",
                ready_count,
                self.num_partitions
            );

            tokio::time::sleep(check_interval).await;
        }
    }

    /// Count how many partitions have leaders
    async fn count_ready_partitions(&self) -> usize {
        let mut ready_count = 0;

        for (partition_id, raft_node) in &self.raft_groups {
            let state = raft_node.get_state().await;
            let has_leader =
                state == super::node::RaftState::Leader || raft_node.get_leader().await.is_some();

            if has_leader {
                ready_count += 1;
            } else {
                tracing::trace!(
                    "Partition {} not ready yet (state: {:?})",
                    partition_id,
                    state
                );
            }
        }

        ready_count
    }

    /// Route a key to its partition using consistent hashing
    ///
    /// Uses seahash for fast, consistent key distribution
    pub fn get_partition_for_key(&self, key: &[u8]) -> u64 {
        let hash = seahash::hash(key);
        hash % self.num_partitions as u64
    }

    /// Get the RaftNode responsible for a given key
    pub fn get_raft_for_key(&self, key: &[u8]) -> Arc<RaftNode> {
        let partition_id = self.get_partition_for_key(key);
        self.raft_groups
            .get(&partition_id)
            .expect("Partition not found")
            .clone()
    }

    /// Get a specific partition's RaftNode by ID
    pub fn get_partition(&self, partition_id: u64) -> Option<Arc<RaftNode>> {
        self.raft_groups.get(&partition_id).cloned()
    }

    /// Get a specific partition's storage adapter by ID
    pub fn get_partition_storage(&self, partition_id: u64) -> Option<Arc<WalStorageAdapter>> {
        self.storage_adapters.get(&partition_id).cloned()
    }

    /// Get total number of partitions
    pub fn partition_count(&self) -> usize {
        self.num_partitions
    }

    /// Get statistics for all partitions
    pub async fn get_statistics(&self) -> PartitionManagerStats {
        let mut total_log_entries = 0;
        let mut total_committed = 0;
        let mut leaders_count = 0;

        for (partition_id, raft_node) in &self.raft_groups {
            let log_len = raft_node.log_size().await as u64;
            let commit_index = raft_node.commit_index().await;
            let is_leader = raft_node.get_state().await == super::node::RaftState::Leader;

            total_log_entries += log_len;
            total_committed += commit_index;
            if is_leader {
                leaders_count += 1;
            }

            tracing::trace!(
                "Partition {} stats: log_len={}, commit_index={}, is_leader={}",
                partition_id,
                log_len,
                commit_index,
                is_leader
            );
        }

        PartitionManagerStats {
            total_partitions: self.num_partitions,
            leaders_count,
            total_log_entries,
            total_committed,
        }
    }
    /// Get topology information for all partitions
    pub async fn get_topology(&self) -> Vec<(u64, Option<u64>, Vec<u64>)> {
        // Optimization: Pre-size vector to avoid reallocations
        let mut topology = Vec::with_capacity(self.raft_groups.len());

        for (partition_id, raft_node) in &self.raft_groups {
            let leader_id = raft_node.get_leader().await;
            // For now, we assume all nodes are replicas for all partitions (simplification)
            // In a real system, we'd query the configuration
            let replicas = raft_node
                .get_config()
                .await
                .nodes
                .iter()
                .map(|(id, _)| *id)
                .collect();

            topology.push((*partition_id, leader_id, replicas));
        }

        topology
    }
}

/// Statistics across all partitions
#[derive(Debug, Clone)]
pub struct PartitionManagerStats {
    pub total_partitions: usize,
    pub leaders_count: usize,
    pub total_log_entries: u64,
    pub total_committed: u64,
}
