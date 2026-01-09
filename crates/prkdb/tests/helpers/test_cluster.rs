use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

static CLUSTER_OFFSET: AtomicU16 = AtomicU16::new(0);

use super::NetworkSimulator;

/// Test cluster manager for chaos testing
///
/// Manages a multi-node PrkDB cluster for testing:
/// - Spawns server processes
/// - Manages data directories
/// - Controls node lifecycle
/// - Integrates with NetworkSimulator
pub struct TestCluster {
    pub nodes: HashMap<u64, TestNode>,
    pub network: Arc<NetworkSimulator>,
    pub base_dir: TempDir,
    pub num_partitions: usize,
}

pub struct TestNode {
    pub node_id: u64,
    pub process: Option<Child>,
    pub data_port: u16,
    pub raft_port: u16,
    pub data_dir: PathBuf,
    pub log_file: PathBuf,
}

impl TestCluster {
    /// Create a new test cluster with N nodes
    pub async fn new(num_nodes: usize) -> anyhow::Result<Self> {
        let base_dir = TempDir::new()?;
        let config_path = base_dir.path().join("chaos_rules.json");
        let network = Arc::new(NetworkSimulator::new(Some(config_path)));
        let mut nodes = HashMap::new();

        // Reserve ports for each node
        // Use atomic offset to avoid port conflicts in parallel tests
        let offset = CLUSTER_OFFSET.fetch_add(10, Ordering::Relaxed);
        let base_data_port = 19000 + offset; // Start from 19000 block
        let base_raft_port = 60000 + offset; // Start from 60000 block

        for i in 0..num_nodes {
            let node_id = (i + 1) as u64;
            let data_port = base_data_port + i as u16;
            let raft_port = base_raft_port + i as u16;
            let data_dir = base_dir.path().join(format!("node{}", node_id));
            let log_file = base_dir.path().join(format!("node{}.log", node_id));

            std::fs::create_dir_all(&data_dir)?;

            nodes.insert(
                node_id,
                TestNode {
                    node_id,
                    process: None,
                    data_port,
                    raft_port,
                    data_dir,
                    log_file,
                },
            );
        }

        Ok(Self {
            nodes,
            network,
            base_dir,
            num_partitions: 1,
        })
    }

    /// Start all nodes in the cluster
    pub async fn start_all(&mut self) -> anyhow::Result<()> {
        // Build cluster_nodes string: "1@127.0.0.1:60011,2@127.0.0.1:60012,..."
        let cluster_nodes: Vec<String> = self
            .nodes
            .values()
            .map(|n| format!("{}@127.0.0.1:{}", n.node_id, n.raft_port))
            .collect();
        let cluster_nodes_str = cluster_nodes.join(",");
        let num_partitions = self.num_partitions;
        let chaos_config_path = self.base_dir.path().join("chaos_rules.json");

        for node in self.nodes.values_mut() {
            Self::start_node_internal(node, &cluster_nodes_str, num_partitions, &chaos_config_path)
                .await?;
        }

        // Wait for leader election
        sleep(Duration::from_secs(3)).await;

        Ok(())
    }

    /// Start a specific node
    pub async fn start_node(&mut self, node_id: u64) -> anyhow::Result<()> {
        let cluster_nodes: Vec<String> = self
            .nodes
            .values()
            .map(|n| format!("{}@127.0.0.1:{}", n.node_id, n.raft_port))
            .collect();
        let cluster_nodes_str = cluster_nodes.join(",");
        let num_partitions = self.num_partitions;
        let chaos_config_path = self.base_dir.path().join("chaos_rules.json");

        if let Some(node) = self.nodes.get_mut(&node_id) {
            Self::start_node_internal(node, &cluster_nodes_str, num_partitions, &chaos_config_path)
                .await?;
        }

        Ok(())
    }

    async fn start_node_internal(
        node: &mut TestNode,
        cluster_nodes: &str,
        num_partitions: usize,
        chaos_config_path: &std::path::Path,
    ) -> anyhow::Result<()> {
        let log_file = std::fs::File::create(&node.log_file)?;

        // Find the binary by checking current dir and parent dirs
        // Find the binary by checking likely locations
        let mut binary_path = PathBuf::new();
        let profiles = ["debug", "release"];
        let locations = ["target", "../target", "../../target"];

        let mut found = false;
        'outer: for profile in profiles {
            for loc in locations {
                if let Ok(cwd) = std::env::current_dir() {
                    let path = cwd.join(loc).join(profile).join("prkdb-server");
                    if path.exists() {
                        binary_path = path;
                        found = true;
                        break 'outer;
                    }
                }
            }
        }

        if !found {
            // Try explicit workspace root search as fallback
            let cwd = std::env::current_dir()?;
            if let Some(workspace_root) = cwd.parent().and_then(|p| p.parent()) {
                for profile in profiles {
                    let path = workspace_root
                        .join("target")
                        .join(profile)
                        .join("prkdb-server");
                    if path.exists() {
                        binary_path = path;
                        found = true;
                        break;
                    }
                }
            }
        }

        if !binary_path.exists() {
            // Fallback: try to find it relative to where we might be
            if let Ok(cwd) = std::env::current_dir() {
                println!("Current directory: {:?}", cwd);
            }
            anyhow::bail!(
                "Binary not found at {:?}. Run 'cargo build --bin prkdb-server --release' first.",
                binary_path
            );
        }

        let child = Command::new(binary_path)
            .env("NODE_ID", node.node_id.to_string())
            .env("CLUSTER_NODES", cluster_nodes)
            .env("NUM_PARTITIONS", num_partitions.to_string())
            .env("STORAGE_PATH", node.data_dir.to_str().unwrap())
            .env("GRPC_PORT", node.data_port.to_string())
            .env("RUST_LOG", "prkdb::raft=debug,info")
            .env("CHAOS_CONFIG_PATH", chaos_config_path)
            .stdout(Stdio::from(log_file.try_clone()?))
            .stderr(Stdio::from(log_file))
            .spawn()?;

        node.process = Some(child);

        Ok(())
    }

    /// Stop a specific node
    pub async fn stop_node(&mut self, node_id: u64) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            if let Some(mut process) = node.process.take() {
                let _ = process.kill();
                let _ = process.wait();
            }
        }
    }

    /// Stop all nodes
    pub async fn stop_all(&mut self) {
        for node in self.nodes.values_mut() {
            if let Some(mut process) = node.process.take() {
                let _ = process.kill();
                let _ = process.wait();
            }
        }
    }

    /// Restart a node (stop then start)
    pub async fn restart_node(&mut self, node_id: u64) -> anyhow::Result<()> {
        self.stop_node(node_id).await;
        sleep(Duration::from_millis(500)).await;
        self.start_node(node_id).await
    }

    /// Create a network partition
    pub async fn partition(&self, group1: Vec<u64>, group2: Vec<u64>) {
        self.network.partition(group1, group2).await;
    }

    /// Heal all network partitions
    pub async fn heal_partitions(&self) {
        self.network.heal_partitions().await;
    }

    /// Dump logs of all nodes to stdout
    pub fn dump_logs(&self) {
        println!("\n=== DUMPING CLUSTER LOGS ===");
        for (id, node) in &self.nodes {
            println!("\n--- Node {} Log ---", id);
            if let Ok(content) = std::fs::read_to_string(&node.log_file) {
                println!("{}", content);
            } else {
                println!("(Failed to read log file)");
            }
            println!("-------------------");
        }
        println!("==========================\n");
    }

    /// Get a node by ID
    pub fn get_node(&self, node_id: u64) -> Option<&TestNode> {
        self.nodes.get(&node_id)
    }

    /// Get node count
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Read node log file
    pub fn read_node_log(&self, node_id: u64) -> anyhow::Result<String> {
        if let Some(node) = self.nodes.get(&node_id) {
            Ok(std::fs::read_to_string(&node.log_file)?)
        } else {
            anyhow::bail!("Node {} not found", node_id)
        }
    }

    /// Check if node is running
    pub fn is_node_running(&self, node_id: u64) -> bool {
        if let Some(node) = self.nodes.get(&node_id) {
            node.process.is_some()
        } else {
            false
        }
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        let temp_path = self.base_dir.path();
        println!("TestCluster temp dir: {:?}", temp_path);

        // Copy log files to /tmp for post-mortem analysis
        let debug_dir = std::path::PathBuf::from("/tmp/prkdb_test_logs");
        let _ = std::fs::create_dir_all(&debug_dir);

        for i in 1..=3 {
            let src = temp_path.join(format!("node{}.log", i));
            let dst = debug_dir.join(format!("node{}.log", i));
            if src.exists() {
                let _ = std::fs::copy(&src, &dst);
                println!("Copied {} to {}", src.display(), dst.display());
            }
        }
        println!("Log files saved to: {}", debug_dir.display());

        // Clean up all processes
        for node in self.nodes.values_mut() {
            if let Some(mut process) = node.process.take() {
                let _ = process.kill();
                let _ = process.wait();
            }
        }

        // Prevent TempDir from being dropped (and auto-deleted)
        // This leaks memory but allows us to examine log files
        let base_dir_ptr = &self.base_dir as *const _;
        std::mem::forget(unsafe { std::ptr::read(base_dir_ptr) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_creation() {
        let cluster = TestCluster::new(3).await.unwrap();
        assert_eq!(cluster.node_count(), 3);
        assert!(cluster.get_node(1).is_some());
        assert!(cluster.get_node(4).is_none());
    }

    #[tokio::test]
    async fn test_cluster_lifecycle() {
        let cluster = TestCluster::new(3).await.unwrap();

        // Initially no nodes running
        assert!(!cluster.is_node_running(1));

        // Note: We can't actually start nodes in a unit test without the binary
        // These would be integration tests
    }
}
