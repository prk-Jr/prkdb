use prkdb::raft::{ClusterConfig, RaftNode, RpcClientPool};
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Get node ID from args (1, 2, or 3)
    let args: Vec<String> = std::env::args().collect();
    let node_id: u64 = if args.len() > 1 {
        args[1].parse().unwrap_or(1)
    } else {
        1
    };

    // Define cluster topology
    let nodes = vec![
        (1, "127.0.0.1:50051".parse::<SocketAddr>().unwrap()),
        (2, "127.0.0.1:50052".parse::<SocketAddr>().unwrap()),
        (3, "127.0.0.1:50053".parse::<SocketAddr>().unwrap()),
    ];

    let listen_addr = nodes
        .iter()
        .find(|(id, _)| *id == node_id)
        .map(|(_, addr)| *addr)
        .expect("Invalid node ID");

    let config = ClusterConfig {
        local_node_id: node_id,
        listen_addr,
        nodes: nodes.clone(),
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        heartbeat_interval_ms: 50,
        partition_id: 0,
    };

    // Create WAL storage path
    let db_path = std::path::PathBuf::from(format!("tmp/raft_node_{}", node_id));
    if db_path.exists() {
        std::fs::remove_dir_all(&db_path)?;
    }
    std::fs::create_dir_all(&db_path)?;

    // Create PartitionManager
    let pm = Arc::new(
        prkdb::raft::PartitionManager::new(1, config.clone(), db_path, |_part_id, storage| {
            Arc::new(prkdb::raft::PrkDbStateMachine::new(storage))
        })
        .unwrap(),
    );

    // Create RPC pool
    let rpc_pool = Arc::new(RpcClientPool::new(node_id));

    // Start background tasks
    pm.start_all(rpc_pool.clone(), &[]);

    // Start gRPC server
    let pm_server = pm.clone();
    tokio::spawn(async move {
        if let Err(e) = prkdb::raft::server::start_raft_server(pm_server, listen_addr).await {
            eprintln!("Server error: {}", e);
        }
    });

    // Get the node for local operations
    let node = pm.get_partition(0).unwrap();

    // Wait a bit for server to start
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Start Raft logic
    node.start(rpc_pool);

    tracing::info!("==============================================");
    tracing::info!("Raft Node {} started on {}", node_id, listen_addr);
    tracing::info!("Cluster: 3 nodes (ports 50051, 50052, 50053)");
    tracing::info!("==============================================");
    tracing::info!("Watching for leader election...");
    tracing::info!("Tip: Start all 3 nodes in separate terminals:");
    tracing::info!("  cargo run --example raft_cluster --release -- 1");
    tracing::info!("  cargo run --example raft_cluster --release -- 2");
    tracing::info!("  cargo run --example raft_cluster --release -- 3");
    tracing::info!("==============================================");

    // Keep alive
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}
