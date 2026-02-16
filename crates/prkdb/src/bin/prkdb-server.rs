use prkdb::db::PrkDb;
use prkdb::raft::{ClusterConfig, PrkDbGrpcService, RpcClientPool};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;
use tonic::transport::Server;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Read configuration from environment
    let node_id = env::var("NODE_ID")
        .unwrap_or_else(|_| "1".to_string())
        .parse::<u64>()?;

    let cluster_nodes_str =
        env::var("CLUSTER_NODES").unwrap_or_else(|_| "1@127.0.0.1:50000".to_string());

    let num_partitions = env::var("NUM_PARTITIONS")
        .unwrap_or_else(|_| "3".to_string())
        .parse::<usize>()?;

    // Parse cluster nodes
    let mut nodes = Vec::new();
    for node_str in cluster_nodes_str.split(',') {
        let parts: Vec<&str> = node_str.split('@').collect();
        if parts.len() == 2 {
            let id = parts[0].parse::<u64>()?;
            let addr: SocketAddr = parts[1].parse()?;
            nodes.push((id, addr));
        }
    }

    // Get listen address for this node
    let listen_addr = nodes
        .iter()
        .find(|(id, _)| *id == node_id)
        .map(|(_, addr)| *addr)
        .unwrap_or_else(|| "0.0.0.0:50000".parse().unwrap());

    info!(
        "Starting PrkDB node {} with {} partitions",
        node_id, num_partitions
    );
    info!("Listen address: {}", listen_addr);
    info!("Cluster nodes: {:?}", nodes);

    // Create cluster configuration
    let config = ClusterConfig {
        local_node_id: node_id,
        listen_addr,
        nodes,
        election_timeout_min_ms: 1000,
        election_timeout_max_ms: 2000,
        heartbeat_interval_ms: 200,
        partition_id: 0,
    };

    // Create database with Multi-Raft
    let storage_path = env::var("STORAGE_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/data/prkdb"));

    // Ensure directory exists
    if let Some(parent) = storage_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }

    let db = PrkDb::new_multi_raft(num_partitions, config, storage_path)?;

    // Start Multi-Raft
    let rpc_pool = Arc::new(RpcClientPool::new(node_id));
    db.start_multi_raft(rpc_pool, &[]);

    // Wait for leaders
    info!("Waiting for leader election...");
    match db
        .wait_for_leaders(std::time::Duration::from_secs(30))
        .await
    {
        Ok(_) => info!("All partitions have leaders elected"),
        Err(e) => info!("Leader election timeout (may be normal): {}", e),
    }

    info!("PrkDB server ready!");

    // Initialize Prometheus metrics
    prkdb::prometheus_metrics::init_prometheus_metrics();
    prkdb::prometheus_metrics::SERVER_UP
        .with_label_values(&[&node_id.to_string()])
        .set(1.0);

    // Start metrics HTTP server on port 9090 + node_id (unique per node)
    let metrics_port = 9090 + node_id as u16;
    let metrics_addr: SocketAddr = format!("0.0.0.0:{}", metrics_port).parse()?;
    tokio::spawn(async move {
        use axum::{routing::get, Router};

        async fn metrics_handler() -> String {
            prkdb::prometheus_metrics::export_metrics()
        }

        let app = Router::new().route("/metrics", get(metrics_handler));

        let listener = match tokio::net::TcpListener::bind(metrics_addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!("Failed to bind metrics server: {}", e);
                return;
            }
        };

        info!("Prometheus metrics server listening on {}", metrics_addr);

        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("Metrics server error: {}", e);
        }
    });

    // Create gRPC service for client data operations
    let db_arc = Arc::new(db);
    let admin_token = env::var("PRKDB_ADMIN_TOKEN").unwrap_or_default();
    let grpc_service = PrkDbGrpcService::new(db_arc.clone(), admin_token).into_server();

    // Create Raft service for multiplexed Raft traffic
    // We must register this service on the SAME server/port as the client API
    // because we are using port multiplexing (one port for everything)
    let raft_service_opt = if let Some(pm) = &db_arc.partition_manager {
        use prkdb::raft::rpc::raft_service_server::RaftServiceServer;
        use prkdb::raft::service::RaftServiceImpl;

        info!("Registering multiplexed Raft service");
        Some(RaftServiceServer::new(RaftServiceImpl::new(pm.clone())))
    } else {
        None
    };

    // Start gRPC data service on port 8080 (like Kafka's binary protocol)
    let grpc_port = env::var("GRPC_PORT").unwrap_or_else(|_| "8080".to_string());
    let data_addr: SocketAddr = format!("0.0.0.0:{}", grpc_port).parse()?;
    info!("Starting gRPC data service on {}", data_addr);

    //  Run gRPC server until shutdown
    let mut router = Server::builder().add_service(grpc_service);

    if let Some(raft_service) = raft_service_opt {
        router = router.add_service(raft_service);
    }

    router
        .serve_with_shutdown(data_addr, async {
            signal::ctrl_c().await.ok();
            info!("Shutting down...");
            prkdb::prometheus_metrics::SERVER_UP
                .with_label_values(&[&node_id.to_string()])
                .set(0.0);
        })
        .await?;

    Ok(())
}
