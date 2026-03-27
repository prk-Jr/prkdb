use anyhow::{Context, Result};
use prkdb::db::PrkDb;
use prkdb::raft::{ClusterConfig, PrkDbGrpcService, RpcClientPool};
use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;
use tonic::transport::Server;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Read configuration from environment
    let node_id = env::var("NODE_ID")
        .unwrap_or_else(|_| "1".to_string())
        .parse::<u64>()?;

    let cluster_nodes_str =
        env::var("CLUSTER_NODES").unwrap_or_else(|_| "1@127.0.0.1:8080".to_string());

    let num_partitions = env::var("NUM_PARTITIONS")
        .unwrap_or_else(|_| "3".to_string())
        .parse::<usize>()?;

    let nodes = parse_cluster_nodes(&cluster_nodes_str)?;
    let listen_addr = resolve_listen_addr(node_id, &nodes)?;

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
        nodes: nodes.clone(),
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

    let db = PrkDb::new_multi_raft(num_partitions, config, storage_path.clone())?;

    // Start Multi-Raft
    let rpc_pool = Arc::new(RpcClientPool::new(node_id));
    db.start_multi_raft(rpc_pool, &[]);

    // Wait for leaders in the background so the server can bind its network listeners
    let db_clone = db.clone();
    tokio::spawn(async move {
        info!("Waiting for leader election...");
        match db_clone
            .wait_for_leaders(std::time::Duration::from_secs(30))
            .await
        {
            Ok(_) => info!("All partitions have leaders elected"),
            Err(e) => info!("Leader election timeout (may be normal): {}", e),
        }
        info!("PrkDB server ready!");
    });

    // Initialize Prometheus metrics
    prkdb::prometheus_metrics::init_prometheus_metrics();
    prkdb::prometheus_metrics::SERVER_UP
        .with_label_values(&[&node_id.to_string()])
        .set(1.0);

    // Start metrics HTTP server on port 9090 + node_id (unique per node)
    let disable_metrics = env_var_is_truthy("PRKDB_DISABLE_METRICS");
    let metrics_addr_override = env::var("PRKDB_METRICS_ADDR")
        .ok()
        .filter(|value| !value.trim().is_empty());
    if let Some(metrics_addr) =
        resolve_metrics_bind_address(node_id, disable_metrics, metrics_addr_override.as_deref())?
    {
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
    } else {
        info!("Prometheus metrics server disabled");
    }

    // Create gRPC service for client data operations
    let db_arc = Arc::new(db);
    let admin_token = env::var("PRKDB_ADMIN_TOKEN").unwrap_or_default();
    let schema_path = storage_path.join("schemas");
    let explicit_advertised_grpc_address = env::var("PRKDB_ADVERTISED_GRPC_ADDR")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let advertised_grpc_address =
        resolve_advertised_grpc_address(listen_addr, explicit_advertised_grpc_address.as_deref())?;
    let node_address_overrides = parse_node_address_overrides(
        env::var("PRKDB_ADVERTISED_NODE_ADDRS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .as_deref(),
    )?;
    let advertised_node_addresses = build_advertised_node_addresses(
        &nodes,
        node_id,
        &advertised_grpc_address,
        &node_address_overrides,
    )?;

    info!("Advertised client address: {}", advertised_grpc_address);

    let grpc_service =
        PrkDbGrpcService::with_schema_storage_path(db_arc.clone(), admin_token, schema_path)
            .await
            .with_local_node_id(node_id)
            .with_public_address(advertised_grpc_address)
            .with_advertised_node_addresses(advertised_node_addresses)
            .into_server();

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

    let configured_grpc_port = env::var("GRPC_PORT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.parse::<u16>())
        .transpose()?;
    let grpc_port = configured_grpc_port.unwrap_or_else(|| listen_addr.port());
    if grpc_port != listen_addr.port() {
        anyhow::bail!(
            "GRPC_PORT ({}) must match this node's CLUSTER_NODES port ({}) in multiplexed mode",
            grpc_port,
            listen_addr.port()
        );
    }

    let data_addr = resolve_grpc_bind_address(listen_addr, grpc_port);
    info!("Starting gRPC data service on {}", data_addr);

    //  Run gRPC server until shutdown
    let mut router = Server::builder().add_service(grpc_service);

    if let Some(raft_service) = raft_service_opt {
        router = router.add_service(raft_service);
    }

    let shutdown_db = db_arc.clone();

    router
        .serve_with_shutdown(data_addr, async {
            signal::ctrl_c().await.ok();

            if let Err(error) = flush_db_state(&shutdown_db).await {
                tracing::warn!("Failed to flush storage during shutdown: {}", error);
            }

            info!("Shutting down...");
            prkdb::prometheus_metrics::SERVER_UP
                .with_label_values(&[&node_id.to_string()])
                .set(0.0);
        })
        .await?;

    Ok(())
}

fn parse_cluster_nodes(cluster_nodes_str: &str) -> Result<Vec<(u64, SocketAddr)>> {
    if cluster_nodes_str.trim().is_empty() {
        anyhow::bail!("CLUSTER_NODES must not be empty");
    }

    let mut nodes = Vec::new();
    let mut seen_ids = HashSet::new();

    for node_str in cluster_nodes_str
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let (id_str, address_str) = node_str
            .split_once('@')
            .with_context(|| format!("Invalid CLUSTER_NODES entry '{node_str}'"))?;
        let node_id = id_str
            .parse::<u64>()
            .with_context(|| format!("Invalid node ID in CLUSTER_NODES entry '{node_str}'"))?;
        let address = address_str.parse::<SocketAddr>().with_context(|| {
            format!("Invalid socket address in CLUSTER_NODES entry '{node_str}'")
        })?;

        if !seen_ids.insert(node_id) {
            anyhow::bail!("Duplicate node ID {node_id} in CLUSTER_NODES");
        }

        nodes.push((node_id, address));
    }

    if nodes.is_empty() {
        anyhow::bail!("CLUSTER_NODES must define at least one node");
    }

    nodes.sort_by_key(|(node_id, _)| *node_id);
    Ok(nodes)
}

fn resolve_listen_addr(node_id: u64, nodes: &[(u64, SocketAddr)]) -> Result<SocketAddr> {
    nodes
        .iter()
        .find(|(candidate_id, _)| *candidate_id == node_id)
        .map(|(_, address)| *address)
        .ok_or_else(|| anyhow::anyhow!("NODE_ID {node_id} is not present in CLUSTER_NODES"))
}

fn normalize_http_address(address: &str) -> Result<String> {
    let trimmed = address.trim();
    if trimmed.is_empty() {
        anyhow::bail!("Advertised address must not be empty");
    }

    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Ok(trimmed.to_string());
    }

    Ok(format!("http://{trimmed}"))
}

fn parse_node_address_overrides(value: Option<&str>) -> Result<HashMap<u64, String>> {
    let mut overrides = HashMap::new();

    let Some(value) = value else {
        return Ok(overrides);
    };

    for entry in value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (node_id_str, address) = entry
            .split_once('=')
            .or_else(|| entry.split_once('@'))
            .with_context(|| format!("Invalid advertised node address override '{entry}'"))?;
        let node_id = node_id_str
            .parse::<u64>()
            .with_context(|| format!("Invalid node ID in override '{entry}'"))?;
        let normalized = normalize_http_address(address)?;

        if overrides.insert(node_id, normalized).is_some() {
            anyhow::bail!("Duplicate advertised address override for node {node_id}");
        }
    }

    Ok(overrides)
}

fn resolve_advertised_grpc_address(
    listen_addr: SocketAddr,
    explicit_address: Option<&str>,
) -> Result<String> {
    if let Some(address) = explicit_address {
        return normalize_http_address(address);
    }

    if listen_addr.ip().is_unspecified() {
        anyhow::bail!(
            "PRKDB_ADVERTISED_GRPC_ADDR must be set when CLUSTER_NODES advertises an unspecified listen address ({listen_addr})"
        );
    }

    Ok(format!("http://{listen_addr}"))
}

fn build_advertised_node_addresses(
    nodes: &[(u64, SocketAddr)],
    local_node_id: u64,
    local_public_address: &str,
    overrides: &HashMap<u64, String>,
) -> Result<HashMap<u64, String>> {
    let known_nodes: HashSet<u64> = nodes.iter().map(|(node_id, _)| *node_id).collect();
    if let Some(unknown_node_id) = overrides
        .keys()
        .copied()
        .find(|node_id| !known_nodes.contains(node_id))
    {
        anyhow::bail!("Advertised address override provided for unknown node {unknown_node_id}");
    }

    let mut advertised = HashMap::with_capacity(nodes.len());

    for (node_id, address) in nodes {
        let public_address = if *node_id == local_node_id {
            local_public_address.to_string()
        } else if let Some(address) = overrides.get(node_id) {
            address.clone()
        } else if address.ip().is_unspecified() {
            anyhow::bail!(
                "Node {node_id} uses unspecified listen address {address} and has no advertised override"
            );
        } else {
            format!("http://{address}")
        };

        advertised.insert(*node_id, public_address);
    }

    Ok(advertised)
}

fn resolve_metrics_bind_address(
    node_id: u64,
    disabled: bool,
    explicit_address: Option<&str>,
) -> Result<Option<SocketAddr>> {
    if disabled {
        return Ok(None);
    }

    if let Some(address) = explicit_address {
        let parsed = address
            .parse::<SocketAddr>()
            .with_context(|| format!("Invalid PRKDB_METRICS_ADDR value '{address}'"))?;
        return Ok(Some(parsed));
    }

    let node_offset = u16::try_from(node_id)
        .with_context(|| format!("NODE_ID {node_id} is too large to derive a metrics port"))?;
    let port = 9090u16
        .checked_add(node_offset)
        .context("Derived metrics port overflowed u16")?;
    let metrics_addr = format!("127.0.0.1:{port}")
        .parse::<SocketAddr>()
        .context("Failed to build metrics bind address")?;

    Ok(Some(metrics_addr))
}

fn resolve_grpc_bind_address(listen_addr: SocketAddr, grpc_port: u16) -> SocketAddr {
    SocketAddr::new(listen_addr.ip(), grpc_port)
}

async fn flush_db_state(db: &Arc<PrkDb>) -> Result<()> {
    if let Some(pm) = &db.partition_manager {
        for partition_id in 0..pm.partition_count() {
            if let Some(storage) = pm.get_partition_storage(partition_id as u64) {
                storage.flush().await?;
            }
        }
    } else {
        db.storage().flush().await?;
    }

    Ok(())
}

fn env_var_is_truthy(key: &str) -> bool {
    env::var(key)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_listen_addr_requires_local_node_membership() {
        let nodes = vec![(2, "127.0.0.1:8081".parse().unwrap())];

        let err = resolve_listen_addr(1, &nodes).unwrap_err().to_string();
        assert!(err.contains("NODE_ID 1"));
    }

    #[test]
    fn build_advertised_node_addresses_prefers_explicit_peer_overrides() {
        let nodes = vec![
            (1, "127.0.0.1:8080".parse().unwrap()),
            (2, "127.0.0.1:8081".parse().unwrap()),
        ];
        let overrides = parse_node_address_overrides(Some("2=db-2.example.com:18081")).unwrap();

        let addresses =
            build_advertised_node_addresses(&nodes, 1, "http://db-1.example.com:18080", &overrides)
                .unwrap();

        assert_eq!(
            addresses.get(&1).map(String::as_str),
            Some("http://db-1.example.com:18080")
        );
        assert_eq!(
            addresses.get(&2).map(String::as_str),
            Some("http://db-2.example.com:18081")
        );
    }

    #[test]
    fn resolve_metrics_bind_address_supports_disable_and_override() {
        assert_eq!(resolve_metrics_bind_address(1, true, None).unwrap(), None);

        assert_eq!(
            resolve_metrics_bind_address(2, false, Some("127.0.0.1:9910")).unwrap(),
            Some("127.0.0.1:9910".parse().unwrap())
        );
    }

    #[test]
    fn resolve_grpc_bind_address_reuses_listen_host() {
        let listen_addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        assert_eq!(
            resolve_grpc_bind_address(listen_addr, 50051),
            "127.0.0.1:50051".parse::<SocketAddr>().unwrap()
        );
    }
}
