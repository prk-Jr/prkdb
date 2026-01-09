// Standalone Raft Node Server with optional TLS
// Run as separate process for multi-node cluster testing
//
// Plain:
//   cargo run --release --example raft_node -- \
//     --node-id 1 --listen 127.0.0.1:50051 \
//     --peers 2=127.0.0.1:50052,3=127.0.0.1:50053
//
// With TLS:
//   cargo run --release --example raft_node -- \
//     --node-id 1 --listen 127.0.0.1:50051 \
//     --peers 2=127.0.0.1:50052,3=127.0.0.1:50053 \
//     --tls-cert certs/server.crt --tls-key certs/server.key

use clap::Parser;
use prkdb::raft::server::TlsConfig;
use prkdb::raft::{ClusterConfig, PrkDbStateMachine, RaftNode};
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "raft_node")]
#[command(about = "PrkDB Raft Node Server")]
struct Args {
    /// Node ID (unique in cluster)
    #[arg(long)]
    node_id: u64,

    /// Address to listen on (e.g., 127.0.0.1:50051)
    #[arg(long)]
    listen: SocketAddr,

    /// Peer addresses (e.g., "2=127.0.0.1:50052,3=127.0.0.1:50053")
    #[arg(long)]
    peers: String,

    /// Data directory for WAL and state
    #[arg(long, default_value = "/tmp/prkdb")]
    data_dir: std::path::PathBuf,

    /// TLS certificate path (enables TLS if provided)
    #[arg(long)]
    tls_cert: Option<String>,

    /// TLS private key path
    #[arg(long)]
    tls_key: Option<String>,

    /// CA certificate path (for mTLS client verification)
    #[arg(long)]
    tls_ca: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let tls_enabled = args.tls_cert.is_some() && args.tls_key.is_some();

    println!("🔐 PrkDB Raft Node Server");
    println!("========================");
    println!();
    println!("  Node ID:    {}", args.node_id);
    println!("  Listen:     {}", args.listen);
    println!("  Peers:      {}", args.peers);
    println!("  Data Dir:   {:?}", args.data_dir);
    println!(
        "  TLS:        {}",
        if tls_enabled {
            "✅ ENABLED"
        } else {
            "❌ disabled"
        }
    );
    println!();

    // Parse peers
    let mut nodes: Vec<(u64, SocketAddr)> = vec![(args.node_id, args.listen)];
    for peer in args.peers.split(',') {
        if peer.is_empty() {
            continue;
        }
        let parts: Vec<&str> = peer.split('=').collect();
        if parts.len() == 2 {
            let id: u64 = parts[0].parse()?;
            let addr: SocketAddr = parts[1].parse()?;
            nodes.push((id, addr));
        }
    }
    nodes.sort_by_key(|(id, _)| *id);

    println!("📡 Cluster nodes:");
    for (id, addr) in &nodes {
        let marker = if *id == args.node_id { " (this)" } else { "" };
        println!("    Node {}: {}{}", id, addr, marker);
    }
    println!();

    // Create data directory
    std::fs::create_dir_all(&args.data_dir)?;

    // Create storage
    println!("⏳ Initializing storage...");
    let storage = tokio::task::spawn_blocking({
        let path = args.data_dir.clone();
        move || {
            WalStorageAdapter::new(WalConfig {
                log_dir: path,
                ..WalConfig::test_config()
            })
        }
    })
    .await??;
    let storage = Arc::new(storage);
    println!("✅ Storage ready");

    // Create cluster config
    let config = ClusterConfig {
        local_node_id: args.node_id,
        listen_addr: args.listen,
        nodes: nodes.clone(),
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        heartbeat_interval_ms: 50,
    };

    // Create state machine
    let state_machine = Arc::new(PrkDbStateMachine::new(storage.clone()));

    // Create Raft node
    println!("⏳ Creating Raft node...");
    let raft_node = Arc::new(RaftNode::new(config, storage, state_machine));
    println!("✅ Raft node created");
    println!();

    // Start gRPC server (with or without TLS)
    if tls_enabled {
        let tls_config = TlsConfig {
            cert_path: args.tls_cert.unwrap(),
            key_path: args.tls_key.unwrap(),
            ca_path: args.tls_ca,
        };
        println!("🔒 Starting gRPC server with TLS on {}...", args.listen);
        println!();
        println!("📊 Status updates will appear here.");
        println!("   Press Ctrl+C to stop.");
        println!();

        prkdb::raft::server::start_raft_server_tls(raft_node, args.listen, tls_config)
            .await
            .map_err(|e| anyhow::anyhow!("Raft server error: {}", e))?;
    } else {
        println!("🚀 Starting gRPC server on {}...", args.listen);
        println!();
        println!("📊 Status updates will appear here.");
        println!("   Press Ctrl+C to stop.");
        println!();

        prkdb::raft::server::start_raft_server(raft_node, args.listen)
            .await
            .map_err(|e| anyhow::anyhow!("Raft server error: {}", e))?;
    }

    Ok(())
}
