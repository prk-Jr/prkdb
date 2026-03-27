use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;

mod collection_metadata;
mod commands;
mod database_manager;
mod output;
mod storage_keys;
mod uptime_tracker;

use commands::*;
use database_manager::init_database_manager;

#[derive(Parser)]
#[command(
    name = "prkdb",
    about = "PrkDB - A reactive database with compute facilities",
    version = env!("CARGO_PKG_VERSION"),
    long_about = None
)]
pub struct Cli {
    /// Database file path
    #[arg(short, long, default_value = "./prkdb.db")]
    pub database: PathBuf,

    /// Output format
    #[arg(short, long, value_enum, default_value = "table")]
    pub format: OutputFormat,

    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,

    /// Admin token for secured operations
    #[arg(long, env = "PRKDB_ADMIN_TOKEN")]
    pub admin_token: Option<String>,

    /// Use local embedded database instead of remote server
    #[arg(long)]
    pub local: bool,

    /// gRPC bootstrap server address.
    ///
    /// Use `http://127.0.0.1:8080` for `prkdb-server`, or
    /// `http://127.0.0.1:50051` for the local gRPC endpoint exposed by
    /// `prkdb-cli serve`.
    #[arg(long, default_value = "http://127.0.0.1:8080", env = "PRKDB_SERVER")]
    pub server: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Collection management
    #[command(subcommand)]
    Collection(CollectionCommands),

    /// Consumer group management
    #[command(subcommand)]
    Consumer(ConsumerCommands),

    /// Partition management and monitoring
    #[command(subcommand)]
    Partition(PartitionCommands),

    /// Replication management
    #[command(subcommand)]
    Replication(ReplicationCommands),

    /// Metrics and monitoring
    #[command(subcommand)]
    Metrics(MetricsCommands),

    /// Database operations
    #[command(subcommand)]
    Database(DatabaseCommands),

    /// Start HTTP server for data and metrics
    Serve {
        /// Port to listen on
        #[arg(short, long, default_value = "8080")]
        port: u16,
        /// Host to bind to
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        /// Enable Prometheus metrics endpoint
        #[arg(long)]
        prometheus: bool,
        /// Enable CORS for web dashboards
        #[arg(long)]
        cors: bool,
        /// Enable real-time WebSocket connections
        #[arg(long)]
        websockets: bool,
        /// Port to serve gRPC on (for Admin & Raft)
        #[arg(long, default_value = "50051")]
        grpc_port: u16,
        /// Public gRPC address advertised to smart clients and metadata consumers
        #[arg(long, env = "PRKDB_ADVERTISED_GRPC_ADDR")]
        advertised_grpc_address: Option<String>,
        /// Node ID for Raft cluster
        #[arg(long, default_value = "1")]
        id: u64,
        /// Peers in the cluster (format: "id=host:port,id=host:port")
        #[arg(long)]
        peers: Option<String>,
        /// Number of partitions for default collection creation
        #[arg(long, default_value = "16")]
        num_partitions: usize,
    },

    // --- Data Commands (via prkdb-client) ---
    /// Get a value by key
    Get(data::GetArgs),

    /// Put a key-value pair
    Put(data::PutArgs),

    /// Delete a key
    Delete(data::DeleteArgs),

    /// Batch put from file
    BatchPut(data::BatchPutArgs),

    /// Subscribe to real-time updates
    Subscribe(subscribe::SubscribeArgs),

    /// Backup database (Offline)
    Backup(backup::BackupArgs),

    /// Restore database (Offline)
    Restore(backup::RestoreArgs),

    /// Generate cross-language SDK clients from schemas
    Codegen(codegen::CodegenArgs),

    /// Schema registry management
    Schema(schema::SchemaArgs),
}

#[derive(clap::ValueEnum, Clone)]
pub enum OutputFormat {
    Table,
    Json,
    Yaml,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize logging based on verbosity
    // Initialize logging based on verbosity or RUST_LOG env var
    if cli.verbose || std::env::var("RUST_LOG").is_ok() {
        tracing_subscriber::fmt::init();
    } else if cli.verbose {
        // Fallback if RUST_LOG not set but verbose is on (though fmt::init handles defaults)
        tracing_subscriber::fmt::init();
    }

    // Execute command
    match &cli.command {
        Commands::Collection(cmd) => collection::execute(cmd.clone(), &cli).await,
        Commands::Consumer(cmd) => consumer::execute(cmd.clone(), &cli).await,
        Commands::Partition(cmd) => partition::execute(cmd.clone(), &cli).await,
        Commands::Replication(cmd) => replication::execute(cmd.clone(), &cli).await,
        Commands::Metrics(cmd) => {
            init_database_manager(&cli.database, None);
            metrics::execute(cmd.clone(), &cli).await
        }
        Commands::Database(cmd) => {
            init_database_manager(&cli.database, None);
            database::execute(cmd.clone(), &cli).await
        }
        Commands::Serve {
            port,
            grpc_port,
            host,
            prometheus,
            cors,
            websockets,
            id,
            peers,
            advertised_grpc_address,
            num_partitions,
        } => {
            let advertised_http_address = std::env::var("PRKDB_ADVERTISED_HTTP_ADDR")
                .ok()
                .filter(|value| !value.trim().is_empty());
            let peer_advertised_grpc_addresses = std::env::var("PRKDB_PEER_ADVERTISED_GRPC_ADDRS")
                .ok()
                .filter(|value| !value.trim().is_empty());
            let peer_http_addresses = std::env::var("PRKDB_PEER_HTTP_ADDRS")
                .ok()
                .filter(|value| !value.trim().is_empty());
            let raft_options = if let Some(peers_str) = peers {
                let peers_vec = parse_peer_nodes(peers_str)?;
                let listen_addr = format!("{}:{}", host, grpc_port)
                    .parse::<SocketAddr>()
                    .map_err(|error| {
                        anyhow::anyhow!(
                            "Invalid serve listen address '{}:{}': {}",
                            host,
                            grpc_port,
                            error
                        )
                    })?;

                Some(database_manager::RaftOptions {
                    node_id: *id,
                    listen_addr,
                    peers: peers_vec,
                    num_partitions: *num_partitions,
                })
            } else {
                None
            };

            let peers_for_serve = raft_options
                .as_ref()
                .map(|r| r.peers.clone())
                .unwrap_or_default();

            init_database_manager(&cli.database, raft_options);
            let args = commands::serve::ServeArgs {
                port: *port,
                grpc_port: *grpc_port,
                host: host.clone(),
                prometheus: *prometheus,
                cors: *cors,
                websockets: *websockets,
                id: *id,
                peers: peers_for_serve,
                advertised_grpc_address: advertised_grpc_address.clone(),
                advertised_http_address,
                peer_advertised_grpc_addresses,
                peer_http_addresses,
            };
            commands::serve::handle_serve(args).await
        }
        // Client commands - DO NOT init database manager (pure remote)
        Commands::Get(args) => data::handle_get(args.clone()).await,
        Commands::Put(args) => data::handle_put(args.clone()).await,
        Commands::Delete(args) => data::handle_delete(args.clone()).await,
        Commands::BatchPut(args) => data::handle_batch_put(args.clone()).await,
        Commands::Subscribe(args) => subscribe::handle_subscribe(args.clone()).await,

        // Backup/Restore commands (Offline)
        Commands::Backup(args) => backup::handle_backup(args.clone(), &cli).await,
        Commands::Restore(args) => backup::handle_restore(args.clone()).await,

        // Codegen command (pure remote)
        Commands::Codegen(args) => codegen::handle_codegen(args.clone()).await,

        // Schema command (pure remote)
        Commands::Schema(args) => schema::handle_schema(args.clone()).await,
    }
}

fn parse_peer_nodes(peers: &str) -> anyhow::Result<Vec<(u64, SocketAddr)>> {
    let mut parsed = Vec::new();

    for entry in peers
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (node_id_str, address_str) = entry
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("Invalid peer entry '{entry}'"))?;
        let node_id = node_id_str
            .parse::<u64>()
            .map_err(|error| anyhow::anyhow!("Invalid peer node ID in '{entry}': {}", error))?;
        let address = address_str.parse::<SocketAddr>().map_err(|error| {
            anyhow::anyhow!("Invalid peer socket address in '{entry}': {}", error)
        })?;
        parsed.push((node_id, address));
    }

    if parsed.is_empty() {
        anyhow::bail!("At least one valid peer must be provided when --peers is set");
    }

    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_peer_nodes_rejects_invalid_entries() {
        let error = parse_peer_nodes("2=127.0.0.1:not-a-port")
            .unwrap_err()
            .to_string();

        assert!(error.contains("2=127.0.0.1:not-a-port"));
    }
}
