use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod collection_metadata;
mod commands;
mod database_manager;
mod output;
mod uptime_tracker;

use commands::*;
use database_manager::init_database_manager;

#[derive(Parser)]
#[command(
    name = "prkdb",
    about = "PrkDB - A reactive database with compute facilities",
    version = "0.1.0",
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

    /// Bootstrap server address (e.g. http://127.0.0.1:50051)
    #[arg(long, default_value = "http://127.0.0.1:50051", env = "PRKDB_SERVER")]
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
    if cli.verbose {
        tracing_subscriber::fmt::init();
    }

    // Execute command
    match &cli.command {
        Commands::Collection(cmd) => collection::execute(cmd.clone(), &cli).await,
        Commands::Consumer(cmd) => consumer::execute(cmd.clone(), &cli).await,
        Commands::Partition(cmd) => partition::execute(cmd.clone(), &cli).await,
        Commands::Replication(cmd) => replication::execute(cmd.clone(), &cli).await,
        Commands::Metrics(cmd) => {
            init_database_manager(&cli.database);
            metrics::execute(cmd.clone(), &cli).await
        }
        Commands::Database(cmd) => {
            init_database_manager(&cli.database);
            database::execute(cmd.clone(), &cli).await
        }
        Commands::Serve {
            port,
            grpc_port,
            host,
            prometheus,
            cors,
            websockets,
        } => {
            init_database_manager(&cli.database);
            let args = commands::serve::ServeArgs {
                port: *port,
                grpc_port: *grpc_port,
                host: host.clone(),
                prometheus: *prometheus,
                cors: *cors,
                websockets: *websockets,
            };
            commands::serve::handle_serve(args).await
        }
        // Client commands - DO NOT init database manager (pure remote)
        Commands::Get(args) => data::handle_get(args.clone()).await,
        Commands::Put(args) => data::handle_put(args.clone()).await,
        Commands::Delete(args) => data::handle_delete(args.clone()).await,
        Commands::BatchPut(args) => data::handle_batch_put(args.clone()).await,
    }
}
