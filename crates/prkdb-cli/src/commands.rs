use clap::Subcommand;

pub mod collection;
pub mod consumer;
pub mod database;
pub mod metrics;
pub mod partition;
pub mod replication;
pub mod serve;

#[derive(Subcommand, Clone)]
pub enum CollectionCommands {
    /// List all collections
    List,
    /// Show collection details
    #[command(alias = "desc")]
    Describe { name: String },
    /// Count items in collection
    Count { name: String },
    /// Sample items from collection
    Sample {
        name: String,
        #[arg(short, long, default_value = "10")]
        limit: usize,
    },
    /// Browse collection data with pagination
    Data {
        name: String,
        #[arg(short, long, default_value = "20")]
        limit: usize,
        #[arg(short, long, default_value = "0")]
        offset: usize,
        #[arg(long)]
        filter: Option<String>,
        #[arg(long)]
        sort: Option<String>,
    },
}

#[derive(Subcommand, Clone)]
pub enum ConsumerCommands {
    /// List consumer groups
    List,
    /// Show consumer group details
    #[command(alias = "desc")]
    Describe { group: String },
    /// Reset consumer group offset
    Reset {
        group: String,
        #[arg(short, long)]
        offset: Option<u64>,
        #[arg(long)]
        earliest: bool,
        #[arg(long)]
        latest: bool,
    },
    /// Show consumer lag
    Lag {
        #[arg(short, long)]
        group: Option<String>,
    },
}

#[derive(Subcommand, Clone)]
pub enum PartitionCommands {
    /// List partitions
    List {
        #[arg(short, long)]
        collection: Option<String>,
    },
    /// Show partition details
    #[command(alias = "desc")]
    Describe { collection: String, partition: u32 },
    /// Show partition assignment
    Assignment {
        #[arg(short, long)]
        group: Option<String>,
    },
    /// Trigger rebalancing
    Rebalance { group: String },
}

#[derive(Subcommand, Clone)]
pub enum ReplicationCommands {
    /// Show replication status
    Status,
    /// List replication nodes
    Nodes,
    /// Show replication lag
    Lag,
    /// Start replication manager
    Start {
        #[arg(short, long)]
        config: String,
    },
}

#[derive(Subcommand, Clone)]
pub enum MetricsCommands {
    /// Show current metrics
    Show,
    /// Show partition metrics
    Partition {
        #[arg(short, long)]
        collection: Option<String>,
    },
    /// Show consumer metrics
    Consumer {
        #[arg(short, long)]
        group: Option<String>,
    },
    /// Reset metrics
    Reset,
}

#[derive(Subcommand, Clone)]
pub enum DatabaseCommands {
    /// Show database info
    Info,
    /// Health check
    Health,
    /// Compact database
    Compact,
    /// Backup database
    Backup { path: String },
}
