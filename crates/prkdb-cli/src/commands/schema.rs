use clap::{Args, Subcommand, ValueEnum};
use prkdb_client::{CompatibilityMode, PrkDbClient};
use std::io::Write;
use std::path::PathBuf;
use tokio::fs;

#[derive(Args, Clone)]
pub struct SchemaArgs {
    /// Server address
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    pub server: String,

    #[command(subcommand)]
    pub command: SchemaCommands,
}

#[derive(Subcommand, Clone)]
pub enum SchemaCommands {
    /// Register a schema
    Register {
        /// Collection name
        #[arg(long)]
        collection: String,

        /// Path to proto file
        #[arg(long)]
        proto: PathBuf,

        /// Compatibility mode
        #[arg(long, value_enum, default_value = "backward")]
        compatibility: CompatibilityModeArg,

        /// Migration ID (if needed for breaking changes)
        #[arg(long)]
        migration_id: Option<String>,
    },

    /// Get a schema
    Get {
        /// Collection name
        #[arg(long)]
        collection: String,

        /// Version (optional, defaults to latest)
        #[arg(long)]
        version: Option<u32>,
    },

    /// List all schemas
    List,

    /// Check compatibility
    Check {
        /// Collection name
        #[arg(long)]
        collection: String,

        /// Path to proto file
        #[arg(long)]
        proto: PathBuf,
    },
}

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq)]
pub enum CompatibilityModeArg {
    None,
    Backward,
    Forward,
    Full,
}

impl From<CompatibilityModeArg> for CompatibilityMode {
    fn from(arg: CompatibilityModeArg) -> Self {
        match arg {
            CompatibilityModeArg::None => CompatibilityMode::CompatibilityNone,
            CompatibilityModeArg::Backward => CompatibilityMode::CompatibilityBackward,
            CompatibilityModeArg::Forward => CompatibilityMode::CompatibilityForward,
            CompatibilityModeArg::Full => CompatibilityMode::CompatibilityFull,
        }
    }
}

pub async fn handle_schema(args: SchemaArgs) -> anyhow::Result<()> {
    // Connect to server
    let client = PrkDbClient::new(vec![args.server.clone()]).await?;

    match args.command {
        SchemaCommands::Register {
            collection,
            proto,
            compatibility,
            migration_id,
        } => {
            println!("📝 Registering schema for collection '{}'", collection);
            let schema_bytes = fs::read(&proto).await?;
            let version = client
                .register_schema(
                    &collection,
                    schema_bytes,
                    compatibility.into(),
                    migration_id,
                )
                .await?;
            println!("✅ Registered schema version {}", version);
        }
        SchemaCommands::Get {
            collection,
            version,
        } => {
            let schema_bytes = client.get_schema(&collection, version).await?;
            std::io::stdout().write_all(&schema_bytes)?;
        }
        SchemaCommands::List => {
            let schemas = client.list_schemas().await?;
            println!("📋 Registered Schemas:");
            for info in schemas {
                println!(
                    "  - {}: (latest version: {})",
                    info.collection, info.latest_version
                );
            }
        }
        SchemaCommands::Check { collection, proto } => {
            println!("Checking compatibility for '{}'", collection);
            let schema_bytes = fs::read(&proto).await?;
            let compatible = client
                .check_compatibility(&collection, schema_bytes)
                .await?;
            if compatible {
                println!("✅ Schema is compatible");
            } else {
                println!("❌ Schema is NOT compatible");
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
