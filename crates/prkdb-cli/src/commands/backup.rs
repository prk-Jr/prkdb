use crate::Cli;
use clap::Args;
use prkdb::storage::snapshot::{CompressionType, SnapshotReader};
use prkdb::PrkDb;
use std::path::PathBuf;

#[derive(Args, Clone, Debug)]
pub struct BackupArgs {
    /// Output snapshot file path
    #[arg(short, long)]
    pub output: PathBuf,

    /// Compression type (none, gzip)
    #[arg(short, long, default_value = "gzip")]
    pub compression: String,
}

#[derive(Args, Clone, Debug)]
pub struct RestoreArgs {
    /// Input snapshot file path
    #[arg(short, long)]
    pub input: PathBuf,

    /// Target data directory (must be empty or output to new dir)
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Overforce overwrite if directory exists and is not empty
    #[arg(long)]
    pub force: bool,
}

/// Handle offline backup (direct storage access)
pub async fn handle_backup(args: BackupArgs, cli: &Cli) -> anyhow::Result<()> {
    tracing::info!(
        "Starting offline backup using database at {:?}",
        cli.database
    );

    // 1. Open database (embedded mode)
    // Use Builder to construct PrkDb with WAL storage
    let db = PrkDb::builder()
        .with_data_dir(&cli.database)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))?;

    // 2. Parse compression
    let compression = match args.compression.to_lowercase().as_str() {
        "gzip" => CompressionType::Gzip,
        "none" => CompressionType::None,
        _ => anyhow::bail!("Invalid compression type. Supported: gzip, none"),
    };

    // 3. Take snapshot
    let offset = db
        .take_snapshot(&args.output, compression)
        .await
        .map_err(|e| anyhow::anyhow!("Snapshot failed: {}", e))?;

    println!("Backup successful!");
    println!("Output: {:?}", args.output);
    println!("Max Offset: {}", offset);

    Ok(())
}

pub async fn handle_restore(args: RestoreArgs) -> anyhow::Result<()> {
    tracing::info!("Starting restore to {:?}", args.data_dir);

    // 1. Check directory
    if args.data_dir.exists() {
        if args.data_dir.read_dir()?.next().is_some() && !args.force {
            anyhow::bail!(
                "Target directory {:?} is not empty. Use --force to overwrite.",
                args.data_dir
            );
        }
    }

    // 2. Open snapshot reader
    // Uses prkdb::storage::snapshot::SnapshotReader
    let mut reader = SnapshotReader::open(&args.input)
        .map_err(|e| anyhow::anyhow!("Failed to open snapshot: {}", e))?;

    println!("Snapshot Info:");
    println!("  Version: {}", reader.header.version);
    println!("  Entries: {}", reader.header.index_entries);
    println!("  Max Offset: {}", reader.header.max_offset);

    // 3. Open new DB at target location
    let db = PrkDb::builder()
        .with_data_dir(&args.data_dir)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create database: {}", e))?;

    // 4. Replay entries
    let mut count = 0;
    while let Some((key, val)) = reader.next_entry().map_err(|e| anyhow::anyhow!(e))? {
        db.put(&key, &val)
            .await
            .map_err(|e| anyhow::anyhow!("Restore put failed: {}", e))?;
        count += 1;
        if count % 10000 == 0 {
            tracing::info!("Restored {} entries...", count);
        }
    }

    // Force flush/checkpoint if possible via db API?
    // Currently no explicit flush in PrkDb public API, but storage usually flushes on close/write.
    // Since we just did Puts, they are WAL appended.

    println!("Restore complete! Restored {} entries.", count);

    Ok(())
}
