use crate::commands::DatabaseCommands;
use crate::output::{display_single, info, success};
use crate::uptime_tracker::{format_uptime, get_uptime_seconds, record_startup_time};
use crate::Cli;
use anyhow::Result;
use prkdb::prelude::*;
use prkdb_storage_sled::SledAdapter;
use prkdb_types::storage::StorageAdapter;
use serde::Serialize;
use std::fs;

#[derive(Serialize)]
struct DatabaseInfo {
    path: String,
    size_bytes: u64,
    collections: u32,
    total_items: u64,
    version: String,
    uptime: String,
}

#[derive(Serialize)]
struct HealthStatus {
    status: String,
    database: String,
    storage: String,
    partitioning: String,
    replication: String,
    metrics: String,
    timestamp: String,
}

pub async fn execute(cmd: DatabaseCommands, cli: &Cli) -> Result<()> {
    match cmd {
        DatabaseCommands::Info => show_database_info(cli).await,
        DatabaseCommands::Health => show_health_status(cli).await,
        DatabaseCommands::Compact => compact_database(cli).await,
        DatabaseCommands::Backup { path } => backup_database(&path, cli).await,
    }
}

async fn show_database_info(cli: &Cli) -> Result<()> {
    info("Gathering database information...");

    // Calculate total size of database directory (Sled stores as directory)
    let db_size = if cli.database.is_dir() {
        // Sum all files in the database directory
        let mut total_size = 0u64;
        if let Ok(entries) = fs::read_dir(&cli.database) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    total_size += metadata.len();
                }
            }
        }
        total_size
    } else {
        fs::metadata(&cli.database).map(|m| m.len()).unwrap_or(0)
    };

    // Create database instance to scan real storage data
    let storage = SledAdapter::open(&cli.database)?;

    // Record startup time and get uptime
    record_startup_time(&storage).await.unwrap_or(());
    let uptime_seconds = get_uptime_seconds(&storage).await.unwrap_or(0);
    let uptime_formatted = format_uptime(uptime_seconds);

    // Scan all keys directly from storage to get real collection count and items
    let all_entries = storage.scan_prefix(b"").await?;

    // Group by collection prefix to count unique collections (exclude metadata)
    let mut collection_prefixes = std::collections::HashSet::new();
    for (key, _) in &all_entries {
        let key_str = String::from_utf8_lossy(key);
        // Skip metadata keys
        if !key_str.starts_with("__prkdb_metadata:") {
            if let Some(prefix) = key_str.split(':').next() {
                collection_prefixes.insert(prefix.to_string());
            }
        }
    }

    // Count non-metadata items
    let total_items = all_entries
        .iter()
        .filter(|(key, _)| !String::from_utf8_lossy(key).starts_with("__prkdb_metadata:"))
        .count() as u64;

    let info = DatabaseInfo {
        path: cli.database.display().to_string(),
        size_bytes: db_size,
        collections: collection_prefixes.len() as u32,
        total_items,
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime: uptime_formatted,
    };

    let json_value = serde_json::to_value(&info)?;
    display_single(&json_value, cli)?;
    Ok(())
}

async fn show_health_status(cli: &Cli) -> Result<()> {
    info("Checking system health...");

    // Try to determine actual health by checking database connectivity
    let database_status = match fs::metadata(&cli.database) {
        Ok(_) => "Connected",
        Err(_) => "Disconnected",
    };

    let health = HealthStatus {
        status: if database_status == "Connected" {
            "Healthy"
        } else {
            "Degraded"
        }
        .to_string(),
        database: database_status.to_string(),
        storage: "Unknown".to_string(), // Would check actual storage health
        partitioning: "Unknown".to_string(), // Would check partitioning status
        replication: "Unknown".to_string(), // Would check replication status
        metrics: "Unknown".to_string(), // Would check metrics system status
        timestamp: chrono::Utc::now().to_rfc3339(),
    };

    let json_value = serde_json::to_value(&health)?;
    display_single(&json_value, cli)?;
    Ok(())
}

async fn compact_database(cli: &Cli) -> Result<()> {
    info("Starting database compaction...");

    // Create database instance to perform real compaction
    let storage = SledAdapter::open(&cli.database)?;
    let db = PrkDb::builder().with_storage(storage).build()?;

    match db.compact_database().await {
        Ok(space_saved) => {
            success(&format!(
                "Database compaction completed. Space saved: {} bytes",
                space_saved
            ));
        }
        Err(e) => {
            return Err(anyhow::anyhow!("Compaction failed: {}", e));
        }
    }

    Ok(())
}

async fn backup_database(backup_path: &str, cli: &Cli) -> Result<()> {
    info(&format!("Creating backup at: {}", backup_path));

    // Create database instance to perform real backup
    let storage = SledAdapter::open(&cli.database)?;
    let db = PrkDb::builder().with_storage(storage).build()?;

    match db.backup_database(backup_path).await {
        Ok(()) => {
            success(&format!("Database backed up to: {}", backup_path));
        }
        Err(e) => {
            // Fall back to basic file copy if storage-specific backup fails
            info("Storage-specific backup not available, using basic file copy...");

            // Basic file copy as fallback
            if let Err(copy_err) = std::fs::copy(&cli.database, backup_path) {
                return Err(anyhow::anyhow!(
                    "Backup failed: {} (fallback also failed: {})",
                    e,
                    copy_err
                ));
            }

            success(&format!(
                "Database backed up to: {} (using basic copy)",
                backup_path
            ));
        }
    }

    Ok(())
}
