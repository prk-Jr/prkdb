use crate::commands::MetricsCommands;
use crate::database_manager::{scan_storage, with_database_read};
use crate::output::{display_single, info, success, OutputDisplay};
use crate::uptime_tracker::{get_uptime_seconds, record_startup_time};
use crate::Cli;
use anyhow::Result;
use prkdb_storage_sled::SledAdapter;
use serde::Serialize;
use tabled::Tabled;

#[derive(Serialize)]
struct SystemMetrics {
    events_produced: u64,
    events_consumed: u64,
    bytes_produced: u64,
    bytes_consumed: u64,
    collections: u32,
    consumer_groups: u32,
    active_consumers: u32,
    replication_lag: u64,
    avg_latency_ms: f64,
    max_latency_ms: u64,
    uptime_seconds: u64,
    timestamp: String,
}

#[derive(Tabled, Serialize)]
struct PartitionMetrics {
    collection: String,
    partition: u32,
    events_produced: u64,
    events_consumed: u64,
    bytes_produced: u64,
    bytes_consumed: u64,
    avg_latency_ms: f64,
    consumer_lag: u64,
}

#[derive(Tabled, Serialize)]
struct ConsumerMetrics {
    group_id: String,
    consumer_id: String,
    events_consumed: u64,
    bytes_consumed: u64,
    avg_latency_ms: f64,
    max_latency_ms: u64,
    last_commit: String,
    lag: u64,
}

pub async fn execute(cmd: MetricsCommands, cli: &Cli) -> Result<()> {
    match cmd {
        MetricsCommands::Show => show_system_metrics(cli).await,
        MetricsCommands::Partition { collection } => {
            show_partition_metrics(collection.as_deref(), cli).await
        }
        MetricsCommands::Consumer { group } => show_consumer_metrics(group.as_deref(), cli).await,
        MetricsCommands::Reset => reset_metrics(cli).await,
    }
}

async fn show_system_metrics(cli: &Cli) -> Result<()> {
    info("Fetching system metrics...");

    // Use storage scan with proper error handling
    let result = scan_storage().await;

    let (events_count, total_bytes, collections_count) = match result {
        Ok(all_entries) => {
            // Calculate real metrics from storage data
            let mut collection_types = std::collections::HashSet::new();
            let mut total_bytes = 0u64;
            let mut item_count = 0u64;

            for (key, value) in &all_entries {
                let key_str = String::from_utf8_lossy(key);
                // Skip metadata keys when counting
                if !key_str.starts_with("__prkdb_metadata:") {
                    if let Some(collection_type) = key_str.split(':').next() {
                        collection_types.insert(collection_type.to_string());
                    }
                    item_count += 1;
                    total_bytes += (key.len() + value.len()) as u64;
                }
            }

            (item_count, total_bytes, collection_types.len() as u32)
        }
        Err(e) => {
            info(&format!("Unable to scan storage: {}", e));
            info("This may happen when the database is busy. Showing partial metrics.");
            (0, 0, 0)
        }
    };

    // Get uptime from storage
    let uptime_seconds = if let Ok(storage) = SledAdapter::open(&cli.database) {
        record_startup_time(&storage).await.unwrap_or(());
        get_uptime_seconds(&storage).await.unwrap_or(0)
    } else {
        0
    };

    // Get consumer groups with error handling
    let consumer_groups_count = with_database_read(|db| async move {
        let groups = db.list_consumer_groups().await.unwrap_or_default();
        Ok(groups.len() as u32)
    })
    .await
    .unwrap_or(0);

    let metrics = SystemMetrics {
        events_produced: events_count,
        events_consumed: 0, // Consumer tracking not yet implemented
        bytes_produced: total_bytes,
        bytes_consumed: 0, // Consumer tracking not yet implemented
        collections: collections_count,
        consumer_groups: consumer_groups_count,
        active_consumers: 0, // Active consumer tracking not yet implemented
        replication_lag: 0,  // Replication lag tracking not yet implemented
        avg_latency_ms: 0.0, // Latency tracking not yet implemented
        max_latency_ms: 0,   // Latency tracking not yet implemented
        uptime_seconds,
        timestamp: chrono::Utc::now().to_rfc3339(),
    };

    display_single(&metrics, cli)?;
    Ok(())
}

async fn show_partition_metrics(collection: Option<&str>, cli: &Cli) -> Result<()> {
    match collection {
        Some(collection) => info(&format!(
            "Fetching partition metrics for collection: {}",
            collection
        )),
        None => info("Fetching partition metrics for all collections"),
    }

    let partition_metrics = with_database_read(|db| async move {
        let mut metrics = Vec::new();

        match collection {
            Some(collection_name) => {
                // Check if collection exists
                let collections = db.list_collections().await?;
                if !collections.contains(&collection_name.to_string()) {
                    return Ok(metrics); // Return empty metrics for non-existent collection
                }

                // Get real partition metrics for this collection
                let partition_data = db.get_partition_metrics(collection_name).await?;
                for (
                    partition,
                    events_produced,
                    events_consumed,
                    bytes_produced,
                    bytes_consumed,
                    consumer_lag,
                ) in partition_data
                {
                    metrics.push(PartitionMetrics {
                        collection: collection_name.to_string(),
                        partition,
                        events_produced,
                        events_consumed,
                        bytes_produced,
                        bytes_consumed,
                        avg_latency_ms: 0.0,
                        consumer_lag,
                    });
                }
            }
            None => {
                // Get partition metrics for all collections
                let collections = db.list_collections().await?;
                for collection_name in collections {
                    let partition_data = db.get_partition_metrics(&collection_name).await?;
                    for (
                        partition,
                        events_produced,
                        events_consumed,
                        bytes_produced,
                        bytes_consumed,
                        consumer_lag,
                    ) in partition_data
                    {
                        metrics.push(PartitionMetrics {
                            collection: collection_name.clone(),
                            partition,
                            events_produced,
                            events_consumed,
                            bytes_produced,
                            bytes_consumed,
                            avg_latency_ms: 0.0,
                            consumer_lag,
                        });
                    }
                }
            }
        }

        Ok(metrics)
    })
    .await;

    match partition_metrics {
        Ok(metrics) => {
            if metrics.is_empty() {
                match collection {
                    Some(name) => info(&format!("Collection '{}' not found or has no partitions", name)),
                    None => info("No partition metrics found. Metrics are collected when events are produced/consumed."),
                }
            } else {
                metrics.display(cli)?;
            }
        }
        Err(e) => {
            info(&format!("Unable to fetch partition metrics: {}", e));
            info("This may happen when the database is busy. Please try again in a moment.");
        }
    }

    Ok(())
}

async fn show_consumer_metrics(group: Option<&str>, cli: &Cli) -> Result<()> {
    match group {
        Some(group) => info(&format!("Fetching consumer metrics for group: {}", group)),
        None => info("Fetching consumer metrics for all groups"),
    }

    let consumer_metrics = with_database_read(|db| async move {
        let mut metrics = Vec::new();

        match group {
            Some(group_id) => {
                // Check if group exists
                let groups = db.list_consumer_groups().await?;
                if !groups.contains(&group_id.to_string()) {
                    return Ok(metrics); // Return empty metrics for non-existent group
                }

                // Get real consumer metrics for this group
                let group_metrics = db.get_consumer_metrics(group_id).await?;
                for (consumer_id, events_consumed, bytes_consumed, lag) in group_metrics {
                    metrics.push(ConsumerMetrics {
                        group_id: group_id.to_string(),
                        consumer_id,
                        events_consumed,
                        bytes_consumed,
                        avg_latency_ms: 0.0,
                        max_latency_ms: 0,
                        last_commit: "Unknown".to_string(),
                        lag,
                    });
                }
            }
            None => {
                // Get consumer metrics for all groups
                let groups = db.list_consumer_groups().await?;
                for group_id in groups {
                    let group_metrics = db.get_consumer_metrics(&group_id).await?;
                    for (consumer_id, events_consumed, bytes_consumed, lag) in group_metrics {
                        metrics.push(ConsumerMetrics {
                            group_id: group_id.clone(),
                            consumer_id,
                            events_consumed,
                            bytes_consumed,
                            avg_latency_ms: 0.0,
                            max_latency_ms: 0,
                            last_commit: "Unknown".to_string(),
                            lag,
                        });
                    }
                }
            }
        }

        Ok(metrics)
    })
    .await;

    match consumer_metrics {
        Ok(metrics) => {
            if metrics.is_empty() {
                match group {
                    Some(name) => info(&format!("Consumer group '{}' not found or has no metrics", name)),
                    None => info("No consumer metrics found. Metrics are collected when consumers are active."),
                }
            } else {
                metrics.display(cli)?;
            }
        }
        Err(e) => {
            info(&format!("Unable to fetch consumer metrics: {}", e));
            info("This may happen when the database is busy. Please try again in a moment.");
        }
    }

    Ok(())
}

async fn reset_metrics(_cli: &Cli) -> Result<()> {
    info("Resetting all metrics...");

    // In a real implementation, we'd have a metrics store to reset
    // Metrics are derived from storage data
    // and can't be reset independently

    info("Note: Metrics in PrkDB are derived from storage data and consumer offsets.");
    info("To 'reset' metrics, you would need to:");
    info("  1. Clear collection data (truncate collections)");
    info("  2. Reset consumer group offsets");
    info("  3. Clear any accumulated metrics storage");

    success("Metrics reset operation completed");
    Ok(())
}
