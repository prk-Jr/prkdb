use crate::commands::PartitionCommands;
use crate::output::{display_single, info, success, OutputDisplay};
use crate::Cli;
use anyhow::Result;
use prkdb_client::PrkDbClient;
use serde::Serialize;
use tabled::Tabled;

#[derive(Tabled, Serialize)]
struct PartitionInfo {
    collection: String,
    partition: u32,
    size_bytes: u64,
    items: u64,
    assigned_to: String,
    status: String,
}

#[derive(Tabled, Serialize)]
struct PartitionAssignmentInfo {
    group_id: String,
    consumer_id: String,
    collection: String,
    partition: u32,
    current_offset: u64,
    lag: u64,
}

#[derive(Serialize)]
struct PartitionDetails {
    collection: String,
    partition: u32,
    size_bytes: u64,
    items: u64,
    first_offset: u64,
    last_offset: u64,
    assigned_consumers: Vec<String>,
    metrics: PartitionMetrics,
}

#[derive(Serialize)]
struct PartitionMetrics {
    events_produced: u64,
    events_consumed: u64,
    bytes_produced: u64,
    bytes_consumed: u64,
    avg_latency_ms: f64,
    max_latency_ms: u64,
}

pub async fn execute(cmd: PartitionCommands, cli: &Cli) -> Result<()> {
    if cli.local {
        // Local mode: use embedded database
        crate::init_database_manager(&cli.database, None);
        return execute_local(cmd, cli).await;
    }

    // Remote mode: use prkdb-client
    match cmd {
        PartitionCommands::List { collection } => list_partitions(collection.as_deref(), cli).await,
        PartitionCommands::Describe {
            collection,
            partition,
        } => describe_partition(&collection, partition, cli).await,
        PartitionCommands::Assignment { group } => {
            show_partition_assignment(group.as_deref(), cli).await
        }
        PartitionCommands::Rebalance { group } => rebalance_partitions(&group, cli).await,
    }
}

async fn execute_local(cmd: PartitionCommands, _cli: &Cli) -> Result<()> {
    use crate::database_manager::with_database_read;
    use crate::output::info;

    match cmd {
        PartitionCommands::List { collection } => {
            info(&format!(
                "Listing partitions (local mode){}",
                collection
                    .as_ref()
                    .map(|c| format!(" for collection: {}", c))
                    .unwrap_or_default()
            ));
            let partitions = with_database_read(|db| {
                let coll = collection.clone();
                async move {
                    match coll {
                        Some(name) => db
                            .get_partitions(&name)
                            .await
                            .map_err(|e| anyhow::anyhow!("{}", e)),
                        None => {
                            let collections = db
                                .list_collections()
                                .await
                                .map_err(|e| anyhow::anyhow!("{}", e))?;
                            let mut all = Vec::new();
                            for c in collections {
                                if let Ok(parts) = db.get_partitions(&c).await {
                                    for (p, items, size) in parts {
                                        all.push((c.clone(), p, items, size));
                                    }
                                }
                            }
                            Ok(all
                                .into_iter()
                                .map(|(_, p, items, size)| (p, items, size))
                                .collect())
                        }
                    }
                }
            })
            .await?;
            for (partition, items, size) in partitions {
                println!(
                    "  partition: {}, items: {}, size: {} bytes",
                    partition, items, size
                );
            }
            Ok(())
        }
        PartitionCommands::Describe {
            collection,
            partition,
        } => {
            info(&format!(
                "Describing partition {} of {} (local mode)",
                partition, collection
            ));
            let details = with_database_read(|db| {
                let c = collection.clone();
                async move {
                    db.get_partition_details(&c, partition)
                        .await
                        .map_err(|e| anyhow::anyhow!("{}", e))
                }
            })
            .await?;
            if let Some((items, size, first, last)) = details {
                println!("Partition: {}", partition);
                println!("Items: {}, Size: {} bytes", items, size);
                println!("Offsets: {} - {}", first, last);
            } else {
                info("Partition not found.");
            }
            Ok(())
        }
        PartitionCommands::Assignment { group } => {
            info("Showing partition assignments (local mode)...");
            let assignments = with_database_read(|db| {
                let g = group.clone();
                async move {
                    db.get_partition_assignments(g.as_deref())
                        .await
                        .map_err(|e| anyhow::anyhow!("{}", e))
                }
            })
            .await?;
            for (group_id, consumer, coll, part, offset, lag) in assignments {
                println!(
                    "  {}: {} -> {}:{} (offset: {}, lag: {})",
                    group_id, consumer, coll, part, offset, lag
                );
            }
            Ok(())
        }
        PartitionCommands::Rebalance { group } => {
            info(&format!(
                "Rebalance not implemented in local mode for group: {}",
                group
            ));
            Ok(())
        }
    }
}

async fn create_client(cli: &Cli) -> Result<PrkDbClient> {
    let client = PrkDbClient::new(vec![cli.server.clone()]).await?;
    let client = if let Some(token) = &cli.admin_token {
        client.with_admin_token(token)
    } else {
        client
    };
    Ok(client)
}

async fn list_partitions(collection: Option<&str>, cli: &Cli) -> Result<()> {
    match collection {
        Some(collection) => {
            info(&format!(
                "Listing partitions for collection: {}",
                collection
            ));
        }
        None => {
            info("Listing all partitions");
        }
    }

    let client = create_client(cli).await?;
    let partitions = client.list_partitions(collection).await?;

    if partitions.is_empty() {
        match collection {
            Some(name) => info(&format!(
                "Collection '{}' not found or has no partitions",
                name
            )),
            None => info(
                "No partitions found. Partitions are created when data is added to collections.",
            ),
        }
    } else {
        let display_partitions: Vec<PartitionInfo> = partitions
            .into_iter()
            .map(|p| PartitionInfo {
                collection: p.collection,
                partition: p.partition,
                size_bytes: p.size_bytes,
                items: p.items,
                assigned_to: p.assigned_to,
                status: p.status,
            })
            .collect();
        display_partitions.display(cli)?;
    }

    Ok(())
}

async fn describe_partition(collection: &str, partition: u32, cli: &Cli) -> Result<()> {
    info(&format!(
        "Describing partition {} of collection: {}",
        partition, collection
    ));

    let client = create_client(cli).await?;
    let partitions = client.list_partitions(Some(collection)).await?;

    let target_partition = partitions.into_iter().find(|p| p.partition == partition);

    match target_partition {
        Some(p) => {
            // Get partition assignments for this partition
            let assignments = client.get_partition_assignments(None).await?;
            let assigned_consumers: Vec<String> = assignments
                .iter()
                .filter(|a| a.collection == collection && a.partition == partition)
                .map(|a| a.consumer_id.clone())
                .collect();

            let details = PartitionDetails {
                collection: p.collection,
                partition: p.partition,
                size_bytes: p.size_bytes,
                items: p.items,
                first_offset: 0,
                last_offset: p.items,
                assigned_consumers,
                metrics: PartitionMetrics {
                    events_produced: p.items,
                    events_consumed: p.items,
                    bytes_produced: p.size_bytes,
                    bytes_consumed: p.size_bytes,
                    avg_latency_ms: 0.0,
                    max_latency_ms: 0,
                },
            };

            display_single(&details, cli)?;
        }
        None => {
            info(&format!(
                "Collection '{}' not found or partition {} does not exist",
                collection, partition
            ));
        }
    }

    Ok(())
}

async fn show_partition_assignment(group: Option<&str>, cli: &Cli) -> Result<()> {
    match group {
        Some(group) => {
            info(&format!(
                "Showing partition assignment for group: {}",
                group
            ));
        }
        None => info("Showing partition assignments for all groups"),
    }

    let client = create_client(cli).await?;
    let assignments = client.get_partition_assignments(group).await?;

    if assignments.is_empty() {
        match group {
            Some(name) => info(&format!(
                "Consumer group '{}' not found or has no assignments",
                name
            )),
            None => info("No partition assignments found. Assignments are created when consumers join groups."),
        }
    } else {
        let display_assignments: Vec<PartitionAssignmentInfo> = assignments
            .into_iter()
            .map(|a| PartitionAssignmentInfo {
                group_id: a.group_id,
                consumer_id: a.consumer_id,
                collection: a.collection,
                partition: a.partition,
                current_offset: a.current_offset,
                lag: a.lag,
            })
            .collect();
        display_assignments.display(cli)?;
    }

    Ok(())
}

async fn rebalance_partitions(group: &str, cli: &Cli) -> Result<()> {
    info(&format!(
        "Triggering rebalance for consumer group: {}",
        group
    ));

    let client = create_client(cli).await?;

    // Check if group exists by listing consumer groups
    let groups = client.list_consumer_groups().await?;
    let group_exists = groups.iter().any(|g| g.group_id == group);

    if !group_exists {
        info(&format!("Consumer group '{}' not found", group));
        return Ok(());
    }

    // Get current state
    let group_info = client.describe_consumer_group(group).await?;
    let assignments = client.get_partition_assignments(Some(group)).await?;

    info(&format!("Current state for group '{}':", group));
    info(&format!("  Active consumers: {}", group_info.members.len()));
    info(&format!("  Current assignments: {}", assignments.len()));

    if group_info.members.is_empty() {
        info("No active consumers to rebalance partitions among.");
    } else if assignments.is_empty() {
        info("No partition assignments to rebalance.");
    } else {
        info("Rebalancing would redistribute partitions among active consumers.");
        info("Note: Automatic rebalancing is not yet implemented via remote RPC.");
    }

    success(&format!(
        "Rebalance analysis completed for group: {}",
        group
    ));

    Ok(())
}
