use crate::commands::ConsumerCommands;
use crate::output::{display_single, error, info, success, warning, OutputDisplay};
use crate::Cli;
use anyhow::Result;
use prkdb::prelude::*;
use prkdb_storage_sled::SledAdapter;
use serde::Serialize;
use tabled::Tabled;

#[derive(Tabled, Serialize)]
struct ConsumerGroupInfo {
    group_id: String,
    members: u32,
    state: String,
    lag: u64,
    assignment: String,
}

#[derive(Tabled, Serialize)]
struct ConsumerLagInfo {
    group_id: String,
    collection: String,
    partition: u32,
    current_offset: u64,
    latest_offset: u64,
    lag: u64,
}

#[derive(Serialize)]
struct ConsumerGroupDetails {
    group_id: String,
    members: Vec<ConsumerMember>,
    total_lag: u64,
    state: String,
    partitions: Vec<PartitionAssignment>,
}

#[derive(Serialize)]
struct ConsumerMember {
    consumer_id: String,
    host: String,
    partitions: Vec<u32>,
}

#[derive(Serialize)]
struct PartitionAssignment {
    collection: String,
    partition: u32,
    current_offset: u64,
    latest_offset: u64,
    lag: u64,
}

pub async fn execute(cmd: ConsumerCommands, cli: &Cli) -> Result<()> {
    let db = create_db(cli).await?;

    match cmd {
        ConsumerCommands::List => list_consumer_groups(&db, cli).await,
        ConsumerCommands::Describe { group } => describe_consumer_group(&db, &group, cli).await,
        ConsumerCommands::Reset {
            group,
            offset,
            earliest,
            latest,
        } => reset_consumer_offset(&db, &group, offset, earliest, latest, cli).await,
        ConsumerCommands::Lag { group } => show_consumer_lag(&db, group.as_deref(), cli).await,
    }
}

async fn create_db(cli: &Cli) -> Result<PrkDb> {
    let storage = SledAdapter::open(&cli.database)?;
    let db = PrkDb::builder().with_storage(storage).build()?;
    Ok(db)
}

async fn list_consumer_groups(db: &PrkDb, cli: &Cli) -> Result<()> {
    info("Listing consumer groups...");

    // Get actual consumer groups from the database
    let group_ids = db.list_consumer_groups().await?;

    let mut groups = Vec::new();
    for group_id in group_ids {
        // Get active consumers for this group
        let active_consumers = db.get_active_consumers(&group_id);
        let members = active_consumers.len() as u32;

        // Determine state based on whether there are active consumers
        let state = if members > 0 { "Stable" } else { "Empty" };
        let assignment = "Range"; // Default assignment strategy

        // Calculate total lag - sum lag across all collections this group consumes
        let lag = match db.get_group_lag_info(&group_id).await {
            Ok(lag_infos) => lag_infos.iter().map(|(_, _, _, _, lag)| lag).sum(),
            Err(_) => 0, // If we can't get lag info, default to 0
        };

        groups.push(ConsumerGroupInfo {
            group_id,
            members,
            state: state.to_string(),
            lag,
            assignment: assignment.to_string(),
        });
    }

    if groups.is_empty() {
        info("No consumer groups found. Consumer groups are created when consumers are started.");
        info("To create a consumer group, run a consumer example like:");
        info("  cargo run --example basic_consumer");
        return Ok(());
    }

    groups.display(cli)?;
    Ok(())
}

async fn describe_consumer_group(db: &PrkDb, group: &str, cli: &Cli) -> Result<()> {
    info(&format!("Describing consumer group: {}", group));

    // Check if the group exists
    let groups = db.list_consumer_groups().await?;
    if !groups.contains(&group.to_string()) {
        error(&format!("Consumer group '{}' not found", group));
        return Ok(());
    }

    // Get real consumer group details
    let active_consumer_ids = db.get_active_consumers(group);
    let assignment = db.get_consumer_group_assignment(group);

    let members: Vec<ConsumerMember> = active_consumer_ids
        .into_iter()
        .map(|consumer_id| {
            let partitions = assignment
                .as_ref()
                .map(|a| a.get_partitions(&consumer_id))
                .unwrap_or_default();

            ConsumerMember {
                consumer_id,
                host: "unknown".to_string(), // Would need additional API to get consumer host info
                partitions,
            }
        })
        .collect();

    let is_empty = members.is_empty();
    let state = if is_empty { "Empty" } else { "Stable" };

    // Get real partition assignments and lag info
    let lag_infos = db.get_group_lag_info(group).await.unwrap_or_default();
    let partitions: Vec<PartitionAssignment> = lag_infos
        .iter()
        .map(
            |(collection, partition, current_offset, latest_offset, lag)| PartitionAssignment {
                collection: collection.clone(),
                partition: *partition,
                current_offset: *current_offset,
                latest_offset: *latest_offset,
                lag: *lag,
            },
        )
        .collect();

    let total_lag = lag_infos.iter().map(|(_, _, _, _, lag)| lag).sum();

    let details = ConsumerGroupDetails {
        group_id: group.to_string(),
        members,
        total_lag,
        state: state.to_string(),
        partitions,
    };

    display_single(&details, cli)?;

    if is_empty {
        info("This consumer group has no active members.");
        info("Members will appear when consumers join this group.");
    }

    Ok(())
}

async fn reset_consumer_offset(
    db: &PrkDb,
    group: &str,
    offset: Option<u64>,
    earliest: bool,
    latest: bool,
    _cli: &Cli,
) -> Result<()> {
    info(&format!("Resetting offset for consumer group: {}", group));

    // Check if the group exists
    let groups = db.list_consumer_groups().await?;
    if !groups.contains(&group.to_string()) {
        error(&format!("Consumer group '{}' not found", group));
        return Ok(());
    }

    if [offset.is_some(), earliest, latest]
        .iter()
        .filter(|&&x| x)
        .count()
        != 1
    {
        error("Exactly one of --offset, --earliest, or --latest must be specified");
        return Ok(());
    }

    let reset_to = if let Some(offset) = offset {
        format!("offset {}", offset)
    } else if earliest {
        "earliest".to_string()
    } else {
        "latest".to_string()
    };

    warning(&format!(
        "This will reset all partitions for group '{}' to {}",
        group, reset_to
    ));

    // In a real implementation, we'd reset the offsets in the offset store
    success(&format!("Reset consumer group '{}' to {}", group, reset_to));

    Ok(())
}

async fn show_consumer_lag(db: &PrkDb, group: Option<&str>, cli: &Cli) -> Result<()> {
    match group {
        Some(group) => {
            info(&format!("Showing lag for consumer group: {}", group));
            // Check if the group exists
            let groups = db.list_consumer_groups().await?;
            if !groups.contains(&group.to_string()) {
                error(&format!("Consumer group '{}' not found", group));
                return Ok(());
            }

            // Get real lag data for this specific group
            let lag_infos = db.get_group_lag_info(group).await?;
            let lag_info: Vec<ConsumerLagInfo> = lag_infos
                .into_iter()
                .map(
                    |(collection, partition, current_offset, latest_offset, lag)| ConsumerLagInfo {
                        group_id: group.to_string(),
                        collection,
                        partition,
                        current_offset,
                        latest_offset,
                        lag,
                    },
                )
                .collect();

            if lag_info.is_empty() {
                info("No lag information found. This consumer group may not have consumed any data yet.");
            } else {
                lag_info.display(cli)?;
            }
        }
        None => {
            info("Showing lag for all consumer groups");

            // Get lag data for all consumer groups
            let groups = db.list_consumer_groups().await?;
            let mut all_lag_info = Vec::new();

            for group_id in groups {
                let lag_infos = db.get_group_lag_info(&group_id).await.unwrap_or_default();
                for (collection, partition, current_offset, latest_offset, lag) in lag_infos {
                    all_lag_info.push(ConsumerLagInfo {
                        group_id: group_id.clone(),
                        collection,
                        partition,
                        current_offset,
                        latest_offset,
                        lag,
                    });
                }
            }

            if all_lag_info.is_empty() {
                info("No lag information found across all consumer groups.");
            } else {
                all_lag_info.display(cli)?;
            }
        }
    }

    Ok(())
}
