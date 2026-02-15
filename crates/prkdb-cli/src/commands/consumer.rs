use crate::commands::ConsumerCommands;
use crate::output::{display_single, error, info, success, warning, OutputDisplay};
use crate::Cli;
use anyhow::Result;
use prkdb_client::PrkDbClient;
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
    if cli.local {
        // Local mode: use embedded database
        crate::init_database_manager(&cli.database, None);
        return execute_local(cmd, cli).await;
    }

    // Remote mode: use prkdb-client
    match cmd {
        ConsumerCommands::List => list_consumer_groups(cli).await,
        ConsumerCommands::Describe { group } => describe_consumer_group(&group, cli).await,
        ConsumerCommands::Reset {
            group,
            offset,
            earliest,
            latest,
        } => reset_consumer_offset(&group, offset, earliest, latest, cli).await,
        ConsumerCommands::Lag { group } => show_consumer_lag(group.as_deref(), cli).await,
    }
}

async fn execute_local(cmd: ConsumerCommands, _cli: &Cli) -> Result<()> {
    use crate::database_manager::with_database_read;

    match cmd {
        ConsumerCommands::List => {
            info("Listing consumer groups (local mode)...");
            let groups = with_database_read(|db| async move {
                db.list_consumer_groups()
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            })
            .await?;
            if groups.is_empty() {
                info("No consumer groups found.");
            } else {
                for group_id in groups {
                    println!("  - {}", group_id);
                }
            }
            Ok(())
        }
        ConsumerCommands::Describe { group } => {
            info(&format!(
                "Describing consumer group (local mode): {}",
                group
            ));
            let result = with_database_read(|db| {
                let group_id = group.clone();
                async move {
                    let consumers = db.get_active_consumers(&group_id);
                    let lag = db.get_group_lag_info(&group_id).await.unwrap_or_default();
                    Ok::<_, anyhow::Error>((consumers, lag))
                }
            })
            .await?;
            let (consumers, lag) = result;
            println!("Group: {}", group);
            println!("Active consumers: {}", consumers.len());
            for c in consumers {
                println!("  - {}", c);
            }
            println!(
                "Total lag: {}",
                lag.iter().map(|(_, _, _, _, l)| l).sum::<u64>()
            );
            Ok(())
        }
        ConsumerCommands::Reset { group, .. } => {
            warning(&format!(
                "Reset not fully implemented in local mode for group: {}",
                group
            ));
            Ok(())
        }
        ConsumerCommands::Lag { group } => {
            info("Showing consumer lag (local mode)...");
            if let Some(group_id) = group {
                let lag = with_database_read(|db| {
                    let g = group_id.to_string();
                    async move {
                        db.get_group_lag_info(&g)
                            .await
                            .map_err(|e| anyhow::anyhow!("{}", e))
                    }
                })
                .await?;
                for (coll, part, current, latest, lag) in lag {
                    println!(
                        "  {}:{} - offset: {}/{} lag: {}",
                        coll, part, current, latest, lag
                    );
                }
            } else {
                info("Specify --group for lag details in local mode.");
            }
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

async fn list_consumer_groups(cli: &Cli) -> Result<()> {
    info("Listing consumer groups...");

    let client = create_client(cli).await?;
    let groups = client.list_consumer_groups().await?;

    if groups.is_empty() {
        info("No consumer groups found. Consumer groups are created when consumers are started.");
        info("To create a consumer group, run a consumer example like:");
        info("  cargo run --example basic_consumer");
        return Ok(());
    }

    let display_groups: Vec<ConsumerGroupInfo> = groups
        .into_iter()
        .map(|g| ConsumerGroupInfo {
            group_id: g.group_id,
            members: g.members,
            state: g.state,
            lag: g.lag,
            assignment: g.assignment_strategy,
        })
        .collect();

    display_groups.display(cli)?;
    Ok(())
}

async fn describe_consumer_group(group: &str, cli: &Cli) -> Result<()> {
    info(&format!("Describing consumer group: {}", group));

    let client = create_client(cli).await?;
    let response = client.describe_consumer_group(group).await?;

    if !response.success {
        error(&format!("Consumer group '{}' not found", group));
        return Ok(());
    }

    let members: Vec<ConsumerMember> = response
        .members
        .into_iter()
        .map(|m| ConsumerMember {
            consumer_id: m.consumer_id,
            host: m.host,
            partitions: m.partitions,
        })
        .collect();

    let partitions: Vec<PartitionAssignment> = response
        .partitions
        .into_iter()
        .map(|p| PartitionAssignment {
            collection: p.collection,
            partition: p.partition,
            current_offset: p.current_offset,
            latest_offset: p.latest_offset,
            lag: p.lag,
        })
        .collect();

    let is_empty = members.is_empty();

    let details = ConsumerGroupDetails {
        group_id: response.group_id,
        members,
        total_lag: response.total_lag,
        state: response.state,
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
    group: &str,
    offset: Option<u64>,
    earliest: bool,
    latest: bool,
    cli: &Cli,
) -> Result<()> {
    info(&format!("Resetting offset for consumer group: {}", group));

    // Validate flags
    if [offset.is_some(), earliest, latest]
        .iter()
        .filter(|&&x| x)
        .count()
        != 1
    {
        error("Exactly one of --offset, --earliest, or --latest must be specified");
        return Ok(());
    }

    let target = if let Some(offset) = offset {
        offset.to_string()
    } else if earliest {
        "earliest".to_string()
    } else {
        "latest".to_string()
    };

    // Call remote RPC
    let client = create_client(cli).await?;
    match client.reset_consumer_offset(group, None, &target).await {
        Ok(partitions_reset) => {
            success(&format!(
                "Reset consumer group '{}' to {} ({} partitions affected)",
                group, target, partitions_reset
            ));
        }
        Err(e) => {
            error(&format!("Failed to reset consumer offset: {}", e));
        }
    }

    Ok(())
}

async fn show_consumer_lag(group: Option<&str>, cli: &Cli) -> Result<()> {
    let client = create_client(cli).await?;

    match group {
        Some(group) => {
            info(&format!("Showing lag for consumer group: {}", group));

            let response = client.describe_consumer_group(group).await?;

            if !response.success {
                error(&format!("Consumer group '{}' not found", group));
                return Ok(());
            }

            let lag_info: Vec<ConsumerLagInfo> = response
                .partitions
                .into_iter()
                .map(|p| ConsumerLagInfo {
                    group_id: group.to_string(),
                    collection: p.collection,
                    partition: p.partition,
                    current_offset: p.current_offset,
                    latest_offset: p.latest_offset,
                    lag: p.lag,
                })
                .collect();

            if lag_info.is_empty() {
                info("No lag information found. This consumer group may not have consumed any data yet.");
            } else {
                lag_info.display(cli)?;
            }
        }
        None => {
            info("Showing lag for all consumer groups");

            let groups = client.list_consumer_groups().await?;
            let mut all_lag_info = Vec::new();

            for group_summary in groups {
                let response = client
                    .describe_consumer_group(&group_summary.group_id)
                    .await?;
                if response.success {
                    for p in response.partitions {
                        all_lag_info.push(ConsumerLagInfo {
                            group_id: group_summary.group_id.clone(),
                            collection: p.collection,
                            partition: p.partition,
                            current_offset: p.current_offset,
                            latest_offset: p.latest_offset,
                            lag: p.lag,
                        });
                    }
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
