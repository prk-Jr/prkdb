use crate::commands::ReplicationCommands;
use crate::output::{display_single, error, info, success, warning, OutputDisplay};
use crate::Cli;
use anyhow::Result;
use prkdb_client::PrkDbClient;
use serde::Serialize;
use tabled::Tabled;

#[derive(Serialize)]
struct ReplicationStatus {
    node_id: String,
    role: String,
    leader_address: Option<String>,
    followers: Vec<String>,
    state: String,
    last_sync: String,
    total_changes: u64,
    changes_applied: u64,
}

#[derive(Tabled, Serialize)]
struct ReplicationNode {
    node_id: String,
    address: String,
    role: String,
    status: String,
    lag_ms: u64,
    last_seen: String,
}

#[derive(Tabled, Serialize)]
struct ReplicationLag {
    follower_node: String,
    leader_offset: u64,
    follower_offset: u64,
    lag_records: u64,
    lag_ms: u64,
    status: String,
}

pub async fn execute(cmd: ReplicationCommands, cli: &Cli) -> Result<()> {
    if cli.local {
        // Local mode: use embedded database
        crate::init_database_manager(&cli.database, None);
        return execute_local(cmd, cli).await;
    }

    // Remote mode: use prkdb-client
    match cmd {
        ReplicationCommands::Status => show_replication_status(cli).await,
        ReplicationCommands::Nodes => list_replication_nodes(cli).await,
        ReplicationCommands::Lag => show_replication_lag(cli).await,
        ReplicationCommands::Start { config } => start_replication(&config, cli).await,
    }
}

async fn execute_local(cmd: ReplicationCommands, _cli: &Cli) -> Result<()> {
    use crate::database_manager::with_database_read;

    match cmd {
        ReplicationCommands::Status => {
            info("Fetching replication status (local mode)...");
            let status = with_database_read(|db| async move {
                db.get_replication_status()
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            })
            .await?;
            let (node_id, role, leader, followers, state, last_sync, total, applied) = status;
            println!("Node ID: {}", node_id);
            println!("Role: {}", role);
            println!("Leader: {}", leader.unwrap_or_else(|| "none".to_string()));
            println!("Followers: {:?}", followers);
            println!("State: {}", state);
            println!("Last sync: {}", last_sync);
            println!("Changes: {}/{}", applied, total);
            Ok(())
        }
        ReplicationCommands::Nodes => {
            info("Listing replication nodes (local mode)...");
            let nodes = with_database_read(|db| async move {
                db.get_replication_nodes()
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            })
            .await?;
            for (node_id, addr, role, status, lag, last_seen) in nodes {
                println!(
                    "  {}: {} ({}) - {} lag: {}ms last_seen: {}",
                    node_id, addr, role, status, lag, last_seen
                );
            }
            Ok(())
        }
        ReplicationCommands::Lag => {
            info("Checking replication lag (local mode)...");
            let lags = with_database_read(|db| async move {
                db.get_replication_lag()
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            })
            .await?;
            for (node, leader_off, follower_off, lag_records, lag_ms, status) in lags {
                println!(
                    "  {}: leader={} follower={} lag={} records ({}ms) - {}",
                    node, leader_off, follower_off, lag_records, lag_ms, status
                );
            }
            Ok(())
        }
        ReplicationCommands::Start { config } => {
            warning(&format!(
                "Start replication not implemented in local mode. Config: {}",
                config
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

async fn show_replication_status(cli: &Cli) -> Result<()> {
    info("Fetching replication status...");

    let client = create_client(cli).await?;
    let response = client.get_replication_status().await?;

    let status = ReplicationStatus {
        node_id: response.node_id,
        role: response.role,
        leader_address: if response.leader_address.is_empty() {
            None
        } else {
            Some(response.leader_address)
        },
        followers: response.followers,
        state: response.state,
        last_sync: response.last_sync,
        total_changes: response.total_changes,
        changes_applied: response.changes_applied,
    };

    display_single(&status, cli)?;

    if status.state == "Not configured" {
        info("Note: Replication is not currently configured. Use 'replication start' to configure replication.");
    }

    Ok(())
}

async fn list_replication_nodes(cli: &Cli) -> Result<()> {
    info("Listing replication nodes...");

    let client = create_client(cli).await?;
    let nodes_data = client.get_replication_nodes().await?;

    if nodes_data.is_empty() {
        info("No replication nodes found. Replication is not currently configured.");
        info("To configure replication, use the 'replication start' command with a configuration file.");
    } else {
        let nodes: Vec<ReplicationNode> = nodes_data
            .into_iter()
            .map(|n| ReplicationNode {
                node_id: n.node_id,
                address: n.address,
                role: n.role,
                status: n.status,
                lag_ms: n.lag_ms,
                last_seen: n.last_seen,
            })
            .collect();
        nodes.display(cli)?;
    }

    Ok(())
}

async fn show_replication_lag(cli: &Cli) -> Result<()> {
    info("Checking replication lag...");

    let client = create_client(cli).await?;
    let lag_data = client.get_replication_lag().await?;

    if lag_data.is_empty() {
        info("No replication lag information found. Replication is not currently configured.");
        info("Lag information is available when followers are actively replicating from a leader.");
    } else {
        let lag_info: Vec<ReplicationLag> = lag_data
            .into_iter()
            .map(|l| ReplicationLag {
                follower_node: l.follower_node,
                leader_offset: l.leader_offset,
                follower_offset: l.follower_offset,
                lag_records: l.lag_records,
                lag_ms: l.lag_ms,
                status: l.status,
            })
            .collect();
        lag_info.display(cli)?;
    }

    Ok(())
}

async fn start_replication(config_path: &str, cli: &Cli) -> Result<()> {
    info(&format!(
        "Starting replication with config: {}",
        config_path
    ));

    // Check if configuration file exists
    if !std::path::Path::new(config_path).exists() {
        return Err(anyhow::anyhow!(
            "Configuration file not found: {}",
            config_path
        ));
    }

    // Read configuration file - expect it to contain target address
    let target_address = std::fs::read_to_string(config_path)?
        .lines()
        .next()
        .map(|s| s.trim().to_string())
        .ok_or_else(|| anyhow::anyhow!("Config file is empty"))?;

    info(&format!("Target address: {}", target_address));

    // Call remote RPC
    let client = create_client(cli).await?;
    match client.start_replication(&target_address).await {
        Ok(node_id) => {
            success(&format!(
                "Replication started successfully. Assigned NodeId: {}",
                node_id
            ));
        }
        Err(e) => {
            error(&format!("Failed to start replication: {}", e));
        }
    }

    Ok(())
}
