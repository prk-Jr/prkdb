/// Helper functions for chaos tests to handle leader redirect automatically
///
/// These functions wrap gRPC client operations with automatic leader discovery
/// and redirect logic, making tests resilient to leader elections and changes.
use crate::helpers::test_cluster::TestCluster;
use prkdb::raft::rpc::prk_db_service_client::PrkDbServiceClient;
use prkdb::raft::rpc::{GetRequest, PutRequest};
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::Channel;

/// Connect to a gRPC endpoint with retry logic
pub async fn connect_with_retry(
    addr: String,
) -> Result<PrkDbServiceClient<Channel>, tonic::transport::Error> {
    for attempt in 0..10 {
        match PrkDbServiceClient::connect(addr.clone()).await {
            Ok(client) => return Ok(client),
            Err(e) if attempt < 9 => {
                tracing::warn!("Connection attempt {} failed: {}", attempt + 1, e);
                sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

/// Write a key-value pair with automatic leader redirect
///
/// This function will:
/// - Attempt to write to the current known leader
/// - Parse NotLeader errors and redirect to the actual leader
/// - Retry on connection errors
/// - Return error after max_retries attempts
/// Write a key-value pair with automatic leader redirect
///
/// This function will:
/// - Attempt to write to the current known leader
/// - Parse NotLeader errors and redirect to the actual leader
/// - Retry on connection errors
/// - Return error after max_retries attempts
pub async fn write_with_redirect(
    cluster: &TestCluster,
    key: Vec<u8>,
    value: Vec<u8>,
    max_retries: usize,
) -> Result<(), String> {
    let mut current_node_id = 1u64;

    for attempt in 0..max_retries {
        let addr = format!(
            "http://127.0.0.1:{}",
            cluster.get_node(current_node_id).unwrap().data_port
        );

        match connect_with_retry(addr).await {
            Ok(mut client) => {
                let req = tonic::Request::new(PutRequest {
                    key: key.clone(),
                    value: value.clone(),
                });

                match client.put(req).await {
                    Ok(response) => {
                        if response.into_inner().success {
                            return Ok(());
                        }
                        return Err("Put returned success=false".to_string());
                    }
                    Err(status) => {
                        let msg = status.message();

                        // Check for leader redirect
                        if let Some(leader_id) = parse_leader_id(msg) {
                            tracing::info!(
                                "Attempt {}: Redirecting from node {} to leader {}",
                                attempt + 1,
                                current_node_id,
                                leader_id
                            );
                            current_node_id = leader_id;
                            continue;
                        }

                        // Handle connection errors within gRPC
                        if msg.contains("transport error") || msg.contains("h2 protocol error") {
                            tracing::warn!(
                                "Attempt {}: Connection error (gRPC), will retry: {}",
                                attempt + 1,
                                msg
                            );
                            sleep(Duration::from_millis(500)).await;
                            // Try next node
                            current_node_id = if current_node_id >= 3 {
                                1
                            } else {
                                current_node_id + 1
                            };
                            continue;
                        }

                        return Err(format!("Put failed: {}", msg));
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    "Attempt {}: Failed to connect to node {}: {}",
                    attempt + 1,
                    current_node_id,
                    e
                );
                // Try next node
                current_node_id = if current_node_id >= 3 {
                    1
                } else {
                    current_node_id + 1
                };
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        }
    }

    Err(format!("Failed after {} retries", max_retries))
}

/// Read a key with automatic leader redirect
///
/// This function will:
/// - Attempt to read from the current known leader
/// - Parse NotLeader errors and redirect to the actual leader
/// - Retry on connection errors
/// - Return error after max_retries attempts
pub async fn read_with_redirect(
    cluster: &TestCluster,
    key: Vec<u8>,
    max_retries: usize,
) -> Result<Vec<u8>, String> {
    let mut current_node_id = 1u64;

    for attempt in 0..max_retries {
        let addr = format!(
            "http://127.0.0.1:{}",
            cluster.get_node(current_node_id).unwrap().data_port
        );

        match connect_with_retry(addr).await {
            Ok(mut client) => {
                let req = tonic::Request::new(GetRequest {
                    key: key.clone(),
                    read_mode: prkdb::raft::rpc::ReadMode::Linearizable.into(),
                });

                match client.get(req).await {
                    Ok(response) => {
                        let inner = response.into_inner();
                        if inner.found {
                            return Ok(inner.value);
                        } else {
                            return Err("Key not found".to_string());
                        }
                    }
                    Err(status) => {
                        let msg = status.message();

                        // Check for leader redirect
                        if let Some(leader_id) = parse_leader_id(msg) {
                            tracing::info!(
                                "Attempt {}: Redirecting read from node {} to leader {}",
                                attempt + 1,
                                current_node_id,
                                leader_id
                            );
                            current_node_id = leader_id;
                            continue;
                        }

                        // Handle connection errors
                        if msg.contains("transport error") || msg.contains("h2 protocol error") {
                            tracing::warn!(
                                "Attempt {}: Connection error, will retry: {}",
                                attempt + 1,
                                msg
                            );
                            sleep(Duration::from_millis(500)).await;
                            // Try next node in sequence
                            current_node_id = if current_node_id >= 3 {
                                1
                            } else {
                                current_node_id + 1
                            };
                            continue;
                        }

                        return Err(format!("Get failed: {}", msg));
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    "Attempt {}: Failed to connect to node {}: {}",
                    attempt + 1,
                    current_node_id,
                    e
                );
                // Try next node
                current_node_id = if current_node_id >= 3 {
                    1
                } else {
                    current_node_id + 1
                };
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        }
    }

    Err(format!("Failed after {} retries", max_retries))
}

/// Parse leader ID from error message
///
/// Example: "Not leader. Leader is Some(2)" -> Some(2)
fn parse_leader_id(error_msg: &str) -> Option<u64> {
    if let Some(start) = error_msg.find("Some(") {
        if let Some(end) = error_msg[start..].find(")") {
            let leader_str = &error_msg[start + 5..start + end];
            return leader_str.parse::<u64>().ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_leader_id() {
        assert_eq!(parse_leader_id("Not leader. Leader is Some(2)"), Some(2));
        assert_eq!(parse_leader_id("Not leader. Leader is Some(42)"), Some(42));
        assert_eq!(parse_leader_id("Not leader. Leader is None"), None);
        assert_eq!(parse_leader_id("Some other error"), None);
    }
}
