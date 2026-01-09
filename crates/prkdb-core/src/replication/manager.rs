use crate::replication::config::{AckLevel, ReplicationConfig};
use crate::replication::protocol::{ReplicateRequest, ReplicateResponse};
use crate::replication::replica_client::{ReplicaClient, ReplicaClientError};
use crate::wal::LogRecord;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::Duration;

/// Error type for replication manager operations
#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("Replica client error: {0}")]
    ClientError(#[from] ReplicaClientError),

    #[error("Insufficient replicas: needed {needed}, got {actual}")]
    InsufficientReplicas { needed: usize, actual: usize },

    #[error("All replicas failed")]
    AllReplicasFailed,

    #[error("Configuration error: {0}")]
    ConfigError(String),
}

/// Manages replication to multiple follower replicas
pub struct ReplicationManager {
    /// Configuration
    config: ReplicationConfig,

    /// Connected replica clients
    replicas: Vec<ReplicaClient>,

    /// Semaphore for controlling in-flight requests (pipeline depth)
    in_flight: Arc<Semaphore>,

    /// Batch ID counter
    next_batch_id: Arc<AtomicU64>,
}

impl ReplicationManager {
    /// Create a new replication manager (connects to all replicas)
    pub async fn new(config: ReplicationConfig) -> Result<Self, ReplicationError> {
        // Validate config
        config.validate().map_err(ReplicationError::ConfigError)?;

        // Connect to all replicas
        let mut replicas = Vec::new();
        for address in &config.replica_addresses {
            let mut client = ReplicaClient::new(address.clone());
            client.connect().await?;
            replicas.push(client);
        }

        let in_flight = Arc::new(Semaphore::new(config.max_in_flight));

        Ok(Self {
            config,
            replicas,
            in_flight,
            next_batch_id: Arc::new(AtomicU64::new(1)),
        })
    }

    /// Replicate a batch of records to followers
    pub async fn replicate_batch(
        &mut self,
        records: Vec<LogRecord>,
        leader_offset: u64,
    ) -> Result<(), ReplicationError> {
        // Check if we should even replicate
        if self.replicas.is_empty() {
            return Ok(()); // No replicas configured
        }

        // Acquire semaphore permit for backpressure
        let permit = self.in_flight.acquire().await.unwrap();

        let batch_id = self.next_batch_id.fetch_add(1, Ordering::SeqCst);

        // Serialize and compress batch
        let config = bincode::config::standard();
        let serialized = bincode::encode_to_vec(&records, config)
            .map_err(|e| ReplicationError::ConfigError(format!("Serialization failed: {}", e)))?;

        // Use default compression (LZ4)
        let compression_config = crate::wal::compression::CompressionConfig::default();
        let compressed = crate::wal::compression::compress(&serialized, &compression_config)
            .map_err(|e| ReplicationError::ConfigError(format!("Compression failed: {}", e)))?;

        // Check if compression actually happened (compress() returns uncompressed data if too small)
        let actual_compression_type = if compressed.len() < serialized.len() {
            compression_config.compression_type
        } else {
            crate::wal::compression::CompressionType::None
        };

        let request = ReplicateRequest {
            batch_id,
            records: vec![], // Send empty records as we use compressed_data
            leader_offset,
            leader_id: self.config.node_id.clone(),
            compression: Some(actual_compression_type),
            compressed_data: Some(compressed),
        };

        // Handle based on ack level
        match self.config.ack_level {
            AckLevel::None => {
                // Fire-and-forget - spawn background task
                let replicas_clone = self
                    .replicas
                    .iter()
                    .map(|r| r.address().to_string())
                    .collect::<Vec<_>>();
                let request_clone = request.clone();

                drop(permit); // Release permit before spawning

                tokio::spawn(async move {
                    // Send to all replicas, don't wait for responses
                    tracing::debug!(
                        "Fire-and-forget replication to {} replicas",
                        replicas_clone.len()
                    );
                    let _ = Self::send_to_all_static(replicas_clone, request_clone).await;
                });

                Ok(())
            }

            AckLevel::Leader => {
                // Leader already wrote, spawn background task for replicas
                let replicas_clone = self
                    .replicas
                    .iter()
                    .map(|r| r.address().to_string())
                    .collect::<Vec<_>>();
                let request_clone = request;

                drop(permit); // Release permit before spawning

                tokio::spawn(async move {
                    tracing::debug!(
                        "Leader ack - async replication to {} replicas",
                        replicas_clone.len()
                    );
                    let _ = Self::send_to_all_static(replicas_clone, request_clone).await;
                });

                Ok(())
            }

            AckLevel::Quorum => {
                // Wait for majority
                let required = self.config.required_acks();
                drop(permit); // Release permit before mutable borrow
                self.wait_for_n_acks(request, required).await
            }

            AckLevel::All => {
                // Wait for all replicas
                let required = self.replicas.len();
                drop(permit); // Release permit before mutable borrow
                self.wait_for_n_acks(request, required).await
            }
        }
    }

    /// Wait for N successful acknowledgments
    async fn wait_for_n_acks(
        &mut self,
        request: ReplicateRequest,
        required: usize,
    ) -> Result<(), ReplicationError> {
        use tokio::time::timeout;

        let mut successful = 0;
        let mut errors = Vec::new();

        for replica in &mut self.replicas {
            let result = timeout(
                self.config.replication_timeout,
                replica.send_replicate(request.clone()),
            )
            .await;

            match result {
                Ok(Ok(_response)) => {
                    successful += 1;
                    if successful >= required {
                        return Ok(());
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!("Replica {} failed: {}", replica.address(), e);
                    errors.push(e);
                }
                Err(_) => {
                    tracing::warn!("Replica {} timeout", replica.address());
                }
            }
        }

        if successful >= required {
            Ok(())
        } else if successful == 0 {
            Err(ReplicationError::AllReplicasFailed)
        } else {
            Err(ReplicationError::InsufficientReplicas {
                needed: required,
                actual: successful,
            })
        }
    }

    /// Static helper for fire-and-forget replication with retry logic
    async fn send_to_all_static(
        addresses: Vec<String>,
        request: ReplicateRequest,
    ) -> Vec<Result<ReplicateResponse, ReplicaClientError>> {
        let mut futures = Vec::new();
        let max_retries = 3;

        for address in addresses {
            let req = request.clone();
            let fut = async move {
                let mut last_error = None;

                // Retry up to max_retries times with exponential backoff
                for attempt in 0..max_retries {
                    let mut client = ReplicaClient::new(address.clone());

                    match client.connect().await {
                        Ok(()) => match client.send_replicate(req.clone()).await {
                            Ok(response) => {
                                return Ok(response);
                            }
                            Err(e) => {
                                last_error = Some(e);
                                if attempt < max_retries - 1 {
                                    let backoff_ms = 100 * (attempt as u64 + 1);
                                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                }
                            }
                        },
                        Err(e) => {
                            last_error = Some(e);
                            if attempt < max_retries - 1 {
                                let backoff_ms = 100 * (attempt as u64 + 1);
                                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                            }
                        }
                    }
                }

                // All retries failed, return the last error
                Err(last_error.unwrap_or(ReplicaClientError::NotConnected))
            };
            futures.push(fut);
        }

        futures::future::join_all(futures).await
    }

    /// Get number of connected replicas
    pub fn replica_count(&self) -> usize {
        self.replicas.len()
    }

    /// Get current ack level
    pub fn ack_level(&self) -> AckLevel {
        self.config.ack_level
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{LogOperation, LogRecord};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // Helper: Create a mock follower server
    async fn mock_follower_server(port: u16, should_succeed: bool) {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
            .await
            .unwrap();

        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();

                // Read request
                let mut len_buf = [0u8; 4];
                if stream.read_exact(&mut len_buf).await.is_err() {
                    continue;
                }
                let len = u32::from_le_bytes(len_buf) as usize;

                let mut request_buf = vec![0u8; len];
                if stream.read_exact(&mut request_buf).await.is_err() {
                    continue;
                }

                let config = bincode::config::standard();
                let (request, _): (ReplicateRequest, _) =
                    bincode::decode_from_slice(&request_buf, config).unwrap();

                // Send response
                let response = ReplicateResponse {
                    batch_id: request.batch_id,
                    success: should_succeed,
                    error: if should_succeed {
                        None
                    } else {
                        Some("Mock error".to_string())
                    },
                    follower_offset: request.leader_offset,
                    follower_id: format!("follower-{}", port),
                };

                let payload = bincode::encode_to_vec(&response, config).unwrap();
                let response_len = payload.len() as u32;

                stream.write_all(&response_len.to_le_bytes()).await.unwrap();
                stream.write_all(&payload).await.unwrap();
            }
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_replication_manager_creation() {
        // Start 2 mock servers
        mock_follower_server(19001, true).await;
        mock_follower_server(19002, true).await;

        let config = ReplicationConfig {
            replica_addresses: vec!["127.0.0.1:19001".to_string(), "127.0.0.1:19002".to_string()],
            ack_level: AckLevel::Quorum,
            ..ReplicationConfig::test_config()
        };

        let manager = ReplicationManager::new(config).await.unwrap();
        assert_eq!(manager.replica_count(), 2);
        assert_eq!(manager.ack_level(), AckLevel::Quorum);
    }

    #[tokio::test]
    async fn test_replication_leader_ack() {
        // Start mock server
        mock_follower_server(19003, true).await;

        let config = ReplicationConfig {
            replica_addresses: vec!["127.0.0.1:19003".to_string()],
            ack_level: AckLevel::Leader,
            ..ReplicationConfig::test_config()
        };

        let mut manager = ReplicationManager::new(config).await.unwrap();

        let record = LogRecord::new(LogOperation::Put {
            collection: "test".to_string(),
            id: b"key1".to_vec(),
            data: b"value1".to_vec(),
        });

        // Should return immediately (fire-and-forget)
        let result = manager.replicate_batch(vec![record], 100).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_replication_quorum_ack() {
        // Start 3 mock servers
        mock_follower_server(19004, true).await;
        mock_follower_server(19005, true).await;
        mock_follower_server(19006, true).await;

        let config = ReplicationConfig {
            replica_addresses: vec![
                "127.0.0.1:19004".to_string(),
                "127.0.0.1:19005".to_string(),
                "127.0.0.1:19006".to_string(),
            ],
            ack_level: AckLevel::Quorum,
            ..ReplicationConfig::test_config()
        };

        let mut manager = ReplicationManager::new(config).await.unwrap();

        let record = LogRecord::new(LogOperation::Put {
            collection: "test".to_string(),
            id: b"key1".to_vec(),
            data: b"value1".to_vec(),
        });

        // Should wait for quorum (2 out of 3)
        let result = manager.replicate_batch(vec![record], 100).await;
        assert!(result.is_ok());
    }
}
