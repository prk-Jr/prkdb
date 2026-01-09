use crate::replication::protocol::{
    HealthCheckRequest, HealthCheckResponse, ReplicateRequest, ReplicateResponse,
};
use crate::wal::WriteAheadLog;
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Error type for follower server operations
#[derive(Debug, thiserror::Error)]
pub enum FollowerServerError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("WAL error: {0}")]
    WalError(String),
}

/// Follower server that receives and applies replication requests
pub struct FollowerServer {
    /// Local WAL for storing replicated data
    wal: Arc<WriteAheadLog>,

    /// TCP listener (public for tests to get address)
    pub listener: TcpListener,

    /// Follower node ID
    follower_id: String,
}

impl FollowerServer {
    /// Create a new follower server
    pub async fn new(
        bind_address: &str,
        wal: Arc<WriteAheadLog>,
        follower_id: String,
    ) -> Result<Self, FollowerServerError> {
        let listener = TcpListener::bind(bind_address).await?;

        Ok(Self {
            wal,
            listener,
            follower_id,
        })
    }

    /// Run the server (accepts connections in a loop)
    pub async fn run(self) -> Result<(), FollowerServerError> {
        tracing::info!(
            "Follower server {} listening on {}",
            self.follower_id,
            self.listener.local_addr()?
        );

        loop {
            let (stream, addr) = self.listener.accept().await?;
            tracing::debug!("Accepted connection from {}", addr);

            let wal = Arc::clone(&self.wal);
            let follower_id = self.follower_id.clone();

            // Spawn a task to handle this connection
            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(stream, wal, follower_id).await {
                    tracing::error!("Connection error: {}", e);
                }
            });
        }
    }

    /// Handle a single connection
    async fn handle_connection(
        mut stream: TcpStream,
        wal: Arc<WriteAheadLog>,
        follower_id: String,
    ) -> Result<(), FollowerServerError> {
        loop {
            // Read request length
            let mut len_buf = [0u8; 4];
            if stream.read_exact(&mut len_buf).await.is_err() {
                // Connection closed
                break;
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            // Read request payload
            let mut request_buf = vec![0u8; len];
            stream.read_exact(&mut request_buf).await?;

            // Try to deserialize as ReplicateRequest first
            let config = bincode::config::standard();

            // Check if this is a health check or replication request
            // (We can distinguish by trying to deserialize as each type)
            if let Ok((request, _)) =
                bincode::decode_from_slice::<ReplicateRequest, _>(&request_buf, config)
            {
                // Handle replication request
                let response = Self::handle_replicate_request(request, &wal, &follower_id).await;
                Self::send_response(&mut stream, &response).await?;
            } else if let Ok((request, _)) =
                bincode::decode_from_slice::<HealthCheckRequest, _>(&request_buf, config)
            {
                // Handle health check request
                let response = Self::handle_health_check(request, &wal, &follower_id);
                Self::send_response(&mut stream, &response).await?;
            } else {
                tracing::warn!("Unknown request type");
                break;
            }
        }

        Ok(())
    }

    /// Handle a replication request
    async fn handle_replicate_request(
        request: ReplicateRequest,
        wal: &WriteAheadLog,
        follower_id: &str,
    ) -> ReplicateResponse {
        tracing::debug!(
            "Received replication batch {} from leader {}",
            request.batch_id,
            request.leader_id
        );

        // Apply records to WAL
        let records = if let Some(compressed) = request.compressed_data {
            // Decompress
            let compression_type = request
                .compression
                .unwrap_or(crate::wal::compression::CompressionType::None);

            let decompressed =
                match crate::wal::compression::decompress(&compressed, compression_type) {
                    Ok(d) => d,
                    Err(e) => {
                        return ReplicateResponse {
                            batch_id: request.batch_id,
                            success: false,
                            error: Some(format!("Decompression failed: {}", e)),
                            follower_offset: 0,
                            follower_id: follower_id.to_string(),
                        };
                    }
                };

            // Deserialize
            let config = bincode::config::standard();
            match bincode::decode_from_slice::<Vec<crate::wal::LogRecord>, _>(&decompressed, config)
            {
                Ok((r, _)) => r,
                Err(e) => {
                    return ReplicateResponse {
                        batch_id: request.batch_id,
                        success: false,
                        error: Some(format!("Deserialization failed: {}", e)),
                        follower_offset: 0,
                        follower_id: follower_id.to_string(),
                    };
                }
            }
        } else {
            request.records
        };

        // Use batch append if available (it is now)
        let follower_offset = match wal.append_batch(records) {
            Ok(offset) => offset,
            Err(e) => {
                return ReplicateResponse {
                    batch_id: request.batch_id,
                    success: false,
                    error: Some(format!("WAL append error: {}", e)),
                    follower_offset: 0,
                    follower_id: follower_id.to_string(),
                };
            }
        };

        // Success
        ReplicateResponse {
            batch_id: request.batch_id,
            success: true,
            error: None,
            follower_offset,
            follower_id: follower_id.to_string(),
        }
    }

    /// Handle a health check request
    fn handle_health_check(
        request: HealthCheckRequest,
        wal: &WriteAheadLog,
        follower_id: &str,
    ) -> HealthCheckResponse {
        let follower_offset = wal.next_offset();
        let lag = if request.leader_offset >= follower_offset {
            request.leader_offset - follower_offset
        } else {
            0
        };

        HealthCheckResponse {
            follower_id: follower_id.to_string(),
            follower_offset,
            lag,
            healthy: lag < 1000, // Arbitrary threshold for "healthy"
        }
    }

    /// Send a response (generic over response type)
    async fn send_response<T>(
        stream: &mut TcpStream,
        response: &T,
    ) -> Result<(), FollowerServerError>
    where
        T: bincode::Encode,
    {
        let config = bincode::config::standard();
        let payload = bincode::encode_to_vec(response, config)
            .map_err(|e| FollowerServerError::Serialization(e.to_string()))?;
        let len = payload.len() as u32;

        stream.write_all(&len.to_le_bytes()).await?;
        stream.write_all(&payload).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{LogOperation, LogRecord, WalConfig};
    use std::env;

    #[tokio::test]
    async fn test_follower_server_replication() {
        // Create WAL for follower
        let dir = env::temp_dir().join("test_follower_wal");
        let _ = std::fs::remove_dir_all(&dir);

        let config = WalConfig {
            log_dir: dir.clone(),
            ..WalConfig::test_config()
        };

        let wal = Arc::new(WriteAheadLog::create(config).unwrap());

        // Start follower server
        let server =
            FollowerServer::new("127.0.0.1:0", Arc::clone(&wal), "test-follower".to_string())
                .await
                .unwrap();

        let addr = server.listener.local_addr().unwrap();

        tokio::spawn(async move {
            server.run().await.unwrap();
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Connect as a client and send replication request
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(addr).await.unwrap();

        let record = LogRecord::new(LogOperation::Put {
            collection: "test".to_string(),
            id: b"key1".to_vec(),
            data: b"value1".to_vec(),
        });

        let request = ReplicateRequest {
            batch_id: 1,
            records: vec![record],
            leader_offset: 100,
            leader_id: "test-leader".to_string(),
            compression: None,
            compressed_data: None,
        };

        // Send request
        let config = bincode::config::standard();
        let payload = bincode::encode_to_vec(&request, config).unwrap();
        let len = payload.len() as u32;

        stream.write_all(&len.to_le_bytes()).await.unwrap();
        stream.write_all(&payload).await.unwrap();

        // Read response
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let response_len = u32::from_le_bytes(len_buf) as usize;

        let mut response_buf = vec![0u8; response_len];
        stream.read_exact(&mut response_buf).await.unwrap();

        let (response, _): (ReplicateResponse, _) =
            bincode::decode_from_slice(&response_buf, config).unwrap();

        assert_eq!(response.batch_id, 1);
        assert!(response.success);
        assert_eq!(response.follower_id, "test-follower");

        // Verify data was written to WAL
        assert_eq!(wal.next_offset(), 1);

        // Clean up
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn test_follower_server_health_check() {
        // Create WAL for follower
        let dir = env::temp_dir().join("test_follower_health");
        let _ = std::fs::remove_dir_all(&dir);

        let config = WalConfig {
            log_dir: dir.clone(),
            ..WalConfig::test_config()
        };

        let wal = Arc::new(WriteAheadLog::create(config).unwrap());

        // Start follower server
        let server =
            FollowerServer::new("127.0.0.1:0", Arc::clone(&wal), "test-follower".to_string())
                .await
                .unwrap();

        let addr = server.listener.local_addr().unwrap();

        tokio::spawn(async move {
            server.run().await.unwrap();
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Connect and send health check
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(addr).await.unwrap();

        let request = HealthCheckRequest {
            leader_id: "test-leader".to_string(),
            leader_offset: 50,
        };

        // Send request
        let config = bincode::config::standard();
        let payload = bincode::encode_to_vec(&request, config).unwrap();
        let len = payload.len() as u32;

        stream.write_all(&len.to_le_bytes()).await.unwrap();
        stream.write_all(&payload).await.unwrap();

        // Read response
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let response_len = u32::from_le_bytes(len_buf) as usize;

        let mut response_buf = vec![0u8; response_len];
        stream.read_exact(&mut response_buf).await.unwrap();

        let (response, _): (HealthCheckResponse, _) =
            bincode::decode_from_slice(&response_buf, config).unwrap();

        assert_eq!(response.follower_id, "test-follower");
        assert_eq!(response.follower_offset, 0);
        assert_eq!(response.lag, 50);
        assert!(response.healthy);

        // Clean up
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
