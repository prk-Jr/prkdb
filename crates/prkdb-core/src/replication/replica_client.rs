use crate::replication::protocol::{
    HealthCheckRequest, HealthCheckResponse, ReplicateRequest, ReplicateResponse,
};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

/// Error type for replica client operations
#[derive(Debug, thiserror::Error)]
pub enum ReplicaClientError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Connection timeout")]
    Timeout,

    #[error("Not connected")]
    NotConnected,

    #[error("Response error: {0}")]
    ResponseError(String),
}

/// Client for communicating with a follower replica
pub struct ReplicaClient {
    /// Address of the replica (e.g., "localhost:9001")
    address: String,

    /// TCP connection (None if disconnected)
    stream: Option<TcpStream>,

    /// Connection timeout
    connect_timeout: Duration,

    /// Request timeout
    request_timeout: Duration,
}

impl ReplicaClient {
    /// Create a new replica client (not yet connected)
    pub fn new(address: String) -> Self {
        Self {
            address,
            stream: None,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
        }
    }

    /// Connect to the replica
    pub async fn connect(&mut self) -> Result<(), ReplicaClientError> {
        let stream = timeout(self.connect_timeout, TcpStream::connect(&self.address))
            .await
            .map_err(|_| ReplicaClientError::Timeout)?
            .map_err(ReplicaClientError::Io)?;

        self.stream = Some(stream);
        Ok(())
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    /// Send a replication request
    pub async fn send_replicate(
        &mut self,
        request: ReplicateRequest,
    ) -> Result<ReplicateResponse, ReplicaClientError> {
        let stream = self
            .stream
            .as_mut()
            .ok_or(ReplicaClientError::NotConnected)?;

        // Serialize request
        let config = bincode::config::standard();
        let payload = bincode::encode_to_vec(&request, config)
            .map_err(|e| ReplicaClientError::Serialization(e.to_string()))?;
        let len = payload.len() as u32;

        // Send with timeout
        let response_buf = timeout(self.request_timeout, async {
            // Write length prefix
            stream.write_all(&len.to_le_bytes()).await?;

            // Write payload
            stream.write_all(&payload).await?;

            // Read response length
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await?;
            let response_len = u32::from_le_bytes(len_buf) as usize;

            // Read response payload
            let mut response_buf = vec![0u8; response_len];
            stream.read_exact(&mut response_buf).await?;

            Ok::<Vec<u8>, io::Error>(response_buf)
        })
        .await
        .map_err(|_| ReplicaClientError::Timeout)?
        .map_err(ReplicaClientError::Io)?;

        // Deserialize response
        let (response, _): (ReplicateResponse, _) =
            bincode::decode_from_slice(&response_buf, config)
                .map_err(|e| ReplicaClientError::Serialization(e.to_string()))?;

        // Check for error in response
        if !response.success {
            return Err(ReplicaClientError::ResponseError(
                response
                    .error
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ));
        }

        Ok(response)
    }

    /// Send a health check request
    pub async fn health_check(
        &mut self,
        request: HealthCheckRequest,
    ) -> Result<HealthCheckResponse, ReplicaClientError> {
        let stream = self
            .stream
            .as_mut()
            .ok_or(ReplicaClientError::NotConnected)?;

        // Serialize request
        let config = bincode::config::standard();
        let payload = bincode::encode_to_vec(&request, config)
            .map_err(|e| ReplicaClientError::Serialization(e.to_string()))?;
        let len = payload.len() as u32;

        // Send with timeout
        let response_buf = timeout(self.request_timeout, async {
            // Write length prefix
            stream.write_all(&len.to_le_bytes()).await?;

            // Write payload
            stream.write_all(&payload).await?;

            // Read response length
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await?;
            let response_len = u32::from_le_bytes(len_buf) as usize;

            // Read response payload
            let mut response_buf = vec![0u8; response_len];
            stream.read_exact(&mut response_buf).await?;

            Ok::<_, io::Error>(response_buf)
        })
        .await
        .map_err(|_| ReplicaClientError::Timeout)?
        .map_err(ReplicaClientError::Io)?;

        // Deserialize response
        let (response, _): (HealthCheckResponse, _) =
            bincode::decode_from_slice(&response_buf, config)
                .map_err(|e| ReplicaClientError::Serialization(e.to_string()))?;

        Ok(response)
    }

    /// Disconnect from replica
    pub fn disconnect(&mut self) {
        self.stream = None;
    }

    /// Get replica address
    pub fn address(&self) -> &str {
        &self.address
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{LogOperation, LogRecord};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_replica_client_connect() {
        // Start a mock server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Accept connection in background
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            // Keep connection open
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        // Connect client
        let mut client = ReplicaClient::new(addr.to_string());
        assert!(!client.is_connected());

        client.connect().await.unwrap();
        assert!(client.is_connected());
    }

    #[tokio::test]
    async fn test_replica_client_send_replicate() {
        // Start a mock server that echoes responses
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Mock server
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();

            // Read request
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_le_bytes(len_buf) as usize;

            let mut request_buf = vec![0u8; len];
            stream.read_exact(&mut request_buf).await.unwrap();

            let config = bincode::config::standard();
            let (request, _): (ReplicateRequest, _) =
                bincode::decode_from_slice(&request_buf, config).unwrap();

            // Send response
            let response = ReplicateResponse {
                batch_id: request.batch_id,
                success: true,
                error: None,
                follower_offset: request.leader_offset,
                follower_id: "test-follower".to_string(),
            };

            let payload = bincode::encode_to_vec(&response, config).unwrap();
            let response_len = payload.len() as u32;

            stream.write_all(&response_len.to_le_bytes()).await.unwrap();
            stream.write_all(&payload).await.unwrap();
        });

        // Client sends request
        let mut client = ReplicaClient::new(addr.to_string());
        client.connect().await.unwrap();

        let record = LogRecord::new(LogOperation::Put {
            collection: "test".to_string(),
            id: b"key1".to_vec(),
            data: b"value1".to_vec(),
        });

        let request = ReplicateRequest {
            batch_id: 42,
            records: vec![record],
            leader_offset: 100,
            leader_id: "test-leader".to_string(),
            compression: None,
            compressed_data: None,
        };

        let response = client.send_replicate(request).await.unwrap();
        assert_eq!(response.batch_id, 42);
        assert!(response.success);
        assert_eq!(response.follower_offset, 100);
    }
}
