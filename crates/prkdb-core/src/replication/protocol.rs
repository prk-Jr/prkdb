use crate::wal::LogRecord;
use serde::{Deserialize, Serialize};

/// Replication request sent from leader to follower
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct ReplicateRequest {
    /// Unique batch identifier for tracking
    pub batch_id: u64,

    /// Records to replicate
    pub records: Vec<LogRecord>,

    /// Leader's current offset after this batch
    pub leader_offset: u64,

    /// Leader identifier
    pub leader_id: String,

    /// Compression type used for the batch (optional)
    pub compression: Option<crate::wal::compression::CompressionType>,

    /// Compressed records data (if compression is used)
    pub compressed_data: Option<Vec<u8>>,
}

/// Represents a single change event for replication
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub enum Change {
    /// Key-value put operation
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        version: u64,
    },
    /// Key delete operation
    Delete { key: Vec<u8>, version: u64 },
}

/// Replication response sent from follower to leader
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct ReplicateResponse {
    /// Matches request batch_id
    pub batch_id: u64,

    /// Whether replication succeeded
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,

    /// Follower's current offset after applying this batch
    pub follower_offset: u64,

    /// Follower identifier
    pub follower_id: String,
}

/// Health check request from leader to follower
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct HealthCheckRequest {
    /// Leader identifier
    pub leader_id: String,

    /// Leader's current offset
    pub leader_offset: u64,
}

/// Health check response from follower to leader
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct HealthCheckResponse {
    /// Follower identifier
    pub follower_id: String,

    /// Follower's current offset
    pub follower_offset: u64,

    /// Replication lag (leader_offset - follower_offset)
    pub lag: u64,

    /// Whether follower is healthy
    pub healthy: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{LogOperation, LogRecord};

    #[test]
    fn test_replicate_request_serialization() {
        let record = LogRecord::new(LogOperation::Put {
            collection: "test".to_string(),
            id: b"key1".to_vec(),
            data: b"value1".to_vec(),
        });

        let request = ReplicateRequest {
            batch_id: 123,
            records: vec![record],
            leader_offset: 456,
            leader_id: "leader-1".to_string(),
            compression: None,
            compressed_data: None,
        };

        // Serialize
        let config = bincode::config::standard();
        let bytes = bincode::encode_to_vec(&request, config).unwrap();

        // Deserialize
        let (decoded, _): (ReplicateRequest, _) =
            bincode::decode_from_slice(&bytes, config).unwrap();

        assert_eq!(decoded.batch_id, 123);
        assert_eq!(decoded.leader_offset, 456);
        assert_eq!(decoded.leader_id, "leader-1");
        assert_eq!(decoded.records.len(), 1);
    }

    #[test]
    fn test_replicate_response_serialization() {
        let response = ReplicateResponse {
            batch_id: 123,
            success: true,
            error: None,
            follower_offset: 456,
            follower_id: "follower-1".to_string(),
        };

        let config = bincode::config::standard();
        let bytes = bincode::encode_to_vec(&response, config).unwrap();
        let (decoded, _): (ReplicateResponse, _) =
            bincode::decode_from_slice(&bytes, config).unwrap();

        assert_eq!(decoded.batch_id, 123);
        assert!(decoded.success);
        assert_eq!(decoded.follower_offset, 456);
    }

    #[test]
    fn test_health_check_serialization() {
        let request = HealthCheckRequest {
            leader_id: "leader-1".to_string(),
            leader_offset: 1000,
        };

        let response = HealthCheckResponse {
            follower_id: "follower-1".to_string(),
            follower_offset: 950,
            lag: 50,
            healthy: true,
        };

        let config = bincode::config::standard();

        // Test request
        let bytes = bincode::encode_to_vec(&request, config).unwrap();
        let (decoded, _): (HealthCheckRequest, _) =
            bincode::decode_from_slice(&bytes, config).unwrap();
        assert_eq!(decoded.leader_offset, 1000);

        // Test response
        let bytes = bincode::encode_to_vec(&response, config).unwrap();
        let (decoded, _): (HealthCheckResponse, _) =
            bincode::decode_from_slice(&bytes, config).unwrap();
        assert_eq!(decoded.lag, 50);
        assert!(decoded.healthy);
    }
}
