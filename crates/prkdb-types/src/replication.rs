//! Replication types and configuration for PrkDB.
//!
//! This module provides types for replication protocol messages and configuration.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Acknowledgment level for replication
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum AckLevel {
    /// Fire-and-forget - don't wait for any replicas (fastest, no durability guarantee)
    None,

    /// Wait for leader write only - return as soon as leader commits (default)
    #[default]
    Leader,

    /// Wait for majority of replicas to acknowledge
    Quorum,

    /// Wait for all replicas to acknowledge
    All,
}

/// Configuration for replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Addresses of replica nodes (e.g., ["localhost:9001", "localhost:9002"])
    pub replica_addresses: Vec<String>,

    /// Acknowledgment level
    pub ack_level: AckLevel,

    /// Maximum number of in-flight replication requests (pipeline depth)
    pub max_in_flight: usize,

    /// Timeout for replication requests
    pub replication_timeout: Duration,

    /// Maximum retry attempts on failure
    pub max_retries: usize,

    /// Interval between health checks
    pub health_check_interval: Duration,

    /// This node's identifier
    pub node_id: String,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            replica_addresses: vec![],
            ack_level: AckLevel::Leader,
            max_in_flight: 100,
            replication_timeout: Duration::from_secs(5),
            max_retries: 3,
            health_check_interval: Duration::from_secs(10),
            node_id: "node-0".to_string(),
        }
    }
}

impl ReplicationConfig {
    /// Create config for testing (shorter timeouts)
    pub fn test_config() -> Self {
        Self {
            replica_addresses: vec![],
            ack_level: AckLevel::Leader,
            max_in_flight: 10,
            replication_timeout: Duration::from_millis(500),
            max_retries: 2,
            health_check_interval: Duration::from_secs(1),
            node_id: "test-node".to_string(),
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.max_in_flight == 0 {
            return Err("max_in_flight must be > 0".to_string());
        }

        if self.replication_timeout.as_millis() == 0 {
            return Err("replication_timeout must be > 0".to_string());
        }

        // For Quorum/All, need at least one replica
        match self.ack_level {
            AckLevel::Quorum | AckLevel::All => {
                if self.replica_addresses.is_empty() {
                    return Err(format!(
                        "ack_level {:?} requires at least one replica",
                        self.ack_level
                    ));
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Calculate required acks based on ack level
    pub fn required_acks(&self) -> usize {
        match self.ack_level {
            AckLevel::None => 0,
            AckLevel::Leader => 0, // Leader already wrote
            AckLevel::Quorum => (self.replica_addresses.len() / 2) + 1,
            AckLevel::All => self.replica_addresses.len(),
        }
    }

    /// Add a replica address
    pub fn with_replica(mut self, address: impl Into<String>) -> Self {
        self.replica_addresses.push(address.into());
        self
    }

    /// Set ack level
    pub fn with_ack_level(mut self, level: AckLevel) -> Self {
        self.ack_level = level;
        self
    }

    /// Set node ID
    pub fn with_node_id(mut self, id: impl Into<String>) -> Self {
        self.node_id = id.into();
        self
    }
}

/// Represents a single change event for replication
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

impl Change {
    /// Create a new Put change
    pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>, version: u64) -> Self {
        Self::Put {
            key: key.into(),
            value: value.into(),
            version,
        }
    }

    /// Create a new Delete change
    pub fn delete(key: impl Into<Vec<u8>>, version: u64) -> Self {
        Self::Delete {
            key: key.into(),
            version,
        }
    }

    /// Get the key affected by this change
    pub fn key(&self) -> &[u8] {
        match self {
            Change::Put { key, .. } => key,
            Change::Delete { key, .. } => key,
        }
    }

    /// Get the version of this change
    pub fn version(&self) -> u64 {
        match self {
            Change::Put { version, .. } => *version,
            Change::Delete { version, .. } => *version,
        }
    }

    /// Check if this is a put operation
    pub fn is_put(&self) -> bool {
        matches!(self, Change::Put { .. })
    }

    /// Check if this is a delete operation
    pub fn is_delete(&self) -> bool {
        matches!(self, Change::Delete { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ack_level_default() {
        assert_eq!(AckLevel::default(), AckLevel::Leader);
    }

    #[test]
    fn test_config_default() {
        let config = ReplicationConfig::default();
        assert_eq!(config.ack_level, AckLevel::Leader);
        assert_eq!(config.max_in_flight, 100);
    }

    #[test]
    fn test_config_validation() {
        let mut config = ReplicationConfig::default();

        // Valid config
        assert!(config.validate().is_ok());

        // Invalid: zero max_in_flight
        config.max_in_flight = 0;
        assert!(config.validate().is_err());
        config.max_in_flight = 100;

        // Invalid: Quorum without replicas
        config.ack_level = AckLevel::Quorum;
        assert!(config.validate().is_err());

        // Valid: Quorum with replicas
        config.replica_addresses = vec!["localhost:9001".to_string()];
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_required_acks() {
        let config = ReplicationConfig::default()
            .with_replica("localhost:9001")
            .with_replica("localhost:9002")
            .with_replica("localhost:9003");

        let mut config_copy = config.clone();
        config_copy.ack_level = AckLevel::None;
        assert_eq!(config_copy.required_acks(), 0);

        config_copy.ack_level = AckLevel::Leader;
        assert_eq!(config_copy.required_acks(), 0);

        config_copy.ack_level = AckLevel::Quorum;
        assert_eq!(config_copy.required_acks(), 2); // (3/2) + 1 = 2

        config_copy.ack_level = AckLevel::All;
        assert_eq!(config_copy.required_acks(), 3);
    }

    #[test]
    fn test_change() {
        let put = Change::put(b"key1", b"value1", 1);
        assert!(put.is_put());
        assert!(!put.is_delete());
        assert_eq!(put.key(), b"key1");
        assert_eq!(put.version(), 1);

        let delete = Change::delete(b"key2", 2);
        assert!(delete.is_delete());
        assert!(!delete.is_put());
        assert_eq!(delete.key(), b"key2");
        assert_eq!(delete.version(), 2);
    }
}
