use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Acknowledgment level for replication
#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, bincode::Encode, bincode::Decode,
)]
pub enum AckLevel {
    /// Fire-and-forget - don't wait for any replicas (fastest, no durability guarantee)
    None,

    /// Wait for leader write only - return as soon as leader commits (default)
    Leader,

    /// Wait for majority of replicas to acknowledge
    Quorum,

    /// Wait for all replicas to acknowledge
    All,
}

impl Default for AckLevel {
    fn default() -> Self {
        AckLevel::Leader
    }
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
        let mut config = ReplicationConfig::default();
        config.replica_addresses = vec![
            "localhost:9001".to_string(),
            "localhost:9002".to_string(),
            "localhost:9003".to_string(),
        ];

        config.ack_level = AckLevel::None;
        assert_eq!(config.required_acks(), 0);

        config.ack_level = AckLevel::Leader;
        assert_eq!(config.required_acks(), 0);

        config.ack_level = AckLevel::Quorum;
        assert_eq!(config.required_acks(), 2); // (3/2) + 1 = 2

        config.ack_level = AckLevel::All;
        assert_eq!(config.required_acks(), 3);
    }
}
