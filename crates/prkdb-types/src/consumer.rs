//! Consumer types and traits for PrkDB.
//!
//! This module provides Kafka-style consumer abstractions for streaming data from PrkDB.

use crate::collection::Collection;
use crate::error::Error;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display};
use std::time::Duration;

/// Auto offset reset behavior when no committed offset exists
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum AutoOffsetReset {
    /// Start consuming from the latest offset
    #[default]
    Latest,
    /// Start consuming from the earliest offset
    Earliest,
    /// Fail if no committed offset exists
    None,
}

/// Consumer group identifier
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct ConsumerGroupId(pub String);

impl Display for ConsumerGroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for ConsumerGroupId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ConsumerGroupId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Offset in a topic/partition
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Offset(pub u64);

impl Offset {
    pub fn zero() -> Self {
        Self(0)
    }

    pub fn from_value(v: u64) -> Self {
        Self(v)
    }

    pub fn value(&self) -> u64 {
        self.0
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Offset {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

/// A record received by a consumer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerRecord<C> {
    /// Topic name
    pub topic: String,
    /// Partition number
    pub partition: u32,
    /// Offset within the partition
    pub offset: u64,
    /// Record key
    pub key: Vec<u8>,
    /// Record value (deserialized)
    pub value: C,
    /// Timestamp (milliseconds since epoch)
    pub timestamp: i64,
}

impl<C> ConsumerRecord<C> {
    pub fn new(
        topic: String,
        partition: u32,
        offset: u64,
        key: Vec<u8>,
        value: C,
        timestamp: i64,
    ) -> Self {
        Self {
            topic,
            partition,
            offset,
            key,
            value,
            timestamp,
        }
    }
}

/// Consumer configuration
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Consumer group ID
    pub group_id: String,
    /// Optional consumer instance ID
    pub consumer_id: Option<String>,
    /// What to do when no committed offset exists
    pub auto_offset_reset: AutoOffsetReset,
    /// Whether to auto-commit offsets
    pub auto_commit: bool,
    /// Interval between auto-commits
    pub auto_commit_interval: Duration,
    /// Maximum records to return in a single poll
    pub max_poll_records: usize,
    /// Dead letter queue topic name (None = disabled)
    pub dead_letter_topic: Option<String>,
    /// Max retries before sending to DLQ (default: 3)
    pub max_retries: u8,
    /// Backoff between retries
    pub retry_backoff: Duration,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: "default".to_string(),
            consumer_id: None,
            auto_offset_reset: AutoOffsetReset::Latest,
            auto_commit: true,
            auto_commit_interval: Duration::from_secs(5),
            max_poll_records: 100,
            dead_letter_topic: None,
            max_retries: 3,
            retry_backoff: Duration::from_millis(100),
        }
    }
}

impl ConsumerConfig {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            ..Default::default()
        }
    }

    pub fn with_consumer_id(mut self, id: impl Into<String>) -> Self {
        self.consumer_id = Some(id.into());
        self
    }

    pub fn with_auto_offset_reset(mut self, reset: AutoOffsetReset) -> Self {
        self.auto_offset_reset = reset;
        self
    }

    pub fn with_auto_commit(mut self, enabled: bool) -> Self {
        self.auto_commit = enabled;
        self
    }

    pub fn with_max_poll_records(mut self, max: usize) -> Self {
        self.max_poll_records = max;
        self
    }

    pub fn with_dead_letter_topic(mut self, topic: impl Into<String>) -> Self {
        self.dead_letter_topic = Some(topic.into());
        self
    }

    pub fn with_max_retries(mut self, max: u8) -> Self {
        self.max_retries = max;
        self
    }

    pub fn with_retry_backoff(mut self, backoff: Duration) -> Self {
        self.retry_backoff = backoff;
        self
    }
}

/// Result of a commit operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitResult {
    Success,
    Failure,
}

impl CommitResult {
    pub fn success(_offset: Offset) -> Self {
        Self::Success
    }

    pub fn failure(_offset: Offset, _reason: String) -> Self {
        Self::Failure
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }
}

/// Result of processing a message
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessResult {
    /// Message processed successfully
    Ok,
    /// Retry processing later (will respect retry_backoff)
    RetryLater,
    /// Send to DLQ with error message
    SendToDlq(String),
}

/// A record that was sent to the Dead Letter Queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqRecord<C> {
    /// The original record that failed processing
    pub original_record: ConsumerRecord<C>,
    /// Error message describing the failure
    pub error_message: String,
    /// Number of retry attempts before DLQ
    pub retry_count: u8,
    /// Timestamp when sent to DLQ (millis since epoch)
    pub dlq_timestamp: i64,
    /// Original topic/collection
    pub source_topic: String,
}

/// Consumer trait for streaming data from collections
#[async_trait]
pub trait Consumer<C: Collection>: Send + Sync {
    /// Poll for new records
    async fn poll(&mut self) -> Result<Vec<ConsumerRecord<C>>, Error>;

    /// Commit the current position
    async fn commit(&mut self) -> Result<CommitResult, Error>;

    /// Seek to a specific offset
    async fn seek(&mut self, offset: Offset) -> Result<(), Error>;

    /// Commit a specific offset
    async fn commit_offset(&mut self, offset: Offset) -> Result<CommitResult, Error>;

    /// Get current position
    fn position(&self) -> Offset;

    /// Get committed offset
    async fn committed(&self) -> Result<Option<Offset>, Error>;

    /// Close the consumer
    async fn close(&mut self) -> Result<(), Error>;
}

/// Trait for storing consumer offsets
#[async_trait]
pub trait OffsetStore: Send + Sync {
    /// Get committed offset for a consumer group/topic/partition
    async fn get_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
    ) -> Result<Option<Offset>, Error>;

    /// Save committed offset
    async fn save_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: Offset,
    ) -> Result<(), Error>;

    /// List all consumer groups
    async fn list_groups(&self) -> Result<Vec<ConsumerGroupId>, Error>;

    /// Get all offsets for a group
    async fn get_group_offsets(&self, group_id: &str) -> Result<DashMap<String, Offset>, Error>;

    /// Delete a consumer group
    async fn delete_group(&self, group_id: &str) -> Result<(), Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_operations() {
        let offset = Offset::zero();
        assert_eq!(offset.value(), 0);

        let next = offset.next();
        assert_eq!(next.value(), 1);

        let from_val: Offset = 42u64.into();
        assert_eq!(from_val.value(), 42);
    }

    #[test]
    fn test_consumer_group_id() {
        let id: ConsumerGroupId = "my-group".into();
        assert_eq!(id.to_string(), "my-group");
    }

    #[test]
    fn test_consumer_config_builder() {
        let config = ConsumerConfig::new("test-group")
            .with_consumer_id("consumer-1")
            .with_auto_offset_reset(AutoOffsetReset::Earliest)
            .with_auto_commit(false)
            .with_max_poll_records(50);

        assert_eq!(config.group_id, "test-group");
        assert_eq!(config.consumer_id, Some("consumer-1".to_string()));
        assert_eq!(config.auto_offset_reset, AutoOffsetReset::Earliest);
        assert!(!config.auto_commit);
        assert_eq!(config.max_poll_records, 50);
    }

    #[test]
    fn test_commit_result() {
        let success = CommitResult::success(Offset::zero());
        assert!(success.is_success());

        let failure = CommitResult::failure(Offset::zero(), "error".to_string());
        assert!(!failure.is_success());
    }
}
