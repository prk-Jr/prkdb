use crate::collection::Collection;
use crate::error::Error;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum AutoOffsetReset {
    #[default]
    Latest,
    Earliest,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct ConsumerGroupId(pub String);

impl Display for ConsumerGroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

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
}

impl Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerRecord<C> {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub key: Vec<u8>,
    pub value: C,
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

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub group_id: String,
    pub consumer_id: Option<String>,
    pub auto_offset_reset: AutoOffsetReset,
    pub auto_commit: bool,
    pub auto_commit_interval: Duration,
    pub max_poll_records: usize,
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
        }
    }
}

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
}

#[async_trait]
pub trait Consumer<C: Collection>: Send + Sync {
    async fn poll(&mut self) -> Result<Vec<ConsumerRecord<C>>, Error>;
    async fn commit(&mut self) -> Result<CommitResult, Error>;
    async fn seek(&mut self, offset: Offset) -> Result<(), Error>;
    async fn commit_offset(&mut self, offset: Offset) -> Result<CommitResult, Error>;
    fn position(&self) -> Offset;
    async fn committed(&self) -> Result<Option<Offset>, Error>;
    async fn close(&mut self) -> Result<(), Error>;
}

#[async_trait]
pub trait OffsetStore: Send + Sync {
    async fn get_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
    ) -> Result<Option<Offset>, Error>;
    async fn save_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: Offset,
    ) -> Result<(), Error>;
    async fn list_groups(&self) -> Result<Vec<ConsumerGroupId>, Error>;
    async fn get_group_offsets(&self, group_id: &str) -> Result<DashMap<String, Offset>, Error>;
    async fn delete_group(&self, group_id: &str) -> Result<(), Error>;
}
