//! Dead Letter Queue (DLQ) support for PrkDB consumers.
//!
//! This module provides:
//! - `MessageHandler` trait for processing records with retry/DLQ routing
//! - `DlqProducer` for sending failed messages to DLQ topics
//! - `DlqConsumer` for consuming from DLQ topics
//! - Utilities for DLQ topic naming and management

use crate::db::PrkDb;
use async_trait::async_trait;
use prkdb_types::collection::Collection;
use prkdb_types::consumer::{ConsumerRecord, DlqRecord, ProcessResult};
use prkdb_types::error::Error;
use serde::{de::DeserializeOwned, Serialize};
use std::time::Duration;
use tracing::{debug, info, warn};

/// Trait for handling messages with automatic retry/DLQ routing
#[async_trait]
pub trait MessageHandler<C: Collection>: Send + Sync {
    /// Process a single record
    ///
    /// Return:
    /// - `ProcessResult::Ok` - Message processed successfully
    /// - `ProcessResult::RetryLater` - Retry after backoff
    /// - `ProcessResult::SendToDlq(error)` - Send to DLQ immediately
    async fn handle(&self, record: &ConsumerRecord<C>) -> ProcessResult;
}

/// Get the default DLQ topic name for a collection
pub fn dlq_topic_name<C: Collection>() -> String {
    format!("{}.dlq", std::any::type_name::<C>())
}

/// Internal state for tracking retries per record
#[derive(Debug, Clone)]
pub(crate) struct RetryState {
    pub retry_count: u8,
    pub last_error: Option<String>,
}

impl Default for RetryState {
    fn default() -> Self {
        Self {
            retry_count: 0,
            last_error: None,
        }
    }
}

/// Send a record to the Dead Letter Queue
pub async fn send_to_dlq<C>(
    db: &PrkDb,
    record: ConsumerRecord<C>,
    error_message: String,
    retry_count: u8,
    dlq_topic: &str,
) -> Result<(), Error>
where
    C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let dlq_record = DlqRecord {
        source_topic: record.topic.clone(),
        original_record: record,
        error_message,
        retry_count,
        dlq_timestamp: now,
    };

    // Serialize and store in DLQ topic
    let key = format!(
        "{}:{}:{}",
        dlq_topic, dlq_record.original_record.partition, now
    );

    let value = bincode::serde::encode_to_vec(&dlq_record, bincode::config::standard())
        .map_err(|e| Error::Serialization(e.to_string()))?;

    db.storage.put(key.as_bytes(), &value).await?;

    info!(
        dlq_topic = dlq_topic,
        original_topic = dlq_record.source_topic,
        partition = dlq_record.original_record.partition,
        offset = dlq_record.original_record.offset,
        retry_count = retry_count,
        "Message sent to DLQ"
    );

    #[cfg(feature = "metrics")]
    {
        db.metrics.inc_dlq_saved();
    }

    Ok(())
}

/// Process records with automatic retry and DLQ routing
pub async fn process_with_handler<C, H>(
    db: &PrkDb,
    records: Vec<ConsumerRecord<C>>,
    handler: &H,
    max_retries: u8,
    retry_backoff: Duration,
    dlq_topic: Option<&str>,
) -> Result<ProcessingStats, Error>
where
    C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    H: MessageHandler<C>,
{
    let mut stats = ProcessingStats::default();

    for record in records {
        let mut retry_count = 0u8;

        loop {
            match handler.handle(&record).await {
                ProcessResult::Ok => {
                    stats.processed += 1;
                    debug!(
                        topic = record.topic,
                        partition = record.partition,
                        offset = record.offset,
                        "Message processed successfully"
                    );
                    break;
                }
                ProcessResult::RetryLater => {
                    retry_count += 1;
                    stats.retries += 1;

                    if retry_count >= max_retries {
                        if let Some(dlq) = dlq_topic {
                            send_to_dlq(
                                db,
                                record.clone(),
                                "Max retries exceeded".to_string(),
                                retry_count,
                                dlq,
                            )
                            .await?;
                            stats.dlq_sent += 1;
                        } else {
                            warn!(
                                topic = record.topic,
                                partition = record.partition,
                                offset = record.offset,
                                "Max retries exceeded but no DLQ configured, dropping message"
                            );
                            stats.dropped += 1;
                        }
                        break;
                    }

                    debug!(
                        topic = record.topic,
                        partition = record.partition,
                        offset = record.offset,
                        retry_count = retry_count,
                        max_retries = max_retries,
                        "Retrying message"
                    );

                    tokio::time::sleep(retry_backoff).await;
                }
                ProcessResult::SendToDlq(error) => {
                    if let Some(dlq) = dlq_topic {
                        send_to_dlq(db, record.clone(), error, retry_count, dlq).await?;
                        stats.dlq_sent += 1;
                    } else {
                        warn!(
                            topic = record.topic,
                            partition = record.partition,
                            offset = record.offset,
                            error = error,
                            "DLQ requested but no DLQ configured, dropping message"
                        );
                        stats.dropped += 1;
                    }
                    break;
                }
            }
        }
    }

    Ok(stats)
}

/// Statistics from processing a batch of records
#[derive(Debug, Clone, Default)]
pub struct ProcessingStats {
    /// Successfully processed messages
    pub processed: u64,
    /// Total retry attempts
    pub retries: u64,
    /// Messages sent to DLQ
    pub dlq_sent: u64,
    /// Messages dropped (no DLQ configured)
    pub dropped: u64,
}

impl ProcessingStats {
    pub fn total(&self) -> u64 {
        self.processed + self.dlq_sent + self.dropped
    }

    pub fn success_rate(&self) -> f64 {
        if self.total() == 0 {
            1.0
        } else {
            self.processed as f64 / self.total() as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dlq_topic_name() {
        // The exact name depends on type_name which varies
        let name = dlq_topic_name::<TestCollection>();
        assert!(name.ends_with(".dlq"));
    }

    #[test]
    fn test_processing_stats() {
        let mut stats = ProcessingStats::default();
        stats.processed = 8;
        stats.dlq_sent = 2;

        assert_eq!(stats.total(), 10);
        assert!((stats.success_rate() - 0.8).abs() < 0.001);
    }

    // Dummy collection for testing
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct TestCollection {
        id: u64,
    }

    impl Collection for TestCollection {
        type Id = u64;
        fn id(&self) -> &Self::Id {
            &self.id
        }
    }
}
