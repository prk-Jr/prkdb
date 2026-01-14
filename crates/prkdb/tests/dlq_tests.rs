//! Dead Letter Queue Integration Tests

use prkdb::dlq::{process_with_handler, MessageHandler};
use prkdb::prelude::*;
use prkdb::storage::InMemoryAdapter;
use prkdb_types::consumer::{ConsumerConfig, ConsumerRecord, ProcessResult};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct TestEvent {
    #[id]
    id: u64,
    data: String,
}

/// Handler that succeeds after N retries
struct RetryableHandler {
    attempts: Arc<AtomicU32>,
    succeed_after: u32,
}

impl RetryableHandler {
    fn new(succeed_after: u32) -> Self {
        Self {
            attempts: Arc::new(AtomicU32::new(0)),
            succeed_after,
        }
    }

    fn attempts(&self) -> u32 {
        self.attempts.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl MessageHandler<TestEvent> for RetryableHandler {
    async fn handle(&self, _record: &ConsumerRecord<TestEvent>) -> ProcessResult {
        let attempt = self.attempts.fetch_add(1, Ordering::Relaxed);
        if attempt >= self.succeed_after {
            ProcessResult::Ok
        } else {
            ProcessResult::RetryLater
        }
    }
}

/// Handler that always fails with DLQ
struct AlwaysFailHandler {
    error_message: String,
}

impl AlwaysFailHandler {
    fn new(msg: impl Into<String>) -> Self {
        Self {
            error_message: msg.into(),
        }
    }
}

#[async_trait::async_trait]
impl MessageHandler<TestEvent> for AlwaysFailHandler {
    async fn handle(&self, _record: &ConsumerRecord<TestEvent>) -> ProcessResult {
        ProcessResult::SendToDlq(self.error_message.clone())
    }
}

/// Handler that succeeds immediately
struct SuccessHandler;

#[async_trait::async_trait]
impl MessageHandler<TestEvent> for SuccessHandler {
    async fn handle(&self, _record: &ConsumerRecord<TestEvent>) -> ProcessResult {
        ProcessResult::Ok
    }
}

#[tokio::test]
async fn test_process_with_handler_success() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<TestEvent>()
        .build()
        .unwrap();

    let records = vec![
        ConsumerRecord::new(
            "TestEvent".to_string(),
            0,
            1,
            vec![],
            TestEvent {
                id: 1,
                data: "test1".to_string(),
            },
            0,
        ),
        ConsumerRecord::new(
            "TestEvent".to_string(),
            0,
            2,
            vec![],
            TestEvent {
                id: 2,
                data: "test2".to_string(),
            },
            0,
        ),
    ];

    let handler = SuccessHandler;
    let stats = process_with_handler(
        &db,
        records,
        &handler,
        3,
        Duration::from_millis(10),
        Some("TestEvent.dlq"),
    )
    .await
    .unwrap();

    assert_eq!(stats.processed, 2);
    assert_eq!(stats.retries, 0);
    assert_eq!(stats.dlq_sent, 0);
    assert_eq!(stats.dropped, 0);
    assert!((stats.success_rate() - 1.0).abs() < 0.001);
}

#[tokio::test]
async fn test_process_with_retry_then_success() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<TestEvent>()
        .build()
        .unwrap();

    let records = vec![ConsumerRecord::new(
        "TestEvent".to_string(),
        0,
        1,
        vec![],
        TestEvent {
            id: 1,
            data: "retry-test".to_string(),
        },
        0,
    )];

    // Handler succeeds after 2 retries
    let handler = RetryableHandler::new(2);
    let stats = process_with_handler(
        &db,
        records,
        &handler,
        5, // max_retries > succeed_after
        Duration::from_millis(1),
        Some("TestEvent.dlq"),
    )
    .await
    .unwrap();

    assert_eq!(stats.processed, 1, "Should succeed after retries");
    assert_eq!(stats.retries, 2, "Should have 2 retry attempts");
    assert_eq!(stats.dlq_sent, 0, "Should not send to DLQ");
    assert_eq!(handler.attempts(), 3, "Should have 3 total attempts");
}

#[tokio::test]
async fn test_process_exhausted_retries_sends_to_dlq() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<TestEvent>()
        .build()
        .unwrap();

    let records = vec![ConsumerRecord::new(
        "TestEvent".to_string(),
        0,
        1,
        vec![],
        TestEvent {
            id: 1,
            data: "exhaust-retries".to_string(),
        },
        0,
    )];

    // Handler never succeeds (requires 10, max is 3)
    let handler = RetryableHandler::new(10);
    let stats = process_with_handler(
        &db,
        records,
        &handler,
        3, // max_retries < succeed_after
        Duration::from_millis(1),
        Some("TestEvent.dlq"),
    )
    .await
    .unwrap();

    assert_eq!(stats.processed, 0, "Should not process");
    assert_eq!(stats.retries, 3, "Should exhaust retries");
    assert_eq!(stats.dlq_sent, 1, "Should send to DLQ");
}

#[tokio::test]
async fn test_process_immediate_dlq() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<TestEvent>()
        .build()
        .unwrap();

    let records = vec![ConsumerRecord::new(
        "TestEvent".to_string(),
        0,
        1,
        vec![],
        TestEvent {
            id: 1,
            data: "immediate-dlq".to_string(),
        },
        0,
    )];

    let handler = AlwaysFailHandler::new("Validation failed: invalid format");
    let stats = process_with_handler(
        &db,
        records,
        &handler,
        3,
        Duration::from_millis(10),
        Some("TestEvent.dlq"),
    )
    .await
    .unwrap();

    assert_eq!(stats.processed, 0);
    assert_eq!(stats.retries, 0, "No retries for immediate DLQ");
    assert_eq!(stats.dlq_sent, 1);
}

#[tokio::test]
async fn test_process_no_dlq_configured_drops_message() {
    let db = PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<TestEvent>()
        .build()
        .unwrap();

    let records = vec![ConsumerRecord::new(
        "TestEvent".to_string(),
        0,
        1,
        vec![],
        TestEvent {
            id: 1,
            data: "no-dlq".to_string(),
        },
        0,
    )];

    let handler = AlwaysFailHandler::new("Error");
    let stats = process_with_handler(
        &db,
        records,
        &handler,
        3,
        Duration::from_millis(10),
        None, // No DLQ configured
    )
    .await
    .unwrap();

    assert_eq!(stats.processed, 0);
    assert_eq!(stats.dlq_sent, 0);
    assert_eq!(stats.dropped, 1, "Should drop when no DLQ configured");
}

#[tokio::test]
async fn test_dlq_config_builder() {
    let config = ConsumerConfig::new("test-group")
        .with_dead_letter_topic("my-topic.dlq")
        .with_max_retries(5)
        .with_retry_backoff(Duration::from_millis(100));

    assert_eq!(config.dead_letter_topic, Some("my-topic.dlq".to_string()));
    assert_eq!(config.max_retries, 5);
    assert_eq!(config.retry_backoff, Duration::from_millis(100));
}
