//! Basic consumer example
//!
//! This example demonstrates:
//! - Creating a consumer with a consumer group
//! - Polling for events
//! - Manual offset commits
//! - Graceful shutdown
//!
//! Run with:
//! ```bash
//! cargo run -p prkdb --example basic_consumer
//! ```

use prkdb::prelude::*;
use prkdb_core::consumer::{AutoOffsetReset, Consumer, ConsumerConfig};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::signal;
use tracing::{error, info, warn};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct Event {
    id: u64,
    event_type: String,
    payload: String,
    timestamp: i64,
}

impl Collection for Event {
    type Id = u64;
    fn id(&self) -> &Self::Id {
        &self.id
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    println!("=== PrkDB Basic Consumer Example ===");
    println!();

    // Create database with in-memory storage
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<Event>()
        .build()?;

    println!("✓ Database initialized");

    // Produce some sample events
    println!("\nProducing sample events...");
    let collection = db.collection::<Event>();
    let mut events_produced = 0;

    for i in 1..=10 {
        collection
            .put(Event {
                id: i,
                event_type: "user.signup".to_string(),
                payload: format!("{{\"user_id\": {}}}", i),
                timestamp: chrono::Utc::now().timestamp_millis(),
            })
            .await?;
        events_produced += 1;
        println!("  Produced event {}", i);
    }

    // Assert production
    assert_eq!(
        events_produced, 10,
        "Should have produced exactly 10 events"
    );
    println!("\n✅ Produced {} events", events_produced);

    // Create consumer
    println!("\nCreating consumer...");
    let config = ConsumerConfig {
        group_id: "example-consumer-group".to_string(),
        consumer_id: Some("consumer-1".to_string()),
        auto_offset_reset: AutoOffsetReset::Earliest,
        auto_commit: false, // Manual commit for demonstration
        max_poll_records: 5,
        ..Default::default()
    };

    let mut consumer = db.consumer::<Event>(config).await?;
    let initial_offset = consumer.position();
    println!("✓ Consumer created (group: example-consumer-group)");
    println!("  Starting offset: {}", initial_offset);

    // Note: offset is always non-negative as it's an unsigned integer

    // Consume events
    println!("\nConsuming events (Ctrl+C to stop)...");
    println!();

    let mut total_processed = 0;
    let mut shutdown = Box::pin(signal::ctrl_c());

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                println!("\n\nReceived shutdown signal");
                break;
            }
            result = consumer.poll() => {
                let records = result?;

                if records.is_empty() {
                    // No new events, wait a bit
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }

                println!("Polled {} records:", records.len());

                for record in &records {
                    println!(
                        "  [offset={}] Event {}: {} - {}",
                        record.offset,
                        record.value.id,
                        record.value.event_type,
                        record.value.payload
                    );
                    total_processed += 1;

                    // Assert record validity
                    assert!(record.value.id > 0, "Event ID should be positive");
                    assert_eq!(record.value.event_type, "user.signup", "Event type should match");
                    assert!(!record.value.payload.is_empty(), "Payload should not be empty");
                }

                // Manually commit after processing
                match consumer.commit().await {
                    Ok(result) => {
                        if matches!(result, prkdb_core::consumer::CommitResult::Success) {
                            info!("Manual commit successful");
                        } else {
                            warn!("Manual commit failed");
                        }
                    }
                    Err(e) => error!("Commit error: {}", e),
                }

                println!();

                // Stop after processing all events for this example
                if total_processed >= 10 {
                    println!("Processed all events, exiting...");
                    break;
                }
            }
        }
    }

    // Summary
    println!("\n=== Summary ===");
    println!("Total events processed: {}", total_processed);
    let final_offset = consumer.position();
    println!("Final offset: {}", final_offset);

    // Assert processing results
    assert_eq!(
        total_processed, events_produced,
        "Should have processed all produced events"
    );
    assert!(
        final_offset.value() > initial_offset.value(),
        "Final offset should be greater than initial offset"
    );

    // Graceful shutdown
    println!("\nShutting down consumer...");
    consumer.close().await?;
    println!("✓ Consumer closed");

    // Demonstrate offset persistence
    println!("\n=== Demonstrating Offset Persistence ===");
    println!("Creating new consumer with same group ID...");

    let config2 = ConsumerConfig {
        group_id: "example-consumer-group".to_string(),
        consumer_id: Some("consumer-2".to_string()),
        auto_offset_reset: AutoOffsetReset::Earliest,
        ..Default::default()
    };

    let consumer2 = db.consumer::<Event>(config2).await?;
    let committed = consumer2.committed().await?;

    match committed {
        Some(offset) => {
            println!("✓ Found committed offset: {}", offset);
            println!("  New consumer will start from this offset");

            // Assert offset persistence
            assert_eq!(
                offset, final_offset,
                "Committed offset should match final offset"
            );
        }
        None => {
            println!("✗ No committed offset found");
            panic!("Expected to find a committed offset");
        }
    }

    println!("\n=== Example Complete ===");
    println!("✅ All assertions passed!");
    println!("  ✅ Produced {} events", events_produced);
    println!("  ✅ Consumed {} events", total_processed);
    println!("  ✅ Offset persistence verified");
    println!("  ✅ Consumer lifecycle managed correctly");
    Ok(())
}
