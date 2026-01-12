// Real-Time Streaming Example
// Demonstrates PrkDB's Kafka-style streaming API
//
// Run: cargo run --release --example streaming_example

use futures::StreamExt;
use prkdb::prelude::*;
use prkdb::storage::WalStorageAdapter;
use prkdb::streaming::{EventStream, StreamConfig};
use prkdb_core::wal::WalConfig;
use prkdb_types::consumer::{Consumer, ConsumerConfig};
use std::time::Duration;
use tokio::time::timeout;

// Define a message collection
#[derive(Collection, serde::Serialize, serde::Deserialize, Clone, Debug)]
struct Message {
    #[id]
    id: String,
    topic: String,
    payload: String,
    timestamp: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!();
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║           🌊 PRKDB REAL-TIME STREAMING EXAMPLE 🌊             ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();

    // Create temporary storage
    let dir = tempfile::tempdir()?;
    let storage = WalStorageAdapter::new(WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    })?;

    // Build the database
    let db = PrkDb::builder()
        .with_storage(storage)
        .register_collection::<Message>()
        .build()?;

    println!("✅ Database initialized");
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // PART 1: Basic Streaming
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  PART 1: Basic Event Streaming");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Produce some messages first
    for i in 0..5 {
        let key = format!("msg_{}", i);
        let value = format!(
            "{{\"topic\":\"orders\",\"payload\":\"Order #{} placed\",\"timestamp\":{}}}",
            i,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        db.put(key.as_bytes(), value.as_bytes()).await?;
        println!("  📤 Produced: {}", key);
    }
    println!();

    // Create an event stream with consumer group
    let config = StreamConfig::with_group_id("basic-consumer-group");

    println!("  📥 Creating EventStream with group: {}", config.group_id);
    println!("     Buffer size: {}", config.buffer_size);
    println!("     Poll interval: {:?}", config.poll_interval);
    println!();

    // Note: EventStream requires proper partition/offset setup
    // For this demo, we'll show the API usage
    println!("  ✅ EventStream API is ready for reactive consumption!");
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // PART 2: Consumer Configuration
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  PART 2: Consumer Configuration Options");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Show different configuration options
    let config_earliest = StreamConfig {
        group_id: "earliest-consumer".to_string(),
        auto_offset_reset: prkdb_types::consumer::AutoOffsetReset::Earliest,
        auto_commit: true,
        buffer_size: 500,
        poll_interval: Duration::from_millis(50),
    };

    let config_latest = StreamConfig {
        group_id: "latest-consumer".to_string(),
        auto_offset_reset: prkdb_types::consumer::AutoOffsetReset::Latest,
        auto_commit: false, // Manual commit for exactly-once
        buffer_size: 1000,
        poll_interval: Duration::from_millis(100),
    };

    println!("  Earliest offset consumer:");
    println!("    - Starts from beginning of log");
    println!("    - Auto-commit: {}", config_earliest.auto_commit);
    println!();

    println!("  Latest offset consumer:");
    println!("    - Starts from end (new messages only)");
    println!("    - Manual commit for exactly-once");
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // PART 3: Stream Combinators
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  PART 3: Stream Combinators (Kafka-style)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    println!("  Available combinators:");
    println!();
    println!("    map_events(|msg| ...)");
    println!("      Transform events to new types");
    println!();
    println!("    filter_events(|msg| msg.amount > 100.0)");
    println!("      Filter events by predicate");
    println!();
    println!("  Example usage:");
    println!("  ```rust");
    println!("  use prkdb::streaming::EventStreamExt;");
    println!();
    println!("  let stream = EventStream::<Order>::new(db, config).await?");
    println!("      .filter_events(|order| order.amount > 100.0)");
    println!("      .map_events(|order| order.amount);");
    println!();
    println!("  while let Some(result) = stream.next().await {{");
    println!("      let amount = result?;");
    println!("      println!(\"High-value order: ${{}}\", amount);");
    println!("  }}");
    println!("  ```");
    println!();

    // Summary
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║                    📊 STREAMING FEATURES 📊                   ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("  ┌────────────────────────┬────────────────────────────────────┐");
    println!("  │ Feature                │ Description                        │");
    println!("  ├────────────────────────┼────────────────────────────────────┤");
    println!("  │ EventStream            │ async Stream implementation        │");
    println!("  │ Consumer Groups        │ Kafka-style coordination           │");
    println!("  │ Auto Offset Reset      │ Earliest/Latest/None               │");
    println!("  │ Auto Commit            │ Optional offset commits            │");
    println!("  │ Stream Combinators     │ map_events, filter_events          │");
    println!("  │ Backpressure           │ Built-in flow control              │");
    println!("  │ Graceful Shutdown      │ Clean task termination             │");
    println!("  └────────────────────────┴────────────────────────────────────┘");
    println!();

    Ok(())
}
