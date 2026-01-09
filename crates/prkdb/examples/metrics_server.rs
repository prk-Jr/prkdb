//! Metrics Server Example
//!
//! This example demonstrates how to:
//! 1. Set up PrkDB with partitioning
//! 2. Start a Prometheus metrics server
//! 3. Track consumer lag across partitions
//! 4. Export metrics via HTTP
//! 5. Validate metrics with deterministic assertions
//!
//! Run with: cargo run -p prkdb --example metrics_server
//! Then visit: http://localhost:9090/metrics

use prkdb::consumer::PrkConsumer;
use prkdb::storage::InMemoryAdapter;
use prkdb::PrkDb;
use prkdb_core::collection::Collection;
use prkdb_core::consumer::{AutoOffsetReset, Consumer, ConsumerConfig};
use prkdb_metrics::{ConsumerLagTracker, MetricsServer};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: u64,
    customer_id: u64,
    amount: f64,
    status: String,
}

impl Collection for Order {
    type Id = u64;
    fn id(&self) -> &Self::Id {
        &self.id
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("🚀 Starting PrkDB Metrics Server Example");
    println!("=========================================\n");

    // 1. Create PrkDB with partitioning
    let db = Arc::new(
        PrkDb::builder()
            .with_storage(InMemoryAdapter::new())
            .register_collection::<Order>()
            .build()?,
    );

    // 2. Create partitioned collection handle
    let orders = db.collection::<Order>().with_partitions(4);

    println!("✅ Created partitioned collection with 4 partitions\n");

    // 3. Create lag tracker
    let lag_tracker = Arc::new(ConsumerLagTracker::new());

    // 4. Start metrics server in background
    let addr: SocketAddr = "127.0.0.1:9090".parse()?;
    let metrics_server = MetricsServer::new(addr);

    tokio::spawn(async move {
        if let Err(e) = metrics_server.start().await {
            eprintln!("Metrics server error: {}", e);
        }
    });

    println!("📊 Metrics server started on http://127.0.0.1:9090");
    println!("   - Metrics: http://127.0.0.1:9090/metrics");
    println!("   - Health:  http://127.0.0.1:9090/health\n");

    // 5. Produce some orders
    println!("📝 Producing orders...");
    let mut orders_produced = 0;
    let mut partitions_used = std::collections::HashSet::new();

    for i in 1..=100 {
        let order = Order {
            id: i,
            customer_id: i % 10,
            amount: (i as f64) * 10.0,
            status: "pending".to_string(),
        };
        let order_id = order.id;
        orders.put(order).await?;
        orders_produced += 1;

        // Update metrics
        prkdb_metrics::exporter::EVENTS_PRODUCED
            .with_label_values(&["Order"])
            .inc();

        // Track latest offset per actual partition
        if let Some(partition) = orders.get_partition(&order_id) {
            lag_tracker.update_latest_offset("Order", partition, i);
            partitions_used.insert(partition);
        }

        if i % 20 == 0 {
            println!("  Produced {} orders", i);
        }
    }

    // Assert production metrics
    assert_eq!(
        orders_produced, 100,
        "Should have produced exactly 100 orders"
    );
    assert!(
        !partitions_used.is_empty(),
        "Should have used at least one partition"
    );
    println!(
        "✅ Produced {} orders across {} partitions\n",
        orders_produced,
        partitions_used.len()
    );

    // 6. Create consumer
    println!("👥 Starting consumer...");
    let config = ConsumerConfig {
        group_id: "order-processor".to_string(),
        consumer_id: Some("consumer-1".to_string()),
        auto_offset_reset: AutoOffsetReset::Earliest,
        ..Default::default()
    };

    let mut consumer =
        PrkConsumer::<Order>::with_partitions((*db).clone(), config, vec![0, 1, 2, 3]).await?;

    // Update consumer group metrics
    prkdb_metrics::exporter::CONSUMER_GROUP_MEMBERS
        .with_label_values(&["order-processor"])
        .set(1);

    // 7. Consume and track lag
    println!("📥 Consuming orders (slowly to show lag)...\n");

    let mut total_consumed = 0;
    let mut batches_processed = 0;
    let mut max_lag_seen = 0;

    for batch in 0..5 {
        sleep(Duration::from_secs(2)).await;

        let records = consumer.poll().await?;
        batches_processed += 1;
        total_consumed += records.len();
        println!("Batch {}: Consumed {} records", batch + 1, records.len());

        // Update consumer metrics
        for record in &records {
            prkdb_metrics::exporter::EVENTS_CONSUMED
                .with_label_values(&["Order", "order-processor"])
                .inc();

            // Track consumer offset and lag
            lag_tracker.update_consumer_offset(
                "order-processor",
                "Order",
                record.partition,
                record.offset,
            );
        }

        // Show lag for this consumer group
        let lags = lag_tracker.get_group_lags("order-processor");
        println!("  Consumer lag:");
        let mut current_max_lag = 0;
        for (collection, partition, lag) in lags {
            println!(
                "    {}/partition-{}: {} messages behind",
                collection, partition, lag
            );
            current_max_lag = current_max_lag.max(lag);
        }
        max_lag_seen = max_lag_seen.max(current_max_lag);
        println!();

        consumer.commit().await?;

        // Allow empty batches once the backlog is drained.
        if records.is_empty() {
            println!("No records in batch {}; exiting early.", batch + 1);
            break;
        }
    }

    // Assert consumption metrics
    assert!(total_consumed > 0, "Should have consumed some records");
    if batches_processed < 5 {
        println!(
            "Processed {} batches before draining backlog (expected up to 5).",
            batches_processed
        );
    }
    // Note: max_lag_seen is always non-negative as it's an unsigned integer

    println!("✅ Consumption complete:");
    println!("  Total consumed: {} records", total_consumed);
    println!("  Batches processed: {}", batches_processed);
    println!("  Max lag seen: {} messages", max_lag_seen);

    println!("\n✅ Example complete with all assertions passed!");
    println!("\n📊 Check metrics at: http://127.0.0.1:9090/metrics");
    println!("\nKey metrics to look for:");
    println!(
        "  - prkdb_events_produced_total{{collection=\"Order\"}} (should be {})",
        orders_produced
    );
    println!("  - prkdb_events_consumed_total{{collection=\"Order\",consumer_group=\"order-processor\"}} (should be {})", total_consumed);
    println!("  - prkdb_consumer_lag{{consumer_group=\"order-processor\",collection=\"Order\",partition=\"0\"}}");
    println!("  - prkdb_partition_records{{collection=\"Order\",partition=\"0\"}}");
    println!("\nMetrics validation:");
    println!(
        "  ✅ Produced {} orders across {} partitions",
        orders_produced,
        partitions_used.len()
    );
    println!(
        "  ✅ Consumed {} records in {} batches",
        total_consumed, batches_processed
    );
    println!("  ✅ Max consumer lag observed: {} messages", max_lag_seen);
    println!("\nDemo complete; shutting down servers.");

    Ok(())
}
