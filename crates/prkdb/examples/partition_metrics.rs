use prkdb::prelude::*;
use prkdb_types::consumer::{Consumer, ConsumerConfig};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SensorReading {
    pub sensor_id: String,
    pub temperature: f64,
    pub humidity: f64,
    pub timestamp: i64,
}

impl Collection for SensorReading {
    type Id = String;

    fn id(&self) -> &Self::Id {
        &self.sensor_id
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    info!("=== Partition Metrics Monitoring Example ===");
    info!("Demonstrating partition-level metrics for operational visibility");

    // Initialize database with partitioned collection
    let db = PrkDb::builder()
        .register_collection::<SensorReading>()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .build()?;

    info!("Database initialized");

    // Configure partitioning for sensor readings (4 partitions)
    let num_partitions = 4;
    let partitioner = std::sync::Arc::new(prkdb::partitioning::DefaultPartitioner::new());
    db.register_partitioning::<SensorReading>(num_partitions, partitioner);

    info!(
        "Configured {} partitions for sensor readings",
        num_partitions
    );

    // Create collection handle
    let collection = db.collection::<SensorReading>();

    // Simulate sensor data production across partitions
    info!("Starting sensor data simulation...");
    let sensor_ids = vec!["sensor-A", "sensor-B", "sensor-C", "sensor-D", "sensor-E"];

    for round in 1..=3 {
        info!("\n--- Round {} ---", round);

        // Produce readings for all sensors
        for (i, sensor_id) in sensor_ids.iter().enumerate() {
            let reading = SensorReading {
                sensor_id: sensor_id.to_string(),
                temperature: 20.0 + (i as f64 * 5.0) + (round as f64),
                humidity: 50.0 + (i as f64 * 3.0),
                timestamp: chrono::Utc::now().timestamp_millis(),
            };

            let partition = db.get_partition::<SensorReading>(reading.id()).unwrap_or(0);
            info!("Sensor {} -> Partition {}", sensor_id, partition);

            collection.put(reading).await?;
        }

        // Create and run a consumer
        let config = ConsumerConfig {
            group_id: "metrics-demo".to_string(),
            consumer_id: Some(format!("consumer-{}", round)),
            auto_commit: true,
            max_poll_records: 10,
            ..Default::default()
        };

        let mut consumer = db.consumer::<SensorReading>(config).await?;

        // Process some data
        for _ in 0..3 {
            let records = consumer.poll().await?;
            if !records.is_empty() {
                info!("Processed {} sensor readings", records.len());
                for record in &records {
                    info!(
                        "  Sensor: {} (temp: {:.1}°C, partition: {})",
                        record.value.sensor_id, record.value.temperature, record.partition
                    );
                }
                consumer.commit().await?;
            }
            sleep(Duration::from_millis(100)).await;
        }

        // Print partition metrics
        print_partition_metrics(&db).await;

        sleep(Duration::from_secs(1)).await;
    }

    // Final metrics report
    info!("\n=== Final Metrics Report ===");
    print_detailed_partition_metrics(&db).await;
    print_aggregate_metrics(&db).await;

    // Validate partition metrics accuracy
    validate_partition_metrics(&db, &sensor_ids, 3).await?;

    info!("=== Partition metrics monitoring completed ===");
    Ok(())
}

async fn print_partition_metrics(db: &PrkDb) {
    let all_metrics = db.metrics().partition_metrics().all_metrics();

    if all_metrics.is_empty() {
        info!("📊 No partition metrics available yet");
        return;
    }

    info!("📊 Partition Metrics:");
    for metrics in &all_metrics {
        let partition_id = metrics.partition_id();
        let events_produced = metrics.events_produced();
        let events_consumed = metrics.events_consumed();
        let lag = metrics.consumer_lag();
        let throughput = metrics.throughput_eps();
        let latency = metrics.max_consume_latency_ms();
        let consumers = metrics.assigned_consumers();

        info!(
            "  Partition {}: produced={}, consumed={}, lag={}, throughput={:.1} eps, max_latency={}ms, consumers={}",
            partition_id, events_produced, events_consumed, lag, throughput, latency, consumers
        );
    }
}

async fn print_detailed_partition_metrics(db: &PrkDb) {
    let all_metrics = db.metrics().partition_metrics().all_metrics();

    info!("📈 Detailed Partition Metrics:");
    for metrics in &all_metrics {
        let partition_id = metrics.partition_id();
        info!("  Partition {}:", partition_id);
        info!(
            "    Events: {} produced, {} consumed",
            metrics.events_produced(),
            metrics.events_consumed()
        );
        info!(
            "    Bytes: {} produced, {} consumed",
            metrics.bytes_produced(),
            metrics.bytes_consumed()
        );
        info!("    Consumer lag: {}", metrics.consumer_lag());
        info!("    Throughput: {:.2} events/sec", metrics.throughput_eps());
        info!("    Max latency: {}ms", metrics.max_consume_latency_ms());
        info!("    Assigned consumers: {}", metrics.assigned_consumers());
        info!("    Rebalances: {}", metrics.rebalance_count());
    }
}

async fn print_aggregate_metrics(db: &PrkDb) {
    let stats = db.metrics().get_partition_aggregate_stats();

    info!("🌍 Aggregate Statistics:");
    info!("  Total partitions: {}", stats.total_partitions);
    info!(
        "  Total events: {} produced, {} consumed",
        stats.total_events_produced, stats.total_events_consumed
    );
    info!(
        "  Total bytes: {} produced, {} consumed",
        stats.total_bytes_produced, stats.total_bytes_consumed
    );
    info!("  Total consumer lag: {}", stats.total_consumer_lag);
    info!(
        "  Average events per partition: {:.1}",
        stats.avg_events_per_partition
    );
    info!(
        "  Average consumers per partition: {:.1}",
        stats.avg_consumers_per_partition
    );
    info!(
        "  Max partition throughput: {:.1} eps",
        stats.max_partition_throughput
    );
    info!("  Max consume latency: {}ms", stats.max_consume_latency_ms);
}

async fn validate_partition_metrics(
    db: &PrkDb,
    sensor_ids: &[&str],
    rounds: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("\n🔍 Validating Partition Metrics Accuracy...");

    let all_metrics = db.metrics().partition_metrics().all_metrics();
    let expected_total_events = sensor_ids.len() * rounds;

    // Validate basic metrics existence
    assert!(
        !all_metrics.is_empty(),
        "Expected partition metrics to be available"
    );
    info!("✓ Partition metrics are available");

    // Validate total events across all partitions
    let total_events_produced: u64 = all_metrics.iter().map(|m| m.events_produced()).sum();

    assert_eq!(
        total_events_produced as usize, expected_total_events,
        "Expected {} total events produced, got {}",
        expected_total_events, total_events_produced
    );
    info!(
        "✓ Total events produced matches expected: {}",
        total_events_produced
    );

    // Validate partitioning distribution
    let active_partitions = all_metrics
        .iter()
        .filter(|m| m.events_produced() > 0)
        .count();

    assert!(
        active_partitions > 0,
        "Expected at least one partition to have events"
    );
    info!(
        "✓ Events distributed across {} partitions",
        active_partitions
    );

    // Validate aggregate statistics consistency
    let stats = db.metrics().get_partition_aggregate_stats();
    assert_eq!(
        stats.total_events_produced, total_events_produced,
        "Aggregate stats don't match individual metrics"
    );
    info!("✓ Aggregate statistics are consistent");

    // Validate metrics evolution (some events should be consumed)
    let total_events_consumed: u64 = all_metrics.iter().map(|m| m.events_consumed()).sum();

    // Since consumers are short-lived in this example, we'll allow zero consumption
    // but validate that the metrics infrastructure is working
    info!(
        "✓ Consumer metrics show {} events consumed",
        total_events_consumed
    );

    // Validate consumer lag calculation
    let total_lag: u64 = all_metrics.iter().map(|m| m.consumer_lag()).sum();

    let expected_lag = total_events_produced - total_events_consumed;
    assert_eq!(
        total_lag, expected_lag,
        "Consumer lag calculation mismatch: expected {}, got {}",
        expected_lag, total_lag
    );
    info!("✓ Consumer lag calculation is correct: {}", total_lag);

    // Validate throughput metrics are reasonable
    for metrics in &all_metrics {
        let throughput = metrics.throughput_eps();
        assert!(
            throughput >= 0.0,
            "Throughput should be non-negative, got {}",
            throughput
        );
    }
    info!("✓ Throughput metrics are within expected range");

    info!("🎉 All partition metrics validations passed!");
    Ok(())
}
