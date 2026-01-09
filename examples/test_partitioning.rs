use prkdb::{ConsumerConfig, PrkDb};
use prkdb::consumer::{AutoOffsetReset, Consumer, ConsumerExt};
use prkdb_macros::Collection;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, Level};

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TestOrder {
    #[id]
    pub id: u64,
    pub customer: String,
    pub amount: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .init();

    info!("=== Partitioning Coordination Test ===");

    // Create database with partitioned collection
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<TestOrder>()
        .build()?;

    let orders_collection = db.collection::<TestOrder>().with_partitions(4);
    info!("Created partitioned collection with 4 partitions");

    // Produce test orders
    for i in 1..=12 {
        let order = TestOrder {
            id: i,
            customer: format!("Customer-{}", i % 4),
            amount: i as u32 * 10,
        };
        
        if let Some(partition) = orders_collection.get_partition(&order.id) {
            info!("Order {} -> Partition {}", order.id, partition);
        }
        
        orders_collection.put(&order).await?;
    }

    sleep(Duration::from_millis(100)).await;

    // Test coordinated consumer group creation
    info!("Creating coordinated consumer group...");
    let consumer_ids = vec!["consumer-1".to_string(), "consumer-2".to_string(), "consumer-3".to_string()];
    let mut consumers = db.consumer_group::<TestOrder>("test-group", consumer_ids).await?;

    // Log partition assignments
    for consumer in &consumers {
        info!("Consumer {} assigned partitions: {:?}", 
              consumer.config.consumer_id, 
              consumer.assigned_partitions());
    }

    // Process records briefly
    let mut total_processed = 0;
    for _ in 0..5 {
        for consumer in &mut consumers {
            match consumer.poll().await {
                Ok(records) => {
                    for record in records {
                        info!("Consumer {} processed order {} from partition {}", 
                              consumer.config.consumer_id, 
                              record.value.id,
                              record.partition);
                        total_processed += 1;
                    }
                }
                Err(e) => {
                    eprintln!("Poll error: {}", e);
                }
            }
        }
        sleep(Duration::from_millis(200)).await;
    }

    info!("Total orders processed: {}", total_processed);
    info!("Test completed successfully");

    Ok(())
}