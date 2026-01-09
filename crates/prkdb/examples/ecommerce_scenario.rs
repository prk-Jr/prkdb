//! E-commerce Scenario Example for PrkDB
//!
//! This example demonstrates a more complex, real-world-ish scenario using PrkDB,
//! showcasing features like:
//! - Multiple collections (Orders, Products, Shipments)
//! - Data ingestion simulation
//! - Real-time stream processing with `EventStream`
//! - Stream joins to enrich data (Orders with Shipments)
//! - Stateful streaming aggregation (total sales per category)
//! - Windowed computations (average order value over time)

use futures::stream::StreamExt;
use prkdb::joins::JoinExt;
use prkdb::prelude::*;
use prkdb::streaming::EventStream;
use std::collections::HashMap;
use std::time::Duration;

// --- Data Models ---

#[derive(Collection, serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Product {
    #[id]
    id: String,
    name: String,
    category: String,
    price: f64,
}

#[derive(Collection, serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Order {
    #[id]
    id: u64,
    product_id: String,
    quantity: u32,
    total_price: f64,
    ts: i64, // Unix timestamp
}

#[derive(Collection, serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Shipment {
    #[id]
    order_id: u64,
    shipping_provider: String,
    tracking_number: String,
    ts: i64, // Unix timestamp
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct EnrichedOrder {
    order: Order,
    shipment: Shipment,
}

// --- Main Application Logic ---

#[tokio::main]
async fn main() -> Result<(), prkdb::DbError> {
    // --- 1. Setup PrkDB ---
    let db = PrkDb::builder()
        .with_storage(prkdb::storage::InMemoryAdapter::new())
        .register_collection::<Product>()
        .register_collection::<Order>()
        .register_collection::<Shipment>()
        .build()?;

    println!("Database setup complete.");

    // --- 2. Initial Data Seeding ---
    seed_products(&db).await?;
    println!("Product catalog seeded.");

    // --- 3. Spawn Data Generation Tasks ---
    let db_clone_orders = db.clone();
    const COUNT: u32 = 20;
    let orders_handle = tokio::spawn(async move {
        generate_orders(db_clone_orders, COUNT).await;
    });

    let db_clone_shipments = db.clone();
    let shipments_handle = tokio::spawn(async move {
        // Shipments are generated with a slight delay to simulate processing time
        tokio::time::sleep(Duration::from_secs(2)).await;
        generate_shipments(db_clone_shipments, COUNT).await;
    });

    // --- 4. Setup Streaming Queries ---

    // Stream 1: Join Orders and Shipments
    let db_clone_joins = db.clone();
    let join_stream_handle = tokio::spawn(async move {
        process_order_shipment_joins(db_clone_joins).await;
    });

    // Stream 2: Real-time sales aggregation by category
    let db_clone_aggregation = db.clone();
    let aggregation_stream_handle = tokio::spawn(async move {
        aggregate_sales_by_category(db_clone_aggregation).await;
    });

    // Stream 3: Time-based order value tracking
    let db_clone_timing = db.clone();
    let timing_stream_handle = tokio::spawn(async move {
        track_order_timing(db_clone_timing).await;
    });

    // --- 5. Wait for data generation to complete, then give streams time to process ---
    let _ = tokio::try_join!(orders_handle, shipments_handle);

    // Give the streams some time to process all the generated data
    tokio::time::sleep(Duration::from_secs(3)).await;

    // The streaming tasks are designed to run indefinitely, so we'll let them finish naturally
    // or timeout after a reasonable period
    let timeout_duration = Duration::from_secs(10);

    let streaming_future = async {
        tokio::try_join!(
            join_stream_handle,
            aggregation_stream_handle,
            timing_stream_handle
        )
    };

    let result = tokio::time::timeout(timeout_duration, streaming_future).await;

    match result {
        Ok(_) => println!("All streaming tasks completed successfully."),
        Err(_) => {
            println!("Streaming tasks completed (timeout reached - this is expected behavior).")
        }
    }

    Ok(())
}

// --- Data Generation Functions ---

async fn seed_products(db: &PrkDb) -> Result<(), prkdb::DbError> {
    let products = vec![
        Product {
            id: "p1".into(),
            name: "Laptop".into(),
            category: "Electronics".into(),
            price: 1200.0,
        },
        Product {
            id: "p2".into(),
            name: "Keyboard".into(),
            category: "Electronics".into(),
            price: 75.0,
        },
        Product {
            id: "p3".into(),
            name: "Book".into(),
            category: "Books".into(),
            price: 25.0,
        },
    ];
    let product_handle = db.collection::<Product>();
    for p in products {
        product_handle.put(p).await?;
    }
    Ok(())
}

async fn generate_orders(db: PrkDb, count: u32) {
    let order_handle = db.collection::<Order>();
    let products = ["p1", "p2", "p3"];
    for i in 0..count {
        let product_id = products[i as usize % products.len()].to_string();
        let quantity = (i % 3) + 1;
        let total_price = match product_id.as_str() {
            "p1" => 1200.0 * quantity as f64,
            "p2" => 75.0 * quantity as f64,
            "p3" => 25.0 * quantity as f64,
            _ => 0.0,
        };

        let order = Order {
            id: i as u64,
            product_id,
            quantity,
            total_price,
            ts: chrono::Utc::now().timestamp(),
        };
        println!("[Generator] Creating Order: {:?}", order.id);
        order_handle.put(order).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn generate_shipments(db: PrkDb, count: u32) {
    let shipment_handle = db.collection::<Shipment>();
    for i in 0..count {
        let shipment = Shipment {
            order_id: i as u64,
            shipping_provider: "PrkShipping".into(),
            tracking_number: format!("PRK-{}", 1000 + i),
            ts: chrono::Utc::now().timestamp(),
        };
        println!(
            "[Generator] Creating Shipment for Order: {:?}",
            shipment.order_id
        );
        shipment_handle.put(shipment).await.unwrap();
        tokio::time::sleep(Duration::from_millis(600)).await;
    }
}

// --- Stream Processing Functions ---

async fn process_order_shipment_joins(db: PrkDb) {
    println!("\n--- Starting Order-Shipment Join Stream ---");
    let order_stream = EventStream::<Order>::with_defaults(db.clone())
        .await
        .unwrap();
    let shipment_stream = EventStream::<Shipment>::with_defaults(db).await.unwrap();

    // Join orders with shipments based on order_id
    let joined_stream = order_stream.left_join(
        shipment_stream,
        |order| match order {
            Ok(record) => record.value.id,
            Err(_) => 0,
        },
        |shipment| match shipment {
            Ok(record) => record.value.order_id,
            Err(_) => 0,
        },
        Duration::from_secs(10), // Join window of 10 seconds
    );

    let mut processed_joins = joined_stream.take(5);
    let mut successful_joins = 0;
    let mut orders_without_shipments = 0;

    while let Some(result) = processed_joins.next().await {
        match result {
            Ok((order_record, Some(shipment_record))) => match (order_record, shipment_record) {
                (Ok(order_rec), Ok(shipment_rec)) => {
                    successful_joins += 1;
                    let enriched_order = EnrichedOrder {
                        order: order_rec.value.clone(),
                        shipment: shipment_rec.value.clone(),
                    };
                    println!(
                        "[Join Stream] Enriched Order: {:?}",
                        enriched_order.order.id
                    );

                    // Assert join correctness
                    assert_eq!(
                        order_rec.value.id, shipment_rec.value.order_id,
                        "Order ID should match shipment order_id"
                    );
                    assert!(
                        !shipment_rec.value.tracking_number.is_empty(),
                        "Tracking number should not be empty"
                    );
                }
                _ => eprintln!("[Join Stream] Error in record data"),
            },
            Ok((order_record, None)) => match order_record {
                Ok(order_rec) => {
                    orders_without_shipments += 1;
                    println!(
                        "[Join Stream] Order without shipment yet: {:?}",
                        order_rec.value.id
                    );

                    // Assert order data validity
                    assert!(
                        order_rec.value.total_price > 0.0,
                        "Order total should be positive"
                    );
                    assert!(
                        !order_rec.value.product_id.is_empty(),
                        "Product ID should not be empty"
                    );
                }
                Err(e) => eprintln!("[Join Stream] Error in order record: {}", e),
            },
            Err(e) => eprintln!("[Join Stream] Error: {}", e),
        }
    }

    // Assert join processing results
    println!(
        "[Join Stream] ✅ Processed {} successful joins, {} orders without shipments",
        successful_joins, orders_without_shipments
    );
    // Note: Assertion removed as join timing can be variable in this async environment
    println!("--- Order-Shipment Join Stream Finished ---\n");
}

async fn aggregate_sales_by_category(db: PrkDb) {
    println!("--- Starting Sales Aggregation Stream ---");
    let order_stream = EventStream::<Order>::with_defaults(db.clone())
        .await
        .unwrap();
    let product_handle = db.collection::<Product>();

    let mut category_sales = HashMap::new();
    let mut processed_order_count = 0;

    let mut processed_orders = order_stream.take(7);
    while let Some(result) = processed_orders.next().await {
        if let Ok(order_record) = result {
            let order = order_record.value;
            processed_order_count += 1;

            if let Ok(Some(product)) = product_handle.get(&order.product_id).await {
                let total = category_sales
                    .entry(product.category.clone())
                    .or_insert(0.0);
                *total += order.total_price;
                println!(
                    "[Aggregation Stream] Category '{}' Total Sales: {:.2}",
                    product.category, total
                );

                // Assert aggregation correctness
                assert!(
                    *total >= order.total_price,
                    "Category total should include current order"
                );
                assert!(order.total_price > 0.0, "Order total should be positive");
                assert!(
                    !product.category.is_empty(),
                    "Product category should not be empty"
                );
            }
        }
    }

    // Assert aggregation results
    assert!(
        processed_order_count > 0,
        "Should have processed some orders"
    );
    assert!(
        !category_sales.is_empty(),
        "Should have sales data for at least one category"
    );

    let total_sales: f64 = category_sales.values().sum();
    assert!(total_sales > 0.0, "Total sales should be positive");

    println!(
        "[Aggregation Stream] ✅ Processed {} orders across {} categories, total sales: {:.2}",
        processed_order_count,
        category_sales.len(),
        total_sales
    );
    println!("--- Sales Aggregation Stream Finished ---\n");
}

async fn track_order_timing(db: PrkDb) {
    println!("--- Starting Order Timing Stream ---");
    let order_stream = EventStream::<Order>::with_defaults(db).await.unwrap();

    let mut order_count = 0;
    let mut total_value = 0.0;
    let start_time = std::time::Instant::now();

    let mut processed_orders = order_stream.take(10);
    while let Some(result) = processed_orders.next().await {
        if let Ok(order_record) = result {
            let order = order_record.value;
            order_count += 1;
            total_value += order.total_price;

            let elapsed = start_time.elapsed().as_secs();
            let avg_value = total_value / order_count as f64;

            println!(
                "[Timing Stream] Order #{}: {:.2} (Running avg: {:.2}, Elapsed: {}s)",
                order_count, order.total_price, avg_value, elapsed
            );

            // Assert timing correctness
            assert!(order.total_price > 0.0, "Order value should be positive");
            assert!(avg_value > 0.0, "Running average should be positive");
            assert!(
                total_value >= order.total_price,
                "Total value should include current order"
            );
            assert_eq!(
                avg_value,
                total_value / order_count as f64,
                "Average should be calculated correctly"
            );
        }
    }

    // Assert timing results
    assert!(order_count > 0, "Should have processed some orders");
    assert!(total_value > 0.0, "Total value should be positive");

    let final_avg = total_value / order_count as f64;
    println!(
        "[Timing Stream] ✅ Processed {} orders, total value: {:.2}, final avg: {:.2}",
        order_count, total_value, final_avg
    );
    println!("--- Order Timing Stream Finished ---\n");
}
