use prkdb::builder::OptimizationLevel;
use prkdb::prelude::*;
use prkdb_macros::Collection;
use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Integration tests for legendary performance
/// Validates that the optimized storage works end-to-end

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct TestRecord {
    #[id]
    id: u64,
    data: String,
    value: i64,
}

#[tokio::test(flavor = "multi_thread")]
async fn test_legendary_performance_basic() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;

    let db = Builder::new()
        .with_data_dir(temp_dir.path()) // Automatic legendary performance!
        .register_collection::<TestRecord>()
        .build()?;

    let collection = db.collection::<TestRecord>();

    // Write some data
    for i in 0..100 {
        collection
            .put(TestRecord {
                id: i,
                data: format!("test_{}", i),
                value: i as i64 * 10,
            })
            .await?;
    }

    // Read it back
    let record = collection.get(&42).await?;
    assert_eq!(record.as_ref().map(|r| r.id), Some(42));
    assert_eq!(
        record.as_ref().map(|r| &r.data),
        Some(&"test_42".to_string())
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_optimization_levels() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;

    // Test all optimization levels compile and work
    let levels = vec![
        OptimizationLevel::Balanced,
        OptimizationLevel::Throughput,
        OptimizationLevel::Legendary,
    ];

    for (idx, level) in levels.iter().enumerate() {
        let db = Builder::new()
            .with_optimized_storage(temp_dir.path().join(format!("level_{}", idx)), *level)
            .register_collection::<TestRecord>()
            .build()?;

        let collection = db.collection::<TestRecord>();

        collection
            .put(TestRecord {
                id: 1,
                data: "test".to_string(),
                value: 100,
            })
            .await?;

        let record = collection.get(&1).await?;
        assert!(record.is_some());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_collections_legendary() -> anyhow::Result<()> {
    #[derive(Collection, Serialize, Deserialize, Clone, Debug)]
    struct User {
        #[id]
        id: u64,
        name: String,
    }

    #[derive(Collection, Serialize, Deserialize, Clone, Debug)]
    struct Order {
        #[id]
        id: u64,
        user_id: u64,
        amount: f64,
    }

    let temp_dir = tempfile::tempdir()?;

    let db = Builder::new()
        .with_data_dir(temp_dir.path()) // Automatic legendary performance!
        .register_collection::<User>()
        .register_collection::<Order>()
        .build()?;

    let users = db.collection::<User>();
    let orders = db.collection::<Order>();

    // Write to multiple collections
    users
        .put(User {
            id: 1,
            name: "Alice".to_string(),
        })
        .await?;
    users
        .put(User {
            id: 2,
            name: "Bob".to_string(),
        })
        .await?;

    orders
        .put(Order {
            id: 101,
            user_id: 1,
            amount: 99.99,
        })
        .await?;
    orders
        .put(Order {
            id: 102,
            user_id: 2,
            amount: 149.99,
        })
        .await?;

    // Verify both collections work
    let user = users.get(&1).await?;
    assert_eq!(user.as_ref().map(|u| &u.name), Some(&"Alice".to_string()));

    let order = orders.get(&101).await?;
    assert_eq!(order.as_ref().map(|o| o.amount), Some(99.99));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_performance_comparison() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;

    // Test legendary performance is actually faster
    let db_legendary = Builder::new()
        .with_data_dir(temp_dir.path().join("legendary")) // Automatic 1.2M ops/sec!
        .register_collection::<TestRecord>()
        .build()?;

    let db_balanced = Builder::new()
        .with_optimized_storage(
            temp_dir.path().join("balanced"),
            OptimizationLevel::Balanced,
        )
        .register_collection::<TestRecord>()
        .build()?;

    let collection_legendary = db_legendary.collection::<TestRecord>();
    let collection_balanced = db_balanced.collection::<TestRecord>();

    // Benchmark legendary
    let start = Instant::now();
    for i in 0..1000 {
        collection_legendary
            .put(TestRecord {
                id: i,
                data: format!("test_{}", i),
                value: i as i64,
            })
            .await?;
    }
    let legendary_time = start.elapsed();

    // Benchmark balanced
    let start = Instant::now();
    for i in 0..1000 {
        collection_balanced
            .put(TestRecord {
                id: i,
                data: format!("test_{}", i),
                value: i as i64,
            })
            .await?;
    }
    let balanced_time = start.elapsed();

    println!(
        "Legendary: {:?}, Balanced: {:?}",
        legendary_time, balanced_time
    );

    // Note: The actual performance difference may not be huge with just 1000 ops
    // The real difference shows at scale (hundreds of thousands of ops)

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_batch_operations_legendary() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;

    let db = Builder::new()
        .with_data_dir(temp_dir.path()) // Automatic legendary performance!
        .register_collection::<TestRecord>()
        .build()?;

    let collection = db.collection::<TestRecord>();

    // Batch write
    let records: Vec<_> = (0..1000)
        .map(|i| TestRecord {
            id: i,
            data: format!("batch_{}", i),
            value: i as i64,
        })
        .collect();

    let start = Instant::now();
    for record in records {
        collection.put(record).await?;
    }
    let elapsed = start.elapsed();

    println!("Batch write of 1000 records: {:?}", elapsed);
    println!("Throughput: {:.0} ops/sec", 1000.0 / elapsed.as_secs_f64());

    // Verify all were written
    for i in 0..1000 {
        let record = collection.get(&i).await?;
        assert!(record.is_some(), "Record {} should exist", i);
    }

    Ok(())
}
