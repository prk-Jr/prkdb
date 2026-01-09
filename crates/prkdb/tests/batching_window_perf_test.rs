#[cfg(test)]
mod batching_window_performance {
    use prkdb::prelude::*;
    use prkdb_core::batch_config::BatchConfig;
    use std::time::Instant;

    #[derive(Collection, serde::Serialize, serde::Deserialize, Clone, Debug)]
    struct PerfItem {
        #[id]
        id: u64,
        value: String,
    }

    #[tokio::test]
    async fn test_batching_window_performance() {
        const NUM_ITEMS: u64 = 5000;

        // Test WITHOUT batching
        let db1 = PrkDb::builder()
            .with_storage(prkdb::storage::InMemoryAdapter::new())
            .build()
            .unwrap();
        let handle1 = db1.collection::<PerfItem>();

        let start = Instant::now();
        for i in 0..NUM_ITEMS {
            handle1
                .put(PerfItem {
                    id: i,
                    value: format!("value_{}", i),
                })
                .await
                .unwrap();
        }
        let no_batch_time = start.elapsed();

        // Test WITH batching window (default: 10ms linger, 1000 batch size)
        let db2 = PrkDb::builder()
            .with_storage(prkdb::storage::InMemoryAdapter::new())
            .build()
            .unwrap();
        let handle2 = db2
            .collection::<PerfItem>()
            .with_batching(BatchConfig::default());

        let start = Instant::now();
        for i in 0..NUM_ITEMS {
            handle2
                .put(PerfItem {
                    id: i,
                    value: format!("value_{}", i),
                })
                .await
                .unwrap();
        }
        let with_batch_time = start.elapsed();

        println!("\n========================================");
        println!("Async Batching Window Performance Test");
        println!("========================================");
        println!("Items: {}", NUM_ITEMS);
        println!();
        println!("WITHOUT batching: {:?}", no_batch_time);
        println!("WITH batching:    {:?}", with_batch_time);
        println!();

        let improvement = no_batch_time.as_secs_f64() / with_batch_time.as_secs_f64();
        println!("Performance improvement: {:.2}x", improvement);

        if improvement > 1.0 {
            println!("✅ Batching window is FASTER!");
        } else {
            println!(
                "⚠️  Batching added {:?} overhead (expected for this test pattern)",
                with_batch_time - no_batch_time
            );
            println!("Note: Batching window benefits sustained high-throughput workloads");
        }
        println!("========================================\n");
    }

    #[tokio::test]
    async fn test_concurrent_workload_performance() {
        const NUM_TASKS: usize = 10;
        const ITEMS_PER_TASK: u64 = 500;

        println!("\n========================================");
        println!("Concurrent Workload Performance Test");
        println!("========================================");
        println!("Tasks: {}, Items per task: {}", NUM_TASKS, ITEMS_PER_TASK);
        println!();

        // Test WITHOUT batching - concurrent tasks
        let db1 = PrkDb::builder()
            .with_storage(prkdb::storage::InMemoryAdapter::new())
            .build()
            .unwrap();
        let start = Instant::now();

        let mut handles = vec![];
        for task_id in 0..NUM_TASKS {
            let handle = db1.collection::<PerfItem>();
            let h = tokio::spawn(async move {
                for i in 0..ITEMS_PER_TASK {
                    handle
                        .put(PerfItem {
                            id: task_id as u64 * 1000 + i,
                            value: format!("task_{}_value_{}", task_id, i),
                        })
                        .await
                        .unwrap();
                }
            });
            handles.push(h);
        }

        for h in handles {
            h.await.unwrap();
        }
        let no_batch_time = start.elapsed();

        // Test WITH batching - concurrent tasks
        let db2 = PrkDb::builder()
            .with_storage(prkdb::storage::InMemoryAdapter::new())
            .build()
            .unwrap();
        let start = Instant::now();

        let mut handles = vec![];
        for task_id in 0..NUM_TASKS {
            let handle = db2
                .collection::<PerfItem>()
                .with_batching(BatchConfig::default());
            let h = tokio::spawn(async move {
                for i in 0..ITEMS_PER_TASK {
                    handle
                        .put(PerfItem {
                            id: task_id as u64 * 1000 + i,
                            value: format!("task_{}_value_{}", task_id, i),
                        })
                        .await
                        .unwrap();
                }
            });
            handles.push(h);
        }

        for h in handles {
            h.await.unwrap();
        }
        let with_batch_time = start.elapsed();

        println!("WITHOUT batching (concurrent): {:?}", no_batch_time);
        println!("WITH batching (concurrent):    {:?}", with_batch_time);
        println!();

        let improvement = no_batch_time.as_secs_f64() / with_batch_time.as_secs_f64();
        println!("Performance improvement: {:.2}x", improvement);

        if improvement > 1.0 {
            println!(
                "✅ Batching window is {:.2}x FASTER for concurrent workloads!",
                improvement
            );
        }
        println!("========================================\n");
    }
}
