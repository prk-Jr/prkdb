use prkdb::raft::rpc::prk_db_service_client::PrkDbServiceClient;
use prkdb::raft::rpc::PutRequest;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let data_addr = "http://127.0.0.1:8091";
    println!("🚀 Starting compaction load test...");
    println!("📍 Connecting to {}", data_addr);

    // Connect to Node 1's data service
    let mut client = PrkDbServiceClient::connect(data_addr.to_string()).await?;

    // Write 12,000 entries to exceed the 10k threshold
    const NUM_ENTRIES: usize = 12_000;
    let start = Instant::now();

    println!(
        "📝 Writing {} entries to trigger compaction...",
        NUM_ENTRIES
    );

    for i in 0..NUM_ENTRIES {
        let key = format!("key_{:06}", i).into_bytes();
        let value = format!("value_{:06}_with_some_extra_data_to_make_it_bigger", i).into_bytes();

        let put_req = tonic::Request::new(PutRequest {
            key: key.clone(),
            value: value.clone(),
        });

        let result = client.put(put_req).await?;

        if !result.into_inner().success {
            println!("❌ PUT failed at key {}", i);
            return Ok(());
        }

        // Progress indicator
        if (i + 1) % 1000 == 0 {
            let elapsed = start.elapsed();
            let rate = (i + 1) as f64 / elapsed.as_secs_f64();
            println!(
                "  ✓ {}/{} entries ({:.0} ops/sec)",
                i + 1,
                NUM_ENTRIES,
                rate
            );
        }
    }

    let elapsed = start.elapsed();
    let avg_rate = NUM_ENTRIES as f64 / elapsed.as_secs_f64();

    println!("\n✅ Load test complete!");
    println!("   Total time: {:.2?}", elapsed);
    println!("   Average rate: {:.0} ops/sec", avg_rate);
    println!("\n⏳ Waiting 5 seconds for compaction to occur...");

    tokio::time::sleep(Duration::from_secs(5)).await;

    println!("\n📊 Check server logs for compaction messages:");
    println!("   grep 'Compacted log' node*.log");
    println!("   grep 'Saved snapshot' node*.log");
    println!("\n💾 Check for snapshot files:");
    println!("   ls -lh /tmp/prkdb_node*/snapshot.bin");

    Ok(())
}
