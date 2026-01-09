// Chaos Test Harness for PrkDb Multi-Raft Cluster
// Simulates failures while generating load to verify resilience

use prkdb::raft::rpc::prk_db_service_client::PrkDbServiceClient;
use prkdb::raft::rpc::PutRequest;
use std::process::Command;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== PrkDB Chaos Monkey 🐵 💥 ===\n");

    // 1. Setup connections
    let nodes = vec![
        "http://127.0.0.1:8081", // node1
        "http://127.0.0.1:8082", // node2
        "http://127.0.0.1:8083", // node3
    ];

    println!("Connecting to cluster...");
    let mut clients = Vec::new();
    for addr in &nodes {
        match PrkDbServiceClient::connect(addr.to_string()).await {
            Ok(client) => clients.push(client),
            Err(e) => {
                println!("Failed to connect to {}: {}", addr, e);
                return Ok(());
            }
        }
    }
    println!("✓ Connected to {} nodes\n", clients.len());

    // 2. Start Load Generator (Background Task)
    println!("🚀 Starting continuous load...");
    let load_handle = tokio::spawn(async move {
        let mut counter = 0;
        let mut success = 0;
        let mut failures = 0;

        // Round-robin client selection
        let nodes_clone = vec![
            "http://127.0.0.1:8081",
            "http://127.0.0.1:8082",
            "http://127.0.0.1:8083",
        ];

        loop {
            let node_idx = counter % nodes_clone.len();
            let addr = &nodes_clone[node_idx];

            // Create fresh connection (to handle node restarts)
            if let Ok(mut client) = PrkDbServiceClient::connect(addr.to_string()).await {
                let request = tonic::Request::new(PutRequest {
                    key: format!("chaos_key_{}", counter).into_bytes(),
                    value: vec![0u8; 64],
                });

                match client.put(request).await {
                    Ok(_) => success += 1,
                    Err(_) => failures += 1,
                }
            } else {
                failures += 1;
            }

            counter += 1;
            if counter % 100 == 0 {
                print!("\rStatus: {} success, {} failures", success, failures);
            }

            // Slight delay to not overwhelm
            sleep(Duration::from_millis(10)).await;
        }
    });

    // 3. Chaos Scenario 1: The Sniper (Kill Follower)
    println!("\n\n🧪 SCENARIO 1: The Sniper (Killing Node 3)...");
    sleep(Duration::from_secs(2)).await;

    run_command("docker", &["kill", "prkdb-node3"]);
    println!("💀 Node 3 killed!");

    println!("Waiting 5 seconds...");
    sleep(Duration::from_secs(5)).await;

    println!("🚑 Reviving Node 3...");
    run_command("docker", &["start", "prkdb-node3"]);

    println!("Waiting for recovery...");
    sleep(Duration::from_secs(10)).await;

    // 4. Chaos Scenario 2: Decapitation (Kill Leader - Node 1)
    println!("\n🧪 SCENARIO 2: Decapitation (Killing Node 1)...");
    run_command("docker", &["kill", "prkdb-node1"]);
    println!("💀 Node 1 killed!");

    println!("Waiting 5 seconds (expecting leader election)...");
    sleep(Duration::from_secs(5)).await;

    println!("🚑 Reviving Node 1...");
    run_command("docker", &["start", "prkdb-node1"]);

    println!("Waiting for recovery...");
    sleep(Duration::from_secs(10)).await;

    // 5. Stop Load
    load_handle.abort();
    println!("\n\n✅ Chaos Test Complete!");

    Ok(())
}

fn run_command(cmd: &str, args: &[&str]) {
    let output = Command::new(cmd)
        .args(args)
        .output()
        .expect("Failed to execute command");

    if !output.status.success() {
        println!("Command failed: {:?}", output);
    }
}
