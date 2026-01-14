use futures_util::StreamExt;
use prkdb_client::{ClientConfig, PrkDbClient, WsConfig, WsConsumer, WsEvent};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;

struct TestServer {
    process: Child,
    http_port: u16,
    grpc_port: u16,
    db_path: PathBuf,
}

impl TestServer {
    async fn start() -> Self {
        let http_port = 18080 + (rand::random::<u16>() % 1000);
        let grpc_port = 50051 + (rand::random::<u16>() % 1000);
        let db_path = PathBuf::from(format!("test_prkdb_{}.db", http_port));

        // Ensure clean slate
        if db_path.exists() {
            let _ = std::fs::remove_dir_all(&db_path);
        }

        // Use the binary built by cargo for integration tests
        let binary_path = env!("CARGO_BIN_EXE_prkdb-cli");
        println!("Using binary: {}", binary_path);
        println!("Using database: {:?}", db_path);

        let child = Command::new(binary_path)
            .arg("--database")
            .arg(&db_path)
            .arg("serve")
            .arg("--websockets")
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(http_port.to_string())
            .arg("--grpc-port")
            .arg(grpc_port.to_string())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("Failed to spawn prkdb-cli");

        // Wait for server to start
        let client = reqwest::Client::new();
        let health_url = format!("http://127.0.0.1:{}/health", http_port);

        let mut attempts = 0;
        loop {
            if client.get(&health_url).send().await.is_ok() {
                break;
            }
            if attempts > 30 {
                panic!("Server failed to start after 3 seconds");
            }
            sleep(Duration::from_millis(100)).await;
            attempts += 1;
        }

        Self {
            process: child,
            http_port,
            grpc_port,
            db_path,
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();

        // Clean up database directory
        if self.db_path.exists() {
            let _ = std::fs::remove_dir_all(&self.db_path);
        }
    }
}

#[tokio::test]
#[ignore] // Integration test: requires server binary, may be flaky in CI
async fn test_websocket_streaming_flow() {
    // 1. Start Server
    let server = TestServer::start().await;
    let collection = "ws-test-collection";

    // 2. Setup WebSocket Client
    let ws_url = format!("ws://127.0.0.1:{}", server.http_port);
    let ws_config = WsConfig::new(&ws_url, collection).with_auto_reconnect(false); // fast fail

    let mut ws_consumer = WsConsumer::new(ws_config);
    let stream = ws_consumer.subscribe().await;

    // Pin the stream so we can iterate it
    tokio::pin!(stream);

    // Wait for connection event
    if let Some(event) = stream.next().await {
        match event {
            WsEvent::Connected { .. } => println!("Connected!"),
            _ => panic!("Expected Connected event, got {:?}", event),
        }
    } else {
        panic!("Stream closed immediately");
    }

    // 3. Write data via gRPC
    let grpc_url = format!("http://127.0.0.1:{}", server.grpc_port);

    // Retry connection a few times if needed (though health check passed)
    let client = PrkDbClient::with_config(vec![grpc_url], ClientConfig::default())
        .await
        .expect("Failed to connect gRPC client");

    let key = format!("{}:1", collection); // Must match serve.rs expectation
    let value = serde_json::json!({"foo": "bar"});

    // PrkDbClient::put takes slices
    client
        .put(key.as_bytes(), value.to_string().as_bytes())
        .await
        .expect("Failed to write data");

    // 4. Verify Update Received
    // Set timeout
    let timeout = tokio::time::sleep(Duration::from_secs(4));
    tokio::pin!(timeout);

    let mut found = false;
    loop {
        tokio::select! {
            Some(event) = stream.next() => {
                println!("Received event: {:?}", event);
                match event {
                    WsEvent::Update { data, .. } => {
                        if data == value {
                            found = true;
                            break;
                        }
                    },
                    _ => {}
                }
            }
            _ = &mut timeout => {
                break;
            }
        }
    }

    assert!(found, "Did not receive WebSocket update for inserted data");
}

#[tokio::test]
#[ignore] // Integration test: requires server binary, may be flaky in CI
async fn test_websocket_resume_from_offset() {
    let server = TestServer::start().await;
    let collection = "resume-test";

    // 1. Write some data first
    let grpc_url = format!("http://127.0.0.1:{}", server.grpc_port);
    let client = PrkDbClient::with_config(vec![grpc_url], ClientConfig::default())
        .await
        .expect("Failed to connect gRPC client");

    for i in 1..=5 {
        let key = format!("{}:{}", collection, i);
        client
            .put(
                key.as_bytes(),
                serde_json::json!({"i": i}).to_string().as_bytes(),
            )
            .await
            .expect("Failed to write");
    }

    // Wait for data to persist/be visible in polling
    sleep(Duration::from_millis(1000)).await; // Increased wait for stability

    // 2. Connect with from_offset=0 (should receive all)
    let ws_url = format!("ws://127.0.0.1:{}", server.http_port);
    let ws_config = WsConfig::new(&ws_url, collection)
        .with_from_offset(0)
        .with_auto_reconnect(false);

    let mut ws_consumer = WsConsumer::new(ws_config);
    let stream = ws_consumer.subscribe().await;

    // Pin stream
    tokio::pin!(stream);

    // Skip Connected
    let _ = stream.next().await;

    let mut items = Vec::new();
    let timeout = tokio::time::sleep(Duration::from_secs(4)); // Increased timeout
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            Some(event) = stream.next() => {
                if let WsEvent::Update { data, .. } = event {
                    items.push(data);
                    if items.len() >= 5 { break; }
                }
            }
            _ = &mut timeout => break
        }
    }

    // We expect some items.
    assert!(!items.is_empty(), "Should have received historical items");
    // Depending on timing/persistence, we might not get all 5 if write completion vs poll loop race?
    // But we waited 1s.
    // If serve.rs polling uses prefix scan, it should find them.
    assert_eq!(items.len(), 5, "Should have received all 5 items");
}
