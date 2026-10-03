use futures_util::StreamExt;
use prkdb_client::{ClientConfig, PrkDbClient, WsConfig, WsConsumer, WsEvent};
use std::time::Duration;
use tokio::time::sleep;

#[path = "common/server.rs"]
pub mod server;

struct TestServer {
    _process: server::ServerProcess,
    http_port: u16,
    grpc_port: u16,
}

impl TestServer {
    async fn start() -> Self {
        use prkdb_proto::raft::{prk_db_service_client::PrkDbServiceClient, MetadataRequest};

        let mut process =
            server::ServerProcess::spawn(&["--websockets", "--port", "0", "--grpc-port", "0"]);
        let listening = process.listening().await.expect("server listening record");
        tokio::time::timeout(Duration::from_secs(30), async {
            let response = reqwest::get(format!("{}/health", listening.http_url))
                .await
                .expect("HTTP health request");
            assert!(response.status().is_success());
            let mut grpc = PrkDbServiceClient::connect(listening.grpc_url.clone())
                .await
                .expect("gRPC endpoint");
            grpc.metadata(MetadataRequest { topics: vec![] })
                .await
                .expect("gRPC metadata request");
        })
        .await
        .expect("both owned endpoints must serve within the startup deadline");
        Self {
            _process: process,
            http_port: listening.http_address.port(),
            grpc_port: listening.grpc_address.port(),
        }
    }
}

#[tokio::test]
async fn test_websocket_streaming_flow() {
    // 1. Start Server
    let mut server = TestServer::start().await;
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
                if let WsEvent::Update { data, .. } = event {
                    if data == value {
                        found = true;
                        break;
                    }
                }
            }
            _ = &mut timeout => {
                break;
            }
        }
    }

    assert!(found, "Did not receive WebSocket update for inserted data");
    // Reap and drain the child before inspecting all output, so the assertion does
    // not race a pipe-reader thread or delayed WebSocket diagnostic.
    server._process.stop();
    let stdout = server._process.stdout();
    let records: Vec<serde_json::Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).expect("JSON serve stdout after WebSocket traffic"))
        .collect();
    assert_eq!(
        records.len(),
        1,
        "unexpected serve stdout records: {stdout}"
    );
    assert_eq!(records[0]["event"], "server_listening");
}

#[tokio::test]
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
