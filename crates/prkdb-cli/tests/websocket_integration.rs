use futures_util::StreamExt;
use prkdb_client::{ClientConfig, PrkDbClient, WsConfig, WsConsumer, WsEvent};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;

struct TestServer {
    process: Child,
    http_port: u16,
    grpc_port: u16,
    _database: tempfile::TempDir,
}

impl TestServer {
    async fn start() -> Self {
        // Hold the startup lock through readiness: releasing ephemeral reservations
        // before the child binds must not let another test select the same endpoints.
        static STARTUP: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
        let _startup = STARTUP.lock().await;
        let http_reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let grpc_reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let http_port = http_reservation.local_addr().unwrap().port();
        let grpc_port = grpc_reservation.local_addr().unwrap().port();
        let database = tempfile::tempdir().unwrap();
        let db_path = database.path().join("database");

        // Use the binary built by cargo for integration tests
        let binary_path = env!("CARGO_BIN_EXE_prkdb-cli");
        println!("Using binary: {}", binary_path);
        println!("Using database: {:?}", db_path);

        drop((http_reservation, grpc_reservation));
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
            // These tests exercise WebSocket streaming, not authorization, which has its
            // own suite in http_authz.rs. Since Task 1 the server refuses to start with
            // no principals, so the opt-out has to be explicit.
            .arg("--allow-anonymous")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("Failed to spawn prkdb-cli");

        // Own the child before polling so startup failures also kill and reap it.
        let server = Self {
            process: child,
            http_port,
            grpc_port,
            _database: database,
        };

        // Wait for server to start
        let client = reqwest::Client::new();
        let health_url = format!("http://127.0.0.1:{}/health", http_port);

        let mut attempts = 0;
        loop {
            if client.get(&health_url).send().await.is_ok() {
                break;
            }
            // 30s, not 3. Three seconds is enough on a developer machine and not under
            // llvm-cov instrumentation on a shared runner, which is where this failed —
            // the Coverage job runs the whole workspace instrumented. The other harnesses
            // in this repository allow 15-30s for the same reason.
            if attempts > 300 {
                panic!("Server failed to start after 30 seconds");
            }
            sleep(Duration::from_millis(100)).await;
            attempts += 1;
        }

        let grpc_url = format!("http://127.0.0.1:{}", grpc_port);
        let mut grpc_attempts = 0;
        loop {
            if PrkDbClient::with_config(vec![grpc_url.clone()], ClientConfig::default())
                .await
                .is_ok()
            {
                break;
            }

            if grpc_attempts > 300 {
                panic!("gRPC server failed to start after 30 seconds");
            }

            sleep(Duration::from_millis(100)).await;
            grpc_attempts += 1;
        }

        server
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

#[tokio::test]
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

/// cargo-mutants runs this native test binary concurrently, unlike nextest's
/// per-test server group. Each endpoint must still reach its own database.
#[tokio::test]
async fn parallel_websocket_servers_keep_their_databases_isolated() {
    let (first, second) = tokio::join!(TestServer::start(), TestServer::start());
    let mut ports = vec![
        first.http_port,
        first.grpc_port,
        second.http_port,
        second.grpc_port,
    ];
    ports.sort_unstable();
    ports.dedup();
    assert_eq!(ports.len(), 4, "every listener must have its own port");

    let first_client = PrkDbClient::with_config(
        vec![format!("http://127.0.0.1:{}", first.grpc_port)],
        ClientConfig::default(),
    )
    .await
    .unwrap();
    let second_client = PrkDbClient::with_config(
        vec![format!("http://127.0.0.1:{}", second.grpc_port)],
        ClientConfig::default(),
    )
    .await
    .unwrap();
    first_client.put(b"isolated:key", b"first").await.unwrap();
    second_client.put(b"isolated:key", b"second").await.unwrap();
    assert_eq!(
        first_client.get(b"isolated:key").await.unwrap(),
        Some(b"first".to_vec())
    );
    assert_eq!(
        second_client.get(b"isolated:key").await.unwrap(),
        Some(b"second".to_vec())
    );
}
