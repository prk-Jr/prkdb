//! Full Client-Server Integration Test
//!
//! This integration test demonstrates PrkDB's full capabilities by:
//! 1. Starting a real gRPC server
//! 2. Using direct gRPC client to make actual network requests
//! 3. Testing complete workflows with assertions
//!
//! Run with: cargo test --test client_server_integration -- --nocapture

use prkdb::raft::grpc_service::PrkDbGrpcService;
use prkdb::storage::InMemoryAdapter;
use prkdb::PrkDb;
use prkdb_proto::raft::prk_db_service_client::PrkDbServiceClient;
use prkdb_proto::raft::prk_db_service_server::PrkDbServiceServer;
use prkdb_proto::raft::*;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::{Channel, Server};

const ADMIN_TOKEN: &str = "integration-test-token-xyz";

/// Find an available port for the test server
async fn find_available_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Create test database with in-memory storage
fn create_test_db() -> Arc<PrkDb> {
    let storage = InMemoryAdapter::new();
    let db = PrkDb::builder().with_storage(storage).build().unwrap();
    Arc::new(db)
}

/// Test client wrapper for direct gRPC calls
struct TestClient {
    client: PrkDbServiceClient<Channel>,
    admin_token: String,
}

impl TestClient {
    async fn connect(server_url: &str, admin_token: &str) -> Self {
        let client = PrkDbServiceClient::connect(server_url.to_string())
            .await
            .expect("Failed to connect");
        Self {
            client,
            admin_token: admin_token.to_string(),
        }
    }

    async fn list_collections(&mut self) -> Result<Vec<String>, String> {
        let response = self
            .client
            .list_collections(tonic::Request::new(ListCollectionsRequest {
                admin_token: self.admin_token.clone(),
            }))
            .await
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        if resp.success {
            Ok(resp.collections)
        } else {
            Err(resp.error)
        }
    }

    async fn create_collection(&mut self, name: &str) -> Result<(), String> {
        let response = self
            .client
            .create_collection(tonic::Request::new(CreateCollectionRequest {
                admin_token: self.admin_token.clone(),
                name: name.to_string(),
            }))
            .await
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(resp.error)
        }
    }

    async fn drop_collection(&mut self, name: &str) -> Result<(), String> {
        let response = self
            .client
            .drop_collection(tonic::Request::new(DropCollectionRequest {
                admin_token: self.admin_token.clone(),
                name: name.to_string(),
            }))
            .await
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(resp.error)
        }
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
        let response = self
            .client
            .put(tonic::Request::new(PutRequest {
                key: key.to_vec(),
                value: value.to_vec(),
            }))
            .await
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err("put failed".to_string())
        }
    }

    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let response = self
            .client
            .get(tonic::Request::new(GetRequest {
                key: key.to_vec(),
                read_mode: 0, // Default
            }))
            .await
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        if resp.value.is_empty() {
            Ok(None)
        } else {
            Ok(Some(resp.value))
        }
    }

    async fn delete(&mut self, key: &[u8]) -> Result<(), String> {
        let response = self
            .client
            .delete(tonic::Request::new(DeleteRequest { key: key.to_vec() }))
            .await
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err("delete failed".to_string())
        }
    }

    async fn batch_put(&mut self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<u32, String> {
        let kv_pairs = pairs
            .into_iter()
            .map(|(k, v)| KvPair { key: k, value: v })
            .collect();

        let response = self
            .client
            .batch_put(tonic::Request::new(BatchPutRequest { pairs: kv_pairs }))
            .await
            .map_err(|e| e.to_string())?;
        Ok(response.into_inner().successful_count)
    }

    async fn list_consumer_groups(&mut self) -> Result<Vec<String>, String> {
        let response = self
            .client
            .list_consumer_groups(tonic::Request::new(ListConsumerGroupsRequest {
                admin_token: self.admin_token.clone(),
            }))
            .await
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        if resp.success {
            Ok(resp.groups.into_iter().map(|g| g.group_id).collect())
        } else {
            Err(resp.error)
        }
    }

    async fn list_partitions(&mut self) -> Result<Vec<PartitionSummary>, String> {
        let response = self
            .client
            .list_partitions(tonic::Request::new(ListPartitionsRequest {
                admin_token: self.admin_token.clone(),
                collection: String::new(),
            }))
            .await
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        if resp.success {
            Ok(resp.partitions)
        } else {
            Err(resp.error)
        }
    }

    async fn get_replication_status(&mut self) -> Result<GetReplicationStatusResponse, String> {
        let response = self
            .client
            .get_replication_status(tonic::Request::new(GetReplicationStatusRequest {
                admin_token: self.admin_token.clone(),
            }))
            .await
            .map_err(|e| e.to_string())?;
        Ok(response.into_inner())
    }

    // --- Schema Registry Helpers ---

    async fn register_schema(
        &mut self,
        collection: &str,
        schema_proto: Vec<u8>,
    ) -> Result<u32, String> {
        let response = self
            .client
            .register_schema(tonic::Request::new(RegisterSchemaRequest {
                admin_token: self.admin_token.clone(),
                collection: collection.to_string(),
                schema_proto,
                compatibility: CompatibilityMode::CompatibilityBackward as i32,
                migration_id: None,
            }))
            .await
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        if resp.success {
            Ok(resp.version)
        } else {
            Err(resp.error)
        }
    }

    async fn get_schema(
        &mut self,
        collection: &str,
        version: Option<u32>,
    ) -> Result<Vec<u8>, String> {
        let response = self
            .client
            .get_schema(tonic::Request::new(GetSchemaRequest {
                collection: collection.to_string(),
                version: version.unwrap_or(0),
            }))
            .await
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        if resp.success {
            Ok(resp.schema_proto)
        } else {
            Err(resp.error)
        }
    }

    async fn list_schemas(&mut self) -> Result<Vec<SchemaInfo>, String> {
        let response = self
            .client
            .list_schemas(tonic::Request::new(ListSchemasRequest {
                admin_token: self.admin_token.clone(),
            }))
            .await
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        if resp.success {
            Ok(resp.schemas)
        } else {
            Err("ListSchemas failed".to_string())
        }
    }

    async fn check_compatibility(
        &mut self,
        collection: &str,
        schema_proto: Vec<u8>,
    ) -> Result<bool, String> {
        let response = self
            .client
            .check_compatibility(tonic::Request::new(CheckCompatibilityRequest {
                collection: collection.to_string(),
                schema_proto,
            }))
            .await
            .map_err(|e| e.to_string())?;
        Ok(response.into_inner().compatible)
    }
}

/// Start gRPC server and return URL + shutdown handle
async fn start_server() -> (String, oneshot::Sender<()>) {
    let port = find_available_port().await;
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let server_url = format!("http://127.0.0.1:{}", port);

    println!("🚀 Starting PrkDB gRPC server on port {}...", port);

    let db = create_test_db();
    let service = PrkDbGrpcService::new(db, ADMIN_TOKEN.to_string());

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(PrkDbServiceServer::new(service))
            .serve_with_shutdown(addr, async {
                shutdown_rx.await.ok();
            })
            .await
            .unwrap();
    });

    // Wait for server to be ready
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!("✅ Server started at {}", server_url);

    (server_url, shutdown_tx)
}

// =============================================================================
// Integration Test: Full API Flow with Assertions
// =============================================================================

#[tokio::test]
async fn test_full_client_server_integration() {
    println!("\n╔═══════════════════════════════════════════════════════════════╗");
    println!("║     PrkDB Client-Server Integration Test                       ║");
    println!("╚═══════════════════════════════════════════════════════════════╝\n");

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 1: Server Startup
    // ─────────────────────────────────────────────────────────────────────────
    let (server_url, shutdown) = start_server().await;

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 2: Authentication Tests
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n📋 Phase 2: Authentication Tests");

    // Test with wrong token - should fail
    let mut bad_client = TestClient::connect(&server_url, "wrong-token").await;
    let result = bad_client.list_collections().await;
    assert!(result.is_err(), "Should reject wrong token");
    println!("✅ Unauthorized request correctly rejected");

    // Test with correct token - should succeed
    let mut client = TestClient::connect(&server_url, ADMIN_TOKEN).await;
    let collections = client.list_collections().await;
    assert!(
        collections.is_ok(),
        "Should accept correct token: {:?}",
        collections
    );
    println!("✅ Authorized request succeeded");

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 3: Collection Management
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n📋 Phase 3: Collection Management");

    // List initial collections (should be empty or minimal)
    let initial_collections = client.list_collections().await.unwrap();
    let initial_count = initial_collections.len();
    println!("   Initial collections: {} found", initial_count);

    // Create collections
    let collections_to_create = ["users", "orders", "products", "audit_events"];

    for name in &collections_to_create {
        let result = client.create_collection(name).await;
        assert!(result.is_ok(), "Failed to create collection: {}", name);
        println!("✅ Created collection: {}", name);
    }

    // Verify all created
    let collections = client.list_collections().await.unwrap();
    assert_eq!(
        collections.len(),
        initial_count + collections_to_create.len(),
        "Should have {} more collections",
        collections_to_create.len()
    );
    println!("✅ Verified {} collections exist", collections.len());

    // Verify each collection exists
    for name in &collections_to_create {
        assert!(
            collections.contains(&name.to_string()),
            "Collection {} should exist",
            name
        );
    }
    println!("✅ All created collections verified");

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 4: Key-Value Operations
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n📋 Phase 4: Key-Value Operations");

    // Put some data
    let test_data = vec![
        (
            "user:1",
            r#"{"id":1,"name":"Alice","email":"alice@example.com"}"#,
        ),
        (
            "user:2",
            r#"{"id":2,"name":"Bob","email":"bob@example.com"}"#,
        ),
        (
            "user:3",
            r#"{"id":3,"name":"Charlie","email":"charlie@example.com"}"#,
        ),
        ("order:100", r#"{"id":100,"user_id":1,"total":9999}"#),
        ("order:101", r#"{"id":101,"user_id":2,"total":4500}"#),
    ];

    for (key, value) in &test_data {
        let result = client.put(key.as_bytes(), value.as_bytes()).await;
        assert!(result.is_ok(), "Failed to put key: {}", key);
        println!("✅ Put: {} -> {} bytes", key, value.len());
    }

    // Get and verify data
    for (key, expected_value) in &test_data {
        let result = client.get(key.as_bytes()).await;
        assert!(result.is_ok(), "Failed to get key: {}", key);

        let value = result.unwrap();
        assert!(value.is_some(), "Key {} should have a value", key);

        let actual = String::from_utf8(value.unwrap()).unwrap();
        assert_eq!(&actual, *expected_value, "Value mismatch for key {}", key);
        println!("✅ Get verified: {}", key);
    }

    // Test non-existent key
    let result = client.get(b"nonexistent:key").await;
    assert!(result.is_ok());
    assert!(
        result.unwrap().is_none(),
        "Non-existent key should return None"
    );
    println!("✅ Non-existent key correctly returns None");

    // Delete a key
    let result = client.delete(b"user:3").await;
    assert!(result.is_ok(), "Failed to delete key");
    println!("✅ Deleted key: user:3");

    // Verify deletion
    let result = client.get(b"user:3").await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none(), "Deleted key should return None");
    println!("✅ Deletion verified");

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 5: Batch Operations
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n📋 Phase 5: Batch Operations");

    let batch_data: Vec<(Vec<u8>, Vec<u8>)> = (1..=10)
        .map(|i| {
            let key = format!("batch:item:{}", i).into_bytes();
            let value = format!(r#"{{"batch_id":{},"data":"batch item {}"}}"#, i, i).into_bytes();
            (key, value)
        })
        .collect();

    let result = client.batch_put(batch_data.clone()).await;
    assert!(result.is_ok(), "Batch put failed");
    assert_eq!(result.unwrap(), 10, "Should have 10 successful puts");
    println!("✅ Batch put: 10 items");

    // Verify batch items
    for (key, expected) in &batch_data {
        let result = client.get(key).await.unwrap();
        assert!(result.is_some(), "Batch item should exist");
        assert_eq!(&result.unwrap(), expected);
    }
    println!("✅ All batch items verified");

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 6: Collection Cleanup
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n📋 Phase 6: Collection Cleanup");

    // Drop collections
    for name in &collections_to_create {
        let result = client.drop_collection(name).await;
        assert!(result.is_ok(), "Failed to drop collection: {}", name);
        println!("✅ Dropped collection: {}", name);
    }

    // Verify cleanup
    let collections = client.list_collections().await.unwrap();
    assert_eq!(
        collections.len(),
        initial_count,
        "Should be back to initial count"
    );
    println!("✅ Collection cleanup verified");

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 7: Consumer Group Operations
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n📋 Phase 7: Consumer Group Operations");

    let groups = client.list_consumer_groups().await;
    assert!(groups.is_ok(), "Should be able to list consumer groups");
    println!("✅ Consumer groups listed: {} found", groups.unwrap().len());

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 8: Partition Operations
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n📋 Phase 8: Partition Operations");

    let partitions = client.list_partitions().await;
    assert!(partitions.is_ok(), "Should be able to list partitions");
    println!("✅ Partitions listed: {} found", partitions.unwrap().len());

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 9: Replication Status
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n📋 Phase 9: Replication Status");

    let status = client.get_replication_status().await;
    assert!(status.is_ok(), "Should be able to get replication status");
    let status = status.unwrap();
    assert!(!status.node_id.is_empty(), "Node ID should not be empty");
    println!(
        "✅ Replication status: node={}, role={}",
        status.node_id, status.role
    );

    // ─────────────────────────────────────────────────────────────────────────
    // Cleanup
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n🧹 Cleaning up...");
    shutdown.send(()).ok();
    println!("✅ Server shutdown");

    println!("\n╔═══════════════════════════════════════════════════════════════╗");
    println!("║  🎉 All Integration Tests Passed!                              ║");
    println!("╚═══════════════════════════════════════════════════════════════╝\n");
}

// =============================================================================
// Integration Test: Error Handling
// =============================================================================

#[tokio::test]
async fn test_error_handling_integration() {
    let (server_url, shutdown) = start_server().await;

    // Test empty token
    let mut client = TestClient::connect(&server_url, "").await;
    let result = client.list_collections().await;
    assert!(result.is_err(), "Empty token should be rejected");
    println!("✅ Empty token correctly rejected");

    // Test malformed requests (drop non-existent collection)
    let mut client = TestClient::connect(&server_url, ADMIN_TOKEN).await;
    let result = client.drop_collection("nonexistent_collection_xyz").await;
    // This may succeed (idempotent) or fail - just ensure no panic
    let _ = result;
    println!("✅ Drop non-existent collection handled gracefully");

    shutdown.send(()).ok();
}

// =============================================================================
// Integration Test: Concurrent Operations
// =============================================================================

#[tokio::test]
async fn test_concurrent_operations_integration() {
    let (server_url, shutdown) = start_server().await;

    // Spawn multiple concurrent operations
    let mut handles = vec![];

    for i in 0..5 {
        let url = server_url.clone();
        let handle = tokio::spawn(async move {
            let mut client = TestClient::connect(&url, ADMIN_TOKEN).await;
            let key = format!("concurrent:key:{}", i);
            let value = format!("value-{}", i);

            // Put
            client.put(key.as_bytes(), value.as_bytes()).await.unwrap();

            // Get and verify
            let result = client.get(key.as_bytes()).await.unwrap();
            assert!(result.is_some());
            assert_eq!(String::from_utf8(result.unwrap()).unwrap(), value);

            i
        });
        handles.push(handle);
    }

    // Wait for all to complete
    let mut completed = vec![];
    for handle in handles {
        completed.push(handle.await.unwrap());
    }

    assert_eq!(completed.len(), 5);
    println!("✅ All 5 concurrent operations completed successfully");

    shutdown.send(()).ok();
}

// =============================================================================
// Integration Test: Data Consistency
// =============================================================================

#[tokio::test]
async fn test_data_consistency_integration() {
    let (server_url, shutdown) = start_server().await;
    let mut client = TestClient::connect(&server_url, ADMIN_TOKEN).await;

    let key = b"consistency:test";

    // Initial put
    client.put(key, b"v1").await.unwrap();
    assert_eq!(client.get(key).await.unwrap().unwrap(), b"v1".to_vec());

    // Update
    client.put(key, b"v2").await.unwrap();
    assert_eq!(client.get(key).await.unwrap().unwrap(), b"v2".to_vec());

    // Another update
    client.put(key, b"v3-final").await.unwrap();
    assert_eq!(
        client.get(key).await.unwrap().unwrap(),
        b"v3-final".to_vec()
    );

    // Delete
    client.delete(key).await.unwrap();
    assert!(client.get(key).await.unwrap().is_none());

    // Re-create
    client.put(key, b"recreated").await.unwrap();
    assert_eq!(
        client.get(key).await.unwrap().unwrap(),
        b"recreated".to_vec()
    );

    println!("✅ Data consistency verified across put/get/delete/recreate cycle");

    shutdown.send(()).ok();
}

// =============================================================================
// Integration Test: Schema Registry Flow
// =============================================================================

#[tokio::test]
async fn test_schema_rpc_integration() {
    let (server_url, shutdown) = start_server().await;
    let mut client = TestClient::connect(&server_url, ADMIN_TOKEN).await;

    println!("\n📋 Schema Registry Integration Test");

    // 1. Create a dummy descriptor (simulating a .proto file)
    // In a real scenario this comes from protoc, but here we just use bytes
    // Minimal valid FileDescriptorProto bytes for a message named "User"
    // Name = field 1, string
    let user_v1_proto = vec![
        10, 4, 85, 115, 101, 114, // name="User"
    ];

    // 2. Register Schema
    println!("   Registering User schema v1...");
    let version = client
        .register_schema("users", user_v1_proto.clone())
        .await
        .expect("RegisterSchema failed");
    assert_eq!(version, 1, "Should be version 1");
    println!("✅ Registered v1");

    // 3. List Schemas
    println!("   Listing schemas...");
    let schemas = client.list_schemas().await.expect("ListSchemas failed");
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].collection, "users");
    assert_eq!(schemas[0].latest_version, 1);
    println!("✅ ListSchemas verified");

    // 4. Get Schema
    println!("   Getting schema v1...");
    let fetched_proto = client
        .get_schema("users", Some(1))
        .await
        .expect("GetSchema failed");
    assert_eq!(fetched_proto, user_v1_proto, "Schema bytes mismatch");
    println!("✅ GetSchema v1 verified");

    // 5. Register v2 (Non-breaking change)
    // Just a slightly different name to make bytes different
    let user_v2_proto = vec![
        10, 4, 85, 115, 101, 114, // name="User"
        18, 0, // file_list (empty) - just adding some dummy field
    ];

    println!("   Registering User schema v2...");
    let version_2 = client
        .register_schema("users", user_v2_proto.clone())
        .await
        .expect("RegisterSchema v2 failed");
    assert_eq!(version_2, 2, "Should be version 2");
    println!("✅ Registered v2");

    // 6. Verify Latest Version
    let latest = client
        .get_schema("users", None)
        .await
        .expect("GetSchema latest failed");
    assert_eq!(latest, user_v2_proto, "Should return v2");
    println!("✅ GetSchema (latest) verified");

    // 7. Verify Isolation (v1 still exists)
    let v1 = client
        .get_schema("users", Some(1))
        .await
        .expect("GetSchema v1 failed separate check");
    assert_eq!(v1, user_v1_proto, "v1 should be preserved");
    println!("✅ Version isolation verified");

    // 8. Check Compatibility
    println!("   Checking compatibility...");
    let compatible = client
        .check_compatibility("users", user_v2_proto.clone())
        .await
        .expect("CheckCompatibility failed");
    assert!(compatible, "v2 should be compatible with v1");
    println!("✅ Compatibility check verified");

    shutdown.send(()).ok();
}
