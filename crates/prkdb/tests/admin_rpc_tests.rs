//! Integration tests for Admin RPC operations
//!
//! These tests verify that the gRPC Admin RPCs work correctly end-to-end.
//! Each test spins up a real gRPC server and uses direct gRPC client calls.

use prkdb::raft::grpc_service::PrkDbGrpcService;
use prkdb::storage::InMemoryAdapter;
use prkdb::PrkDb;
use prkdb_proto::raft::prk_db_service_client::PrkDbServiceClient;
use prkdb_proto::raft::prk_db_service_server::PrkDbServiceServer;
use prkdb_proto::raft::{CreateCollectionRequest, DropCollectionRequest, ListCollectionsRequest};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::Server;

const TEST_ADMIN_TOKEN: &str = "test-admin-token-12345";

/// Helper to find an available port
async fn find_available_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// Helper to create a test database
fn create_test_db() -> Arc<PrkDb> {
    let storage = InMemoryAdapter::new();
    let db = PrkDb::builder().with_storage(storage).build().unwrap();
    Arc::new(db)
}

/// Helper to start a gRPC server and return the client URL + shutdown signal
async fn start_test_server() -> (String, oneshot::Sender<()>) {
    let port = find_available_port().await;
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let server_url = format!("http://127.0.0.1:{}", port);

    let db = create_test_db();
    let service = PrkDbGrpcService::new(db, TEST_ADMIN_TOKEN.to_string());

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // Spawn the server
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
    tokio::time::sleep(Duration::from_millis(100)).await;

    (server_url, shutdown_tx)
}

// =============================================================================
// Collection RPC Tests
// =============================================================================

#[tokio::test]
async fn test_collection_create_list_drop() {
    let (server_url, shutdown) = start_test_server().await;
    let mut client = PrkDbServiceClient::connect(server_url).await.unwrap();

    // 1. List collections - should be empty initially
    let response = client
        .list_collections(tonic::Request::new(ListCollectionsRequest {
            admin_token: TEST_ADMIN_TOKEN.to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.success, "list_collections should succeed");
    let initial_count = response.collections.len();

    // 2. Create a collection
    let response = client
        .create_collection(tonic::Request::new(CreateCollectionRequest {
            admin_token: TEST_ADMIN_TOKEN.to_string(),
            name: "test_users".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.success, "create_collection should succeed");

    // 3. List collections - should have one more
    let response = client
        .list_collections(tonic::Request::new(ListCollectionsRequest {
            admin_token: TEST_ADMIN_TOKEN.to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.success, "list_collections should succeed");
    assert_eq!(
        response.collections.len(),
        initial_count + 1,
        "Expected 1 more collection after create"
    );
    assert!(response.collections.contains(&"test_users".to_string()));

    // 4. Create another collection
    let response = client
        .create_collection(tonic::Request::new(CreateCollectionRequest {
            admin_token: TEST_ADMIN_TOKEN.to_string(),
            name: "test_orders".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.success, "create_collection should succeed");

    // 5. List collections - should have two
    let response = client
        .list_collections(tonic::Request::new(ListCollectionsRequest {
            admin_token: TEST_ADMIN_TOKEN.to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.success, "list_collections should succeed");
    assert_eq!(
        response.collections.len(),
        initial_count + 2,
        "Expected 2 more collections"
    );
    assert!(response.collections.contains(&"test_users".to_string()));
    assert!(response.collections.contains(&"test_orders".to_string()));

    // 6. Drop a collection
    let response = client
        .drop_collection(tonic::Request::new(DropCollectionRequest {
            admin_token: TEST_ADMIN_TOKEN.to_string(),
            name: "test_users".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.success, "drop_collection should succeed");

    // 7. List collections - should have one less
    let response = client
        .list_collections(tonic::Request::new(ListCollectionsRequest {
            admin_token: TEST_ADMIN_TOKEN.to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.success, "list_collections should succeed");
    assert_eq!(
        response.collections.len(),
        initial_count + 1,
        "Expected 1 collection after drop"
    );
    assert!(response.collections.contains(&"test_orders".to_string()));
    assert!(!response.collections.contains(&"test_users".to_string()));

    // Cleanup
    shutdown.send(()).ok();
}

#[tokio::test]
async fn test_collection_auth_failure() {
    let (server_url, shutdown) = start_test_server().await;
    let mut client = PrkDbServiceClient::connect(server_url).await.unwrap();

    // Attempt to list collections with WRONG token - should fail
    let result = client
        .list_collections(tonic::Request::new(ListCollectionsRequest {
            admin_token: "wrong-token".to_string(),
        }))
        .await;

    assert!(result.is_err(), "Expected auth failure with wrong token");

    if let Err(status) = result {
        assert_eq!(
            status.code(),
            tonic::Code::Unauthenticated,
            "Expected Unauthenticated error"
        );
    }

    // Cleanup
    shutdown.send(()).ok();
}

#[tokio::test]
async fn test_collection_empty_token_failure() {
    let (server_url, shutdown) = start_test_server().await;
    let mut client = PrkDbServiceClient::connect(server_url).await.unwrap();

    // Attempt to list collections with empty token - should fail
    let result = client
        .list_collections(tonic::Request::new(ListCollectionsRequest {
            admin_token: "".to_string(),
        }))
        .await;

    assert!(result.is_err(), "Expected auth failure with empty token");

    // Cleanup
    shutdown.send(()).ok();
}

#[tokio::test]
async fn test_full_collection_workflow() {
    let (server_url, shutdown) = start_test_server().await;
    let mut client = PrkDbServiceClient::connect(server_url).await.unwrap();

    // 1. Get initial count
    let response = client
        .list_collections(tonic::Request::new(ListCollectionsRequest {
            admin_token: TEST_ADMIN_TOKEN.to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    let initial_count = response.collections.len();

    // 2. Create 3 collections
    for name in &["workflow_a", "workflow_b", "workflow_c"] {
        let response = client
            .create_collection(tonic::Request::new(CreateCollectionRequest {
                admin_token: TEST_ADMIN_TOKEN.to_string(),
                name: name.to_string(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success, "Create {} should succeed", name);
    }

    // 3. Verify all created
    let response = client
        .list_collections(tonic::Request::new(ListCollectionsRequest {
            admin_token: TEST_ADMIN_TOKEN.to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response.collections.len(),
        initial_count + 3,
        "Should have 3 more collections"
    );

    // 4. Drop all
    for name in &["workflow_a", "workflow_b", "workflow_c"] {
        let response = client
            .drop_collection(tonic::Request::new(DropCollectionRequest {
                admin_token: TEST_ADMIN_TOKEN.to_string(),
                name: name.to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success, "Drop {} should succeed", name);
    }

    // 5. Verify all dropped
    let response = client
        .list_collections(tonic::Request::new(ListCollectionsRequest {
            admin_token: TEST_ADMIN_TOKEN.to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response.collections.len(),
        initial_count,
        "Should be back to initial count"
    );

    // Cleanup
    shutdown.send(()).ok();
}
