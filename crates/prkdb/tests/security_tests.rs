use prkdb::raft::grpc_service::PrkDbGrpcService;
use prkdb::storage::InMemoryAdapter;
use prkdb::PrkDb;
use prkdb_proto::raft::prk_db_service_client::PrkDbServiceClient;
use prkdb_proto::raft::prk_db_service_server::PrkDbServiceServer;
use prkdb_proto::raft::{
    CompatibilityMode, ListSchemasRequest, PutRequest, RegisterSchemaRequest, WatchRequest,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt;
use tonic::transport::Server;

const ADMIN_TOKEN: &str = "security-test-admin-token";

fn create_test_db() -> Arc<PrkDb> {
    let storage = InMemoryAdapter::new();
    let db = PrkDb::builder().with_storage(storage).build().unwrap();
    Arc::new(db)
}

async fn start_server<S>(service: PrkDbGrpcService<S>) -> (String, oneshot::Sender<()>)
where
    S: prkdb_schema::SchemaStorage + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_url = format!("http://{}", addr);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(PrkDbServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                shutdown_rx.await.ok();
            })
            .await
            .unwrap();
    });

    for _ in 0..20 {
        if tonic::transport::Channel::from_shared(server_url.clone())
            .unwrap()
            .connect()
            .await
            .is_ok()
        {
            return (server_url, shutdown_tx);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!("test gRPC server failed to start on {}", addr);
}

fn test_schema_bytes() -> Vec<u8> {
    vec![10, 4, 85, 115, 101, 114]
}

#[tokio::test]
async fn test_schema_registry_requires_admin_token() {
    let db = create_test_db();
    let service = PrkDbGrpcService::new(db, ADMIN_TOKEN.to_string());
    let (server_url, shutdown) = start_server(service).await;
    let mut client = PrkDbServiceClient::connect(server_url).await.unwrap();

    let register_result = client
        .register_schema(tonic::Request::new(RegisterSchemaRequest {
            admin_token: "wrong-token".to_string(),
            collection: "users".to_string(),
            schema_proto: test_schema_bytes(),
            compatibility: CompatibilityMode::CompatibilityBackward as i32,
            migration_id: None,
        }))
        .await;

    assert!(
        register_result.is_err(),
        "register_schema should reject a bad token"
    );
    assert_eq!(
        register_result.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );

    let list_result = client
        .list_schemas(tonic::Request::new(ListSchemasRequest {
            admin_token: String::new(),
        }))
        .await;

    assert!(
        list_result.is_err(),
        "list_schemas should reject an empty token"
    );
    assert_eq!(
        list_result.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );

    shutdown.send(()).ok();
}

#[tokio::test]
async fn test_schema_registry_persists_across_restart() {
    let schema_dir = TempDir::new().unwrap();
    let schema_path = PathBuf::from(schema_dir.path());

    let first_service = PrkDbGrpcService::with_schema_storage_path(
        create_test_db(),
        ADMIN_TOKEN.to_string(),
        schema_path.clone(),
    )
    .await;
    let (server_url, shutdown) = start_server(first_service).await;
    let mut client = PrkDbServiceClient::connect(server_url).await.unwrap();

    let register_response = client
        .register_schema(tonic::Request::new(RegisterSchemaRequest {
            admin_token: ADMIN_TOKEN.to_string(),
            collection: "users".to_string(),
            schema_proto: test_schema_bytes(),
            compatibility: CompatibilityMode::CompatibilityBackward as i32,
            migration_id: None,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(register_response.success);
    shutdown.send(()).ok();

    let second_service = PrkDbGrpcService::with_schema_storage_path(
        create_test_db(),
        ADMIN_TOKEN.to_string(),
        schema_path,
    )
    .await;
    let (server_url, shutdown) = start_server(second_service).await;
    let mut client = PrkDbServiceClient::connect(server_url).await.unwrap();

    let list_response = client
        .list_schemas(tonic::Request::new(ListSchemasRequest {
            admin_token: ADMIN_TOKEN.to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(list_response.success);
    assert_eq!(list_response.schemas.len(), 1);
    assert_eq!(list_response.schemas[0].collection, "users");
    assert_eq!(list_response.schemas[0].latest_version, 1);

    shutdown.send(()).ok();
}

#[tokio::test]
async fn test_watch_stream_receives_put_events() {
    let db = create_test_db();
    let service = PrkDbGrpcService::new(db, ADMIN_TOKEN.to_string());
    let (server_url, shutdown) = start_server(service).await;
    let mut watcher_client = PrkDbServiceClient::connect(server_url.clone())
        .await
        .unwrap();
    let mut writer_client = PrkDbServiceClient::connect(server_url).await.unwrap();

    let response = watcher_client
        .watch(tonic::Request::new(WatchRequest {
            key_prefix: b"user:".to_vec(),
        }))
        .await
        .unwrap();
    let mut stream = response.into_inner();

    writer_client
        .put(tonic::Request::new(PutRequest {
            key: b"user:1".to_vec(),
            value: br#"{"id":"1","name":"Ada"}"#.to_vec(),
        }))
        .await
        .unwrap();

    let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("watch stream timed out")
        .expect("watch stream ended")
        .expect("watch event failed");

    assert_eq!(event.key, b"user:1".to_vec());
    assert_eq!(event.value, br#"{"id":"1","name":"Ada"}"#.to_vec());

    shutdown.send(()).ok();
}
