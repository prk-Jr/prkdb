use prkdb_proto::raft::prk_db_service_client::PrkDbServiceClient;
use prkdb_proto::raft::MetadataRequest;
use prkdb_storage_sled::SledAdapter;
use prkdb_types::storage::StorageAdapter;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

#[derive(Clone, Copy, Default)]
enum SeedMode {
    #[default]
    None,
    TypedUserRecord,
}

#[derive(Default)]
struct TestServerConfig {
    host: String,
    prometheus: bool,
    advertised_grpc_addr: Option<String>,
    seed_mode: SeedMode,
}

struct TestServer {
    process: Child,
    http_port: u16,
    grpc_port: u16,
    _temp_dir: TempDir,
}

impl TestServer {
    async fn start(prometheus: bool) -> Self {
        Self::start_with_config(TestServerConfig {
            host: "127.0.0.1".to_string(),
            prometheus,
            ..Default::default()
        })
        .await
    }

    async fn start_with_config(config: TestServerConfig) -> Self {
        let http_port = reserve_local_port();
        let grpc_port = reserve_local_port();
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("db");
        let binary_path = env!("CARGO_BIN_EXE_prkdb-cli");

        if matches!(config.seed_mode, SeedMode::TypedUserRecord) {
            seed_typed_user_record(&db_path).await;
        }

        let mut command = Command::new(binary_path);
        command
            .arg("--database")
            .arg(&db_path)
            .arg("serve")
            .arg("--host")
            .arg(&config.host)
            .arg("--port")
            .arg(http_port.to_string())
            .arg("--grpc-port")
            .arg(grpc_port.to_string())
            // These tests exercise the HTTP API itself, not authorization, which has its
            // own suite in http_authz.rs. Since Task 1 the server refuses to start with
            // no principals, so the opt-out has to be explicit here.
            .arg("--allow-anonymous")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());

        if config.prometheus {
            command.arg("--prometheus");
        }

        if let Some(advertised_grpc_addr) = config.advertised_grpc_addr {
            command.env("PRKDB_ADVERTISED_GRPC_ADDR", advertised_grpc_addr);
        }

        let mut child = command.spawn().expect("failed to spawn prkdb-cli");
        let client = reqwest::Client::new();
        let health_url = format!("http://127.0.0.1:{http_port}/health");
        let grpc_url = format!("http://127.0.0.1:{grpc_port}");

        for _ in 0..40 {
            let http_ready = match client.get(&health_url).send().await {
                Ok(response) => response.status().is_success(),
                Err(_) => false,
            };
            let grpc_ready = match PrkDbServiceClient::connect(grpc_url.clone()).await {
                Ok(mut grpc_client) => grpc_client
                    .metadata(tonic::Request::new(MetadataRequest { topics: vec![] }))
                    .await
                    .is_ok(),
                Err(_) => false,
            };

            if http_ready && grpc_ready {
                return Self {
                    process: child,
                    http_port,
                    grpc_port,
                    _temp_dir: temp_dir,
                };
            }
            sleep(Duration::from_millis(100)).await;
        }

        let _ = child.kill();
        let _ = child.wait();
        panic!("server failed to start");
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn reserve_local_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TypedUserRecord {
    id: String,
    name: String,
}

/// A typed collection persisted as `user`, as a `#[collection(name = "user")]` derive
/// would declare it.
impl prkdb_types::collection::Collection for TypedUserRecord {
    type Id = String;
    fn id(&self) -> &String {
        &self.id
    }
    fn persisted_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("user")
    }
}

/// Seeds one record through the typed API (`CollectionHandle`: the key codec, the
/// collection catalog and a bincode value), plus an internal metadata key the HTTP
/// listing must hide.
async fn seed_typed_user_record(db_path: &Path) {
    let storage = SledAdapter::open(db_path).unwrap();
    storage
        .put(b"__prkdb_metadata:collection:internal", b"ignore-me")
        .await
        .unwrap();
    let db = prkdb::PrkDb::builder()
        .with_storage(storage)
        .build()
        .unwrap();
    db.collection::<TypedUserRecord>()
        .put(TypedUserRecord {
            id: "1".to_string(),
            name: "Ada Lovelace".to_string(),
        })
        .await
        .unwrap();
    db.storage().flush().await.unwrap();
}

#[tokio::test]
async fn test_http_can_get_item_by_id_and_report_count() {
    let server = TestServer::start(false).await;
    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{}", server.http_port);

    let put_response = client
        .put(format!("{base_url}/collections/users/data"))
        .json(&serde_json::json!({
            "id": "1",
            "name": "Ada Lovelace"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(put_response.status(), StatusCode::OK);

    let item_response = client
        .get(format!("{base_url}/collections/users/data/1"))
        .send()
        .await
        .unwrap();

    assert_eq!(item_response.status(), StatusCode::OK);
    let item_body: Value = item_response.json().await.unwrap();
    assert_eq!(item_body["success"], true);
    assert_eq!(item_body["data"]["id"], "1");
    assert_eq!(item_body["data"]["name"], "Ada Lovelace");

    let count_response = client
        .get(format!("{base_url}/collections/users/count"))
        .send()
        .await
        .unwrap();

    assert_eq!(count_response.status(), StatusCode::OK);
    let count_body: Value = count_response.json().await.unwrap();
    assert_eq!(count_body["success"], true);
    assert_eq!(count_body["data"]["count"], 1);
}

#[tokio::test]
async fn test_http_root_reports_current_package_version() {
    let server = TestServer::start(false).await;
    let response = reqwest::get(format!("http://127.0.0.1:{}/", server.http_port))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body: Value = response.json().await.unwrap();
    assert_eq!(body["name"], "PrkDB HTTP API");
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
}

#[tokio::test]
async fn test_http_supports_typed_collection_keys_and_filters_internal_metadata() {
    let server = TestServer::start_with_config(TestServerConfig {
        host: "127.0.0.1".to_string(),
        seed_mode: SeedMode::TypedUserRecord,
        ..Default::default()
    })
    .await;
    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{}", server.http_port);

    let collections_response = client
        .get(format!("{base_url}/collections"))
        .send()
        .await
        .unwrap();
    assert_eq!(collections_response.status(), StatusCode::OK);
    let collections_body: Value = collections_response.json().await.unwrap();
    let collections = collections_body["data"]["collections"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|value| value.as_str())
        .collect::<Vec<_>>();
    assert!(collections.contains(&"user"), "{collections:?}");
    assert!(!collections.contains(&"__prkdb_metadata"));

    let item_response = client
        .get(format!("{base_url}/collections/user/data/1"))
        .send()
        .await
        .unwrap();
    assert_eq!(item_response.status(), StatusCode::OK);
    let item_body: Value = item_response.json().await.unwrap();
    assert_eq!(item_body["success"], true);
    assert_eq!(item_body["data"]["id"], "1");
    assert_eq!(item_body["data"]["name"], "Ada Lovelace");

    let count_response = client
        .get(format!("{base_url}/collections/user/count"))
        .send()
        .await
        .unwrap();
    assert_eq!(count_response.status(), StatusCode::OK);
    let count_body: Value = count_response.json().await.unwrap();
    assert_eq!(count_body["data"]["count"], 1);
}

#[tokio::test]
async fn test_metadata_uses_explicit_advertised_grpc_address() {
    let advertised_grpc_addr = format!("http://127.0.0.1:{}", reserve_local_port());

    let server = TestServer::start_with_config(TestServerConfig {
        host: "0.0.0.0".to_string(),
        advertised_grpc_addr: Some(advertised_grpc_addr.clone()),
        ..Default::default()
    })
    .await;

    let mut client = PrkDbServiceClient::connect(format!("http://127.0.0.1:{}", server.grpc_port))
        .await
        .unwrap();
    let metadata = client
        .metadata(tonic::Request::new(MetadataRequest { topics: vec![] }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(metadata.nodes.len(), 1);
    assert_eq!(metadata.nodes[0].address, advertised_grpc_addr);
    assert!(!metadata.nodes[0].address.contains("0.0.0.0"));
}

#[tokio::test]
async fn test_metrics_endpoint_exports_prometheus_registry() {
    let server = TestServer::start(true).await;
    let body = reqwest::get(format!("http://127.0.0.1:{}/metrics", server.http_port))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(
        body.contains("prkdb_up"),
        "expected real Prometheus metrics"
    );
    assert!(
        !body.contains("prkdb_requests_total"),
        "placeholder HTTP metrics should not be exposed anymore"
    );
}
