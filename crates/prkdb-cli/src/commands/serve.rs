use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path, Query, State, WebSocketUpgrade,
    },
    http::{
        header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE},
        HeaderMap, Method, StatusCode,
    },
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use clap::Args;
use prkdb_types::error::{Error as PrkdbError, StorageError};
use prkdb_types::storage::StorageAdapter;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::broadcast;
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::commands::collection::{self};
use crate::commands::CollectionCommands;
use crate::database_manager;
use crate::storage_keys::{
    is_internal_metadata_key, logical_collection_name, parse_storage_key, ParsedStorageKey,
};

const HTTP_BROADCAST_CHANNEL_CAPACITY: usize = 100;
const WEBSOCKET_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

#[derive(Args, Clone)]
pub struct ServeArgs {
    /// Port to serve on
    #[arg(short, long, default_value = "8080")]
    pub port: u16,

    /// Port to serve gRPC on (for Admin & Raft)
    #[arg(long, default_value = "50051")]
    pub grpc_port: u16,

    /// Host to bind to
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Enable Prometheus metrics endpoint at /metrics
    #[arg(long)]
    pub prometheus: bool,

    /// Enable CORS for web dashboards
    #[arg(long)]
    pub cors: bool,

    /// Enable WebSocket support for real-time data
    #[arg(long)]
    pub websockets: bool,

    /// Node ID for Raft cluster
    #[arg(long, default_value = "1")]
    pub id: u64,

    /// Peer node addresses (node_id -> gRPC SocketAddr), set programmatically
    #[arg(skip)]
    pub peers: Vec<(u64, SocketAddr)>,

    /// Public gRPC address advertised to smart clients and metadata consumers
    #[arg(skip)]
    pub advertised_grpc_address: Option<String>,

    /// Public HTTP address advertised to peer nodes for write forwarding
    #[arg(skip)]
    pub advertised_http_address: Option<String>,

    /// Explicit peer gRPC addresses advertised to smart clients (node_id -> URL)
    #[arg(skip)]
    pub peer_advertised_grpc_addresses: Option<String>,

    /// Explicit peer HTTP base URLs used for leader forwarding (node_id -> URL)
    #[arg(skip)]
    pub peer_http_addresses: Option<String>,
}

#[derive(Clone)]
struct AppState {
    _database_path: std::path::PathBuf,
    node_id: u64,
    websocket_enabled: bool,
    prometheus_enabled: bool,
    websocket_auth_token: Option<String>,
    #[allow(dead_code)] // Used for future broadcast features
    broadcast_tx: broadcast::Sender<String>,
    /// Maps node_id -> HTTP base URL (e.g., 2 -> "http://127.0.0.1:8082")
    peer_http_addrs: HashMap<u64, String>,
}

#[derive(Deserialize)]
struct DataQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    filter: Option<String>,
    sort: Option<String>,
}

#[derive(Serialize)]
struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    error: Option<String>,
    total: Option<usize>,
}

#[derive(Deserialize)]
struct WsParams {
    from_offset: Option<u64>,
}

struct CollectionRecord {
    key: Vec<u8>,
    raw_collection: String,
    logical_collection: String,
    id_hint: Option<String>,
    value: Vec<u8>,
}

impl<T> ApiResponse<T> {
    fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            total: None,
        }
    }

    fn success_with_total(data: T, total: usize) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            total: Some(total),
        }
    }

    fn error(message: String) -> ApiResponse<()> {
        ApiResponse {
            success: false,
            data: None,
            error: Some(message),
            total: None,
        }
    }
}

pub async fn handle_serve(args: ServeArgs) -> Result<()> {
    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    let node_id_label = args.id.to_string();

    println!("🚀 Starting PrkDB server on http://{}", addr);

    if args.prometheus {
        println!("📊 Prometheus metrics enabled at http://{}/metrics", addr);
    }

    if args.cors {
        println!("🌐 CORS enabled for web dashboards");
    }

    if args.websockets {
        println!("⚡ WebSocket support enabled for real-time data");
    }

    // Create broadcast channel for WebSocket updates
    let (broadcast_tx, _) = broadcast::channel::<String>(HTTP_BROADCAST_CHANNEL_CAPACITY);
    let websocket_auth_token = std::env::var("PRKDB_WS_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());

    if args.prometheus {
        prkdb::prometheus_metrics::init_prometheus_metrics();
        prkdb::prometheus_metrics::SERVER_UP
            .with_label_values(&[&node_id_label])
            .set(1.0);
    }

    let peer_grpc_overrides =
        parse_node_address_overrides(args.peer_advertised_grpc_addresses.as_deref())?;
    let peer_http_overrides = parse_node_address_overrides(args.peer_http_addresses.as_deref())?;
    let advertised_http_address = resolve_advertised_http_address(
        &args.host,
        args.port,
        args.advertised_http_address.as_deref(),
    )?;
    let peer_http_addrs = build_peer_http_addresses(
        args.id,
        advertised_http_address.as_deref(),
        &args.peers,
        &peer_http_overrides,
    )?;
    let missing_peer_http_targets: Vec<u64> = args
        .peers
        .iter()
        .map(|(node_id, _)| *node_id)
        .filter(|node_id| !peer_http_addrs.contains_key(node_id))
        .collect();
    if !missing_peer_http_targets.is_empty() {
        tracing::warn!(
            "HTTP forwarding targets were not configured for peer nodes {:?}; follower write forwarding will be unavailable for those leaders",
            missing_peer_http_targets
        );
    }
    tracing::info!("Peer HTTP addresses: {:?}", peer_http_addrs);

    // Create app state
    let state = AppState {
        _database_path: std::path::PathBuf::from("./prkdb.db"),
        node_id: args.id,
        websocket_enabled: args.websockets,
        prometheus_enabled: args.prometheus,
        websocket_auth_token,
        broadcast_tx: broadcast_tx.clone(),
        peer_http_addrs,
    };

    // Build our application with routes
    let mut app = Router::new()
        .route("/", get(root_handler))
        .route("/health", get(health_handler))
        .route("/collections", get(list_collections_handler))
        .route("/collections/:name", get(get_collection_handler))
        .route(
            "/collections/:name/data",
            get(get_collection_data_handler).put(put_collection_data_handler),
        )
        .route(
            "/collections/:name/data/:id",
            get(get_collection_item_handler).delete(delete_collection_data_handler),
        )
        .route(
            "/collections/:name/count",
            get(get_collection_count_handler),
        )
        .route(
            "/collections/:name/schema",
            get(get_collection_schema_handler),
        );

    // Add WebSocket route if enabled
    if args.websockets {
        app = app.route("/ws/collections/:name", get(websocket_handler));
    }

    // Add metrics route if enabled
    if args.prometheus {
        app = app.route("/metrics", get(metrics_handler));
    }

    // Add state
    let app = app.with_state(state);

    // Add CORS middleware if enabled
    let app = if args.cors {
        app.layer(build_cors_layer(&args.host, args.port)?)
    } else {
        app
    };

    println!("✅ Server started successfully!");
    println!("🔗 Available endpoints:");
    println!("   GET  /               - API documentation");
    println!("   GET  /health         - Health check");
    println!("   GET  /collections    - List all collections");
    println!("   GET  /collections/{{name}}       - Collection details");
    println!("   GET  /collections/{{name}}/data  - Collection data");
    println!("   GET  /collections/{{name}}/count - Collection count");
    println!("   GET  /collections/{{name}}/schema - Collection schema");

    if args.websockets {
        println!("   WS   /ws/collections/{{name}}   - Real-time updates");
    }

    if args.prometheus {
        println!("   GET  /metrics       - Prometheus metrics");
    }

    // Start gRPC server in background
    let grpc_port = args.grpc_port;
    // Use the same host as HTTP server (default 127.0.0.1)
    let grpc_addr_str = format!("{}:{}", args.host, grpc_port);
    let grpc_addr: SocketAddr = grpc_addr_str
        .parse()
        .map_err(|e| anyhow::anyhow!("Failed to parse gRPC address '{}': {}", grpc_addr_str, e))?;
    let advertised_grpc_address = resolve_advertised_grpc_address(
        &args.host,
        grpc_port,
        args.advertised_grpc_address.as_deref(),
    )?;
    let advertised_node_addresses = build_advertised_node_addresses(
        args.id,
        &advertised_grpc_address,
        &args.peers,
        &peer_grpc_overrides,
    );

    // Get DB instance for gRPC service
    if let Ok(db) = crate::database_manager::get_db_instance().await {
        println!("🚀 Starting PrkDB gRPC server on {}", grpc_addr);
        let schema_storage_path =
            crate::database_manager::try_get_database_manager()?.schema_storage_path();

        // Start Multi-Raft partitions (background tasks)
        // Skip serving Partition 0's Raft server here, as we'll multiplex it on the main gRPC server below
        // This avoids port collision on 50051
        let rpc_pool = std::sync::Arc::new(prkdb::raft::RpcClientPool::new(args.id));
        db.start_multi_raft(rpc_pool, &[0]);

        tokio::spawn(async move {
            use prkdb::raft::grpc_service::PrkDbGrpcService;
            use prkdb::raft::rpc::prk_db_service_server::PrkDbServiceServer;
            use prkdb::raft::rpc::raft_service_server::RaftServiceServer;
            use prkdb::raft::service::RaftServiceImpl;
            use std::sync::Arc;
            use tonic::transport::Server;

            use std::env;
            let admin_token = env::var("PRKDB_ADMIN_TOKEN").unwrap_or_default();
            let service = PrkDbGrpcService::with_schema_storage_path(
                Arc::new(db.clone()),
                admin_token,
                schema_storage_path,
            )
            .await
            .with_local_node_id(args.id)
            .with_public_address(advertised_grpc_address)
            .with_advertised_node_addresses(advertised_node_addresses);

            let mut builder = Server::builder();

            // Register client service
            let mut router = builder.add_service(PrkDbServiceServer::new(service));

            // Register Raft service for all partitions (multiplexed on same port)
            if let Some(pm) = &db.partition_manager {
                println!("✨ Multiplexing Raft Service (All Partitions) on main port");
                let raft_service = RaftServiceImpl::new(pm.clone());
                router = router.add_service(RaftServiceServer::new(raft_service));
            }

            if let Err(e) = router.serve(grpc_addr).await {
                eprintln!("❌ gRPC server error: {}", e);
            }
        });
    } else {
        eprintln!("⚠️ Failed to get database instance for gRPC server");
    }

    // Start the server
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let shutdown_db = crate::database_manager::get_db_instance().await.ok();

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(async move {
            shutdown_signal().await;

            if let Some(db) = shutdown_db {
                if let Err(error) = flush_db_state(&db).await {
                    eprintln!("⚠️ Failed to flush storage during shutdown: {}", error);
                }
            }
        })
        .await?;

    if args.prometheus {
        prkdb::prometheus_metrics::SERVER_UP
            .with_label_values(&[&node_id_label])
            .set(0.0);
    }

    println!("\n👋 Server shutting down...");
    Ok(())
}

#[allow(dead_code)]
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

async fn root_handler() -> impl IntoResponse {
    Json(json!({
        "name": "PrkDB HTTP API",
        "version": env!("CARGO_PKG_VERSION"),
        "description": "HTTP API for PrkDB collections and metrics",
        "endpoints": {
            "collections": "/collections",
            "health": "/health",
            "documentation": "/"
        }
    }))
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    match crate::database_manager::get_db_instance().await {
        Ok(db) => {
            let (partitions, leaders_ready) = if let Some(pm) = &db.partition_manager {
                let stats = pm.get_statistics().await;
                (stats.total_partitions, stats.leaders_count)
            } else {
                (1, 1)
            };

            Json(json!({
                "status": "healthy",
                "database": "connected",
                "node_id": state.node_id,
                "websockets": state.websocket_enabled,
                "metrics": state.prometheus_enabled,
                "partitions": partitions,
                "leaders_ready": leaders_ready,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }))
            .into_response()
        }
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "unhealthy",
                "database": "error",
                "error": e.to_string(),
                "timestamp": chrono::Utc::now().to_rfc3339()
            })),
        )
            .into_response(),
    }
}

async fn list_collections_handler() -> impl IntoResponse {
    match execute_collection_command(CollectionCommands::List).await {
        Ok(output) => {
            // Extract the total from the inner data for the outer response
            let total = output.get("total").and_then(|t| t.as_u64()).unwrap_or(0) as usize;
            Json(ApiResponse::success_with_total(output, total)).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<Value>::error(e.to_string())),
        )
            .into_response(),
    }
}

async fn get_collection_handler(Path(name): Path<String>) -> impl IntoResponse {
    match execute_collection_command(CollectionCommands::Describe { name }).await {
        Ok(output) => Json(ApiResponse::success(output)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<Value>::error(e.to_string())),
        )
            .into_response(),
    }
}

async fn get_collection_data_handler(
    Path(name): Path<String>,
    Query(params): Query<DataQuery>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(20);
    let offset = params.offset.unwrap_or(0);

    let cmd = CollectionCommands::Data {
        name,
        limit,
        offset,
        filter: params.filter,
        sort: params.sort,
    };

    match execute_collection_command(cmd).await {
        Ok(output) => Json(ApiResponse::success(output)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<Value>::error(e.to_string())),
        )
            .into_response(),
    }
}

async fn get_collection_item_handler(
    Path((name, id)): Path<(String, String)>,
) -> impl IntoResponse {
    let db = match crate::database_manager::get_db_instance().await {
        Ok(db) => db,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<Value>::error(e.to_string())),
            )
                .into_response()
        }
    };

    if db.partition_manager.is_none() {
        let direct_key = format!("{}:{}", name, id);
        match db.get_local(direct_key.as_bytes()).await {
            Ok(Some(bytes)) => {
                return match decode_stored_value(&bytes) {
                    Ok(value) => Json(ApiResponse::success(value)).into_response(),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiResponse::<Value>::error(e.to_string())),
                    )
                        .into_response(),
                };
            }
            Ok(None) => {}
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResponse::<Value>::error(e.to_string())),
                )
                    .into_response()
            }
        }
    }

    let records = match scan_collection_records(&name).await {
        Ok(records) => records,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<Value>::error(e.to_string())),
            )
                .into_response()
        }
    };

    for record in records {
        let matches_id = if record.id_hint.as_deref() == Some(id.as_str()) {
            true
        } else {
            match decode_stored_value(&record.value) {
                Ok(value) => json_value_matches_id(&value, &id),
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiResponse::<Value>::error(e.to_string())),
                    )
                        .into_response()
                }
            }
        };

        if !matches_id {
            continue;
        }

        let bytes = if db.partition_manager.is_some() {
            match db.get_follower_read(&record.key).await {
                Ok(Some(bytes)) => bytes,
                Ok(None) => continue,
                Err(e) => {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(ApiResponse::<Value>::error(format!(
                            "Consistent collection read failed: {}",
                            e
                        ))),
                    )
                        .into_response()
                }
            }
        } else {
            record.value
        };

        return match decode_stored_value(&bytes) {
            Ok(value) => Json(ApiResponse::success(enrich_collection_value(
                value,
                record.id_hint.as_deref(),
            )))
            .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<Value>::error(e.to_string())),
            )
                .into_response(),
        };
    }

    (
        StatusCode::NOT_FOUND,
        Json(ApiResponse::<Value>::error(format!(
            "Record '{}' not found in collection '{}'",
            id, name
        ))),
    )
        .into_response()
}

async fn put_collection_data_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(data): Json<serde_json::Value>,
) -> impl IntoResponse {
    let db = match crate::database_manager::get_db_instance().await {
        Ok(db) => db,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response()
        }
    };

    // Extract ID
    let id = match data.get("id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Data must have 'id' field"})),
            )
                .into_response()
        }
    };

    let key = format!("{}:{}", name, id);
    let value = match serde_json::to_vec(&data) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e.to_string()})),
            )
                .into_response()
        }
    };

    match db.put(key.as_bytes(), &value).await {
        Ok(_) => Json(json!({"success": true, "id": id})).into_response(),
        Err(e) => {
            let err_str = e.to_string();
            // Check for NotLeader error and forward to leader
            if let Some(leader_id) = leader_id_from_error(&e) {
                if let Some(leader_url) = state.peer_http_addrs.get(&leader_id) {
                    // Forward the PUT to the leader's HTTP endpoint
                    let forward_url = format!("{}/collections/{}/data", leader_url, name);
                    match reqwest::Client::new()
                        .put(&forward_url)
                        .json(&data)
                        .send()
                        .await
                    {
                        Ok(resp) if resp.status().is_success() => {
                            Json::<Value>(json!({"success": true, "id": id, "forwarded_to": leader_id}))
                                .into_response()
                        }
                        Ok(resp) => {
                            let status = resp.status().as_u16();
                            let body = resp.text().await.unwrap_or_default();
                            (
                                StatusCode::from_u16(status)
                                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                                Json::<Value>(json!({"error": body, "forwarded_to": leader_id})),
                            )
                                .into_response()
                        }
                        Err(fwd_err) => (
                            StatusCode::BAD_GATEWAY,
                            Json::<Value>(json!({"error": format!("Forward failed: {}", fwd_err), "leader_id": leader_id})),
                        )
                            .into_response(),
                    }
                } else {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({"error": err_str, "leader_id": leader_id})),
                    )
                        .into_response()
                }
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": err_str})),
                )
                    .into_response()
            }
        }
    }
}

async fn delete_collection_data_handler(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
) -> impl IntoResponse {
    let db = match crate::database_manager::get_db_instance().await {
        Ok(db) => db,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response()
        }
    };

    let key = format!("{}:{}", name, id);

    match db.delete(key.as_bytes()).await {
        Ok(_) => Json(json!({"success": true, "id": id})).into_response(),
        Err(e) => {
            let err_str = e.to_string();
            if let Some(leader_id) = leader_id_from_error(&e) {
                if let Some(leader_url) = state.peer_http_addrs.get(&leader_id) {
                    let forward_url = format!("{}/collections/{}/data/{}", leader_url, name, id);
                    match reqwest::Client::new().delete(&forward_url).send().await {
                        Ok(resp) if resp.status().is_success() => {
                            Json::<Value>(json!({"success": true, "id": id, "forwarded_to": leader_id}))
                                .into_response()
                        }
                        Ok(resp) => {
                            let status = resp.status().as_u16();
                            let body = resp.text().await.unwrap_or_default();
                            (
                                StatusCode::from_u16(status)
                                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                                Json::<Value>(json!({"error": body, "forwarded_to": leader_id})),
                            )
                                .into_response()
                        }
                        Err(fwd_err) => (
                            StatusCode::BAD_GATEWAY,
                            Json::<Value>(json!({"error": format!("Forward failed: {}", fwd_err), "leader_id": leader_id})),
                        )
                            .into_response(),
                    }
                } else {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({"error": err_str, "leader_id": leader_id})),
                    )
                        .into_response()
                }
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": err_str})),
                )
                    .into_response()
            }
        }
    }
}

fn leader_id_from_error(err: &PrkdbError) -> Option<u64> {
    match err {
        PrkdbError::Storage(StorageError::NotLeader { leader_id }) => *leader_id,
        PrkdbError::Storage(StorageError::Internal(message)) | PrkdbError::Internal(message) => {
            parse_leader_id_from_message(message)
        }
        _ => None,
    }
}

/// Parse leader node ID from a legacy "Not leader. Leader is Some(N)" error string
fn parse_leader_id_from_message(err: &str) -> Option<u64> {
    // Match pattern: "Not leader. Leader is Some(N)"
    if let Some(pos) = err.find("Leader is Some(") {
        let start = pos + "Leader is Some(".len();
        if let Some(end) = err[start..].find(')') {
            return err[start..start + end].parse::<u64>().ok();
        }
    }
    None
}

fn resolve_advertised_grpc_address(
    host: &str,
    grpc_port: u16,
    explicit_address: Option<&str>,
) -> Result<String> {
    if let Some(address) = explicit_address {
        return normalize_http_address(address);
    }

    if matches!(host, "0.0.0.0" | "::" | "[::]") {
        anyhow::bail!(
            "A public gRPC address must be provided with --advertised-grpc-address or PRKDB_ADVERTISED_GRPC_ADDR when binding to {}",
            host
        );
    }

    Ok(format!("http://{}:{}", host, grpc_port))
}

fn normalize_http_address(address: &str) -> Result<String> {
    if address.starts_with("http://") || address.starts_with("https://") {
        return Ok(address.to_string());
    }

    Ok(format!("http://{}", address))
}

fn resolve_advertised_http_address(
    host: &str,
    port: u16,
    explicit_address: Option<&str>,
) -> Result<Option<String>> {
    if let Some(address) = explicit_address {
        return normalize_http_address(address).map(Some);
    }

    if matches!(host, "0.0.0.0" | "::" | "[::]") {
        return Ok(None);
    }

    Ok(Some(format!("http://{}:{}", host, port)))
}

fn parse_node_address_overrides(value: Option<&str>) -> Result<HashMap<u64, String>> {
    let mut overrides = HashMap::new();

    let Some(value) = value else {
        return Ok(overrides);
    };

    for entry in value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (node_id_str, address) = entry
            .split_once('=')
            .or_else(|| entry.split_once('@'))
            .ok_or_else(|| anyhow::anyhow!("Invalid node address override '{entry}'"))?;
        let node_id = node_id_str
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("Invalid node ID in override '{entry}'"))?;
        let normalized = normalize_http_address(address)?;

        if overrides.insert(node_id, normalized).is_some() {
            anyhow::bail!("Duplicate node address override for node {node_id}");
        }
    }

    Ok(overrides)
}

fn build_advertised_node_addresses(
    local_node_id: u64,
    local_public_address: &str,
    peers: &[(u64, SocketAddr)],
    overrides: &HashMap<u64, String>,
) -> HashMap<u64, String> {
    let mut node_addresses: HashMap<u64, String> = peers
        .iter()
        .filter_map(|(node_id, address)| {
            overrides
                .get(node_id)
                .cloned()
                .or_else(|| (!address.ip().is_unspecified()).then(|| format!("http://{}", address)))
                .map(|value| (*node_id, value))
        })
        .collect();
    node_addresses.insert(local_node_id, local_public_address.to_string());
    node_addresses
}

fn build_peer_http_addresses(
    local_node_id: u64,
    local_public_address: Option<&str>,
    peers: &[(u64, SocketAddr)],
    overrides: &HashMap<u64, String>,
) -> Result<HashMap<u64, String>> {
    let mut node_addresses = HashMap::new();

    if let Some(address) = local_public_address {
        node_addresses.insert(local_node_id, normalize_http_address(address)?);
    }

    for (node_id, _) in peers {
        if let Some(address) = overrides.get(node_id) {
            node_addresses.insert(*node_id, address.clone());
        }
    }

    Ok(node_addresses)
}

async fn get_collection_count_handler(Path(name): Path<String>) -> impl IntoResponse {
    match execute_collection_command(CollectionCommands::Count { name }).await {
        Ok(output) => Json(ApiResponse::success(output)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<Value>::error(e.to_string())),
        )
            .into_response(),
    }
}

async fn get_collection_schema_handler(Path(name): Path<String>) -> impl IntoResponse {
    // Use describe command to get schema information
    match execute_collection_command(CollectionCommands::Describe { name }).await {
        Ok(output) => {
            // Extract schema information from the describe output
            if let Value::Object(ref obj) = output {
                if let Some(schema) = obj.get("schema_info") {
                    return Json(ApiResponse::success(schema.clone())).into_response();
                }
            }
            Json(ApiResponse::success(json!({
                "schema": "Schema information not available",
                "full_description": output
            })))
            .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::<Value>::error(e.to_string())),
        )
            .into_response(),
    }
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    Path(collection_name): Path<String>,
    Query(params): Query<WsParams>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(expected_token) = &state.websocket_auth_token {
        let provided_token = extract_bearer_token(&headers);
        if provided_token.as_deref() != Some(expected_token.as_str()) {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ApiResponse::<Value>::error(
                    "Missing or invalid WebSocket bearer token".to_string(),
                )),
            )
                .into_response();
        }
    }

    ws.on_upgrade(move |socket| websocket_connection(socket, collection_name, params, state))
        .into_response()
}

async fn websocket_connection(
    mut socket: WebSocket,
    collection_name: String,
    params: WsParams,
    _state: AppState,
) {
    println!("🔌 WebSocket connected for collection: {}", collection_name);

    // Get database instance
    let db = match crate::database_manager::get_db_instance().await {
        Ok(db) => std::sync::Arc::new(db),
        Err(e) => {
            let _ = socket
                .send(Message::Text(
                    json!({
                        "type": "error",
                        "message": format!("Failed to get database instance: {}", e)
                    })
                    .to_string(),
                ))
                .await;
            return;
        }
    };

    let mut last_sequence = 0u64;

    // Determine start offset
    if let Some(offset) = params.from_offset {
        last_sequence = offset;
    } else {
        // Default behavior: start from latest
        // Attempt to find current max sequence for this collection
        {
            // Simple scan to find max sequence (expensive for large history, but okay for MVP/small collections)
            let prefix = format!("{}:", collection_name);
            if let Ok(entries) = load_outbox_entries(&db).await {
                for (id, _) in entries {
                    if id.starts_with(&prefix) {
                        // Find max sequence
                        if let Some(seq_str) = id.rsplit(':').next() {
                            if let Ok(seq) = seq_str.parse::<u64>() {
                                last_sequence = last_sequence.max(seq);
                            }
                        }
                    }
                }
            }
        }
    }

    // Send initial connection success
    if socket
        .send(Message::Text(
            json!({
                "type": "connected",
                "collection": collection_name,
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "start_offset": last_sequence
            })
            .to_string(),
        ))
        .await
        .is_err()
    {
        return;
    }

    // Poll loop
    let mut interval = tokio::time::interval(WEBSOCKET_POLL_INTERVAL);
    let prefix = format!("{}:", collection_name);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                match load_outbox_entries(&db).await {
                    Ok(entries) => {
                        let mut new_sequence = last_sequence;
                        let mut updates = Vec::new();

                        // Filter and sort entries strictly > last_sequence
                        for (id, bytes) in entries {
                             if !id.starts_with(&prefix) { continue; }

                             // Parse sequence
                             let seq = if let Some(seq_str) = id.rsplit(':').next() {
                                if let Ok(s) = seq_str.parse::<u64>() { s } else { continue; }
                             } else { continue; };

                             if seq > last_sequence {
                                 // Try to decode payload
                                 // 1. Try raw JSON (SledAdapter default)
                                 if let Ok(json_val) = serde_json::from_slice::<Value>(&bytes) {
                                     updates.push((seq, json_val));
                                     new_sequence = new_sequence.max(seq);
                                 }
                                 // 2. Try skipping 4-byte header (Raft/Legacy)
                                 else if bytes.len() > 4 {
                                     // Skip 4 bytes (variant index)
                                     let payload = &bytes[4..];
                                     // Try to convert to JSON
                                     if let Ok(json_val) = crate::commands::collection::try_bincode_to_json(payload) {
                                         updates.push((seq, json_val));
                                         new_sequence = new_sequence.max(seq);
                                     }
                                 }
                             }
                        }

                        // Sort by sequence to ensure order
                        updates.sort_by_key(|(seq, _)| *seq);

                        for (seq, data) in updates {
                            let message = json!({
                                "type": "update",
                                "collection": collection_name,
                                "data": data,
                                "offset": seq,
                                "timestamp": chrono::Utc::now().to_rfc3339()
                            });

                            if socket.send(Message::Text(message.to_string())).await.is_err() {
                                return;
                            }
                        }

                        last_sequence = new_sequence;
                    }
                    Err(e) => {
                        let _ = socket
                            .send(Message::Text(
                                json!({
                                    "type": "error",
                                    "message": e.to_string()
                                })
                                .to_string(),
                            ))
                            .await;
                        break;
                    }
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) => break,
                    Some(Err(_)) => break,
                    Some(Ok(Message::Text(text))) => {
                         // Optional: Handle client commands like "seek" or "pause"
                         println!("Received WS message from client: {}", text);
                    }
                    _ => {}
                }
            }
        }
    }

    println!(
        "🔌 WebSocket disconnected for collection: {}",
        collection_name
    );
}

async fn metrics_handler() -> impl IntoResponse {
    let metrics = prkdb::prometheus_metrics::export_metrics();

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        metrics,
    )
}

async fn execute_collection_command(cmd: CollectionCommands) -> Result<Value> {
    match cmd {
        CollectionCommands::List => get_collections_list().await,
        CollectionCommands::Describe { name } => get_collection_description(&name).await,
        CollectionCommands::Count { name } => get_collection_count(&name).await,
        CollectionCommands::Data {
            name,
            limit,
            offset,
            filter,
            sort,
        } => get_collection_data(&name, limit, offset, filter, sort).await,
        CollectionCommands::Sample { name, limit } => {
            get_collection_data(&name, limit, 0, None, None).await
        }
        _ => Ok(serde_json::json!({
            "error": "This command is not supported via the HTTP API"
        })),
    }
}

// Helper functions to get data directly from storage
async fn get_collections_list() -> Result<Value> {
    let db = crate::database_manager::get_db_instance().await?;
    let mut collections: std::collections::BTreeSet<String> =
        db.list_collections().await?.into_iter().collect();

    for (key, _) in database_manager::scan_storage().await? {
        if is_internal_metadata_key(&key) {
            continue;
        }

        if let Some(name) = logical_collection_name(&key) {
            collections.insert(name);
        }
    }

    let collections_vec: Vec<String> = collections.into_iter().collect();

    Ok(json!({
        "collections": collections_vec,
        "total": collections_vec.len()
    }))
}

async fn get_collection_description(name: &str) -> Result<Value> {
    let db = crate::database_manager::get_db_instance().await?;
    let total_items = scan_collection_records(name).await?.len() as u64;
    let collections = db.list_collections().await?;
    if total_items == 0 && !collections.iter().any(|collection| collection == name) {
        return Err(anyhow::anyhow!("Collection '{}' not found", name));
    }
    let sample_output = get_collection_data(name, 5, 0, None, None).await?;
    let items = sample_output
        .get("data")
        .and_then(|data| data.as_array())
        .cloned()
        .unwrap_or_default();

    // Analyze schema from the items
    let schema_info = if !items.is_empty() {
        analyze_schema(&items)
    } else {
        json!({
            "fields": [],
            "primary_key": "Unknown",
            "constraints": []
        })
    };

    Ok(json!({
        "name": name,
        "total_items": total_items,
        "schema_info": schema_info,
        "sample_data": items
    }))
}

async fn get_collection_count(name: &str) -> Result<Value> {
    let count = scan_collection_records(name).await?.len() as u64;

    Ok(json!({
        "collection": name,
        "count": count
    }))
}

async fn get_collection_data(
    name: &str,
    limit: usize,
    offset: usize,
    filter: Option<String>,
    sort: Option<String>,
) -> Result<Value> {
    let data = scan_collection_records(name).await?;
    let mut collection_entries = Vec::new();

    for record in data {
        if let Ok(mut json_entry) = decode_stored_value(&record.value)
            .map(|value| enrich_collection_value(value, record.id_hint.as_deref()))
        {
            let full_key = std::str::from_utf8(&record.key)
                .map(ToString::to_string)
                .unwrap_or_else(|_| storage_key_display(&record));
            let key_part = record.id_hint.as_deref().unwrap_or("unknown").to_string();

            if let Some(obj) = json_entry.as_object_mut() {
                obj.insert("_key".to_string(), serde_json::Value::String(key_part));
                obj.insert(
                    "_full_key".to_string(),
                    serde_json::Value::String(full_key.clone()),
                );
                obj.insert(
                    "_collection".to_string(),
                    serde_json::Value::String(record.logical_collection.clone()),
                );
            }
            collection_entries.push((full_key, json_entry));
        }
    }

    if collection_entries.is_empty() {
        return Ok(json!({
            "collection": name,
            "data": [],
            "total": 0,
            "limit": limit,
            "offset": offset,
            "returned": 0
        }));
    }

    // Apply filter if specified
    if let Some(filter_expr) = &filter {
        collection_entries = apply_simple_filter(collection_entries, filter_expr)?;
    }

    // Apply sort if specified
    if let Some(sort_field) = &sort {
        collection_entries = apply_simple_sort(collection_entries, sort_field)?;
    }

    let total = collection_entries.len();
    let mut items = Vec::new();
    let mut skipped = 0;

    for (_, item) in collection_entries {
        if skipped < offset {
            skipped += 1;
            continue;
        }

        if items.len() >= limit {
            break;
        }

        items.push(item);
    }

    Ok(json!({
        "collection": name,
        "data": items,
        "total": total,
        "limit": limit,
        "offset": offset,
        "returned": items.len()
    }))
}

async fn scan_collection_records(name: &str) -> Result<Vec<CollectionRecord>> {
    let data = database_manager::scan_storage().await?;
    let mut records = Vec::new();

    for (key, value) in data {
        let ParsedStorageKey::Data {
            raw_collection,
            logical_collection,
            id_hint,
            ..
        } = parse_storage_key(&key)
        else {
            continue;
        };

        if collection_matches_name(name, &raw_collection, &logical_collection) {
            records.push(CollectionRecord {
                key,
                raw_collection,
                logical_collection,
                id_hint,
                value,
            });
        }
    }

    records.sort_by_key(storage_key_display);

    Ok(records)
}

fn collection_matches_name(name: &str, raw_collection: &str, logical_collection: &str) -> bool {
    name == logical_collection || name == raw_collection
}

fn storage_key_display(record: &CollectionRecord) -> String {
    match &record.id_hint {
        Some(id_hint) => format!("{}:{}", record.raw_collection, id_hint),
        None => record.raw_collection.clone(),
    }
}

fn json_value_matches_id(value: &Value, expected_id: &str) -> bool {
    value.get("id").and_then(json_scalar_to_string).as_deref() == Some(expected_id)
}

fn enrich_collection_value(mut value: Value, id_hint: Option<&str>) -> Value {
    if let (Some(id_hint), Some(object)) = (id_hint, value.as_object_mut()) {
        let needs_id = match object.get("id") {
            None | Some(Value::Null) => true,
            Some(Value::String(current)) => current.is_empty(),
            _ => false,
        };

        if needs_id {
            object.insert("id".to_string(), Value::String(id_hint.to_string()));
        }
    }

    value
}

fn json_scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

/// Apply simple filter to collection entries
fn apply_simple_filter(
    entries: Vec<(String, serde_json::Value)>,
    filter_expr: &str,
) -> Result<Vec<(String, serde_json::Value)>> {
    // Simple filter format: "field=value" or "field!=value" or "field~value" (contains)
    let filtered = if let Some(eq_pos) = filter_expr.find("!=") {
        let (field, value) = filter_expr.split_at(eq_pos);
        let value = &value[2..]; // Skip "!="
        entries
            .into_iter()
            .filter(|(_, entry)| {
                entry
                    .get(field)
                    .and_then(|v| v.as_str())
                    .map(|s| s != value)
                    .unwrap_or(true)
            })
            .collect()
    } else if let Some(eq_pos) = filter_expr.find('=') {
        let (field, value) = filter_expr.split_at(eq_pos);
        let value = &value[1..]; // Skip "="
        entries
            .into_iter()
            .filter(|(_, entry)| {
                entry
                    .get(field)
                    .and_then(|v| v.as_str())
                    .map(|s| s == value)
                    .unwrap_or(false)
            })
            .collect()
    } else if let Some(tilde_pos) = filter_expr.find('~') {
        let (field, value) = filter_expr.split_at(tilde_pos);
        let value = &value[1..]; // Skip "~"
        entries
            .into_iter()
            .filter(|(_, entry)| {
                entry
                    .get(field)
                    .and_then(|v| v.as_str())
                    .map(|s| s.contains(value))
                    .unwrap_or(false)
            })
            .collect()
    } else {
        entries // No valid filter format, return all
    };

    Ok(filtered)
}

/// Apply simple sort to collection entries
fn apply_simple_sort(
    mut entries: Vec<(String, serde_json::Value)>,
    sort_field: &str,
) -> Result<Vec<(String, serde_json::Value)>> {
    let (field, descending) = if let Some(field) = sort_field.strip_suffix(":desc") {
        (field, true)
    } else if let Some(field) = sort_field.strip_suffix(":asc") {
        (field, false)
    } else {
        (sort_field, false) // Default to ascending
    };

    entries.sort_by(|(_, a), (_, b)| {
        let a_val = a.get(field);
        let b_val = b.get(field);

        let cmp = match (a_val, b_val) {
            (Some(a), Some(b)) => {
                // Try string comparison first
                if let (Some(a_str), Some(b_str)) = (a.as_str(), b.as_str()) {
                    a_str.cmp(b_str)
                }
                // Try number comparison
                else if let (Some(a_num), Some(b_num)) = (a.as_f64(), b.as_f64()) {
                    a_num
                        .partial_cmp(&b_num)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
                // Fallback to string representation
                else {
                    a.to_string().cmp(&b.to_string())
                }
            }
            (Some(_), None) => std::cmp::Ordering::Less, // Non-null values come first
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        };

        if descending {
            cmp.reverse()
        } else {
            cmp
        }
    });

    Ok(entries)
}

fn analyze_schema(items: &[Value]) -> Value {
    if items.is_empty() {
        return json!({
            "fields": [],
            "primary_key": "Unknown",
            "constraints": []
        });
    }

    let mut all_fields = std::collections::HashSet::new();

    for item in items {
        if let Value::Object(obj) = item {
            for key in obj.keys() {
                all_fields.insert(key.clone());
            }
        }
    }

    let fields: Vec<String> = all_fields.into_iter().collect();
    let primary_key = fields.first().unwrap_or(&"id".to_string()).clone();

    json!({
        "fields": fields,
        "primary_key": primary_key,
        "constraints": [],
        "sample_count": items.len()
    })
}

fn decode_stored_value(value: &[u8]) -> Result<Value> {
    if let Ok(json_value) = serde_json::from_slice::<Value>(value) {
        return Ok(json_value);
    }

    collection::try_bincode_to_json(value)
}

async fn load_outbox_entries(db: &std::sync::Arc<prkdb::PrkDb>) -> Result<Vec<(String, Vec<u8>)>> {
    if let Some(pm) = &db.partition_manager {
        let mut entries = Vec::new();
        for partition_id in 0..pm.partition_count() {
            if let Some(storage) = pm.get_partition_storage(partition_id as u64) {
                let mut partition_entries = storage.outbox_list().await?;
                entries.append(&mut partition_entries);
            }
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(entries)
    } else {
        Ok(db.storage().outbox_list().await?)
    }
}

async fn flush_db_state(db: &prkdb::PrkDb) -> Result<()> {
    if let Some(pm) = &db.partition_manager {
        for partition_id in 0..pm.partition_count() {
            if let Some(storage) = pm.get_partition_storage(partition_id as u64) {
                storage.flush().await?;
            }
        }
    } else {
        db.storage().flush().await?;
    }

    Ok(())
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(|value| value.trim().to_string())
}

fn build_cors_layer(host: &str, port: u16) -> Result<CorsLayer> {
    let configured_origins = std::env::var("PRKDB_CORS_ORIGINS")
        .ok()
        .filter(|value| !value.trim().is_empty());

    let origins: Vec<axum::http::HeaderValue> = if let Some(value) = configured_origins {
        value
            .split(',')
            .map(|origin| origin.trim().parse())
            .collect::<std::result::Result<Vec<_>, _>>()?
    } else {
        let mut defaults = vec![
            format!("http://localhost:{port}").parse()?,
            format!("http://127.0.0.1:{port}").parse()?,
        ];

        if host != "127.0.0.1" && host != "localhost" && host != "0.0.0.0" {
            defaults.push(format!("http://{host}:{port}").parse()?);
        }

        defaults
    };

    Ok(CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods([Method::GET, Method::PUT, Method::DELETE])
        .allow_headers([ACCEPT, AUTHORIZATION, CONTENT_TYPE]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_node_address_overrides_normalizes_bare_addresses() {
        let overrides =
            parse_node_address_overrides(Some("2=db-2.example.com:18081,3=http://db-3:18082"))
                .unwrap();

        assert_eq!(
            overrides.get(&2).map(String::as_str),
            Some("http://db-2.example.com:18081")
        );
        assert_eq!(
            overrides.get(&3).map(String::as_str),
            Some("http://db-3:18082")
        );
    }

    #[test]
    fn build_advertised_node_addresses_uses_peer_overrides() {
        let peers = vec![(2, "127.0.0.1:50052".parse().unwrap())];
        let overrides = parse_node_address_overrides(Some("2=db-2.example.com:18081")).unwrap();

        let addresses =
            build_advertised_node_addresses(1, "http://db-1.example.com:18080", &peers, &overrides);

        assert_eq!(
            addresses.get(&1).map(String::as_str),
            Some("http://db-1.example.com:18080")
        );
        assert_eq!(
            addresses.get(&2).map(String::as_str),
            Some("http://db-2.example.com:18081")
        );
    }

    #[test]
    fn build_peer_http_addresses_requires_explicit_peer_urls() {
        let peers = vec![(2, "127.0.0.1:50052".parse().unwrap())];

        let addresses = build_peer_http_addresses(
            1,
            Some("http://db-1.example.com:8080"),
            &peers,
            &HashMap::new(),
        )
        .unwrap();

        assert_eq!(
            addresses.get(&1).map(String::as_str),
            Some("http://db-1.example.com:8080")
        );
        assert!(!addresses.contains_key(&2));
    }

    #[test]
    fn leader_id_from_error_prefers_typed_not_leader_errors() {
        let err = PrkdbError::Storage(StorageError::NotLeader { leader_id: Some(7) });
        assert_eq!(leader_id_from_error(&err), Some(7));
    }

    #[test]
    fn leader_id_from_error_supports_legacy_string_errors() {
        let err = PrkdbError::Storage(StorageError::Internal(
            "ReadIndex failed (not leader?): Not leader. Leader is Some(9)".to_string(),
        ));
        assert_eq!(leader_id_from_error(&err), Some(9));
    }
}
