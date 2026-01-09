use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path, Query, State, WebSocketUpgrade,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::net::SocketAddr;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

use crate::commands::collection::{self};
use crate::commands::CollectionCommands;
use crate::database_manager;
use crate::Commands;

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
}

#[derive(Clone)]
struct AppState {
    _database_path: std::path::PathBuf,
    websocket_enabled: bool,
    prometheus_enabled: bool,
    broadcast_tx: broadcast::Sender<String>,
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
    let (broadcast_tx, _) = broadcast::channel::<String>(100);

    // Create app state
    let state = AppState {
        _database_path: std::path::PathBuf::from("./prkdb.db"), // Default path
        websocket_enabled: args.websockets,
        prometheus_enabled: args.prometheus,
        broadcast_tx: broadcast_tx.clone(),
    };

    // Build our application with routes
    let mut app = Router::new()
        .route("/", get(root_handler))
        .route("/health", get(health_handler))
        .route("/collections", get(list_collections_handler))
        .route("/collections/:name", get(get_collection_handler))
        .route("/collections/:name/data", get(get_collection_data_handler))
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
        app.layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
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

    // Get DB instance for gRPC service
    if let Ok(db) = crate::database_manager::get_db_instance().await {
        println!("🚀 Starting PrkDB gRPC server on {}", grpc_addr);

        tokio::spawn(async move {
            use prkdb::raft::grpc_service::PrkDbGrpcService;
            use prkdb::raft::rpc::prk_db_service_server::PrkDbServiceServer;
            use std::sync::Arc;
            use tonic::transport::Server;

            use std::env;
            let admin_token = env::var("PRKDB_ADMIN_TOKEN").unwrap_or_default();
            let service = PrkDbGrpcService::new(Arc::new(db), admin_token);

            if let Err(e) = Server::builder()
                .add_service(PrkDbServiceServer::new(service))
                .serve(grpc_addr)
                .await
            {
                eprintln!("❌ gRPC server error: {}", e);
            }
        });
    } else {
        eprintln!("⚠️ Failed to get database instance for gRPC server");
    }

    // Start the server
    let listener = tokio::net::TcpListener::bind(addr).await?;

    axum::serve(listener, app.into_make_service()).await?;

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
        "version": "0.1.0",
        "description": "HTTP API for PrkDB collections and metrics",
        "endpoints": {
            "collections": "/collections",
            "health": "/health",
            "documentation": "/"
        }
    }))
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    // Try to access the database to check health
    match database_manager::get_database_manager()
        .scan_storage()
        .await
    {
        Ok(_) => Json(json!({
            "status": "healthy",
            "database": "connected",
            "websockets": state.websocket_enabled,
            "metrics": state.prometheus_enabled,
            "timestamp": chrono::Utc::now().to_rfc3339()
        }))
        .into_response(),
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
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| websocket_connection(socket, collection_name, state))
}

async fn websocket_connection(mut socket: WebSocket, collection_name: String, state: AppState) {
    println!("🔌 WebSocket connected for collection: {}", collection_name);

    let mut rx = state.broadcast_tx.subscribe();

    // Send initial data
    if let Ok(output) = execute_collection_command(CollectionCommands::Sample {
        name: collection_name.clone(),
        limit: 10,
    })
    .await
    {
        let message = json!({
            "type": "initial_data",
            "collection": collection_name,
            "data": output,
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        if socket
            .send(Message::Text(message.to_string()))
            .await
            .is_err()
        {
            return;
        }
    }

    // Listen for updates
    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Ok(update) => {
                        let message = json!({
                            "type": "update",
                            "collection": collection_name,
                            "data": update,
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        });

                        if socket.send(Message::Text(message.to_string())).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) => break,
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }

    println!(
        "� WebSocket disconnected for collection: {}",
        collection_name
    );
}

async fn metrics_handler() -> impl IntoResponse {
    // Basic Prometheus metrics
    let metrics = format!(
        "# HELP prkdb_collections_total Total number of collections\n\
         # TYPE prkdb_collections_total counter\n\
         prkdb_collections_total {}\n\
         \n\
         # HELP prkdb_server_uptime_seconds Server uptime in seconds\n\
         # TYPE prkdb_server_uptime_seconds gauge\n\
         prkdb_server_uptime_seconds {}\n\
         \n\
         # HELP prkdb_requests_total Total number of HTTP requests\n\
         # TYPE prkdb_requests_total counter\n\
         prkdb_requests_total {}\n",
        get_collections_count().await.unwrap_or(0),
        get_uptime_seconds(),
        get_request_count()
    );

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        metrics,
    )
}

async fn execute_collection_command(cmd: CollectionCommands) -> Result<Value> {
    // Create a minimal CLI instance for executing commands
    let _cli = crate::Cli {
        database: std::path::PathBuf::from("./prkdb.db"),
        format: crate::OutputFormat::Json,
        verbose: false,
        admin_token: None,
        local: false,
        server: "http://127.0.0.1:50051".to_string(),
        command: Commands::Collection(cmd.clone()),
    };

    // Since the execute function doesn't return data directly,
    // we'll get data directly from storage for JSON output
    match cmd {
        CollectionCommands::List => get_collections_list().await,
        CollectionCommands::Describe { name } => get_collection_description(&name).await,
        CollectionCommands::Count { name } => get_collection_count(&name).await,
        CollectionCommands::Data {
            name,
            limit,
            offset,
            ..
        } => get_collection_data(&name, limit, offset).await,
        CollectionCommands::Sample { name, limit } => get_collection_data(&name, limit, 0).await,
        _ => Ok(serde_json::json!({
            "error": "This command is not supported via the HTTP API"
        })),
    }
}

// Helper functions to get data directly from storage
async fn get_collections_list() -> Result<Value> {
    let data = database_manager::get_database_manager()
        .scan_storage()
        .await?;
    let mut collections = std::collections::HashSet::new();

    for (key, _) in data {
        let key_str = String::from_utf8_lossy(&key);

        // Handle both formats: "collection:id" and "collection::Type:id"
        let collection_name = if key_str.contains("::") {
            // Format: "collection::Type:id" -> extract "collection"
            key_str.split(':').next()
        } else {
            // Format: "collection:id" -> extract "collection"
            key_str.split(':').next()
        };

        if let Some(collection_type) = collection_name {
            // Skip metadata keys when counting business collections
            if !collection_type.starts_with("__prkdb_metadata") {
                collections.insert(collection_type.to_string());
            }
        }
    }

    let collections_vec: Vec<String> = collections.into_iter().collect();

    Ok(json!({
        "collections": collections_vec,
        "total": collections_vec.len()
    }))
}

async fn get_collection_description(name: &str) -> Result<Value> {
    let data = database_manager::get_database_manager()
        .scan_storage()
        .await?;
    let mut items = Vec::new();
    let mut total = 0;

    for (key, value) in data {
        let key_str = String::from_utf8_lossy(&key);

        // Handle both formats: "collection:id" and "collection::Type:id"
        let matches_collection = if key_str.contains("::") {
            // Format: "collection::Type:id" - check if starts with our collection name
            key_str.starts_with(&format!("{}::", name))
        } else {
            // Format: "collection:id" - check if starts with our collection name
            key_str.starts_with(&format!("{}:", name))
        };

        if matches_collection {
            if let Ok(parsed) = collection::try_bincode_to_json(&value) {
                items.push(parsed);
            }
            total += 1;
            if items.len() >= 5 {
                // Limit sample for description
                break;
            }
        }
    }

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
        "total_items": total,
        "schema_info": schema_info,
        "sample_data": items
    }))
}

async fn get_collection_count(name: &str) -> Result<Value> {
    let data = database_manager::get_database_manager()
        .scan_storage()
        .await?;
    let prefix = format!("collection::{}:", name);
    let mut count = 0;

    for (key, _) in data {
        let key_str = String::from_utf8_lossy(&key);
        if key_str.starts_with(&prefix) {
            count += 1;
        }
    }

    Ok(json!({
        "collection": name,
        "count": count
    }))
}

async fn get_collection_data(name: &str, limit: usize, offset: usize) -> Result<Value> {
    let data = database_manager::get_database_manager()
        .scan_storage()
        .await?;
    let mut items = Vec::new();
    let mut total = 0;
    let mut skipped = 0;

    for (key, value) in data {
        let key_str = String::from_utf8_lossy(&key);

        // Handle both formats: "collection:id" and "collection::Type:id"
        let matches_collection = if key_str.contains("::") {
            // Format: "collection::Type:id" - check if starts with our collection name
            key_str.starts_with(&format!("{}::", name))
        } else {
            // Format: "collection:id" - check if starts with our collection name
            key_str.starts_with(&format!("{}:", name))
        };

        if matches_collection {
            total += 1;

            if skipped < offset {
                skipped += 1;
                continue;
            }

            if items.len() >= limit {
                break;
            }

            if let Ok(parsed) = collection::try_bincode_to_json(&value) {
                items.push(parsed);
            }
        }
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

async fn get_collections_count() -> Result<usize> {
    if let Ok(collections) = get_collections_list().await {
        if let Some(total) = collections.get("total").and_then(|t| t.as_u64()) {
            return Ok(total as usize);
        }
    }
    Ok(0)
}

fn get_uptime_seconds() -> u64 {
    // Simple uptime - could be enhanced with actual start time tracking
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        % 86400 // Reset daily for demo
}

fn get_request_count() -> u64 {
    // Simple counter - could be enhanced with actual request tracking
    42 // Placeholder
}
