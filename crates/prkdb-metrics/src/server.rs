//! HTTP metrics server

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

/// Metrics server state
#[derive(Clone)]
pub struct MetricsServerState {
    // Can add custom state here if needed
}

/// HTTP metrics server
pub struct MetricsServer {
    addr: SocketAddr,
}

impl MetricsServer {
    /// Create a new metrics server
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    /// Start the metrics server
    pub async fn start(self) -> Result<(), Box<dyn std::error::Error>> {
        let state = MetricsServerState {};

        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/health", get(health_handler))
            .with_state(Arc::new(state));

        info!("Starting metrics server on {}", self.addr);

        let listener = tokio::net::TcpListener::bind(&self.addr).await.unwrap();

        axum::serve(listener, app.into_make_service()).await?;

        Ok(())
    }
}

/// Metrics endpoint handler
async fn metrics_handler(State(_state): State<Arc<MetricsServerState>>) -> Response {
    match crate::exporter::export_metrics() {
        Ok(metrics) => (StatusCode::OK, metrics).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error exporting metrics: {}", e),
        )
            .into_response(),
    }
}

/// Health check endpoint
async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}
