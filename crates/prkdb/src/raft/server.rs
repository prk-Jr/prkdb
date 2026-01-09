use crate::raft::node::RaftNode;
use crate::raft::rpc::raft_service_server::RaftServiceServer;
use crate::raft::service::RaftServiceImpl;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

/// TLS configuration for Raft server
#[derive(Clone)]
pub struct TlsConfig {
    /// Server certificate path
    pub cert_path: String,
    /// Server key path
    pub key_path: String,
    /// CA certificate path (for client verification)
    pub ca_path: Option<String>,
}

/// Start the Raft gRPC server (plain)
pub async fn start_raft_server(
    node: Arc<RaftNode>,
    listen_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let service = RaftServiceImpl::new(node);

    tracing::info!("Starting Raft gRPC server on {}", listen_addr);

    Server::builder()
        .add_service(RaftServiceServer::new(service))
        .serve(listen_addr)
        .await?;

    Ok(())
}

/// Start the Raft gRPC server with TLS
pub async fn start_raft_server_tls(
    node: Arc<RaftNode>,
    listen_addr: SocketAddr,
    tls_config: TlsConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let service = RaftServiceImpl::new(node);

    // Read certificate and key
    let cert = tokio::fs::read(&tls_config.cert_path).await?;
    let key = tokio::fs::read(&tls_config.key_path).await?;
    let identity = Identity::from_pem(cert, key);

    let mut tls = ServerTlsConfig::new().identity(identity);

    // Add CA for client certificate verification (mTLS)
    if let Some(ca_path) = &tls_config.ca_path {
        if Path::new(ca_path).exists() {
            let ca_cert = tokio::fs::read(ca_path).await?;
            let ca = Certificate::from_pem(ca_cert);
            tls = tls.client_ca_root(ca);
            tracing::info!("mTLS enabled: client certificates required");
        }
    }

    tracing::info!("Starting Raft gRPC server with TLS on {}", listen_addr);

    Server::builder()
        .tls_config(tls)?
        .add_service(RaftServiceServer::new(service))
        .serve(listen_addr)
        .await?;

    Ok(())
}
