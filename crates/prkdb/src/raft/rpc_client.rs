use crate::raft::config::NodeId;
use crate::raft::rpc::raft_service_client::RaftServiceClient;
use crate::raft::rpc::*;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use tonic::transport::Channel;

#[derive(Error, Debug)]
pub enum RpcError {
    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("RPC error: {0}")]
    Rpc(#[from] tonic::Status),

    #[error("Invalid URI: {0}")]
    InvalidUri(String),
}

#[derive(Debug, serde::Deserialize)]
pub enum ChaosRule {
    Partition { node1: u64, node2: u64 },
    Delay { src: u64, dst: u64, ms: u64 },
    Drop { src: u64, dst: u64, rate: f64 },
}

/// Manages gRPC connections to peer Raft nodes
pub struct RpcClientPool {
    local_node_id: NodeId,
    clients: Arc<RwLock<HashMap<NodeId, RaftServiceClient<Channel>>>>,
}

impl RpcClientPool {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if we should block or delay traffic to the target node based on chaos rules
    async fn check_chaos(&self, target_node: NodeId) -> Result<(), RpcError> {
        if let Ok(config_path) = std::env::var("CHAOS_CONFIG_PATH") {
            if let Ok(content) = tokio::fs::read_to_string(config_path).await {
                if let Ok(rules) = serde_json::from_str::<Vec<ChaosRule>>(&content) {
                    for rule in rules {
                        match rule {
                            ChaosRule::Partition { node1, node2 } => {
                                if (self.local_node_id == node1 && target_node == node2)
                                    || (self.local_node_id == node2 && target_node == node1)
                                {
                                    return Err(RpcError::Rpc(tonic::Status::unavailable(
                                        "Chaos partition",
                                    )));
                                }
                            }
                            ChaosRule::Delay { src, dst, ms } => {
                                if self.local_node_id == src && target_node == dst {
                                    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                                }
                            }
                            ChaosRule::Drop { src, dst, rate } => {
                                if self.local_node_id == src
                                    && target_node == dst
                                    && rand::random::<f64>() < rate
                                {
                                    return Err(RpcError::Rpc(tonic::Status::unavailable(
                                        "Chaos drop",
                                    )));
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Get or create a client for the given node
    async fn get_client(
        &self,
        node_id: NodeId,
        addr: &str,
    ) -> Result<RaftServiceClient<Channel>, RpcError> {
        // Check chaos rules first
        self.check_chaos(node_id).await?;

        // Check if client exists
        {
            let clients = self.clients.read().await;
            if let Some(client) = clients.get(&node_id) {
                return Ok(client.clone());
            }
        }

        // Create new client
        let endpoint = format!("http://{}", addr);
        let channel = Channel::from_shared(endpoint)
            .map_err(|e| RpcError::InvalidUri(e.to_string()))?
            .connect()
            .await?;

        let client = RaftServiceClient::new(channel);

        // Cache it
        {
            let mut clients = self.clients.write().await;
            clients.insert(node_id, client.clone());
        }

        tracing::debug!("Created new RPC client for node {} at {}", node_id, addr);
        Ok(client)
    }

    /// Send RequestVote RPC to a peer
    pub async fn send_request_vote(
        &self,
        node_id: NodeId,
        addr: &str,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RpcError> {
        let mut client = self.get_client(node_id, addr).await?;
        match client.request_vote(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => {
                // Remove client from cache on failure to force reconnection
                self.remove_client(node_id).await;
                Err(RpcError::Rpc(e))
            }
        }
    }

    /// Send PreVote RPC to a peer (§9.6 Pre-Vote protocol)
    pub async fn send_pre_vote(
        &self,
        node_id: NodeId,
        addr: &str,
        request: PreVoteRequest,
    ) -> Result<PreVoteResponse, RpcError> {
        let mut client = self.get_client(node_id, addr).await?;
        match client.pre_vote(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => {
                // Remove client from cache on failure to force reconnection
                self.remove_client(node_id).await;
                Err(RpcError::Rpc(e))
            }
        }
    }

    /// Send AppendEntries RPC to a peer
    pub async fn send_append_entries(
        &self,
        node_id: NodeId,
        addr: &str,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RpcError> {
        let mut client = self.get_client(node_id, addr).await?;
        match client.append_entries(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => {
                // Remove client from cache on failure to force reconnection
                self.remove_client(node_id).await;
                Err(RpcError::Rpc(e))
            }
        }
    }

    /// Send InstallSnapshot RPC to a peer
    pub async fn send_install_snapshot(
        &self,
        node_id: NodeId,
        addr: &str,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RpcError> {
        let mut client = self.get_client(node_id, addr).await?;
        match client.install_snapshot(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => {
                // Remove client from cache on failure to force reconnection
                self.remove_client(node_id).await;
                Err(RpcError::Rpc(e))
            }
        }
    }

    /// Remove a client from the pool (useful for reconnection)
    pub async fn remove_client(&self, node_id: NodeId) {
        let mut clients = self.clients.write().await;
        clients.remove(&node_id);
    }
}
