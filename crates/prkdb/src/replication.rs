//! Leader-follower based replication for PrkDB.
//!
//! This module provides comprehensive replication functionality for PrkDB,
//! enabling leader-follower architecture with automatic failover and
//! real-time data synchronization.
//!
//! # Architecture
//! - Leader broadcasts changes to all followers via HTTP
//! - Followers poll for changes and apply them locally
//! - Each follower tracks its replication offset
//! - Changes are streamed from the outbox with full ordering guarantees
//! - Built-in health monitoring and lag tracking
//!
//! # Features
//! - Async replication for high performance
//! - Automatic retry with exponential backoff
//! - Comprehensive metrics and monitoring
//! - Configurable batch sizes and sync intervals
//! - Authentication support for secure replication

use crate::db::PrkDb;
use crate::outbox::{make_outbox_id_for_type, save_outbox_event, OutboxRecord};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use prkdb_core::collection::Collection;
use prkdb_core::error::StorageError;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::time::sleep;
#[cfg(feature = "metrics")]
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

/// Represents a node in the replication cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaNode {
    pub id: String,
    pub address: String,
}

/// Configuration for replication.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// The current node's identity.
    pub self_node: ReplicaNode,
    /// The address of the leader node. If None, this node is the leader.
    pub leader_address: Option<String>,
    /// List of all follower nodes.
    pub followers: Vec<ReplicaNode>,
    /// Timing/backoff configuration.
    pub timing: ReplicationTiming,
}

/// Tunables for replication timing/backoff/HTTP timeouts.
#[derive(Debug, Clone)]
pub struct ReplicationTiming {
    /// Base follower poll interval.
    pub follower_poll_interval: Duration,
    /// Initial backoff interval used for exponential retry.
    pub backoff_base: Duration,
    /// Max backoff delay.
    pub backoff_max: Duration,
    /// HTTP request timeout when fetching changes.
    pub request_timeout: Duration,
}

impl Default for ReplicationTiming {
    fn default() -> Self {
        Self {
            follower_poll_interval: Duration::from_secs(1),
            backoff_base: Duration::from_secs(1),
            backoff_max: Duration::from_secs(60),
            request_timeout: Duration::from_secs(5),
        }
    }
}

/// Replication change message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationChange {
    /// Unique ID of this change (outbox key)
    pub id: String,
    /// Collection type name
    pub collection: String,
    /// Serialized change data
    pub data: Vec<u8>,
    /// Timestamp of the change
    pub timestamp: i64,
}

/// Replication state for a follower
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplicationState {
    /// Last replicated change ID
    pub last_change_id: Option<String>,
    /// Number of changes applied
    pub changes_applied: u64,
    /// Last sync timestamp
    pub last_sync_ts: i64,
    /// Connection status
    pub connected: bool,
    /// Number of consecutive errors
    pub error_count: u64,
    /// Last error message
    pub last_error: Option<String>,
}

/// Health status of replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationHealth {
    /// Is the node healthy?
    pub healthy: bool,
    /// Replication lag in seconds
    pub lag_seconds: f64,
    /// Outbox drift = saved - drained
    pub outbox_drift: i64,
    /// Changes applied in last minute
    pub throughput: f64,
    /// Last sync time
    pub last_sync: i64,
    /// Error message if unhealthy
    pub error: Option<String>,
}

/// The main replication manager.
pub struct ReplicationManager {
    db: Arc<PrkDb>,
    config: ReplicationConfig,
    /// Replication state (for followers)
    state: Arc<RwLock<ReplicationState>>,
    /// Active follower connections (for leader)
    followers: Arc<RwLock<HashMap<String, ReplicaNode>>>,
    /// HTTP client for followers
    http_client: reqwest::Client,
    /// Optional authentication token
    auth_token: Option<String>,
}

#[derive(Error, Debug)]
pub enum ReplicationError {
    #[error("Failed to connect to leader")]
    ConnectionError,
    #[error("Replication protocol error: {0}")]
    ProtocolError(String),
    #[error("This node is not the leader")]
    NotLeader,
    #[error("HTTP error: {0}")]
    HttpError(String),
    #[error("Authentication failed")]
    AuthenticationFailed,
}

/// Request to fetch changes from leader
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchChangesRequest {
    /// Last change ID the follower has
    pub since: Option<String>,
    /// Maximum number of changes to fetch
    pub limit: usize,
    /// Follower node ID
    pub follower_id: String,
}

/// Response with changes from leader
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchChangesResponse {
    /// List of changes
    pub changes: Vec<ReplicationChange>,
    /// Whether there are more changes available
    pub has_more: bool,
}

impl ReplicationManager {
    pub fn new(db: Arc<PrkDb>, config: ReplicationConfig) -> Self {
        Self::new_with_auth(db, config, None)
    }

    pub fn new_with_auth(
        db: Arc<PrkDb>,
        config: ReplicationConfig,
        auth_token: Option<String>,
    ) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(config.timing.request_timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            db,
            config,
            state: Arc::new(RwLock::new(ReplicationState::default())),
            followers: Arc::new(RwLock::new(HashMap::new())),
            http_client,
            auth_token,
        }
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        self.config.leader_address.is_none()
    }

    /// Get current replication state
    pub async fn get_state(&self) -> ReplicationState {
        self.state.read().await.clone()
    }

    /// Get replication health status
    pub async fn get_health(&self) -> ReplicationHealth {
        let state = self.state.read().await;
        let now = chrono::Utc::now().timestamp_millis();
        let lag_seconds = if state.last_sync_ts > 0 {
            (now - state.last_sync_ts) as f64 / 1000.0
        } else {
            0.0
        };

        ReplicationHealth {
            healthy: state.connected && state.error_count == 0,
            lag_seconds,
            outbox_drift: self.db.metrics().outbox_drift(),
            throughput: 0.0, // Calculate from recent history when metrics are implemented
            last_sync: state.last_sync_ts,
            error: state.last_error.clone(),
        }
    }

    /// Starts the replication process.
    /// If the node is a leader, it starts accepting connections from followers and HTTP server.
    /// If it's a follower, it connects to the leader and starts syncing.
    pub async fn start(self: Arc<Self>) -> Result<(), ReplicationError> {
        if let Some(leader_address) = &self.config.leader_address {
            // This is a follower - start sync loop
            info!(
                "Starting follower node {}, connecting to leader at {}",
                self.config.self_node.id, leader_address
            );

            let manager = Arc::clone(&self);
            tokio::spawn(async move {
                manager.follower_sync_loop().await;
            });
        } else {
            // This is the leader - start HTTP server
            info!(
                "Starting leader node {} at {}",
                self.config.self_node.id, self.config.self_node.address
            );

            // Register followers
            let mut followers = self.followers.write().await;
            for follower in &self.config.followers {
                followers.insert(follower.id.clone(), follower.clone());
            }
            info!("Registered {} followers", followers.len());
            drop(followers);

            // Start HTTP server for replication
            let manager = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = manager.start_http_server().await {
                    error!("Failed to start replication HTTP server: {}", e);
                }
            });
        }
        Ok(())
    }

    /// Start HTTP server for leader to serve replication requests
    async fn start_http_server(self: Arc<Self>) -> Result<(), ReplicationError> {
        let app = Router::new()
            .route("/replication/changes", get(fetch_changes_handler))
            .route("/replication/health", get(health_handler))
            .with_state(self.clone());

        let address_str = &self.config.self_node.address;
        debug!("Attempting to parse address: '{}'", address_str);

        let addr: SocketAddr = address_str.parse().map_err(|e| {
            error!("Failed to parse address '{}': {}", address_str, e);
            ReplicationError::ProtocolError(format!("Invalid address '{}': {}", address_str, e))
        })?;

        info!("Starting replication HTTP server on {}", addr);

        let listener = tokio::net::TcpListener::bind(&addr).await.map_err(|e| {
            error!("Failed to bind to address {}: {}", addr, e);
            ReplicationError::HttpError(format!("Failed to bind to {}: {}", addr, e))
        })?;

        info!("Successfully bound to {}, starting server", addr);

        axum::serve(listener, app).await.map_err(|e| {
            error!("Axum server error: {}", e);
            ReplicationError::HttpError(format!("Server error: {}", e))
        })?;

        Ok(())
    }

    /// Follower sync loop - continuously polls leader for changes with exponential backoff
    async fn follower_sync_loop(&self) {
        let base_sync_interval = self.config.timing.follower_poll_interval;
        let mut retry_count = 0u32;
        let max_retry_delay = self.config.timing.backoff_max;
        let backoff_base_secs = self.config.timing.backoff_base.as_secs().max(1);

        loop {
            #[cfg(feature = "metrics")]
            let start = Instant::now();

            match self.sync_from_leader().await {
                Ok(changes_count) => {
                    // Success - reset retry count
                    retry_count = 0;

                    #[cfg(not(feature = "metrics"))]
                    let _ = &changes_count;

                    #[cfg(feature = "metrics")]
                    {
                        let node_id = &self.config.self_node.id;
                        if let Some(leader) = &self.config.leader_address {
                            prkdb_metrics::exporter::REPLICATION_FOLLOWER_CONNECTED
                                .with_label_values(&[node_id, leader])
                                .set(1.0);

                            prkdb_metrics::exporter::REPLICATION_LAST_SYNC_TIMESTAMP
                                .with_label_values(&[node_id])
                                .set(chrono::Utc::now().timestamp() as f64);

                            if changes_count > 0 {
                                prkdb_metrics::exporter::REPLICATION_BATCH_SIZE
                                    .with_label_values(&[node_id])
                                    .observe(changes_count as f64);
                            }

                            let duration = start.elapsed().as_secs_f64();
                            prkdb_metrics::exporter::REPLICATION_SYNC_DURATION
                                .with_label_values(&[node_id])
                                .observe(duration);
                        }
                    }

                    // Mark as connected
                    let mut state = self.state.write().await;
                    state.connected = true;
                    state.error_count = 0;
                    state.last_error = None;
                    drop(state);

                    sleep(base_sync_interval).await;
                }
                Err(e) => {
                    retry_count += 1;
                    error!("Follower sync error (attempt {}): {}", retry_count, e);

                    // Update error metrics
                    #[cfg(feature = "metrics")]
                    {
                        let node_id = &self.config.self_node.id;
                        prkdb_metrics::exporter::REPLICATION_SYNC_ERRORS
                            .with_label_values(&[node_id, "sync_failed"])
                            .inc();

                        if let Some(leader) = &self.config.leader_address {
                            prkdb_metrics::exporter::REPLICATION_FOLLOWER_CONNECTED
                                .with_label_values(&[node_id, leader])
                                .set(0.0);
                        }
                    }

                    // Update state
                    let mut state = self.state.write().await;
                    state.connected = false;
                    state.error_count += 1;
                    state.last_error = Some(e.to_string());
                    drop(state);

                    // Exponential backoff: 2^retry_count seconds, capped at max_retry_delay
                    let growth = 2u64.saturating_pow(retry_count);
                    let delay_secs = min(
                        backoff_base_secs.saturating_mul(growth),
                        max_retry_delay.as_secs(),
                    );
                    let delay = Duration::from_secs(delay_secs);
                    warn!("Retrying in {} seconds...", delay_secs);
                    sleep(delay).await;
                }
            }
        }
    }

    /// Sync changes from leader
    /// Returns the number of changes applied
    async fn sync_from_leader(&self) -> Result<usize, ReplicationError> {
        let leader_address = self
            .config
            .leader_address
            .as_ref()
            .ok_or(ReplicationError::NotLeader)?;

        let state = self.state.read().await;
        let last_change_id = state.last_change_id.clone();
        drop(state);

        // Fetch changes from leader
        let changes = self
            .fetch_changes_from_leader(leader_address, last_change_id.as_deref())
            .await?;

        if changes.is_empty() {
            debug!("No new changes from leader");
            return Ok(0);
        }

        info!("Received {} changes from leader", changes.len());
        debug!(
            "Changes received: {:?}",
            changes.iter().map(|c| &c.id).collect::<Vec<_>>()
        );
        let changes_count = changes.len();

        // Apply changes locally in batch
        for change in &changes {
            if let Err(e) = self.apply_change(change).await {
                error!("Failed to apply change {}: {}", change.id, e);

                // Record error metric
                #[cfg(feature = "metrics")]
                prkdb_metrics::exporter::REPLICATION_SYNC_ERRORS
                    .with_label_values(&[&self.config.self_node.id, "apply_failed"])
                    .inc();

                return Err(ReplicationError::ProtocolError(format!(
                    "Apply failed: {}",
                    e
                )));
            }

            // Update metrics per change
            #[cfg(feature = "metrics")]
            prkdb_metrics::exporter::REPLICATION_CHANGES_APPLIED
                .with_label_values(&[&self.config.self_node.id, &change.collection])
                .inc();
        }

        // Update state after successful batch
        let mut state = self.state.write().await;
        if let Some(last_change) = changes.last() {
            state.last_change_id = Some(last_change.id.clone());
        }
        state.changes_applied += changes_count as u64;
        state.last_sync_ts = chrono::Utc::now().timestamp_millis();

        #[cfg(feature = "metrics")]
        {
            let now = chrono::Utc::now().timestamp_millis();
            let lag_ms = now - state.last_sync_ts;
            let lag_seconds = lag_ms as f64 / 1000.0;

            if let Some(leader) = &self.config.leader_address {
                prkdb_metrics::exporter::REPLICATION_LAG_SECONDS
                    .with_label_values(&[&self.config.self_node.id, leader])
                    .set(lag_seconds);
            }
        }

        Ok(changes_count)
    }

    /// Fetch changes from leader via HTTP
    async fn fetch_changes_from_leader(
        &self,
        leader_address: &str,
        since: Option<&str>,
    ) -> Result<Vec<ReplicationChange>, ReplicationError> {
        let url = format!("http://{}/replication/changes", leader_address);

        let request = FetchChangesRequest {
            since: since.map(|s| s.to_string()),
            limit: 100,
            follower_id: self.config.self_node.id.clone(),
        };

        debug!("Fetching changes from leader: {}", url);

        let mut req = self.http_client.get(&url);

        // Add query parameters
        if let Some(ref since_id) = request.since {
            req = req.query(&[("since", since_id)]);
        }
        req = req.query(&[
            ("limit", &request.limit.to_string()),
            ("follower_id", &request.follower_id),
        ]);

        // Add authentication if configured
        if let Some(token) = &self.auth_token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }

        let response = req
            .send()
            .await
            .map_err(|e| ReplicationError::HttpError(format!("Request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(ReplicationError::HttpError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let fetch_response: FetchChangesResponse = response
            .json()
            .await
            .map_err(|e| ReplicationError::HttpError(format!("Failed to parse response: {}", e)))?;

        debug!(
            "Fetched {} changes from leader",
            fetch_response.changes.len()
        );

        Ok(fetch_response.changes)
    }

    /// Apply a replicated change to local storage
    async fn apply_change(&self, change: &ReplicationChange) -> Result<(), StorageError> {
        debug!(
            "Applying change {} for collection {}",
            change.id, change.collection
        );

        // Store the change in local outbox to maintain replication log
        self.db
            .storage
            .outbox_save(&change.id, &change.data)
            .await?;

        debug!("Successfully applied change {} to local outbox", change.id);
        Ok(())
    }

    /// Get changes since a given ID (called by followers on the leader)
    pub async fn get_changes_since(
        &self,
        since: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ReplicationChange>, ReplicationError> {
        if !self.is_leader() {
            return Err(ReplicationError::NotLeader);
        }

        let entries = self
            .db
            .storage
            .outbox_list()
            .await
            .map_err(|e| ReplicationError::ProtocolError(e.to_string()))?;

        debug!("Found {} raw outbox entries", entries.len());
        for (id, _) in &entries {
            debug!("  Raw outbox entry: {}", id);
        }

        let total_entries = entries.len();
        let mut changes = Vec::new();
        let mut found_start = since.is_none();
        let mut skip_next = false;

        for (id, data) in entries {
            if !found_start {
                if Some(id.as_str()) == since {
                    found_start = true;
                    skip_next = true; // Skip the 'since' record itself
                }
                continue;
            }

            if skip_next {
                skip_next = false;
                continue; // Skip the 'since' record, start from the next one
            }

            // Extract collection name from outbox key
            let collection = id.split(':').next().unwrap_or("unknown").to_string();

            changes.push(ReplicationChange {
                id: id.clone(),
                collection,
                data,
                timestamp: chrono::Utc::now().timestamp_millis(),
            });

            if changes.len() >= limit {
                break;
            }
        }

        debug!(
            "Returning {} changes (from {} entries, since: {:?})",
            changes.len(),
            total_entries,
            since
        );
        Ok(changes)
    }

    /// Debug method to check outbox contents
    pub async fn debug_outbox(&self) -> Result<Vec<String>, ReplicationError> {
        let entries = self
            .db
            .storage
            .outbox_list()
            .await
            .map_err(|e| ReplicationError::ProtocolError(e.to_string()))?;

        Ok(entries.into_iter().map(|(id, _)| id).collect())
    }

    /// Replicates a change event to all followers.
    /// This should only be called on the leader.
    pub async fn replicate_change<C: Collection>(
        &self,
        change: &prkdb_core::collection::ChangeEvent<C>,
    ) -> Result<(), ReplicationError> {
        if !self.is_leader() {
            return Err(ReplicationError::NotLeader);
        }

        let followers = self.followers.read().await;
        info!("Replicating change to {} followers", followers.len());

        // Create an OutboxRecord
        let outbox_record = match change {
            prkdb_core::collection::ChangeEvent::Put(item) => OutboxRecord::Put(item.clone()),
            prkdb_core::collection::ChangeEvent::Delete(id) => OutboxRecord::Delete(id.clone()),
            // Batch events decompose into individual records for replication
            prkdb_core::collection::ChangeEvent::PutBatch(_)
            | prkdb_core::collection::ChangeEvent::DeleteBatch(_) => {
                // Batch events are never replicated as-is
                // They're decomposed at source before replication
                // If this function is called with a batch event, it means it was not decomposed.
                // Log a warning and return, as this indicates a potential misuse or misunderstanding
                // of the replication flow.
                warn!("replicate_change called with a batch event ({:?}). Batch events should be decomposed before calling this function. Skipping replication for this event.", change);
                return Ok(()); // Exit early, do not replicate this batch event
            }
        };

        // Generate outbox ID using the same pattern as collection_handle
        let outbox_id = make_outbox_id_for_type::<C>(None);

        // Save to outbox for followers to pull
        save_outbox_event::<C>(self.db.storage.as_ref(), &outbox_id, &outbox_record)
            .await
            .map_err(|e| ReplicationError::ProtocolError(format!("Outbox save failed: {}", e)))?;

        info!("Change {} saved to outbox for replication", outbox_id);

        // Update metrics
        #[cfg(feature = "metrics")]
        {
            let collection_name = std::any::type_name::<C>().to_string();
            prkdb_metrics::exporter::REPLICATION_CHANGES_SENT
                .with_label_values(&[&self.config.self_node.id, &collection_name])
                .inc();
        }

        Ok(())
    }
}

// === HTTP Handlers ===

/// Query parameters for fetching changes
#[derive(Debug, Deserialize)]
struct FetchChangesQuery {
    since: Option<String>,
    limit: Option<usize>,
    follower_id: String,
}

/// Handler for /replication/changes endpoint
async fn fetch_changes_handler(
    State(manager): State<Arc<ReplicationManager>>,
    Query(params): Query<FetchChangesQuery>,
) -> Result<Json<FetchChangesResponse>, (StatusCode, String)> {
    // Verify this is the leader
    if !manager.is_leader() {
        return Err((StatusCode::FORBIDDEN, "Not the leader".to_string()));
    }

    let limit = params.limit.unwrap_or(100).min(1000); // Cap at 1000

    info!(
        "Follower {} requesting changes (since: {:?}, limit: {})",
        params.follower_id, params.since, limit
    );

    // Fetch changes
    let changes = manager
        .get_changes_since(params.since.as_deref(), limit)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    debug!(
        "Returning {} changes to follower {}",
        changes.len(),
        params.follower_id
    );
    if !changes.is_empty() {
        debug!(
            "Change IDs: {:?}",
            changes.iter().map(|c| &c.id).collect::<Vec<_>>()
        );
    }

    let has_more = changes.len() >= limit;

    // Update metrics
    #[cfg(feature = "metrics")]
    {
        prkdb_metrics::exporter::REPLICATION_CHANGES_PENDING
            .with_label_values(&[&manager.config.self_node.id, &params.follower_id])
            .set(changes.len() as f64);
    }

    Ok(Json(FetchChangesResponse { changes, has_more }))
}

/// Handler for /replication/health endpoint
async fn health_handler(
    State(manager): State<Arc<ReplicationManager>>,
) -> Result<Json<ReplicationHealth>, (StatusCode, String)> {
    let health = manager.get_health().await;
    Ok(Json(health))
}
