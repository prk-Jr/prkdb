use crate::db::PrkDb;
use crate::raft::rpc::prk_db_service_server::{
    PrkDbService as PrkDbServiceTrait, PrkDbServiceServer,
};
use crate::raft::rpc::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, HealthRequest, HealthResponse,
    PutRequest, PutResponse, WatchEvent, WatchRequest,
};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use tonic::{Request, Response, Status};

// Phase 19: Type alias for watch broadcast channel
pub type WatchBroadcast = broadcast::Sender<WatchEvent>;

/// gRPC service implementation for client data operations
/// This is the binary protocol equivalent to Kafka's producer/consumer API
pub struct PrkDbGrpcService {
    db: Arc<PrkDb>,
    admin_token: String,
}

impl PrkDbGrpcService {
    pub fn new(db: Arc<PrkDb>, admin_token: String) -> Self {
        Self { db, admin_token }
    }

    pub fn into_server(self) -> PrkDbServiceServer<Self> {
        PrkDbServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl PrkDbServiceTrait for PrkDbGrpcService {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();

        match self.db.put(&req.key, &req.value).await {
            Ok(_) => {
                // Determine which partition this went to
                let partition = if let Some(pm) = &self.db.partition_manager {
                    pm.get_partition_for_key(&req.key)
                } else {
                    0
                };

                Ok(Response::new(PutResponse {
                    success: true,
                    partition,
                }))
            }
            Err(e) => Err(Status::internal(format!("Put failed: {}", e))),
        }
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();

        // Handle different read modes
        use crate::raft::rpc::ReadMode;
        let read_mode = req.read_mode();

        let result = match read_mode {
            ReadMode::Stale => {
                // Direct local read - fastest but may be stale
                self.db.get_local(&req.key).await
            }
            ReadMode::Follower => {
                // Follower read: get ReadIndex from leader, wait, then local read
                match self.db.get_follower_read(&req.key).await {
                    Ok(v) => Ok(v),
                    Err(e) => {
                        // Fall back to leader read if follower read fails
                        tracing::debug!("Follower read failed, falling back to leader: {}", e);
                        self.db.get(&req.key).await
                    }
                }
            }
            ReadMode::Linearizable => {
                // Default: linearizable read from leader
                self.db.get(&req.key).await
            }
        };

        match result {
            Ok(Some(value)) => Ok(Response::new(GetResponse {
                success: true,
                value,
                found: true,
            })),
            Ok(None) => Ok(Response::new(GetResponse {
                success: true,
                value: vec![],
                found: false,
            })),
            Err(e) => Err(Status::internal(format!("Get failed: {}", e))),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let key = &request.into_inner().key;

        self.db
            .delete(key)
            .await
            .map_err(|e| Status::internal(format!("Delete failed: {}", e)))?;

        Ok(Response::new(DeleteResponse { success: true }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let (num_partitions, leaders_ready) = if let Some(pm) = &self.db.partition_manager {
            let num = pm.num_partitions as u32;
            // For now, assume all are ready
            (num, num)
        } else {
            (1, 1)
        };

        Ok(Response::new(HealthResponse {
            healthy: true,
            num_partitions,
            leaders_ready,
        }))
    }

    async fn metadata(
        &self,
        _request: Request<crate::raft::rpc::MetadataRequest>,
    ) -> Result<Response<crate::raft::rpc::MetadataResponse>, Status> {
        tracing::info!("Received Metadata request");
        use crate::raft::rpc::{NodeInfo, PartitionInfo};

        // Pre-allocate with reasonable default for typical partition counts
        let mut partitions = Vec::with_capacity(16);
        let mut nodes = Vec::with_capacity(16);
        let mut seen_nodes = std::collections::HashSet::new();

        if let Some(pm) = &self.db.partition_manager {
            let topology = pm.get_topology().await;

            for (partition_id, leader_id, replicas) in topology {
                partitions.push(PartitionInfo {
                    partition_id,
                    leader_id: leader_id.unwrap_or(0), // 0 means no leader
                    replicas: replicas.clone(),
                });

                // Collect unique nodes
                for node_id in replicas {
                    if seen_nodes.insert(node_id) {
                        // In a real system, we'd look up the address from the cluster config
                        // For this prototype, we'll construct it based on convention
                        // Node 1 -> 127.0.0.1:8081, Node 2 -> 127.0.0.1:8082, etc.
                        // This is a hack for the demo, but sufficient for the benchmark
                        let port = 8080 + node_id as u32;
                        let address = format!("http://127.0.0.1:{}", port);

                        nodes.push(NodeInfo { node_id, address });
                    }
                }
            }
        } else {
            // Single node mode / No partition manager
            // Return a default node so clients can connect
            nodes.push(NodeInfo {
                node_id: 1,
                address: "http://127.0.0.1:50051".to_string(), // Default gRPC port
            });
            // We can also return a default partition 0 that this node leads
            partitions.push(PartitionInfo {
                partition_id: 0,
                leader_id: 1,
                replicas: vec![1],
            });
        }

        Ok(Response::new(crate::raft::rpc::MetadataResponse {
            nodes,
            partitions,
        }))
    }

    async fn batch_put(
        &self,
        request: Request<crate::raft::rpc::BatchPutRequest>,
    ) -> Result<Response<crate::raft::rpc::BatchPutResponse>, Status> {
        let req = request.into_inner();

        let mut successful_count = 0;
        let mut failed_count = 0;
        // Optimization: Pre-size for batch count
        let mut errors = Vec::with_capacity(req.pairs.len());

        // Sequential processing is fastest - Raft serializes anyway
        for pair in req.pairs {
            match self.db.put(&pair.key, &pair.value).await {
                Ok(_) => successful_count += 1,
                Err(e) => {
                    failed_count += 1;
                    errors.push(format!("Put failed: {}", e));
                }
            }
        }

        Ok(Response::new(crate::raft::rpc::BatchPutResponse {
            successful_count,
            failed_count,
            errors,
        }))
    }
    async fn create_collection(
        &self,
        request: Request<crate::raft::rpc::CreateCollectionRequest>,
    ) -> Result<Response<crate::raft::rpc::CreateCollectionResponse>, Status> {
        let req = request.into_inner();

        // Security check
        self.validate_admin_token(&req.admin_token)?;

        tracing::info!("Admin: CreateCollection '{}'", req.name);

        // Delegate to PrkDb, which handles distributed proposal (via Raft to Partition 0)
        // or local execution depending on configuration.

        let result = self.db.create_collection(&req.name).await;

        match result {
            Ok(_) => Ok(Response::new(crate::raft::rpc::CreateCollectionResponse {
                success: true,
                error: "".to_string(),
            })),
            Err(e) => Ok(Response::new(crate::raft::rpc::CreateCollectionResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn list_collections(
        &self,
        request: Request<crate::raft::rpc::ListCollectionsRequest>,
    ) -> Result<Response<crate::raft::rpc::ListCollectionsResponse>, Status> {
        let req = request.into_inner();
        self.validate_admin_token(&req.admin_token)?;

        let collections = self
            .db
            .list_collections()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(crate::raft::rpc::ListCollectionsResponse {
            success: true,
            collections,
            error: "".to_string(),
        }))
    }

    async fn drop_collection(
        &self,
        request: Request<crate::raft::rpc::DropCollectionRequest>,
    ) -> Result<Response<crate::raft::rpc::DropCollectionResponse>, Status> {
        let req = request.into_inner();
        self.validate_admin_token(&req.admin_token)?;

        tracing::info!("Admin: DropCollection '{}'", req.name);

        match self.db.drop_collection(&req.name).await {
            Ok(_) => Ok(Response::new(crate::raft::rpc::DropCollectionResponse {
                success: true,
                error: "".to_string(),
            })),
            Err(e) => Ok(Response::new(crate::raft::rpc::DropCollectionResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn list_consumer_groups(
        &self,
        request: Request<crate::raft::rpc::ListConsumerGroupsRequest>,
    ) -> Result<Response<crate::raft::rpc::ListConsumerGroupsResponse>, Status> {
        let req = request.into_inner();
        self.validate_admin_token(&req.admin_token)?;

        let group_ids = self
            .db
            .list_consumer_groups()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut groups = Vec::new();
        for group_id in group_ids {
            let active_consumers = self.db.get_active_consumers(&group_id);
            let members = active_consumers.len() as u32;
            let state = if members > 0 { "Stable" } else { "Empty" };
            let lag = self
                .db
                .get_group_lag_info(&group_id)
                .await
                .map(|infos| infos.iter().map(|(_, _, _, _, l)| l).sum())
                .unwrap_or(0);

            groups.push(crate::raft::rpc::ConsumerGroupSummary {
                group_id,
                members,
                state: state.to_string(),
                lag,
                assignment_strategy: "Range".to_string(),
            });
        }

        Ok(Response::new(
            crate::raft::rpc::ListConsumerGroupsResponse {
                success: true,
                groups,
                error: "".to_string(),
            },
        ))
    }

    async fn describe_consumer_group(
        &self,
        request: Request<crate::raft::rpc::DescribeConsumerGroupRequest>,
    ) -> Result<Response<crate::raft::rpc::DescribeConsumerGroupResponse>, Status> {
        let req = request.into_inner();
        self.validate_admin_token(&req.admin_token)?;

        let group_id = req.group_id;

        // Check if group exists
        let group_ids = self
            .db
            .list_consumer_groups()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        if !group_ids.contains(&group_id) {
            return Ok(Response::new(
                crate::raft::rpc::DescribeConsumerGroupResponse {
                    success: false,
                    group_id: group_id.clone(),
                    state: "".to_string(),
                    members: vec![],
                    partitions: vec![],
                    total_lag: 0,
                    error: format!("Consumer group '{}' not found", group_id),
                },
            ));
        }

        let active_consumer_ids = self.db.get_active_consumers(&group_id);
        let assignment = self.db.get_consumer_group_assignment(&group_id);

        let members: Vec<crate::raft::rpc::ConsumerMemberInfo> = active_consumer_ids
            .into_iter()
            .map(|consumer_id| {
                let partitions = assignment
                    .as_ref()
                    .map(|a| a.get_partitions(&consumer_id))
                    .unwrap_or_default();
                crate::raft::rpc::ConsumerMemberInfo {
                    consumer_id,
                    host: "unknown".to_string(),
                    partitions,
                }
            })
            .collect();

        let is_empty = members.is_empty();
        let state = if is_empty { "Empty" } else { "Stable" };

        let lag_infos = self
            .db
            .get_group_lag_info(&group_id)
            .await
            .unwrap_or_default();

        let partitions: Vec<crate::raft::rpc::PartitionLagInfo> = lag_infos
            .iter()
            .map(
                |(collection, partition, current_offset, latest_offset, lag)| {
                    crate::raft::rpc::PartitionLagInfo {
                        collection: collection.clone(),
                        partition: *partition,
                        current_offset: *current_offset,
                        latest_offset: *latest_offset,
                        lag: *lag,
                    }
                },
            )
            .collect();

        let total_lag: u64 = lag_infos.iter().map(|(_, _, _, _, lag)| lag).sum();

        Ok(Response::new(
            crate::raft::rpc::DescribeConsumerGroupResponse {
                success: true,
                group_id,
                state: state.to_string(),
                members,
                partitions,
                total_lag,
                error: "".to_string(),
            },
        ))
    }

    async fn list_partitions(
        &self,
        request: Request<crate::raft::rpc::ListPartitionsRequest>,
    ) -> Result<Response<crate::raft::rpc::ListPartitionsResponse>, Status> {
        let req = request.into_inner();
        self.validate_admin_token(&req.admin_token)?;

        let collection_filter = if req.collection.is_empty() {
            None
        } else {
            Some(req.collection.as_str())
        };

        let collections = self
            .db
            .list_collections()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let target_collections: Vec<String> = match collection_filter {
            Some(name) => {
                if collections.contains(&name.to_string()) {
                    vec![name.to_string()]
                } else {
                    vec![]
                }
            }
            None => collections,
        };

        let mut partitions = Vec::new();
        for collection_name in target_collections {
            if let Ok(partition_data) = self.db.get_partitions(&collection_name).await {
                for (partition, items, size_bytes) in partition_data {
                    partitions.push(crate::raft::rpc::PartitionSummary {
                        collection: collection_name.clone(),
                        partition,
                        size_bytes,
                        items,
                        assigned_to: "default-consumer".to_string(),
                        status: "active".to_string(),
                    });
                }
            }
        }

        Ok(Response::new(crate::raft::rpc::ListPartitionsResponse {
            success: true,
            partitions,
            error: "".to_string(),
        }))
    }

    async fn get_partition_assignments(
        &self,
        request: Request<crate::raft::rpc::GetPartitionAssignmentsRequest>,
    ) -> Result<Response<crate::raft::rpc::GetPartitionAssignmentsResponse>, Status> {
        let req = request.into_inner();
        self.validate_admin_token(&req.admin_token)?;

        let group_filter = if req.group_id.is_empty() {
            None
        } else {
            Some(req.group_id.as_str())
        };

        let assignment_data = self
            .db
            .get_partition_assignments(group_filter)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let assignments: Vec<crate::raft::rpc::PartitionAssignmentSummary> = assignment_data
            .into_iter()
            .map(
                |(group_id, consumer_id, collection, partition, current_offset, lag)| {
                    crate::raft::rpc::PartitionAssignmentSummary {
                        group_id,
                        consumer_id,
                        collection,
                        partition,
                        current_offset,
                        lag,
                    }
                },
            )
            .collect();

        Ok(Response::new(
            crate::raft::rpc::GetPartitionAssignmentsResponse {
                success: true,
                assignments,
                error: "".to_string(),
            },
        ))
    }

    async fn get_replication_status(
        &self,
        request: Request<crate::raft::rpc::GetReplicationStatusRequest>,
    ) -> Result<Response<crate::raft::rpc::GetReplicationStatusResponse>, Status> {
        let req = request.into_inner();
        self.validate_admin_token(&req.admin_token)?;

        let (
            node_id,
            role,
            leader_address,
            followers,
            state,
            last_sync,
            total_changes,
            changes_applied,
        ) = self
            .db
            .get_replication_status()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(
            crate::raft::rpc::GetReplicationStatusResponse {
                success: true,
                node_id,
                role,
                leader_address: leader_address.unwrap_or_default(),
                followers,
                state,
                last_sync,
                total_changes,
                changes_applied,
                error: "".to_string(),
            },
        ))
    }

    async fn get_replication_nodes(
        &self,
        request: Request<crate::raft::rpc::GetReplicationNodesRequest>,
    ) -> Result<Response<crate::raft::rpc::GetReplicationNodesResponse>, Status> {
        let req = request.into_inner();
        self.validate_admin_token(&req.admin_token)?;

        let node_data = self
            .db
            .get_replication_nodes()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let nodes: Vec<crate::raft::rpc::ReplicationNodeInfo> = node_data
            .into_iter()
            .map(|(node_id, address, role, status, lag_ms, last_seen)| {
                crate::raft::rpc::ReplicationNodeInfo {
                    node_id,
                    address,
                    role,
                    status,
                    lag_ms,
                    last_seen,
                }
            })
            .collect();

        Ok(Response::new(
            crate::raft::rpc::GetReplicationNodesResponse {
                success: true,
                nodes,
                error: "".to_string(),
            },
        ))
    }

    async fn get_replication_lag(
        &self,
        request: Request<crate::raft::rpc::GetReplicationLagRequest>,
    ) -> Result<Response<crate::raft::rpc::GetReplicationLagResponse>, Status> {
        let req = request.into_inner();
        self.validate_admin_token(&req.admin_token)?;

        let lag_data = self
            .db
            .get_replication_lag()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let lags: Vec<crate::raft::rpc::ReplicationLagInfo> = lag_data
            .into_iter()
            .map(
                |(follower_node, leader_offset, follower_offset, lag_records, lag_ms, status)| {
                    crate::raft::rpc::ReplicationLagInfo {
                        follower_node,
                        leader_offset,
                        follower_offset,
                        lag_records,
                        lag_ms,
                        status,
                    }
                },
            )
            .collect();

        Ok(Response::new(crate::raft::rpc::GetReplicationLagResponse {
            success: true,
            lags,
            error: "".to_string(),
        }))
    }

    async fn reset_consumer_offset(
        &self,
        request: Request<crate::raft::rpc::ResetConsumerOffsetRequest>,
    ) -> Result<Response<crate::raft::rpc::ResetConsumerOffsetResponse>, Status> {
        let req = request.into_inner();
        self.validate_admin_token(&req.admin_token)?;

        tracing::info!(
            "Admin: ResetConsumerOffset group='{}' collection='{}'",
            req.group_id,
            req.collection
        );

        // Determine target offset
        let target_offset = match req.target {
            Some(crate::raft::rpc::reset_consumer_offset_request::Target::Offset(o)) => Some(o),
            Some(crate::raft::rpc::reset_consumer_offset_request::Target::Earliest(true)) => {
                // Get earliest offset (0 for simplicity, or query storage)
                Some(0)
            }
            Some(crate::raft::rpc::reset_consumer_offset_request::Target::Latest(true)) => {
                // Get latest offset from storage
                None // Will be resolved by PrkDb
            }
            _ => None,
        };

        let collection_filter = if req.collection.is_empty() {
            None
        } else {
            Some(req.collection.as_str())
        };

        match self
            .db
            .reset_consumer_offset(&req.group_id, collection_filter, target_offset)
            .await
        {
            Ok(partitions_reset) => Ok(Response::new(
                crate::raft::rpc::ResetConsumerOffsetResponse {
                    success: true,
                    partitions_reset,
                    error: "".to_string(),
                },
            )),
            Err(e) => Ok(Response::new(
                crate::raft::rpc::ResetConsumerOffsetResponse {
                    success: false,
                    partitions_reset: 0,
                    error: e.to_string(),
                },
            )),
        }
    }

    async fn start_replication(
        &self,
        request: Request<crate::raft::rpc::StartReplicationRequest>,
    ) -> Result<Response<crate::raft::rpc::StartReplicationResponse>, Status> {
        let req = request.into_inner();
        self.validate_admin_token(&req.admin_token)?;

        tracing::info!("Admin: StartReplication target='{}'", req.target_address);

        match self.db.start_replication(&req.target_address).await {
            Ok(node_id) => Ok(Response::new(crate::raft::rpc::StartReplicationResponse {
                success: true,
                node_id,
                error: "".to_string(),
            })),
            Err(e) => Ok(Response::new(crate::raft::rpc::StartReplicationResponse {
                success: false,
                node_id: "".to_string(),
                error: e.to_string(),
            })),
        }
    }

    async fn stop_replication(
        &self,
        request: Request<crate::raft::rpc::StopReplicationRequest>,
    ) -> Result<Response<crate::raft::rpc::StopReplicationResponse>, Status> {
        let req = request.into_inner();
        self.validate_admin_token(&req.admin_token)?;

        tracing::info!("Admin: StopReplication target='{}'", req.target_address);

        match self.db.stop_replication(&req.target_address).await {
            Ok(_) => Ok(Response::new(crate::raft::rpc::StopReplicationResponse {
                success: true,
                error: "".to_string(),
            })),
            Err(e) => Ok(Response::new(crate::raft::rpc::StopReplicationResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 19: Watch/Subscribe API
    // ─────────────────────────────────────────────────────────────────────────

    type WatchStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<WatchEvent, Status>> + Send>>;

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let req = request.into_inner();
        let prefix = req.key_prefix;

        tracing::info!("Watch: Client subscribed with prefix len={}", prefix.len());

        // Create a broadcast channel for this subscription
        // In a real implementation, we'd subscribe to a global event bus
        let (tx, rx) = broadcast::channel::<WatchEvent>(1024);

        // Store the sender somewhere accessible to put/delete operations
        // For now, we'll just return an empty stream as a placeholder
        // TODO: Integrate with actual write path to publish events

        let _ = tx; // Suppress unused warning

        let stream = BroadcastStream::new(rx).filter_map(move |result| {
            match result {
                Ok(event) => {
                    // Filter by prefix
                    if prefix.is_empty() || event.key.starts_with(&prefix) {
                        Some(Ok(event))
                    } else {
                        None
                    }
                }
                Err(_) => None, // Channel lagged, skip
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }
}

impl PrkDbGrpcService {
    #[allow(clippy::result_large_err)]
    fn validate_admin_token(&self, token: &str) -> Result<(), Status> {
        // Debug trace for token validation
        tracing::debug!(
            "validate_admin_token: received='{}', expected='{}'",
            token,
            self.admin_token
        );

        if self.admin_token.is_empty() {
            // If no token configured on server, deny all admin ops
            return Err(Status::unauthenticated(
                "Server has no admin token configured",
            ));
        }

        if token != self.admin_token {
            return Err(Status::unauthenticated("Invalid admin token"));
        }

        Ok(())
    }
}
