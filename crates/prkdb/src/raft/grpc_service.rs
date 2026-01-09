use crate::db::PrkDb;
use crate::raft::rpc::prk_db_service_server::{
    PrkDbService as PrkDbServiceTrait, PrkDbServiceServer,
};
use crate::raft::rpc::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, HealthRequest, HealthResponse,
    PutRequest, PutResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// gRPC service implementation for client data operations
/// This is the binary protocol equivalent to Kafka's producer/consumer API
pub struct PrkDbGrpcService {
    db: Arc<PrkDb>,
}

impl PrkDbGrpcService {
    pub fn new(db: Arc<PrkDb>) -> Self {
        Self { db }
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
            ReadMode::Linearizable | _ => {
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
}
