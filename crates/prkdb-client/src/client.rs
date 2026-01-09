use prkdb_proto::raft::prk_db_service_client::PrkDbServiceClient;
use prkdb_proto::{
    BatchPutRequest, DeleteRequest, GetRequest, KvPair, MetadataRequest, PutRequest, ReadMode,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;

/// Read consistency level for get operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadConsistency {
    /// Read from leader (strongest consistency)
    #[default]
    Linearizable,
    /// Direct read from any replica (lowest latency, may be stale)
    Stale,
    /// ReadIndex from leader, then local read (strong consistency, lower leader load)
    Follower,
}

impl From<ReadConsistency> for i32 {
    fn from(val: ReadConsistency) -> Self {
        match val {
            ReadConsistency::Linearizable => ReadMode::Linearizable as i32,
            ReadConsistency::Stale => ReadMode::Stale as i32,
            ReadConsistency::Follower => ReadMode::Follower as i32,
        }
    }
}

/// Smart Client for PrkDB with automatic routing and failover
///
/// This client maintains a cache of cluster metadata and routes requests
/// to the appropriate partition leader automatically.
///
/// # Example
///
/// ```rust,ignore
/// use prkdb_client::PrkDbClient;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let client = PrkDbClient::new(vec![
///         "http://127.0.0.1:8081".to_string(),
///     ]).await?;
///     
///     client.put(b"key", b"value").await?;
///     let value = client.get(b"key").await?;
///     
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct PrkDbClient {
    /// Cached metadata
    metadata: Arc<RwLock<ClusterMetadata>>,

    /// Bootstrap servers for initial connection
    bootstrap_servers: Vec<String>,
}

#[derive(Clone, Default)]
struct ClusterMetadata {
    /// Node ID -> gRPC address
    nodes: HashMap<u64, String>,

    /// Partition ID -> Leader Node ID
    partition_leaders: HashMap<u64, u64>,

    /// Connection pool: Node ID -> gRPC Client
    clients: HashMap<u64, PrkDbServiceClient<Channel>>,

    /// Total number of partitions
    num_partitions: usize,
}

impl PrkDbClient {
    /// Create a new client with bootstrap servers
    ///
    /// The client will connect to one of the bootstrap servers to fetch
    /// initial cluster metadata, then cache the topology for future requests.
    ///
    /// # Arguments
    ///
    /// * `bootstrap_servers` - List of server addresses (e.g., "http://127.0.0.1:8081")
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let client = PrkDbClient::new(vec![
    ///     "http://127.0.0.1:8081".to_string(),
    ///     "http://127.0.0.1:8082".to_string(),
    ///     "http://127.0.0.1:8083".to_string(),
    /// ]).await?;
    /// ```
    pub async fn new(bootstrap_servers: Vec<String>) -> anyhow::Result<Self> {
        let client = Self {
            metadata: Arc::new(RwLock::new(ClusterMetadata::default())),
            bootstrap_servers,
        };

        // Fetch initial metadata
        client.refresh_metadata().await?;

        Ok(client)
    }

    /// Refresh cluster metadata from any available node
    async fn refresh_metadata(&self) -> anyhow::Result<()> {
        // Try each bootstrap server until one succeeds
        for server in &self.bootstrap_servers {
            match self.fetch_metadata_from(server).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::debug!("Failed to fetch metadata from {}: {}", server, e);
                    continue;
                }
            }
        }

        // If we have cached clients, try them too as fallbacks (cluster might have evolved)
        // TODO: Implement fallback logic using known nodes

        anyhow::bail!("Failed to fetch metadata from any bootstrap server")
    }

    /// Fetch metadata from a specific server
    async fn fetch_metadata_from(&self, address: &str) -> anyhow::Result<()> {
        let mut client = PrkDbServiceClient::connect(address.to_string()).await?;

        let request = tonic::Request::new(MetadataRequest { topics: vec![] });

        let response = client.metadata(request).await?;
        let metadata_response = response.into_inner();

        // Update cached metadata
        let mut metadata = self.metadata.write().await;

        // Clear only topological data, keep connections if possible?
        // For simplicity, we recreate. In prod, reuse channels.
        metadata.nodes.clear();
        metadata.partition_leaders.clear();
        metadata.clients.clear();

        // Update nodes
        for node_info in metadata_response.nodes {
            metadata
                .nodes
                .insert(node_info.node_id, node_info.address.clone());

            // Create gRPC client for this node
            if let Ok(client) = PrkDbServiceClient::connect(node_info.address.clone()).await {
                metadata.clients.insert(node_info.node_id, client);
            }
        }

        // Update partition leaders
        for partition_info in &metadata_response.partitions {
            if partition_info.leader_id > 0 {
                metadata
                    .partition_leaders
                    .insert(partition_info.partition_id, partition_info.leader_id);
            }
        }

        metadata.num_partitions = metadata_response.partitions.len();

        tracing::info!(
            "Refreshed metadata: {} nodes, {} partitions",
            metadata.nodes.len(),
            metadata.num_partitions
        );

        Ok(())
    }

    /// Put a key-value pair
    pub async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        const MAX_RETRIES: usize = 3;

        for attempt in 0..MAX_RETRIES {
            match self.put_internal(key, value).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::debug!("Put failed (attempt {}): {}", attempt + 1, e);

                    // Refresh metadata and retry
                    if attempt < MAX_RETRIES - 1 {
                        let _ = self.refresh_metadata().await;
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        anyhow::bail!("Put failed after {} retries", MAX_RETRIES)
    }

    /// Batch put multiple key-value pairs
    ///
    /// This method is highly efficient:
    /// 1. Groups keys by partition
    /// 2. Groups partitions by leader
    /// 3. Sends parallel requests to leaders
    pub async fn batch_put(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> anyhow::Result<()> {
        // Group entries by partition
        let mut by_partition: HashMap<u64, Vec<KvPair>> = HashMap::new();

        {
            let meta = self.metadata.read().await;
            for (k, v) in entries {
                let partition = self.hash_key(&k, meta.num_partitions);
                by_partition
                    .entry(partition)
                    .or_default()
                    .push(KvPair { key: k, value: v });
            }
        }

        // Group partitions by leader
        let mut by_leader: HashMap<u64, Vec<KvPair>> = HashMap::new();

        {
            let meta = self.metadata.read().await;
            for (partition, items) in by_partition {
                if let Some(leader) = meta.partition_leaders.get(&partition) {
                    by_leader.entry(*leader).or_default().extend(items);
                } else {
                    // Fallback: if no leader known, maybe pick robustly?
                    // For now, just error or drop (retry logic needed)
                    // In a real client, we'd force metadata refresh here.
                    tracing::warn!("No leader known for partition {}", partition);
                }
            }
        }

        // Execute batch requests in parallel
        let mut handles = Vec::new();

        for (leader_id, items) in by_leader {
            let client_opt = {
                let meta = self.metadata.read().await;
                meta.clients.get(&leader_id).cloned()
            };

            if let Some(mut client) = client_opt {
                handles.push(tokio::spawn(async move {
                    client.batch_put(BatchPutRequest { pairs: items }).await
                }));
            }
        }

        // Wait for all
        let results = futures::future::join_all(handles).await;

        // Check for errors
        for res in results {
            match res {
                Ok(Ok(response)) => {
                    let resp: prkdb_proto::BatchPutResponse = response.into_inner();
                    if resp.failed_count > 0 {
                        // TODO: Implement sophisticated partial retry
                        anyhow::bail!(
                            "Batch put had partial failure: {} failed",
                            resp.failed_count
                        );
                    }
                }
                Ok(Err(e)) => anyhow::bail!("Batch put RPC failed: {}", e),
                Err(e) => anyhow::bail!("Batch put task join error: {}", e),
            }
        }

        Ok(())
    }

    /// Get a value by key (defaults to Linearizable consistency)
    pub async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        self.get_with_consistency(key, ReadConsistency::Linearizable)
            .await
    }

    /// Get a value with specific consistency level
    pub async fn get_with_consistency(
        &self,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        const MAX_RETRIES: usize = 3;

        for attempt in 0..MAX_RETRIES {
            match self.get_internal(key, consistency).await {
                Ok(value) => return Ok(value),
                Err(e) => {
                    tracing::debug!("Get failed (attempt {}): {}", attempt + 1, e);

                    if attempt < MAX_RETRIES - 1 {
                        let _ = self.refresh_metadata().await;
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        anyhow::bail!("Get failed after {} retries", MAX_RETRIES)
    }

    /// Delete a key
    pub async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        const MAX_RETRIES: usize = 3;

        for attempt in 0..MAX_RETRIES {
            match self.delete_internal(key).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::debug!("Delete failed (attempt {}): {}", attempt + 1, e);
                    if attempt < MAX_RETRIES - 1 {
                        let _ = self.refresh_metadata().await;
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        anyhow::bail!("Delete failed after {} retries", MAX_RETRIES)
    }

    /// Internal get implementation
    async fn get_internal(
        &self,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let metadata = self.metadata.read().await;
        let partition_id = self.hash_key(key, metadata.num_partitions);

        let leader_id = metadata
            .partition_leaders
            .get(&partition_id)
            .ok_or_else(|| anyhow::anyhow!("No leader for partition {}", partition_id))?;

        let mut client = metadata
            .clients
            .get(leader_id)
            .ok_or_else(|| anyhow::anyhow!("No client for node {}", leader_id))?
            .clone();

        drop(metadata);

        let request = tonic::Request::new(GetRequest {
            key: key.to_vec(),
            read_mode: i32::from(consistency),
        });

        let response = client.get(request).await?;
        let resp = response.into_inner();

        if !resp.success {
            anyhow::bail!("Get request returned success=false");
        }

        if resp.found {
            Ok(Some(resp.value))
        } else {
            Ok(None)
        }
    }

    /// Internal put implementation
    async fn put_internal(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        let metadata = self.metadata.read().await;
        let partition_id = self.hash_key(key, metadata.num_partitions);

        let leader_id = metadata
            .partition_leaders
            .get(&partition_id)
            .ok_or_else(|| anyhow::anyhow!("No leader for partition {}", partition_id))?;

        let mut client = metadata
            .clients
            .get(leader_id)
            .ok_or_else(|| anyhow::anyhow!("No client for node {}", leader_id))?
            .clone();

        drop(metadata);

        let request = tonic::Request::new(PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
        });

        let response = client.put(request).await?;

        if !response.into_inner().success {
            anyhow::bail!("Put request returned success=false");
        }

        Ok(())
    }

    /// Internal delete implementation
    async fn delete_internal(&self, key: &[u8]) -> anyhow::Result<()> {
        let metadata = self.metadata.read().await;
        let num_partitions = metadata.num_partitions;

        let partition_id = self.hash_key(key, num_partitions);
        let leader_id = metadata
            .partition_leaders
            .get(&partition_id)
            .ok_or_else(|| anyhow::anyhow!("No leader for partition {}", partition_id))?;

        let mut client = metadata
            .clients
            .get(leader_id)
            .ok_or_else(|| anyhow::anyhow!("No client for node {}", leader_id))?
            .clone();

        drop(metadata);

        let request = tonic::Request::new(DeleteRequest { key: key.to_vec() });

        let response = client.delete(request).await?;

        if !response.into_inner().success {
            anyhow::bail!("Delete failed on server");
        }

        Ok(())
    }

    /// Hash key to partition (consistent with server-side logic)
    fn hash_key(&self, key: &[u8], num_partitions: usize) -> u64 {
        if num_partitions == 0 {
            return 0;
        }
        let hash = seahash::hash(key);
        hash % num_partitions as u64
    }

    /// Get list of bootstrap servers
    pub fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_key() {
        let client = PrkDbClient {
            metadata: Arc::new(RwLock::new(ClusterMetadata::default())),
            bootstrap_servers: vec![],
        };

        let key = b"test_key";
        let partition1 = client.hash_key(key, 3);
        let partition2 = client.hash_key(key, 3);
        assert_eq!(partition1, partition2);
        assert!(partition1 < 3);
    }

    #[test]
    fn test_hash_key_zero_partitions() {
        let client = PrkDbClient {
            metadata: Arc::new(RwLock::new(ClusterMetadata::default())),
            bootstrap_servers: vec![],
        };
        assert_eq!(client.hash_key(b"any_key", 0), 0);
    }

    #[test]
    fn test_hash_key_distribution() {
        let client = PrkDbClient {
            metadata: Arc::new(RwLock::new(ClusterMetadata::default())),
            bootstrap_servers: vec![],
        };

        let mut counts = [0usize; 10];
        for i in 0..1000 {
            let key = format!("key_{}", i);
            let partition = client.hash_key(key.as_bytes(), 10) as usize;
            counts[partition] += 1;
        }

        for count in counts.iter() {
            assert!(*count > 50, "Partition has only {} keys", count);
            assert!(*count < 200, "Partition has {} keys", count);
        }
    }

    #[test]
    fn test_read_consistency_conversion() {
        assert_eq!(
            i32::from(ReadConsistency::Linearizable),
            ReadMode::Linearizable as i32
        );
        assert_eq!(i32::from(ReadConsistency::Stale), ReadMode::Stale as i32);
    }
}
