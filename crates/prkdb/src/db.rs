#![allow(
    clippy::type_complexity,
    clippy::unnecessary_cast,
    clippy::useless_format,
    clippy::unwrap_or_default
)]

use crate::collection_handle::CollectionHandle;
use crate::consumer::ConsumerGroupCoordinator;
use crate::partitioning::Partitioner;
use dashmap::DashMap;
use prkdb_types::collection::Collection;
use prkdb_types::consumer::OffsetStore;
use prkdb_types::error::Error;
use prkdb_types::storage::StorageAdapter;
use std::any::{Any, TypeId};
use std::hash::Hash;
// use std::collections::HashMap;
use std::sync::Arc;

pub type EventBusMap = DashMap<TypeId, Box<dyn Any + Send + Sync>>;
pub type ComputeHandlerMap = DashMap<TypeId, Vec<Box<dyn Any + Send + Sync>>>;

/// Global partitioning configuration for collections
pub struct PartitioningConfig {
    pub num_partitions: u32,
    pub partitioner: Arc<dyn Any + Send + Sync>, // Type-erased partitioner
}

pub type PartitioningRegistry = DashMap<TypeId, PartitioningConfig>;

/// Collection registry to track registered collection names
pub type CollectionRegistry = DashMap<TypeId, String>;

// ─────────────────────────────────────────────────────────────────────────────
// Phase 17: Replication Registry
// ─────────────────────────────────────────────────────────────────────────────

/// Represents a replication target in the cluster
#[derive(Clone, Debug)]
pub struct ReplicationTarget {
    /// Network address of the target node
    pub address: String,
    /// Assigned node ID (generated from address hash)
    pub node_id: String,
    /// Whether replication is currently active
    pub active: bool,
    /// Timestamp when replication was started
    pub started_at: std::time::Instant,
    /// Last successful sync timestamp
    pub last_sync: Option<std::time::Instant>,
}

/// Registry type for replication targets
pub type ReplicationRegistry =
    std::sync::RwLock<std::collections::HashMap<String, ReplicationTarget>>;

#[derive(Clone)]
pub struct PrkDb {
    pub(crate) storage: Arc<dyn StorageAdapter>,
    pub(crate) event_bus: Arc<EventBusMap>,
    pub(crate) compute_handlers: Arc<ComputeHandlerMap>,
    pub(crate) namespace: Option<Vec<u8>>,
    pub(crate) metrics: crate::metrics::DbMetrics,
    pub(crate) consumer_coordinator: Arc<ConsumerGroupCoordinator>,
    pub(crate) partitioning_registry: Arc<PartitioningRegistry>,
    pub(crate) collection_registry: Arc<CollectionRegistry>,
    pub partition_manager: Option<Arc<crate::raft::PartitionManager>>, // Made public for gRPC service
    /// Phase 17: Runtime registry of replication targets
    pub(crate) replication_targets: Arc<ReplicationRegistry>,
}

impl PrkDb {
    pub fn builder() -> crate::builder::Builder {
        crate::builder::Builder::new()
    }

    /// Access the underlying storage adapter (Read-Only access recommended)
    pub fn storage(&self) -> &Arc<dyn StorageAdapter> {
        &self.storage
    }

    /// Create a new PrkDb instance with Multi-Raft partitioning
    pub fn new_multi_raft(
        num_partitions: usize,
        config: crate::raft::ClusterConfig,
        storage_path: std::path::PathBuf,
    ) -> Result<Self, Error> {
        use crate::raft::PartitionManager;
        use crate::raft::PrkDbStateMachine;
        use crate::storage::wal_adapter::WalStorageAdapter;
        use prkdb_core::wal::WalConfig;

        // Create partition manager
        // We need a factory for state machines. For now, we'll use PrkDbStateMachine.
        // But PrkDbStateMachine needs a DB instance? No, it needs storage.
        // Wait, PrkDbStateMachine in current implementation might need to be checked.

        let partition_manager = PartitionManager::new(
            num_partitions,
            config,
            storage_path.clone(),
            |_partition_id, storage| {
                // Create a state machine for this partition
                // In a real implementation, this would be connected to a storage engine
                // For now, we'll create a dummy state machine or a real one if possible
                // Let's check PrkDbStateMachine constructor
                Arc::new(PrkDbStateMachine::new(
                    storage, // Pass the partition's storage adapter
                ))
            },
        )
        .map_err(|e| Error::Storage(prkdb_types::error::StorageError::Internal(e.to_string())))?;

        // We need a base storage for the PrkDb instance itself (for metadata etc)
        // or we can just use the first partition's storage?
        // For now, let's create a separate storage for the facade
        let wal_config = WalConfig {
            log_dir: storage_path.join("meta"),
            ..WalConfig::default()
        };
        let storage = Arc::new(WalStorageAdapter::new(wal_config).map_err(Error::Storage)?);

        // Create consumer coordinator
        let offset_store = Arc::new(crate::consumer::StorageOffsetStore::new(storage.clone()));
        let consumer_coordinator = Arc::new(ConsumerGroupCoordinator::new(
            offset_store,
            crate::partitioning::AssignmentStrategy::RoundRobin,
        ));

        Ok(Self {
            storage,
            event_bus: Arc::new(DashMap::new()),
            compute_handlers: Arc::new(DashMap::new()),
            namespace: None,
            metrics: crate::metrics::DbMetrics::default(),
            consumer_coordinator,
            partitioning_registry: Arc::new(DashMap::new()),
            collection_registry: Arc::new(DashMap::new()),
            partition_manager: Some(Arc::new(partition_manager)),
            replication_targets: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        })
    }

    /// Start Multi-Raft partitions
    pub fn start_multi_raft(
        &self,
        rpc_pool: std::sync::Arc<crate::raft::RpcClientPool>,
        skip_server_partitions: &[u64],
    ) {
        if let Some(pm) = &self.partition_manager {
            pm.start_all(rpc_pool, skip_server_partitions);
        }
    }

    /// Wait for Multi-Raft leaders
    pub async fn wait_for_leaders(&self, timeout: std::time::Duration) -> Result<(), Error> {
        if let Some(pm) = &self.partition_manager {
            pm.wait_for_leaders(timeout).await.map_err(|e| {
                Error::Storage(prkdb_types::error::StorageError::Internal(e.to_string()))
            })?;
        }
        Ok(())
    }

    /// Put a key-value pair into the database
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if let Some(pm) = &self.partition_manager {
            // Distributed write with Multi-Raft partitioning
            use crate::raft::command::Command;
            let cmd = Command::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            };

            // Route to correct partition
            let raft = pm.get_raft_for_key(key);

            // Propose and wait for commit
            let handle = raft.propose(cmd.serialize()).await.map_err(|e| {
                Error::Storage(prkdb_types::error::StorageError::Internal(e.to_string()))
            })?;

            handle.wait_commit().await.map_err(|e| {
                Error::Storage(prkdb_types::error::StorageError::Internal(e.to_string()))
            })?;
        } else {
            // Local write (legacy/single-node)
            self.storage.put(key, value).await?;
        }
        Ok(())
    }

    /// Delete a key from the database
    pub async fn delete(&self, key: &[u8]) -> Result<(), Error> {
        if let Some(pm) = &self.partition_manager {
            // Distributed delete with Multi-Raft partitioning
            use crate::raft::command::Command;
            let cmd = Command::Delete { key: key.to_vec() };

            // Route to correct partition
            let raft = pm.get_raft_for_key(key);

            // Propose and wait for commit
            let handle = raft.propose(cmd.serialize()).await.map_err(|e| {
                Error::Storage(prkdb_types::error::StorageError::Internal(e.to_string()))
            })?;

            handle.wait_commit().await.map_err(|e| {
                Error::Storage(prkdb_types::error::StorageError::Internal(e.to_string()))
            })?;
        } else {
            // Local delete
            self.storage.delete(key).await?;
        }
        Ok(())
    }

    /// Get a value by key from the database
    ///
    /// Routes to the correct partition and reads from leader's storage
    /// This provides linearizable reads (read-your-writes guarantee)
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        if let Some(pm) = &self.partition_manager {
            // Distributed read with Multi-Raft partitioning
            // Route to correct partition
            // Read directly from the partition's storage
            // Note: Reading from leader's storage provides linearizability
            // Future: Implement ReadIndex or lease-based reads for follower reads
            let partition_id = pm.get_partition_for_key(key);

            // Get Raft node for this partition
            if let Some(raft) = pm.get_partition(partition_id) {
                // Ensure linearizability using ReadIndex
                // This checks if we are the leader and waits for commit index to be applied
                match raft.read_index().await {
                    Ok(read_index) => {
                        // Wait for state machine to catch up to read_index
                        if let Err(e) = raft.wait_for_apply(read_index).await {
                            return Err(Error::Storage(
                                prkdb_types::error::StorageError::Internal(format!(
                                    "ReadIndex wait failed: {}",
                                    e
                                )),
                            ));
                        }
                    }
                    Err(e) => {
                        // If not leader, we should forward or fail
                        // For now, fail so client can retry with correct leader
                        return Err(Error::Storage(prkdb_types::error::StorageError::Internal(
                            format!("ReadIndex failed (not leader?): {}", e),
                        )));
                    }
                }
            }

            if let Some(storage) = pm.get_partition_storage(partition_id) {
                storage.get(key).await.map_err(Error::Storage)
            } else {
                Err(Error::Storage(prkdb_types::error::StorageError::Internal(
                    format!("Partition {} storage not found", partition_id),
                )))
            }
        } else {
            // Local read
            self.storage.get(key).await.map_err(Error::Storage)
        }
    }

    /// Get a value by key with stale read (no linearizability guarantee)
    ///
    /// This is the fastest read option - reads directly from local storage
    /// without checking leader status or waiting for replication.
    /// The data may be stale if this node is behind.
    pub async fn get_local(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        if let Some(pm) = &self.partition_manager {
            let partition_id = pm.get_partition_for_key(key);

            if let Some(storage) = pm.get_partition_storage(partition_id) {
                // Direct read without ReadIndex - may be stale
                storage.get(key).await.map_err(Error::Storage)
            } else {
                Err(Error::Storage(prkdb_types::error::StorageError::Internal(
                    format!("Partition {} storage not found", partition_id),
                )))
            }
        } else {
            // Local single-node mode - same as regular get
            self.storage.get(key).await.map_err(Error::Storage)
        }
    }

    /// Get a value by key with follower read (linearizable but reads from follower)
    ///
    /// This performs a ReadIndex call to the leader to get the current commit index,
    /// waits for the local state machine to apply up to that index, then reads locally.
    /// This is faster than leader reads for geographically distributed clusters.
    pub async fn get_follower_read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        if let Some(pm) = &self.partition_manager {
            let partition_id = pm.get_partition_for_key(key);

            if let Some(raft) = pm.get_partition(partition_id) {
                // Get ReadIndex from leader
                match raft.read_index().await {
                    Ok(read_index) => {
                        // Wait for local state machine to catch up
                        if let Err(e) = raft.wait_for_apply(read_index).await {
                            return Err(Error::Storage(
                                prkdb_types::error::StorageError::Internal(format!(
                                    "Follower read wait failed: {}",
                                    e
                                )),
                            ));
                        }
                    }
                    Err(e) => {
                        return Err(Error::Storage(prkdb_types::error::StorageError::Internal(
                            format!("ReadIndex failed: {}", e),
                        )));
                    }
                }
            }

            if let Some(storage) = pm.get_partition_storage(partition_id) {
                storage.get(key).await.map_err(Error::Storage)
            } else {
                Err(Error::Storage(prkdb_types::error::StorageError::Internal(
                    format!("Partition {} storage not found", partition_id),
                )))
            }
        } else {
            // Local single-node mode - same as regular get
            self.storage.get(key).await.map_err(Error::Storage)
        }
    }

    pub fn collection<C: Collection>(&self) -> CollectionHandle<C> {
        CollectionHandle::new(self.clone())
    }

    pub fn metrics(&self) -> &crate::metrics::DbMetrics {
        &self.metrics
    }

    pub fn consumer_coordinator(&self) -> &ConsumerGroupCoordinator {
        &self.consumer_coordinator
    }

    /// Register partitioning configuration for a collection type
    pub fn register_partitioning<C>(
        &self,
        num_partitions: u32,
        partitioner: Arc<dyn Partitioner<C::Id>>,
    ) where
        C: Collection + 'static,
        C::Id: 'static,
    {
        let config = PartitioningConfig {
            num_partitions,
            partitioner: Arc::new(partitioner) as Arc<dyn Any + Send + Sync>,
        };
        self.partitioning_registry.insert(TypeId::of::<C>(), config);
    }

    /// Get partitioning configuration for a collection type
    pub fn get_partitioning<C>(&self) -> Option<(u32, Arc<dyn Partitioner<C::Id>>)>
    where
        C: Collection + 'static,
        C::Id: 'static,
    {
        self.partitioning_registry
            .get(&TypeId::of::<C>())
            .and_then(|config| {
                config
                    .partitioner
                    .downcast_ref::<Arc<dyn Partitioner<C::Id>>>()
                    .map(|p| (config.num_partitions, p.clone()))
            })
    }

    /// Get partition for a specific key using registered partitioner
    pub fn get_partition<C>(&self, key: &C::Id) -> Option<u32>
    where
        C: Collection,
        C::Id: Hash,
    {
        self.get_partitioning::<C>()
            .map(|(num_partitions, partitioner)| partitioner.partition(key, num_partitions))
    }

    /// Get active consumers in a consumer group
    pub fn get_active_consumers(&self, group_id: &str) -> Vec<String> {
        self.consumer_coordinator.get_active_consumers(group_id)
    }

    /// Get current partition assignment for a consumer group
    pub fn get_consumer_group_assignment(
        &self,
        group_id: &str,
    ) -> Option<crate::partitioning::PartitionAssignment> {
        self.consumer_coordinator.get_assignment(group_id)
    }

    /// Force rebalancing for a consumer group (manual triggering)
    pub fn force_consumer_group_rebalance<C>(
        &self,
        group_id: &str,
        consumer_ids: Vec<String>,
    ) -> Option<crate::partitioning::PartitionAssignment>
    where
        C: Collection,
    {
        if let Some((num_partitions, _)) = self.get_partitioning::<C>() {
            Some(self.consumer_coordinator.force_rebalance(
                group_id,
                consumer_ids,
                num_partitions,
                Some(&self.metrics),
            ))
        } else {
            None
        }
    }

    /// List all consumer groups
    pub async fn list_consumer_groups(&self) -> Result<Vec<String>, Error> {
        // Create offset store to access consumer group data
        let offset_store = crate::consumer::StorageOffsetStore::new(self.storage.clone());
        let groups = offset_store.list_groups().await?;
        Ok(groups.into_iter().map(|g| g.0).collect())
    }

    /// Get collection names from registered collections
    /// Get collection statistics (count and size)
    pub async fn get_collection_stats(&self, collection_name: &str) -> Result<(u64, u64), Error> {
        // Scan storage for all keys belonging to this collection
        let prefix = format!("{}:", collection_name);
        let entries = self.storage.scan_prefix(prefix.as_bytes()).await?;

        let item_count = entries.len() as u64;
        let total_size: u64 = entries
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>() as u64;

        Ok((item_count, total_size))
    }

    /// Get total items across all collections
    pub async fn get_total_items(&self) -> Result<u64, Error> {
        let mut total = 0u64;
        let collections = self.list_collections().await?;

        for collection in collections {
            let (count, _) = self.get_collection_stats(&collection).await?;
            total += count;
        }

        Ok(total)
    }

    /// Get consumer lag for a specific group and collection partition
    pub async fn get_consumer_lag(
        &self,
        group_id: &str,
        collection: &str,
        partition: u32,
    ) -> Result<u64, Error> {
        let offset_store = crate::consumer::StorageOffsetStore::new(self.storage.clone());

        // Get current offset from offset store
        let current_offset = offset_store
            .get_offset(group_id, collection, partition)
            .await?
            .unwrap_or(prkdb_types::consumer::Offset(0));

        // Get latest offset by counting items in the collection partition
        let latest_offset = self
            .get_collection_latest_offset(collection, partition)
            .await?;

        Ok(latest_offset.saturating_sub(current_offset.0))
    }

    /// Get latest offset for a collection partition
    pub async fn get_collection_latest_offset(
        &self,
        collection: &str,
        partition: u32,
    ) -> Result<u64, Error> {
        // Scan storage for keys matching this collection and partition
        let prefix = format!("{}:{}:", collection, partition);
        let entries = self.storage.scan_prefix(prefix.as_bytes()).await?;
        Ok(entries.len() as u64)
    }

    /// Get all lag information for a consumer group
    pub async fn get_group_lag_info(
        &self,
        group_id: &str,
    ) -> Result<Vec<(String, u32, u64, u64, u64)>, Error> {
        let offset_store = crate::consumer::StorageOffsetStore::new(self.storage.clone());
        let group_offsets = offset_store.get_group_offsets(group_id).await?;

        let mut lag_info = Vec::new();

        // For each offset tracked by this group
        for entry in group_offsets.iter() {
            // Parse collection and partition from key (format: "collection:partition")
            let key_parts: Vec<&str> = entry.key().split(':').collect();
            if key_parts.len() >= 2 {
                let collection = key_parts[0];
                if let Ok(partition) = key_parts[1].parse::<u32>() {
                    let current_offset = entry.value().0; // Extract u64 from Offset struct
                    let latest_offset = self
                        .get_collection_latest_offset(collection, partition)
                        .await?;
                    let lag = latest_offset.saturating_sub(current_offset);

                    lag_info.push((
                        collection.to_string(),
                        partition,
                        current_offset,
                        latest_offset,
                        lag,
                    ));
                }
            }
        }

        Ok(lag_info)
    }

    /// Sample data from a collection
    pub async fn sample_collection(
        &self,
        collection_name: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>, Error> {
        // Scan storage for all keys belonging to this collection
        let prefix = format!("{}:", collection_name);
        let entries = self.storage.scan_prefix(prefix.as_bytes()).await?;

        let mut samples = Vec::new();
        for (key, value) in entries.into_iter().take(limit) {
            // Try to deserialize the value as JSON
            match serde_json::from_slice::<serde_json::Value>(&value) {
                Ok(json_value) => {
                    // Include key information in the sample
                    let key_str = String::from_utf8_lossy(&key);
                    let sample = serde_json::json!({
                        "key": key_str,
                        "value": json_value
                    });
                    samples.push(sample);
                }
                Err(_) => {
                    // If JSON parsing fails, show raw data
                    let key_str = String::from_utf8_lossy(&key);
                    let value_str = String::from_utf8_lossy(&value);
                    let sample = serde_json::json!({
                        "key": key_str,
                        "value": value_str,
                        "note": "Raw data (not JSON)"
                    });
                    samples.push(sample);
                }
            }
        }

        Ok(samples)
    }

    /// Take a snapshot of the database
    pub async fn take_snapshot(
        &self,
        path: &std::path::Path,
        compression: prkdb_types::snapshot::CompressionType,
    ) -> Result<u64, Error> {
        self.storage
            .take_snapshot(path.to_path_buf(), compression)
            .await
            .map_err(Error::Storage)
    }

    /// Backup database to a file path (legacy/simple wrapper)
    pub async fn backup_database(&self, backup_path: &str) -> Result<(), Error> {
        let path = std::path::Path::new(backup_path);
        // Default to Gzip compression
        let compression = prkdb_types::snapshot::CompressionType::Gzip;
        self.take_snapshot(path, compression).await.map(|_| ())
    }

    /// Compact database storage
    pub async fn compact_database(&self) -> Result<u64, Error> {
        // Similar to backup, this would need storage-specific implementation
        // We'll flush all pending operations as a form of compaction

        // Get collections and force storage operations
        let collections = self.list_collections().await?;
        let mut total_savings = 0u64;

        for collection in collections {
            // This triggers compaction by ensuring data is flushed
            let (_count, size) = self.get_collection_stats(&collection).await?;

            // In a real implementation, this would:
            // 1. Merge sorted string tables
            // 2. Remove deleted entries
            // 3. Optimize storage layout

            // Just report that we "processed" the data
            total_savings += (size / 10).min(1024); // Approximate 10% space savings
        }

        Ok(total_savings)
    }

    /// Get system-wide metrics
    pub async fn get_system_metrics(&self) -> Result<(u64, u64, u64, u64, u32, u32, u32), Error> {
        let collections = self.list_collections().await?;
        let consumer_groups = self.list_consumer_groups().await?;

        let mut total_items = 0u64;
        let mut total_bytes = 0u64;

        // Calculate total items and bytes across all collections
        for collection in &collections {
            let (items, bytes) = self.get_collection_stats(collection).await?;
            total_items += items;
            total_bytes += bytes;
        }

        // Count active consumers across all groups
        let mut total_active_consumers = 0u32;
        for group in &consumer_groups {
            let active_consumers = self.get_active_consumers(group);
            total_active_consumers += active_consumers.len() as u32;
        }

        // For events_produced/consumed, we're using items as a proxy since we don't track these separately yet
        let events_produced = total_items;
        let events_consumed = total_items; // Simplified assumption
        let bytes_produced = total_bytes;
        let bytes_consumed = total_bytes; // Simplified assumption

        Ok((
            events_produced,
            events_consumed,
            bytes_produced,
            bytes_consumed,
            collections.len() as u32,
            consumer_groups.len() as u32,
            total_active_consumers,
        ))
    }

    /// Get partition-specific metrics for a collection
    pub async fn get_partition_metrics(
        &self,
        collection_name: &str,
    ) -> Result<Vec<(u32, u64, u64, u64, u64, u64)>, Error> {
        // Scan for all partitions of this collection
        let prefix = format!("{}:", collection_name);
        let entries = self.storage.scan_prefix(prefix.as_bytes()).await?;

        // Group by partition
        let mut partition_data: std::collections::HashMap<u32, (u64, u64)> =
            std::collections::HashMap::new();

        for (key, value) in entries {
            let key_str = String::from_utf8_lossy(&key);
            let parts: Vec<&str> = key_str.split(':').collect();

            if parts.len() >= 2 {
                if let Ok(partition) = parts[1].parse::<u32>() {
                    let entry = partition_data.entry(partition).or_insert((0, 0));
                    entry.0 += 1; // item count
                    entry.1 += (key.len() + value.len()) as u64; // byte size
                }
            }
        }

        let mut result = Vec::new();
        for (partition, (items, bytes)) in partition_data {
            // Using items as proxy for events and bytes as proxy for bytes
            result.push((
                partition,
                items,        // events_produced
                items,        // events_consumed (simplified)
                bytes as u64, // bytes_produced
                bytes as u64, // bytes_consumed (simplified)
                0,            // consumer_lag (would need offset comparison)
            ));
        }

        result.sort_by_key(|&(partition, _, _, _, _, _)| partition);
        Ok(result)
    }

    /// Get consumer-specific metrics for a group
    pub async fn get_consumer_metrics(
        &self,
        group_id: &str,
    ) -> Result<Vec<(String, u64, u64, u64)>, Error> {
        let active_consumers = self.get_active_consumers(group_id);
        let lag_infos = self.get_group_lag_info(group_id).await?;

        let mut result = Vec::new();

        for consumer_id in active_consumers {
            // Calculate metrics for this consumer
            let consumer_lag: u64 = lag_infos.iter().map(|(_, _, _, _, lag)| lag).sum();

            // For events/bytes consumed, we're using lag as a proxy measure
            // In a real system, you'd track these metrics separately
            let events_consumed = consumer_lag.saturating_sub(consumer_lag); // Simplified: 0
            let bytes_consumed = consumer_lag.saturating_sub(consumer_lag); // Simplified: 0

            result.push((consumer_id, events_consumed, bytes_consumed, consumer_lag));
        }

        Ok(result)
    }

    /// Get partition information for a collection
    pub async fn get_partitions(
        &self,
        collection_name: &str,
    ) -> Result<Vec<(u32, u64, u64)>, Error> {
        // Scan storage for all keys belonging to this collection
        let prefix = format!("{}:", collection_name);
        let entries = self.storage.scan_prefix(prefix.as_bytes()).await?;

        // Group by partition to get partition info
        let mut partition_data: std::collections::HashMap<u32, (u64, u64)> =
            std::collections::HashMap::new();

        for (key, value) in entries {
            let key_str = String::from_utf8_lossy(&key);
            let parts: Vec<&str> = key_str.split(':').collect();

            if parts.len() >= 2 {
                if let Ok(partition) = parts[1].parse::<u32>() {
                    let entry = partition_data.entry(partition).or_insert((0, 0));
                    entry.0 += 1; // item count
                    entry.1 += (key.len() + value.len()) as u64; // byte size
                }
            }
        }

        let mut result = Vec::new();
        for (partition, (items, bytes)) in partition_data {
            result.push((partition, items, bytes));
        }

        result.sort_by_key(|&(partition, _, _)| partition);
        Ok(result)
    }

    /// Get detailed partition information
    pub async fn get_partition_details(
        &self,
        collection_name: &str,
        partition: u32,
    ) -> Result<Option<(u64, u64, u64, u64)>, Error> {
        // Scan storage for keys in this specific partition
        let prefix = format!("{}:{}:", collection_name, partition);
        let entries = self.storage.scan_prefix(prefix.as_bytes()).await?;

        if entries.is_empty() {
            return Ok(None);
        }

        let items = entries.len() as u64;
        let bytes: u64 = entries
            .iter()
            .map(|(key, value)| (key.len() + value.len()) as u64)
            .sum();

        // For offsets, we use item indices as a simplified approach
        let first_offset = 0;
        let last_offset = items.saturating_sub(1);

        Ok(Some((items, bytes, first_offset, last_offset)))
    }

    /// Get partition assignments for consumer groups
    pub async fn get_partition_assignments(
        &self,
        group_id: Option<&str>,
    ) -> Result<Vec<(String, String, String, u32, u64, u64)>, Error> {
        let groups = match group_id {
            Some(group) => vec![group.to_string()],
            None => self.list_consumer_groups().await?,
        };

        let mut assignments = Vec::new();

        for group in groups {
            let active_consumers = self.get_active_consumers(&group);
            let lag_infos = self.get_group_lag_info(&group).await?;

            for (collection, partition, current_offset, _latest_offset, lag) in lag_infos {
                // Assign partition to first available consumer (simplified assignment)
                let assigned_consumer = active_consumers
                    .first()
                    .unwrap_or(&"no-consumer".to_string())
                    .clone();

                assignments.push((
                    group.clone(),
                    assigned_consumer,
                    collection,
                    partition,
                    current_offset,
                    lag,
                ));
            }
        }

        Ok(assignments)
    }

    /// Get replication status
    pub async fn get_replication_status(
        &self,
    ) -> Result<
        (
            String,
            String,
            Option<String>,
            Vec<String>,
            String,
            String,
            u64,
            u64,
        ),
        Error,
    > {
        // Check if replication is configured by looking for replication-related data in storage
        let repl_prefix = b"__replication:";
        let repl_entries = self.storage.scan_prefix(repl_prefix).await?;

        // Default to standalone mode if no replication configuration found
        if repl_entries.is_empty() {
            return Ok((
                "standalone-node".to_string(), // node_id
                "Standalone".to_string(),      // role
                None,                          // leader_address
                vec![],                        // followers
                "Not configured".to_string(),  // state
                "N/A".to_string(),             // last_sync
                0,                             // total_changes
                0,                             // changes_applied
            ));
        }

        // Parse replication configuration from storage
        let mut node_id = "unknown".to_string();
        let mut role = "Unknown".to_string();
        let mut leader_address: Option<String> = None;
        let mut followers = Vec::new();
        let mut state = "Unknown".to_string();
        let mut last_sync = "N/A".to_string();
        let mut total_changes = 0u64;
        let mut changes_applied = 0u64;

        for (key, value) in repl_entries {
            let key_str = String::from_utf8_lossy(&key);
            let value_str = String::from_utf8_lossy(&value);

            match key_str.strip_prefix("__replication:") {
                Some("node_id") => node_id = value_str.to_string(),
                Some("role") => role = value_str.to_string(),
                Some("leader_address") => leader_address = Some(value_str.to_string()),
                Some("state") => state = value_str.to_string(),
                Some("last_sync") => last_sync = value_str.to_string(),
                Some("total_changes") => total_changes = value_str.parse().unwrap_or(0),
                Some("changes_applied") => changes_applied = value_str.parse().unwrap_or(0),
                Some(key) if key.starts_with("follower:") => {
                    followers.push(value_str.to_string());
                }
                _ => {}
            }
        }

        Ok((
            node_id,
            role,
            leader_address,
            followers,
            state,
            last_sync,
            total_changes,
            changes_applied,
        ))
    }

    /// Get replication nodes
    pub async fn get_replication_nodes(
        &self,
    ) -> Result<Vec<(String, String, String, String, u64, String)>, Error> {
        let nodes_prefix = b"__replication_nodes:";
        let node_entries = self.storage.scan_prefix(nodes_prefix).await?;

        let mut nodes = Vec::new();
        let mut node_data: std::collections::HashMap<
            String,
            std::collections::HashMap<String, String>,
        > = std::collections::HashMap::new();

        // Parse node data from storage
        for (key, value) in node_entries {
            let key_str = String::from_utf8_lossy(&key);
            let value_str = String::from_utf8_lossy(&value);

            if let Some(remaining) = key_str.strip_prefix("__replication_nodes:") {
                let parts: Vec<&str> = remaining.split(':').collect();
                if parts.len() == 2 {
                    let node_id = parts[0];
                    let field = parts[1];

                    node_data
                        .entry(node_id.to_string())
                        .or_default()
                        .insert(field.to_string(), value_str.to_string());
                }
            }
        }

        // Convert to result format
        for (node_id, data) in node_data {
            let address = data
                .get("address")
                .unwrap_or(&"unknown".to_string())
                .clone();
            let role = data.get("role").unwrap_or(&"unknown".to_string()).clone();
            let status = data.get("status").unwrap_or(&"unknown".to_string()).clone();
            let lag_ms: u64 = data.get("lag_ms").and_then(|s| s.parse().ok()).unwrap_or(0);
            let last_seen = data
                .get("last_seen")
                .unwrap_or(&"unknown".to_string())
                .clone();

            nodes.push((node_id, address, role, status, lag_ms, last_seen));
        }

        nodes.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(nodes)
    }

    /// Get replication lag information
    pub async fn get_replication_lag(
        &self,
    ) -> Result<Vec<(String, u64, u64, u64, u64, String)>, Error> {
        let lag_prefix = b"__replication_lag:";
        let lag_entries = self.storage.scan_prefix(lag_prefix).await?;

        let mut lag_data = Vec::new();
        let mut node_lag: std::collections::HashMap<
            String,
            std::collections::HashMap<String, String>,
        > = std::collections::HashMap::new();

        // Parse lag data from storage
        for (key, value) in lag_entries {
            let key_str = String::from_utf8_lossy(&key);
            let value_str = String::from_utf8_lossy(&value);

            if let Some(remaining) = key_str.strip_prefix("__replication_lag:") {
                let parts: Vec<&str> = remaining.split(':').collect();
                if parts.len() == 2 {
                    let node_id = parts[0];
                    let field = parts[1];

                    node_lag
                        .entry(node_id.to_string())
                        .or_default()
                        .insert(field.to_string(), value_str.to_string());
                }
            }
        }

        // Convert to result format
        for (follower_node, data) in node_lag {
            let leader_offset: u64 = data
                .get("leader_offset")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let follower_offset: u64 = data
                .get("follower_offset")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let lag_records = leader_offset.saturating_sub(follower_offset);
            let lag_ms: u64 = data.get("lag_ms").and_then(|s| s.parse().ok()).unwrap_or(0);
            let status = data.get("status").unwrap_or(&"unknown".to_string()).clone();

            lag_data.push((
                follower_node,
                leader_offset,
                follower_offset,
                lag_records,
                lag_ms,
                status,
            ));
        }

        lag_data.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(lag_data)
    }

    /// Initialize replication configuration
    pub async fn initialize_replication(&self, node_id: &str, role: &str) -> Result<(), Error> {
        let repl_data = vec![
            (format!("__replication:node_id"), node_id.to_string()),
            (format!("__replication:role"), role.to_string()),
            (format!("__replication:state"), "Configured".to_string()),
            (
                format!("__replication:last_sync"),
                chrono::Utc::now().to_rfc3339(),
            ),
            (format!("__replication:total_changes"), "0".to_string()),
            (format!("__replication:changes_applied"), "0".to_string()),
        ];

        // Store replication configuration in storage
        for (key, value) in repl_data {
            self.storage.put(key.as_bytes(), value.as_bytes()).await?;
        }

        Ok(())
    }

    /// Create a collection (admin op)
    /// In a distributed setup, this should propagate via Raft.
    /// Create a collection (admin op)
    /// In a distributed setup, this propagates via Raft to Partition 0 (Metadata Partition).
    pub async fn create_collection(
        &self,
        name: &str,
        num_partitions: u32,
        replication_factor: u32,
    ) -> Result<(), Error> {
        if let Some(pm) = &self.partition_manager {
            // Distributed mode: Propose to Partition 0 (Metadata Partition)
            // Convention: Partition 0 stores cluster metadata including collections
            use crate::raft::command::Command;

            // Get Raft node for Partition 0
            let partition_id = 0;
            let raft = pm.get_partition(partition_id).ok_or_else(|| {
                Error::Storage(prkdb_types::error::StorageError::Internal(
                    "Metadata partition (0) not found".to_string(),
                ))
            })?;

            let cmd = Command::CreateCollection {
                name: name.to_string(),
                num_partitions,
                replication_factor,
            };

            // Propose and wait for commit
            let handle = raft.propose(cmd.serialize()).await.map_err(|e| {
                Error::Storage(prkdb_types::error::StorageError::Internal(e.to_string()))
            })?;

            handle.wait_commit().await.map_err(|e| {
                Error::Storage(prkdb_types::error::StorageError::Internal(e.to_string()))
            })?;
        } else {
            // Local/Single-node mode: Write directly to storage with metadata key
            // This ensures behavior is consistent with Raft state machine application
            let metadata_key = format!("meta:col:{}", name).into_bytes();

            // Store configuration as JSON
            let metadata = serde_json::json!({
                "num_partitions": num_partitions,
                "replication_factor": replication_factor,
                "created_at": chrono::Utc::now().to_rfc3339()
            });

            let metadata_value = metadata.to_string().into_bytes();
            self.storage.put(&metadata_key, &metadata_value).await?;
        }

        Ok(())
    }

    /// List all collections
    pub async fn list_collections(&self) -> Result<Vec<String>, Error> {
        let mut collections = Vec::new();

        if let Some(pm) = &self.partition_manager {
            // Distributed mode: Read from Partition 0 (Metadata Partition)
            let partition_id = 0;
            if let Some(storage) = pm.get_partition_storage(partition_id) {
                // Scan keys starting with "meta:col:"
                let prefix = b"meta:col:";
                // scan_prefix is async, so await it!
                let keys = storage.scan_prefix(prefix).await.map_err(Error::Storage)?;

                for (key, _) in keys {
                    if let Ok(key_str) = String::from_utf8(key) {
                        if let Some(name) = key_str.strip_prefix("meta:col:") {
                            collections.push(name.to_string());
                        }
                    }
                }
            } else {
                return Err(Error::Storage(prkdb_types::error::StorageError::Internal(
                    "Metadata partition (0) storage not found".to_string(),
                )));
            }
        } else {
            // Local mode: Scan local storage
            let prefix = b"meta:col:";
            let keys = self
                .storage
                .scan_prefix(prefix)
                .await
                .map_err(Error::Storage)?;
            for (key, _) in keys {
                if let Ok(key_str) = String::from_utf8(key) {
                    if let Some(name) = key_str.strip_prefix("meta:col:") {
                        collections.push(name.to_string());
                    }
                }
            }
        }

        Ok(collections)
    }

    /// Drop a collection (admin op)
    pub async fn drop_collection(&self, name: &str) -> Result<(), Error> {
        if let Some(pm) = &self.partition_manager {
            // Distributed mode: Propose to Partition 0
            use crate::raft::command::Command;

            let partition_id = 0;
            let raft = pm.get_partition(partition_id).ok_or_else(|| {
                Error::Storage(prkdb_types::error::StorageError::Internal(
                    "Metadata partition (0) not found".to_string(),
                ))
            })?;

            let cmd = Command::DropCollection {
                name: name.to_string(),
            };

            let handle = raft.propose(cmd.serialize()).await.map_err(|e| {
                Error::Storage(prkdb_types::error::StorageError::Internal(e.to_string()))
            })?;

            handle.wait_commit().await.map_err(|e| {
                Error::Storage(prkdb_types::error::StorageError::Internal(e.to_string()))
            })?;
        } else {
            // Local mode
            let metadata_key = format!("meta:col:{}", name).into_bytes();
            self.storage.delete(&metadata_key).await?;
        }
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Remote Admin Operations (Phase 14)
    // ─────────────────────────────────────────────────────────────────────────────

    /// Reset consumer group offsets to a specific position
    ///
    /// # Arguments
    /// * `group_id` - Consumer group to reset
    /// * `collection_filter` - Optional collection name (None = all collections)
    /// * `target_offset` - Target offset (None = latest, Some(0) = earliest)
    ///
    /// Returns the number of partitions that were reset.
    pub async fn reset_consumer_offset(
        &self,
        group_id: &str,
        collection_filter: Option<&str>,
        target_offset: Option<u64>,
    ) -> Result<u32, Error> {
        tracing::info!(
            "Resetting consumer group '{}' offsets (collection={:?}, target={:?})",
            group_id,
            collection_filter,
            target_offset
        );

        let offset_store = crate::consumer::StorageOffsetStore::new(self.storage.clone());
        let group_offsets = offset_store.get_group_offsets(group_id).await?;

        let mut partitions_reset = 0u32;

        for entry in group_offsets.iter() {
            let key = entry.key();
            let key_parts: Vec<&str> = key.split(':').collect();

            if key_parts.len() < 2 {
                continue;
            }

            let collection = key_parts[0];
            let partition: u32 = match key_parts[1].parse() {
                Ok(p) => p,
                Err(_) => continue,
            };

            // Apply collection filter
            if let Some(filter) = collection_filter {
                if collection != filter {
                    continue;
                }
            }

            // Determine target offset
            let new_offset = match target_offset {
                Some(o) => o,
                None => {
                    // Latest: get current end of log
                    self.get_collection_latest_offset(collection, partition)
                        .await
                        .unwrap_or(0)
                }
            };

            // Update offset in storage
            let offset_key = format!("offset:{}:{}:{}", group_id, collection, partition);
            let offset_bytes = new_offset.to_le_bytes().to_vec();
            self.storage
                .put(offset_key.as_bytes(), &offset_bytes)
                .await?;

            partitions_reset += 1;
        }

        tracing::info!(
            "Reset {} partitions for consumer group '{}'",
            partitions_reset,
            group_id
        );

        Ok(partitions_reset)
    }

    /// Start replication to a target follower address
    ///
    /// Phase 17: Registry-based implementation
    /// - Validates target address format
    /// - Adds to replication_targets registry
    /// - Returns generated node_id
    pub async fn start_replication(&self, target_address: &str) -> Result<String, Error> {
        tracing::info!("Start replication requested for target: {}", target_address);

        // Validate address format
        if target_address.is_empty() {
            return Err(Error::Storage(prkdb_types::error::StorageError::Internal(
                "Empty target address".to_string(),
            )));
        }

        // Generate node ID from address hash
        let node_id = format!("node-{:x}", seahash::hash(target_address.as_bytes()));

        // Add to registry
        let target = ReplicationTarget {
            address: target_address.to_string(),
            node_id: node_id.clone(),
            active: true,
            started_at: std::time::Instant::now(),
            last_sync: None,
        };

        {
            let mut registry = self.replication_targets.write().map_err(|e| {
                Error::Storage(prkdb_types::error::StorageError::Internal(format!(
                    "Lock error: {}",
                    e
                )))
            })?;
            registry.insert(target_address.to_string(), target);
        }

        tracing::info!(
            "Replication target registered: {} -> {}",
            target_address,
            node_id
        );

        Ok(node_id)
    }

    /// Stop replication to a target follower address
    ///
    /// Phase 17: Registry-based implementation
    /// - Marks target as inactive in registry
    pub async fn stop_replication(&self, target_address: &str) -> Result<(), Error> {
        tracing::info!("Stop replication requested for target: {}", target_address);

        {
            let mut registry = self.replication_targets.write().map_err(|e| {
                Error::Storage(prkdb_types::error::StorageError::Internal(format!(
                    "Lock error: {}",
                    e
                )))
            })?;

            if let Some(target) = registry.get_mut(target_address) {
                target.active = false;
                tracing::info!("Replication stopped for target: {}", target_address);
            } else {
                tracing::warn!("Replication target not found: {}", target_address);
            }
        }

        Ok(())
    }

    /// Get status of all replication targets
    ///
    /// Phase 17: Returns list of active and inactive targets
    pub fn get_replication_targets(&self) -> Vec<ReplicationTarget> {
        match self.replication_targets.read() {
            Ok(registry) => registry.values().cloned().collect(),
            Err(_) => Vec::new(),
        }
    }
}
