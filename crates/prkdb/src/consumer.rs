//! Concrete consumer implementation for PrkDB
//!
//! This module provides the implementation of the Consumer trait,
//! with support for offset management, auto-commit, and event polling.

use crate::db::PrkDb;
use crate::outbox::{OutboxEnvelope, OutboxRecord};
use crate::partitioning::{AssignmentStrategy, PartitionCoordinator, PartitionId};
use async_trait::async_trait;
use bincode::{
    config,
    serde::{decode_from_slice, encode_to_vec},
};
use dashmap::DashMap;
use prkdb_types::collection::Collection;
use prkdb_types::consumer::{
    AutoOffsetReset, CommitResult, Consumer, ConsumerConfig, ConsumerGroupId, ConsumerRecord,
    Offset, OffsetStore,
};
use prkdb_types::error::{Error, StorageError};
use prkdb_types::storage::StorageAdapter;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

/// Offset store implementation using the storage adapter
pub struct StorageOffsetStore {
    storage: Arc<dyn StorageAdapter>,
}

impl StorageOffsetStore {
    pub fn new(storage: Arc<dyn StorageAdapter>) -> Self {
        Self { storage }
    }

    fn offset_key(group_id: &str, collection: &str, partition: u32) -> Vec<u8> {
        format!(
            "__consumer_offset:{}:{}:{}",
            group_id, collection, partition
        )
        .into_bytes()
    }

    fn group_prefix(group_id: &str) -> Vec<u8> {
        format!("__consumer_offset:{}:", group_id).into_bytes()
    }
}

#[async_trait]
impl OffsetStore for StorageOffsetStore {
    async fn get_offset(
        &self,
        group_id: &str,
        collection_name: &str,
        partition: u32,
    ) -> Result<Option<Offset>, Error> {
        let key = Self::offset_key(group_id, collection_name, partition);
        match self.storage.get(&key).await? {
            Some(bytes) => {
                let (offset, _): (Offset, _) = decode_from_slice(&bytes, config::standard())
                    .map_err(|e| StorageError::Deserialization(e.to_string()))?;
                Ok(Some(offset))
            }
            None => Ok(None),
        }
    }

    async fn save_offset(
        &self,
        group_id: &str,
        collection_name: &str,
        partition: u32,
        offset: Offset,
    ) -> Result<(), Error> {
        let key = Self::offset_key(group_id, collection_name, partition);
        let bytes = encode_to_vec(offset, config::standard())
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        self.storage.put(&key, &bytes).await.map_err(Error::from)
    }

    async fn list_groups(&self) -> Result<Vec<ConsumerGroupId>, Error> {
        let prefix = b"__consumer_offset:";
        let entries = self.storage.scan_prefix(prefix).await?;

        let mut groups = std::collections::HashSet::new();
        for (key, _) in entries {
            if let Ok(key_str) = String::from_utf8(key) {
                let parts: Vec<&str> = key_str.split(':').collect();
                if parts.len() >= 2 {
                    groups.insert(parts[1].to_string());
                }
            }
        }

        Ok(groups.into_iter().map(ConsumerGroupId).collect())
    }

    async fn get_group_offsets(&self, group_id: &str) -> Result<DashMap<String, Offset>, Error> {
        let prefix = Self::group_prefix(group_id);
        let entries = self
            .storage
            .scan_prefix(&prefix)
            .await
            .map_err(Error::from)?;

        let offsets = DashMap::new();
        for (key, value) in entries {
            if let Ok(key_str) = String::from_utf8(key) {
                let (offset, _): (Offset, _) = decode_from_slice(&value, config::standard())
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
                offsets.insert(key_str, offset);
            }
        }

        Ok(offsets)
    }

    async fn delete_group(&self, group_id: &str) -> Result<(), Error> {
        let prefix = Self::group_prefix(group_id);
        let entries = self
            .storage
            .scan_prefix(&prefix)
            .await
            .map_err(Error::from)?;

        for (key, _) in entries {
            self.storage.delete(&key).await.map_err(Error::from)?;
        }

        Ok(())
    }
}

/// PrkDB consumer implementation
pub struct PrkConsumer<C: Collection> {
    db: PrkDb,
    config: ConsumerConfig,
    offset_store: Arc<dyn OffsetStore>,
    /// Offsets per partition (partition_id -> offset)
    partition_offsets: HashMap<PartitionId, Offset>,
    last_commit_time: Instant,
    /// Assigned partitions for this consumer
    assigned_partitions: Vec<PartitionId>,
    _marker: PhantomData<C>,
}

impl<C> PrkConsumer<C>
where
    C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Create a new consumer with default partition assignment (partition 0)
    pub async fn new(db: PrkDb, config: ConsumerConfig) -> Result<Self, Error> {
        Self::with_partitions(db, config, vec![0]).await
    }

    /// Create a new consumer with specific partition assignments
    pub async fn with_partitions(
        db: PrkDb,
        config: ConsumerConfig,
        partitions: Vec<PartitionId>,
    ) -> Result<Self, Error> {
        let offset_store = Arc::new(StorageOffsetStore::new(db.storage.clone()));
        let collection_name = std::any::type_name::<C>();

        // Load offsets for all assigned partitions
        let mut partition_offsets = HashMap::new();
        for partition in &partitions {
            let offset = match offset_store
                .get_offset(&config.group_id, collection_name, *partition)
                .await?
            {
                Some(offset) => offset,
                None => match config.auto_offset_reset {
                    AutoOffsetReset::Earliest => Offset::zero(),
                    AutoOffsetReset::Latest => {
                        Self::get_latest_offset(&db, collection_name).await?
                    }
                    AutoOffsetReset::None => Offset::zero(),
                },
            };
            partition_offsets.insert(*partition, offset);
        }

        info!(
            group_id = %config.group_id,
            consumer_id = ?config.consumer_id,
            collection = collection_name,
            partitions = ?partitions,
            "Consumer initialized"
        );

        Ok(Self {
            db,
            config,
            offset_store,
            partition_offsets,
            last_commit_time: Instant::now(),
            assigned_partitions: partitions,
            _marker: PhantomData,
        })
    }

    /// Get assigned partitions
    pub fn assigned_partitions(&self) -> &[PartitionId] {
        &self.assigned_partitions
    }

    /// Reassign partitions (used during rebalancing)
    pub async fn reassign_partitions(
        &mut self,
        new_partitions: Vec<PartitionId>,
    ) -> Result<(), Error> {
        let collection_name = std::any::type_name::<C>();

        // Load offsets for newly assigned partitions
        for partition in &new_partitions {
            if !self.partition_offsets.contains_key(partition) {
                let maybe_offset = self
                    .offset_store
                    .get_offset(&self.config.group_id, collection_name, *partition)
                    .await?;

                let offset = if let Some(offset) = maybe_offset {
                    offset
                } else {
                    match self.config.auto_offset_reset {
                        AutoOffsetReset::Earliest => Offset::zero(),
                        AutoOffsetReset::Latest => {
                            Self::get_latest_offset(&self.db, collection_name).await?
                        }
                        prkdb_types::consumer::AutoOffsetReset::None => Offset::zero(),
                    }
                };
                self.partition_offsets.insert(*partition, offset);
            }
        }

        // Remove offsets for partitions no longer assigned
        self.partition_offsets
            .retain(|p, _| new_partitions.contains(p));
        self.assigned_partitions = new_partitions;

        info!(
            group_id = %self.config.group_id,
            consumer_id = ?self.config.consumer_id,
            partitions = ?self.assigned_partitions,
            "Partitions reassigned"
        );

        Ok(())
    }

    async fn get_latest_offset(db: &PrkDb, collection_name: &str) -> Result<Offset, Error> {
        let entries = db.storage.outbox_list().await?;
        let prefix = format!("{}:", collection_name);

        let mut max_offset = 0u64;
        for (id, _) in entries {
            if id.starts_with(&prefix) {
                // Extract sequence number as the last ':'-separated segment
                if let Some(seq_str) = id.rsplit(':').next() {
                    if let Ok(seq) = seq_str.parse::<u64>() {
                        max_offset = max_offset.max(seq);
                    }
                }
            }
        }

        // The next offset should be after the max existing one
        Ok(Offset::from_value(max_offset.saturating_add(1)))
    }

    async fn should_auto_commit(&self) -> bool {
        self.config.auto_commit
            && self.last_commit_time.elapsed() >= self.config.auto_commit_interval
    }
}

#[async_trait]
impl<C> Consumer<C> for PrkConsumer<C>
where
    C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn poll(&mut self) -> Result<Vec<ConsumerRecord<C>>, Error> {
        let collection_name = std::any::type_name::<C>();
        let entries = self.db.storage.outbox_list().await?;

        let mut all_records = Vec::new();
        let poll_start_time = std::time::Instant::now();

        // Poll from all assigned partitions
        for &partition in &self.assigned_partitions {
            let current_offset = self
                .partition_offsets
                .get(&partition)
                .copied()
                .unwrap_or(Offset::zero());

            // Filter by partition prefix BEFORE deserializing for efficiency
            // New format: "TypeName:Partition:Seq"
            let partition_prefix = format!("{}:{}:", collection_name, partition);

            let mut matching: Vec<(String, Vec<u8>, u64)> = entries
                .iter()
                .filter(|(id, _)| id.starts_with(&partition_prefix))
                .filter_map(|(id, bytes)| {
                    // Extract sequence number from ID format: "TypeName:Partition:Seq"
                    if let Some(seq_str) = id.rsplit(':').next() {
                        if let Ok(seq) = seq_str.parse::<u64>() {
                            return Some((id.clone(), bytes.clone(), seq));
                        }
                    }
                    None
                })
                .filter(|(_, _, seq)| *seq >= current_offset.value())
                .collect();

            matching.sort_by_key(|(_, _, seq)| *seq);

            let mut partition_records = Vec::new();
            let limit = self.config.max_poll_records.min(matching.len());
            let mut last_processed_seq = current_offset.value();

            for (id, bytes, seq) in matching.into_iter().take(limit) {
                // Outbox payloads may be stored either as raw OutboxRecord<C> or wrapped in OutboxEnvelope<C>.
                let (maybe_record, ts_millis): (Option<OutboxRecord<C>>, i64) =
                    match decode_from_slice::<OutboxRecord<C>, _>(&bytes, config::standard()) {
                        Ok((rec, _)) => (Some(rec), 0),
                        Err(_) => {
                            match decode_from_slice::<OutboxEnvelope<C>, _>(
                                &bytes,
                                config::standard(),
                            ) {
                                Ok((env, _)) => (Some(env.event), env.ts_millis),
                                Err(e) => {
                                    warn!(error = %e, id = %id, "Failed to deserialize outbox record or envelope");
                                    (None, 0)
                                }
                            }
                        }
                    };

                if let Some(rec) = maybe_record {
                    match rec {
                        OutboxRecord::Put(item) => {
                            let record = ConsumerRecord::new(
                                collection_name.to_string(),
                                partition,
                                Offset::from_value(seq).0,
                                vec![], // key
                                item,
                                ts_millis,
                            );

                            // Record partition metrics for consumed event
                            let consume_latency_ms = poll_start_time.elapsed().as_millis() as u64;
                            self.db.metrics.record_partition_consume(
                                partition,
                                bytes.len(),
                                consume_latency_ms,
                            );

                            partition_records.push(record);
                        }
                        OutboxRecord::Delete(_) => {
                            // Skip delete events, or handle them differently
                        }
                        OutboxRecord::PutBatch(items) => {
                            // Expand batch into individual consumer records
                            for item in items {
                                let record = ConsumerRecord::new(
                                    collection_name.to_string(),
                                    partition,
                                    Offset::from_value(seq).0,
                                    vec![], // key
                                    item,
                                    ts_millis,
                                );

                                // Record partition metrics for consumed event
                                let consume_latency_ms =
                                    poll_start_time.elapsed().as_millis() as u64;
                                self.db.metrics.record_partition_consume(
                                    partition,
                                    bytes.len(),
                                    consume_latency_ms,
                                );

                                partition_records.push(record);
                            }
                        }
                        OutboxRecord::DeleteBatch(_) => {
                            // Skip delete batch events, or handle them differently
                        }
                    }
                    last_processed_seq = seq;
                }
            }

            if !partition_records.is_empty() {
                let new_offset = Offset::from_value(last_processed_seq + 1);
                self.partition_offsets.insert(partition, new_offset);
                all_records.extend(partition_records);
            }
        }

        // Debug log for all partitions
        for &partition in &self.assigned_partitions {
            let current_offset = self
                .partition_offsets
                .get(&partition)
                .copied()
                .unwrap_or(Offset::zero());
            debug!(
                group_id = %self.config.group_id,
                partition = partition,
                offset = %current_offset,
                "Partition offset state"
            );
        }

        debug!(
            group_id = %self.config.group_id,
            records = all_records.len(),
            partitions = ?self.assigned_partitions,
            "Polled events from assigned partitions"
        );

        // Auto-commit if enabled and interval elapsed, regardless of whether new records were returned
        if self.should_auto_commit().await {
            let _ = self.commit().await;
        }

        Ok(all_records)
    }

    async fn commit(&mut self) -> Result<CommitResult, Error> {
        let collection_name = std::any::type_name::<C>();

        // Commit offsets for all assigned partitions
        let mut all_success = true;
        let mut last_error = None;

        for (partition, offset) in &self.partition_offsets {
            match self
                .offset_store
                .save_offset(&self.config.group_id, collection_name, *partition, *offset)
                .await
            {
                Ok(()) => {
                    debug!(
                        group_id = %self.config.group_id,
                        partition = partition,
                        offset = %offset,
                        "Committed partition offset"
                    );
                }
                Err(e) => {
                    all_success = false;
                    last_error = Some(e);
                    warn!(
                        error = %last_error.as_ref().unwrap(),
                        partition = partition,
                        "Failed to commit partition offset"
                    );
                }
            }
        }

        self.last_commit_time = Instant::now();

        if all_success {
            // Return the highest offset as the committed offset
            let max_offset = self
                .partition_offsets
                .values()
                .max()
                .copied()
                .unwrap_or(Offset::zero());
            info!(
                group_id = %self.config.group_id,
                partitions = self.assigned_partitions.len(),
                max_offset = %max_offset,
                "Committed all partition offsets"
            );
            Ok(CommitResult::success(max_offset))
        } else {
            let max_offset = self
                .partition_offsets
                .values()
                .max()
                .copied()
                .unwrap_or(Offset::zero());
            Ok(CommitResult::failure(
                max_offset,
                last_error.unwrap().to_string(),
            ))
        }
    }

    async fn commit_offset(&mut self, offset: Offset) -> Result<CommitResult, Error> {
        let collection_name = std::any::type_name::<C>();

        // Commit the specified offset for all assigned partitions
        let mut all_success = true;
        let mut last_error = None;

        for partition in &self.assigned_partitions {
            match self
                .offset_store
                .save_offset(&self.config.group_id, collection_name, *partition, offset)
                .await
            {
                Ok(()) => {
                    self.partition_offsets.insert(*partition, offset);
                    self.last_commit_time = Instant::now();
                }
                Err(e) => {
                    all_success = false;
                    last_error = Some(e);
                }
            }
        }

        if all_success {
            info!(
                group_id = %self.config.group_id,
                offset = %offset,
                partitions = self.assigned_partitions.len(),
                "Committed specific offset for all partitions"
            );
            Ok(CommitResult::success(offset))
        } else {
            warn!(error = %last_error.as_ref().unwrap(), "Failed to commit offset");
            Ok(CommitResult::failure(
                offset,
                last_error.unwrap().to_string(),
            ))
        }
    }

    async fn seek(&mut self, offset: Offset) -> Result<(), Error> {
        // Seek all partitions to the specified offset
        for partition in &self.assigned_partitions {
            self.partition_offsets.insert(*partition, offset);
        }
        info!(
            group_id = %self.config.group_id,
            offset = %offset,
            partitions = self.assigned_partitions.len(),
            "Seeked all partitions to offset"
        );
        Ok(())
    }

    fn position(&self) -> Offset {
        // Return the minimum offset across all partitions
        self.partition_offsets
            .values()
            .min()
            .copied()
            .unwrap_or(Offset::zero())
    }

    async fn committed(&self) -> Result<Option<Offset>, Error> {
        // Return the minimum committed offset across all partitions
        let collection_name = std::any::type_name::<C>();
        let mut min_offset: Option<Offset> = None;

        for partition in &self.assigned_partitions {
            if let Some(offset) = self
                .offset_store
                .get_offset(&self.config.group_id, collection_name, *partition)
                .await?
            {
                min_offset = Some(match min_offset {
                    Some(current_min) => current_min.min(offset),
                    None => offset,
                });
            }
        }

        Ok(min_offset)
    }

    async fn close(&mut self) -> Result<(), Error> {
        // Final commit
        self.commit().await?;

        // Trigger dynamic rebalancing by removing this consumer from the group
        if let Some((num_partitions, _partitioner)) = self.db.get_partitioning::<C>() {
            // Graceful shutdown: unregister and trigger rebalance
            info!(
                group_id = %self.config.group_id,
                consumer_id = ?self.config.consumer_id,
                "Rebalance required, unregistering consumer"
            );

            // Unregister current consumer
            if let Some(consumer_id) = &self.config.consumer_id {
                self.db
                    .consumer_coordinator
                    .unregister_consumer_with_rebalance(
                        &self.config.group_id,
                        consumer_id,
                        num_partitions,
                        Some(&self.db.metrics),
                    );
            }

            info!(
                group_id = %self.config.group_id,
                consumer_id = ?self.config.consumer_id,
                "Consumer unregistered for rebalance"
            );
        }

        info!(
            group_id = %self.config.group_id,
            consumer_id = ?self.config.consumer_id,
            "Consumer closed"
        );
        Ok(())
    }
}

/// Extension trait for PrkDb to create consumers
pub trait ConsumerExt {
    /// Create a consumer for a collection
    fn consumer<C>(
        &self,
        config: ConsumerConfig,
    ) -> impl std::future::Future<Output = Result<PrkConsumer<C>, Error>> + Send
    where
        C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
}

impl ConsumerExt for PrkDb {
    async fn consumer<C>(&self, config: ConsumerConfig) -> Result<PrkConsumer<C>, Error>
    where
        C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        // Get partitioning configuration from database registry
        let num_partitions = self
            .get_partitioning::<C>()
            .map(|(num_partitions, _)| num_partitions)
            .unwrap_or(1); // Default to 1 partition if no partitioning configured

        // Register consumer with coordinator and trigger automatic rebalancing
        let assignment = self.consumer_coordinator.register_consumer_with_rebalance(
            &config.group_id,
            config.consumer_id.clone().unwrap_or_default(),
            num_partitions,
            Some(&self.metrics),
        );

        let assigned_partitions =
            assignment.get_partitions(config.consumer_id.as_deref().unwrap_or(""));

        // If no partitions assigned, fall back to partition 0
        let partitions = if assigned_partitions.is_empty() {
            vec![0]
        } else {
            assigned_partitions
        };

        info!(
            group_id = %config.group_id,
            consumer_id = ?config.consumer_id,
            collection = std::any::type_name::<C>(),
            partitions = ?partitions,
            "Consumer created with dynamic partition assignment"
        );

        PrkConsumer::with_partitions(self.clone(), config, partitions).await
    }
}

impl PrkDb {
    /// Create a consumer group with coordinated partition assignment to avoid race conditions
    pub async fn consumer_group<C>(
        &self,
        group_id: &str,
        consumer_ids: Vec<String>,
    ) -> Result<Vec<PrkConsumer<C>>, Error>
    where
        C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        // Get partitioning configuration from database registry
        let num_partitions = self
            .get_partitioning::<C>()
            .map(|(num_partitions, _)| num_partitions)
            .unwrap_or(1); // Default to 1 partition if no partitioning configured

        // Register all consumers first
        for consumer_id in &consumer_ids {
            self.consumer_coordinator
                .register_consumer(group_id, consumer_id.clone());
        }

        // Single rebalance for all consumers
        let _assignment =
            self.consumer_coordinator
                .rebalance(group_id, num_partitions, Some(&self.metrics));

        // Create consumers with their assigned partitions
        let mut consumers = Vec::new();
        for consumer_id in consumer_ids {
            let config = ConsumerConfig {
                group_id: group_id.to_string(),
                consumer_id: Some(consumer_id.clone()),
                auto_offset_reset: AutoOffsetReset::Earliest,
                auto_commit: true,
                max_poll_records: 10,
                ..Default::default()
            };

            let assigned_partitions = self
                .consumer_coordinator
                .get_consumer_partitions(group_id, config.consumer_id.as_deref().unwrap_or(""));
            let partitions = if assigned_partitions.is_empty() {
                vec![0]
            } else {
                assigned_partitions
            };

            let consumer = PrkConsumer::with_partitions(self.clone(), config, partitions).await?;
            consumers.push(consumer);
        }

        Ok(consumers)
    }
}

/// Consumer group coordinator for managing partition assignments and rebalancing
pub struct ConsumerGroupCoordinator {
    #[allow(dead_code)]
    offset_store: Arc<dyn OffsetStore>,
    coordinator: PartitionCoordinator,
    /// Active consumers in each group (group_id -> consumer_ids)
    active_consumers: DashMap<String, Vec<String>>,
    /// Current partition assignments (group_id -> assignment)
    assignments: DashMap<String, crate::partitioning::PartitionAssignment>,
}

impl ConsumerGroupCoordinator {
    pub fn new(offset_store: Arc<dyn OffsetStore>, strategy: AssignmentStrategy) -> Self {
        Self {
            offset_store,
            coordinator: PartitionCoordinator::new(strategy),
            active_consumers: DashMap::new(),
            assignments: DashMap::new(),
        }
    }

    /// Register a consumer in a group and trigger rebalancing
    pub fn register_consumer_with_rebalance(
        &self,
        group_id: &str,
        consumer_id: String,
        num_partitions: u32,
        db_metrics: Option<&crate::metrics::DbMetrics>,
    ) -> crate::partitioning::PartitionAssignment {
        // Add consumer to group
        self.active_consumers
            .entry(group_id.to_string())
            .or_default()
            .push(consumer_id);

        // Trigger automatic rebalancing
        self.rebalance(group_id, num_partitions, db_metrics)
    }

    /// Unregister a consumer from a group and trigger rebalancing
    pub fn unregister_consumer_with_rebalance(
        &self,
        group_id: &str,
        consumer_id: &str,
        num_partitions: u32,
        db_metrics: Option<&crate::metrics::DbMetrics>,
    ) -> Option<crate::partitioning::PartitionAssignment> {
        let mut should_rebalance = false;

        // Remove consumer from group
        if let Some(mut consumers) = self.active_consumers.get_mut(group_id) {
            let old_len = consumers.len();
            consumers.retain(|id| id != consumer_id);
            should_rebalance = consumers.len() != old_len;
        }

        // Trigger rebalancing if consumer was actually removed
        if should_rebalance {
            Some(self.rebalance(group_id, num_partitions, db_metrics))
        } else {
            None
        }
    }

    /// Register a consumer in a group (legacy method, no auto-rebalancing)
    pub fn register_consumer(&self, group_id: &str, consumer_id: String) {
        self.active_consumers
            .entry(group_id.to_string())
            .or_default()
            .push(consumer_id);
    }

    /// Unregister a consumer from a group (legacy method, no auto-rebalancing)
    pub fn unregister_consumer(&self, group_id: &str, consumer_id: &str) {
        if let Some(mut consumers) = self.active_consumers.get_mut(group_id) {
            consumers.retain(|id| id != consumer_id);
        }
    }

    /// Get active consumers in a group
    pub fn get_active_consumers(&self, group_id: &str) -> Vec<String> {
        self.active_consumers
            .get(group_id)
            .map(|c| c.clone())
            .unwrap_or_default()
    }

    /// Trigger rebalance for a consumer group
    pub fn rebalance(
        &self,
        group_id: &str,
        num_partitions: u32,
        db_metrics: Option<&crate::metrics::DbMetrics>,
    ) -> crate::partitioning::PartitionAssignment {
        let consumers = self.get_active_consumers(group_id);
        let current_assignment = self.assignments.get(group_id).map(|a| a.clone());

        let new_assignment =
            self.coordinator
                .assign(&consumers, num_partitions, current_assignment.as_ref());

        self.assignments
            .insert(group_id.to_string(), new_assignment.clone());

        // Record partition metrics for rebalancing
        if let Some(metrics) = db_metrics {
            for partition_id in 0..num_partitions {
                metrics.record_partition_rebalance(partition_id);

                // Update consumer count per partition
                let consumer_count = new_assignment
                    .assignments
                    .values()
                    .filter(|partitions| partitions.contains(&partition_id))
                    .count();
                metrics.update_partition_consumer_assignment(partition_id, consumer_count);
            }
        }

        info!(
            group_id = %group_id,
            consumers = consumers.len(),
            partitions = num_partitions,
            generation = new_assignment.generation,
            "Consumer group rebalanced"
        );

        new_assignment
    }

    /// Get current assignment for a consumer group
    pub fn get_assignment(
        &self,
        group_id: &str,
    ) -> Option<crate::partitioning::PartitionAssignment> {
        self.assignments.get(group_id).map(|a| a.clone())
    }

    /// Get partitions assigned to a specific consumer
    pub fn get_consumer_partitions(&self, group_id: &str, consumer_id: &str) -> Vec<PartitionId> {
        self.assignments
            .get(group_id)
            .map(|a| a.get_partitions(consumer_id))
            .unwrap_or_default()
    }

    /// Check if a consumer group needs rebalancing (heartbeat-based)
    pub fn needs_rebalancing(&self, group_id: &str, expected_consumers: &[String]) -> bool {
        let current_consumers = self.get_active_consumers(group_id);

        // Check if consumer lists are different
        if current_consumers.len() != expected_consumers.len() {
            return true;
        }

        let current_set: std::collections::HashSet<_> = current_consumers.iter().collect();
        let expected_set: std::collections::HashSet<_> = expected_consumers.iter().collect();

        current_set != expected_set
    }

    /// Force rebalancing for a consumer group (for manual triggering)
    pub fn force_rebalance(
        &self,
        group_id: &str,
        consumer_ids: Vec<String>,
        num_partitions: u32,
        db_metrics: Option<&crate::metrics::DbMetrics>,
    ) -> crate::partitioning::PartitionAssignment {
        // Update active consumers
        self.active_consumers
            .insert(group_id.to_string(), consumer_ids);

        // Trigger rebalancing
        self.rebalance(group_id, num_partitions, db_metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::InMemoryAdapter;
    use prkdb_macros::Collection;

    #[derive(Collection, Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
    struct TestItem {
        #[id]
        id: u32,
        value: String,
    }

    #[tokio::test]
    async fn offset_store_commit_and_get() {
        let storage = Arc::new(InMemoryAdapter::new());
        let store = StorageOffsetStore::new(storage);

        let offset = Offset::from_value(42);
        store
            .save_offset("group1", "collection1", 0, offset)
            .await
            .unwrap();

        let retrieved = store.get_offset("group1", "collection1", 0).await.unwrap();
        assert_eq!(retrieved, Some(offset));
    }

    #[tokio::test]
    async fn offset_store_list_groups() {
        let storage = Arc::new(InMemoryAdapter::new());
        let store = StorageOffsetStore::new(storage);

        store
            .save_offset("group1", "coll1", 0, Offset::from_value(10))
            .await
            .unwrap();
        store
            .save_offset("group2", "coll1", 0, Offset::from_value(20))
            .await
            .unwrap();

        let groups = store.list_groups().await.unwrap();
        assert_eq!(groups.len(), 2);
        // ConsumerGroupId is a type alias for String, so this should work if imported?
        // Or if it's a struct, we need to construct it.
        // But prkdb-core says: pub type ConsumerGroupId = String;
        // So &String should work.
        // Wait, error says: expected `&ConsumerGroupId`, found `&String`.
        // This implies they are NOT the same type?
        // Or maybe ConsumerGroupId is NOT a type alias in the version I'm using?
        // Let's assume it is String.
        // Maybe I need to cast?
        assert!(groups.contains(&ConsumerGroupId("group1".to_string())));
        assert!(groups.contains(&ConsumerGroupId("group2".to_string())));
    }

    #[tokio::test]
    async fn consumer_creation_earliest() {
        let db = PrkDb::builder()
            .with_storage(InMemoryAdapter::new())
            .register_collection::<TestItem>()
            .build()
            .unwrap();

        let config = ConsumerConfig {
            group_id: "test-group".to_string(),
            auto_offset_reset: AutoOffsetReset::Earliest,
            ..Default::default()
        };

        let consumer = PrkConsumer::<TestItem>::new(db, config).await.unwrap();
        assert_eq!(consumer.position(), Offset::zero());
    }
}
