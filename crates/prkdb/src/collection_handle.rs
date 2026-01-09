use crate::batch_accumulator::BatchAccumulator;
use crate::compute::{ComputeHandler, Context};
use crate::db::PrkDb;
use crate::error::DbError;
use crate::outbox::{make_outbox_id_for_type, save_outbox_event, OutboxRecord};
use crate::partitioning::{DefaultPartitioner, PartitionId, Partitioner};
use bincode::{
    config,
    serde::{decode_from_slice, encode_to_vec},
};
use futures::future;
use prkdb_core::collection::{ChangeEvent, Collection};
use prkdb_core::error::StorageError;
use serde::{de::DeserializeOwned, Serialize};
use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Build a namespaced key for the given collection `C` and id of that collection.
/// This ensures keys for different collections don't collide.
/// Optionally includes partition information for partitioned collections.
fn get_namespaced_key<C: Collection>(
    id: &C::Id,
    partition: Option<PartitionId>,
) -> Result<Vec<u8>, StorageError>
where
    C::Id: Serialize,
{
    // simple namespacing: type name + `:` + partition (if any) + `:` + serialized id
    let id_bytes = encode_to_vec(id, config::standard())
        .map_err(|e| StorageError::Serialization(e.to_string()))?;

    let type_name = std::any::type_name::<C>();
    let mut key = if let Some(p) = partition {
        let partition_str = p.to_string();
        let capacity = type_name.len() + 1 + partition_str.len() + 1 + id_bytes.len();
        let mut k = Vec::with_capacity(capacity);
        k.extend_from_slice(type_name.as_bytes());
        k.push(b':');
        k.extend_from_slice(partition_str.as_bytes());
        k.push(b':');
        k
    } else {
        let mut k = Vec::with_capacity(type_name.len() + 1 + id_bytes.len());
        k.extend_from_slice(type_name.as_bytes());
        k.push(b':');
        k
    };

    key.extend_from_slice(&id_bytes);
    Ok(key)
}

impl<C: Collection> CollectionHandle<C> {
    fn apply_namespace(&self, key: Vec<u8>) -> Vec<u8> {
        if let Some(ns) = &self.db.namespace {
            let mut out = Vec::with_capacity(ns.len() + 1 + key.len());
            out.extend_from_slice(ns);
            out.push(b'|');
            out.extend_from_slice(&key);
            return out;
        }
        key
    }
}

#[derive(Clone)]
pub struct CollectionHandle<C: Collection> {
    db: PrkDb,
    /// Number of partitions for this collection (0 = no partitioning)
    num_partitions: u32,
    /// Partitioner for determining partition assignment
    partitioner: Option<Arc<dyn Partitioner<C::Id>>>,
    /// Optional batch accumulator for async batching window
    accumulator: Option<Arc<BatchAccumulator<C>>>,
    _marker: PhantomData<C>,
}

impl<C: Collection> CollectionHandle<C>
where
    C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(db: PrkDb) -> Self {
        // Check if partitioning is configured for this collection type
        let (num_partitions, partitioner) =
            if let Some((partitions, partitioner)) = db.get_partitioning::<C>() {
                (partitions, Some(partitioner))
            } else {
                (0, None)
            };

        Self {
            db,
            num_partitions,
            partitioner,
            accumulator: None, // Batching disabled by default
            _marker: PhantomData,
        }
    }

    /// Enable async batching window for this collection.
    ///
    /// Operations will be buffered and flushed after `config.linger_ms` or
    /// when `config.max_batch_size` is reached.
    ///
    /// # Example
    /// ```ignore
    /// use prkdb_core::batch_config::BatchConfig;
    ///
    /// let handle = db.collection::<MyItem>()
    ///     .with_batching(BatchConfig::throughput_optimized());
    /// ```
    pub fn with_batching(mut self, config: prkdb_core::batch_config::BatchConfig) -> Self {
        // Create executor that calls put_batch
        let db = self.db.clone();
        let executor = move |items: Vec<C>| {
            let db_clone = db.clone();
            async move {
                // Get collection handle and call put_batch
                let handle = db_clone.collection::<C>();
                let _results = handle.put_batch(items).await;

                // For now, assume success
                // TODO: Properly handle individual result errors
                Ok(())
            }
        };

        self.accumulator = Some(Arc::new(BatchAccumulator::new(config, executor)));
        self
    }

    /// Enable partitioning for this collection
    pub fn with_partitions(mut self, num_partitions: u32) -> Self {
        self.num_partitions = num_partitions;
        if num_partitions > 0 {
            let partitioner = Arc::new(DefaultPartitioner::new());
            self.partitioner = Some(partitioner.clone());

            // Register partitioning configuration with the database
            self.db
                .register_partitioning::<C>(num_partitions, partitioner);
        }
        self
    }

    /// Set a custom partitioner
    pub fn with_partitioner<P>(mut self, num_partitions: u32, partitioner: P) -> Self
    where
        P: Partitioner<C::Id> + 'static,
    {
        self.num_partitions = num_partitions;
        let partitioner_arc = Arc::new(partitioner);
        self.partitioner = Some(partitioner_arc.clone());

        // Register partitioning configuration with the database
        self.db
            .register_partitioning::<C>(num_partitions, partitioner_arc);
        self
    }

    /// Get the partition for a given item ID
    pub fn get_partition(&self, id: &C::Id) -> Option<PartitionId> {
        if self.num_partitions == 0 {
            return None;
        }
        self.partitioner
            .as_ref()
            .map(|p| p.partition(id, self.num_partitions))
    }

    /// Put an item into storage, run compute handlers (if any), then notify watchers.
    pub async fn put(&self, item: C) -> Result<(), DbError> {
        // If batching is enabled, route through accumulator
        if let Some(accumulator) = &self.accumulator {
            // Accumulator handles buffering and will call put_immediate when ready
            return accumulator.add_put(item).await.map_err(|e| e.into());
        }

        // Otherwise, execute immediately (current behavior)
        self.put_immediate(item).await
    }

    /// Put an item immediately, bypassing batching.
    ///
    /// This method always executes the write immediately regardless of batching configuration.
    /// Use this when you need guaranteed low-latency writes or for debugging.
    ///
    /// # When to use
    ///
    /// - **Critical updates** that cannot tolerate batching delay
    /// - **Debugging** to isolate timing issues  
    /// - **Low-latency requirements** where every millisecond counts
    ///
    /// For normal operations, prefer `put()` which benefits from batching when enabled.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // High-throughput batched writes
    /// handle.put(item1).await?;
    /// handle.put(item2).await?;
    ///
    /// // Critical write that must complete immediately
    /// handle.put_sync(critical_item).await?;
    /// ```
    pub async fn put_sync(&self, item: C) -> Result<(), DbError> {
        self.put_immediate(item).await
    }

    /// Immediate put without batching (internal helper).
    async fn put_immediate(&self, item: C) -> Result<(), DbError> {
        let partition = self.get_partition(item.id());
        let key = self.apply_namespace(get_namespaced_key::<C>(item.id(), partition)?);
        let item_bytes = encode_to_vec(&item, config::standard())
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        // Persist item and outbox record. Prefer atomic path if adapter supports it.
        let ob_id = make_outbox_id_for_type::<C>(partition);
        let outbox_bytes = encode_to_vec(OutboxRecord::Put(item.clone()), config::standard())
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        if self
            .db
            .storage
            .put_with_outbox(&key, &item_bytes, &ob_id, &outbox_bytes)
            .await
            .is_err()
        {
            self.db.storage.put(&key, &item_bytes).await?;
            save_outbox_event::<C>(
                self.db.storage.as_ref(),
                &ob_id,
                &OutboxRecord::Put(item.clone()),
            )
            .await?;
        }
        self.db.metrics().inc_puts();
        self.db.metrics().inc_outbox_saved();

        // Record partition metrics for produced event
        let partition_id = partition.unwrap_or(0);
        self.db
            .metrics()
            .record_partition_produce(partition_id, item_bytes.len() + outbox_bytes.len());

        let item_arc = Arc::new(item.clone());

        if let Some(handlers) = self.db.compute_handlers.get(&TypeId::of::<C>()) {
            let mut futures_vec = Vec::new();
            for handler_any in handlers.iter() {
                if let Some(concrete_handler) =
                    handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                {
                    let item_clone = Arc::clone(&item_arc);
                    let handler_clone = Arc::clone(concrete_handler);
                    let ctx = Context {
                        db: self.db.clone(),
                    };
                    futures_vec.push(async move {
                        let _ = handler_clone.on_put(&item_clone, &ctx).await;
                    });
                }
            }
            future::join_all(futures_vec).await;
        }

        if let Some(sender_any) = self.db.event_bus.get(&TypeId::of::<C>()) {
            if let Some(sender) = sender_any.downcast_ref::<broadcast::Sender<ChangeEvent<C>>>() {
                let _ = sender.send(ChangeEvent::Put(item_arc.as_ref().clone()));
            }
        }

        Ok(())
    }

    /// Flush any pending batched writes immediately.
    ///
    /// This is useful in tests or when you need to ensure all buffered data
    /// is persisted before proceeding. Only has an effect if batching is enabled.
    pub async fn flush(&self) -> Result<(), DbError> {
        if let Some(accumulator) = &self.accumulator {
            accumulator.flush().await?;
        }
        Ok(())
    }

    /// Batch put multiple items at once for improved throughput.
    /// Returns a vector of results, one for each item in the same order.
    pub async fn put_batch(&self, items: Vec<C>) -> Result<Vec<Result<(), DbError>>, DbError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(items.len());
        let mut storage_ops = Vec::with_capacity(items.len());
        let mut outbox_ops = Vec::with_capacity(items.len());
        let mut serialized_items = Vec::with_capacity(items.len());

        // Phase 1: Serialize all items in batch (reduces overhead)
        for item in &items {
            let partition = self.get_partition(item.id());
            let key = match get_namespaced_key::<C>(item.id(), partition) {
                Ok(k) => self.apply_namespace(k),
                Err(e) => {
                    results.push(Err(e.into()));
                    continue;
                }
            };

            let item_bytes = match encode_to_vec(&item, config::standard()) {
                Ok(b) => b,
                Err(e) => {
                    results.push(Err(StorageError::Serialization(e.to_string()).into()));
                    continue;
                }
            };

            let ob_id = make_outbox_id_for_type::<C>(partition);
            let outbox_bytes =
                match encode_to_vec(OutboxRecord::Put(item.clone()), config::standard()) {
                    Ok(b) => b,
                    Err(e) => {
                        results.push(Err(StorageError::Serialization(e.to_string()).into()));
                        continue;
                    }
                };

            storage_ops.push((key.clone(), item_bytes.clone()));
            outbox_ops.push((ob_id, outbox_bytes.clone()));
            serialized_items.push((
                item.clone(),
                partition,
                item_bytes.len() + outbox_bytes.len(),
            ));
            results.push(Ok(()));
        }

        // Phase 2: Execute storage operations with bulk API
        // Use put_many for better performance (single lock acquisition)
        self.db.storage.put_many(storage_ops.clone()).await?;

        // Save outbox events with BATCH API (Phase 4 optimization: N → 1 WAL append)
        let outbox_records: Vec<OutboxRecord<C>> = items
            .iter()
            .map(|item| OutboxRecord::Put(item.clone()))
            .collect();
        let partition = if !serialized_items.is_empty() {
            serialized_items[0].1
        } else {
            None
        };
        crate::outbox::outbox_save_batch(self.db.storage.as_ref(), outbox_records, partition)
            .await?;

        // Phase 3: Update metrics (batched)
        self.db.metrics().add_puts(items.len() as u64);
        self.db.metrics().add_outbox_saved(items.len() as u64);

        // Record partition metrics
        for (_, partition, size) in &serialized_items {
            let partition_id = partition.unwrap_or(0);
            self.db
                .metrics()
                .record_partition_produce(partition_id, *size);
        }

        // Phase 4: Run compute handlers (still per-item for correctness)
        if let Some(handlers) = self.db.compute_handlers.get(&TypeId::of::<C>()) {
            for item in &items {
                let item_arc = Arc::new(item.clone());
                let mut futures_vec = Vec::new();
                for handler_any in handlers.iter() {
                    if let Some(concrete_handler) =
                        handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                    {
                        let item_clone = Arc::clone(&item_arc);
                        let handler_clone = Arc::clone(concrete_handler);
                        let ctx = Context {
                            db: self.db.clone(),
                        };
                        futures_vec.push(async move {
                            let _ = handler_clone.on_put(&item_clone, &ctx).await;
                        });
                    }
                }
                future::join_all(futures_vec).await;
            }
        }

        // Phase 5: Notify event bus with BATCH event (single broadcast)
        if let Some(sender_any) = self.db.event_bus.get(&TypeId::of::<C>()) {
            if let Some(sender) = sender_any.downcast_ref::<broadcast::Sender<ChangeEvent<C>>>() {
                // Send single batch event instead of N individual events
                let _ = sender.send(ChangeEvent::PutBatch(items));
            }
        }

        Ok(results)
    }

    /// Batch get multiple items at once for improved throughput.
    /// Returns a vector of Option<C>, one for each ID in the same order.
    pub async fn get_batch(&self, ids: Vec<C::Id>) -> Result<Vec<Option<C>>, DbError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(ids.len());
        let mut keys = Vec::with_capacity(ids.len());

        // Phase 1: Build all keys
        for id in &ids {
            let partition = self.get_partition(id);
            let key = self.apply_namespace(get_namespaced_key::<C>(id, partition)?);
            keys.push(key);
        }

        // Phase 2: Fetch all values with bulk API
        let values = self.db.storage.get_many(keys).await?;

        // Phase 3: Deserialize results
        for value_opt in values {
            match value_opt {
                Some(item_bytes) => {
                    let item: C = decode_from_slice::<C, _>(&item_bytes, config::standard())
                        .map_err(|e| StorageError::Deserialization(e.to_string()))?
                        .0;
                    results.push(Some(item));
                }
                None => results.push(None),
            }
        }

        Ok(results)
    }

    /// Batch delete multiple items at once for improved throughput.
    /// Returns a vector of results, one for each ID in the same order.
    pub async fn delete_batch(&self, ids: Vec<C::Id>) -> Result<Vec<Result<(), DbError>>, DbError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(ids.len());
        let mut delete_ops = Vec::new();

        // Phase 1: Verify items exist and prepare delete operations
        for id in &ids {
            let partition = self.get_partition(id);
            let key = self.apply_namespace(get_namespaced_key::<C>(id, partition)?);

            let maybe_item_bytes = self.db.storage.get(&key).await?;
            if maybe_item_bytes.is_none() {
                results.push(Ok(()));
                continue;
            }

            let ob_id = make_outbox_id_for_type::<C>(partition);
            let outbox_bytes =
                match encode_to_vec(OutboxRecord::<C>::Delete(id.clone()), config::standard()) {
                    Ok(b) => b,
                    Err(e) => {
                        results.push(Err(StorageError::Serialization(e.to_string()).into()));
                        continue;
                    }
                };

            delete_ops.push((key, id.clone(), ob_id, outbox_bytes));
            results.push(Ok(()));
        }

        // Phase 2: Execute delete operations with bulk API
        let keys_to_delete: Vec<Vec<u8>> =
            delete_ops.iter().map(|(k, _, _, _)| k.clone()).collect();
        self.db.storage.delete_many(keys_to_delete).await?;

        // Save outbox events with BATCH API (Phase 4 optimization: N → 1 WAL append)
        let outbox_records: Vec<OutboxRecord<C>> = delete_ops
            .iter()
            .map(|(_, id, _, _)| OutboxRecord::Delete(id.clone()))
            .collect();
        let partition = if !delete_ops.is_empty() {
            let (_, id, _, _) = &delete_ops[0];
            self.get_partition(id)
        } else {
            None
        };
        crate::outbox::outbox_save_batch(self.db.storage.as_ref(), outbox_records, partition)
            .await?;

        // Phase 3: Update metrics (batched)
        self.db.metrics().add_deletes(delete_ops.len() as u64);
        self.db.metrics().add_outbox_saved(delete_ops.len() as u64);

        // Phase 4: Run compute handlers
        if let Some(handlers) = self.db.compute_handlers.get(&TypeId::of::<C>()) {
            for (_key, id, _ob_id, _outbox_bytes) in &delete_ops {
                let id_arc = Arc::new(id.clone());
                let mut futures_vec = Vec::new();
                for handler_any in handlers.iter() {
                    if let Some(concrete_handler) =
                        handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                    {
                        let handler_clone = Arc::clone(concrete_handler);
                        let id_clone = Arc::clone(&id_arc);
                        let ctx = Context {
                            db: self.db.clone(),
                        };
                        futures_vec.push(async move {
                            let _ = handler_clone.on_delete(&id_clone, &ctx).await;
                        });
                    }
                }
                future::join_all(futures_vec).await;
            }
        }

        // Phase 5: Notify event bus with BATCH event (single broadcast)
        if let Some(sender_any) = self.db.event_bus.get(&TypeId::of::<C>()) {
            if let Some(sender) = sender_any.downcast_ref::<broadcast::Sender<ChangeEvent<C>>>() {
                // Send single batch event instead of N individual events
                let ids: Vec<C::Id> = delete_ops.iter().map(|(_, id, _, _)| id.clone()).collect();
                let _ = sender.send(ChangeEvent::DeleteBatch(ids));
            }
        }

        Ok(results)
    }

    /// Delete an item by the collection's Id type.
    pub async fn delete(&self, id: &C::Id) -> Result<(), DbError> {
        let partition = self.get_partition(id);
        let key = self.apply_namespace(get_namespaced_key::<C>(id, partition)?);

        let maybe_item_bytes = self.db.storage.get(&key).await?;
        if maybe_item_bytes.is_none() {
            return Ok(());
        }

        // We only need the id for CDC; use provided id
        let owned_id = id.clone();
        let ob_id = make_outbox_id_for_type::<C>(partition);
        let outbox_bytes = encode_to_vec(
            OutboxRecord::<C>::Delete(owned_id.clone()),
            config::standard(),
        )
        .map_err(|e| StorageError::Serialization(e.to_string()))?;
        if self
            .db
            .storage
            .delete_with_outbox(&key, &ob_id, &outbox_bytes)
            .await
            .is_err()
        {
            self.db.storage.delete(&key).await?;
            save_outbox_event::<C>(
                self.db.storage.as_ref(),
                &ob_id,
                &OutboxRecord::Delete(owned_id.clone()),
            )
            .await?;
        }
        let id_arc = Arc::new(owned_id);
        self.db.metrics().inc_deletes();
        self.db.metrics().inc_outbox_saved();

        if let Some(handlers) = self.db.compute_handlers.get(&TypeId::of::<C>()) {
            let mut futures_vec = Vec::new();
            for handler_any in handlers.iter() {
                if let Some(concrete_handler) =
                    handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                {
                    let handler_clone = Arc::clone(concrete_handler);
                    let id_clone = Arc::clone(&id_arc);
                    let ctx = Context {
                        db: self.db.clone(),
                    };
                    futures_vec.push(async move {
                        let _ = handler_clone.on_delete(&id_clone, &ctx).await;
                    });
                }
            }
            future::join_all(futures_vec).await;
        }

        if let Some(sender_any) = self.db.event_bus.get(&TypeId::of::<C>()) {
            if let Some(sender) = sender_any.downcast_ref::<broadcast::Sender<ChangeEvent<C>>>() {
                let _ = sender.send(ChangeEvent::Delete(id_arc.as_ref().clone()));
            }
        }

        Ok(())
    }

    /// Get an item by the collection's Id type.
    pub async fn get(&self, id: &C::Id) -> Result<Option<C>, DbError> {
        let partition = self.get_partition(id);
        let key = self.apply_namespace(get_namespaced_key::<C>(id, partition)?);

        match self.db.storage.get(&key).await? {
            Some(item_bytes) => {
                let item: C = decode_from_slice::<C, _>(&item_bytes, config::standard())
                    .map_err(|e| StorageError::Deserialization(e.to_string()))?
                    .0;
                Ok(Some(item))
            }
            None => Ok(None),
        }
    }

    pub fn watch(&self) -> broadcast::Receiver<ChangeEvent<C>> {
        self.db
            .event_bus
            .get(&TypeId::of::<C>())
            .and_then(|s| {
                s.downcast_ref::<broadcast::Sender<ChangeEvent<C>>>()
                    .cloned()
            })
            .expect("Collection must be registered to be watched.")
            .subscribe()
    }

    /// Iterate over keys with the collection prefix; yields deserialized items.
    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<C>, DbError> {
        let type_prefix = std::any::type_name::<C>().as_bytes().to_vec();
        let mut key_prefix = type_prefix.clone();
        key_prefix.push(b':');
        key_prefix.extend_from_slice(prefix);
        let rows = self.db.storage.scan_prefix(&key_prefix).await?;
        let mut out = Vec::with_capacity(rows.len());
        for (_k, v) in rows {
            let (item, _): (C, _) = decode_from_slice(&v, config::standard())
                .map_err(|e| StorageError::Deserialization(e.to_string()))?;
            out.push(item);
        }
        Ok(out)
    }

    /// Range scan by encoded id bytes [start, end).
    pub async fn scan_range_by_id_bytes(
        &self,
        start_id: &C::Id,
        end_id: &C::Id,
    ) -> Result<Vec<C>, DbError> {
        let start_partition = self.get_partition(start_id);
        let end_partition = self.get_partition(end_id);
        let start_key = self.apply_namespace(get_namespaced_key::<C>(start_id, start_partition)?);
        let end_key = self.apply_namespace(get_namespaced_key::<C>(end_id, end_partition)?);
        let rows = self.db.storage.scan_range(&start_key, &end_key).await?;
        let mut out = Vec::with_capacity(rows.len());
        for (_k, v) in rows {
            let (item, _): (C, _) = decode_from_slice(&v, config::standard())
                .map_err(|e| StorageError::Deserialization(e.to_string()))?;
            out.push(item);
        }
        Ok(out)
    }
}
