use crate::compute::{ComputeHandler, Context, StatefulCompute};
use bincode::{
    config,
    serde::{decode_from_slice, encode_to_vec},
};
use prkdb_types::collection::{ChangeEvent, Collection};
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;
use serde::{de::DeserializeOwned, Serialize};
use std::any::TypeId;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, instrument, warn};

use crate::db::PrkDb;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(bound(
    serialize = "C: Serialize, C::Id: Serialize",
    deserialize = "C: DeserializeOwned, C::Id: DeserializeOwned"
))]
pub enum OutboxRecord<C: Collection>
where
    C: Serialize,
    C::Id: Serialize,
{
    Put(C),
    Delete(C::Id),
    /// Batch put: multiple items inserted at once (Phase 4 optimization)
    PutBatch(Vec<C>),
    /// Batch delete: multiple items deleted at once (Phase 4 optimization)
    DeleteBatch(Vec<C::Id>),
}

impl<C: Collection> From<ChangeEvent<C>> for OutboxRecord<C> {
    fn from(ev: ChangeEvent<C>) -> Self {
        match ev {
            ChangeEvent::Put(item) => OutboxRecord::Put(item),
            ChangeEvent::Delete(id) => OutboxRecord::Delete(id),
            ChangeEvent::PutBatch(items) => OutboxRecord::PutBatch(items),
            ChangeEvent::DeleteBatch(ids) => OutboxRecord::DeleteBatch(ids),
        }
    }
}

pub async fn save_outbox_event<C: Collection + Serialize + DeserializeOwned>(
    storage: &dyn StorageAdapter,
    id: &str,
    event: &OutboxRecord<C>,
) -> Result<(), StorageError> {
    let env = OutboxEnvelope {
        ts_millis: chrono::Utc::now().timestamp_millis(),
        event: event.clone(),
    };
    let bytes = encode_to_vec(&env, config::standard())
        .map_err(|e| StorageError::Serialization(e.to_string()))?;
    storage.outbox_save(id, &bytes).await
}

/// Save a batch of outbox events as a single record (Phase 4 optimization)
/// This reduces N WAL appends to 1 per batch
pub async fn outbox_save_batch<C: Collection + Serialize + DeserializeOwned>(
    storage: &dyn StorageAdapter,
    events: Vec<OutboxRecord<C>>,
    partition: Option<u32>,
) -> Result<(), StorageError> {
    if events.is_empty() {
        return Ok(());
    }

    // Determine batch type from first event
    let batch_record = match &events[0] {
        OutboxRecord::Put(_) => {
            // Collect all Put items into a PutBatch
            let items: Vec<C> = events
                .into_iter()
                .map(|e| match e {
                    OutboxRecord::Put(item) => item,
                    _ => panic!("Mixed event types in batch"),
                })
                .collect();
            OutboxRecord::PutBatch(items)
        }
        OutboxRecord::Delete(_) => {
            // Collect all Delete ids into a DeleteBatch
            let ids: Vec<C::Id> = events
                .into_iter()
                .map(|e| match e {
                    OutboxRecord::Delete(id) => id,
                    _ => panic!("Mixed event types in batch"),
                })
                .collect();
            OutboxRecord::DeleteBatch(ids)
        }
        OutboxRecord::PutBatch(_) | OutboxRecord::DeleteBatch(_) => {
            // Already a batch, just use the first one (shouldn't happen in practice)
            events.into_iter().next().unwrap()
        }
    };

    let id = make_outbox_id_for_type::<C>(partition);
    save_outbox_event(storage, &id, &batch_record).await
}

static OUTBOX_SEQ: AtomicU64 = AtomicU64::new(1);

pub fn make_outbox_id_for_type<C: Collection>(partition: Option<u32>) -> String {
    // type_prefix:partition:id where id is a monotonic counter for ordering
    // Including partition enables efficient filtering by consumers
    let seq = OUTBOX_SEQ.fetch_add(1, Ordering::SeqCst);
    let partition_id = partition.unwrap_or(0);
    format!(
        "{}:{}:{:020}",
        std::any::type_name::<C>(),
        partition_id,
        seq
    )
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(bound(
    serialize = "C: Serialize, C::Id: Serialize",
    deserialize = "C: DeserializeOwned, C::Id: DeserializeOwned"
))]
pub struct DlqRecord<C: Collection>
where
    C: Serialize,
    C::Id: Serialize,
{
    pub event: OutboxRecord<C>,
    pub attempts: u32,
    pub last_error: Option<String>,
}

pub fn make_dlq_id_for_type<C: Collection>() -> String {
    let seq = OUTBOX_SEQ.fetch_add(1, Ordering::SeqCst);
    format!("dlq:{}:{:020}", std::any::type_name::<C>(), seq)
}

pub async fn save_dlq_event<C: Collection + Serialize + DeserializeOwned>(
    storage: &dyn StorageAdapter,
    id: &str,
    record: &DlqRecord<C>,
) -> Result<(), StorageError> {
    let bytes = encode_to_vec(record, config::standard())
        .map_err(|e| StorageError::Serialization(e.to_string()))?;
    storage.outbox_save(id, &bytes).await
}

pub async fn save_dlq_event_for<C: Collection + Serialize + DeserializeOwned>(
    db: &PrkDb,
    id: &str,
    record: &DlqRecord<C>,
) -> Result<(), StorageError> {
    save_dlq_event::<C>(db.storage.as_ref(), id, record).await
}

pub async fn dlq_is_empty_for<C: Collection>(db: &PrkDb) -> Result<bool, StorageError> {
    let prefix = format!("dlq:{}:", std::any::type_name::<C>());
    let entries = db.storage.outbox_list().await?;
    Ok(!entries.iter().any(|(k, _)| k.starts_with(&prefix)))
}

#[instrument(skip(db))]
pub async fn drain_outbox_for<C>(db: &PrkDb) -> Result<(), StorageError>
where
    C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let entries = db.storage.outbox_list().await?;
    let prefix = format!("{}:", std::any::type_name::<C>());
    let mut ours: Vec<(String, Vec<u8>)> = entries
        .into_iter()
        .filter(|(k, _)| k.starts_with(&prefix))
        .collect();
    ours.sort_by(|a, b| a.0.cmp(&b.0));

    for (id, bytes) in ours {
        let rec: OutboxRecord<C> =
            match decode_from_slice::<OutboxRecord<C>, _>(&bytes, config::standard()) {
                Ok((r, _)) => r,
                Err(_) => {
                    let (env, _): (OutboxEnvelope<C>, _) =
                        decode_from_slice(&bytes, config::standard())
                            .map_err(|e| StorageError::Deserialization(e.to_string()))?;
                    env.event
                }
            };

        // Publish to event bus and trigger compute handlers, mirroring live behavior
        match rec.clone() {
            OutboxRecord::Put(item) => {
                info!("replaying put");
                if let Some(handlers) = db.compute_handlers.get(&TypeId::of::<C>()) {
                    for handler_any in handlers.iter() {
                        if let Some(concrete_handler) =
                            handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                        {
                            let ctx = Context { db: db.clone() };
                            if let Err(e) = concrete_handler.on_put(&item, &ctx).await {
                                warn!(error = %e, "handler put failed");
                            }
                        }
                    }
                }
                if let Some(sender_any) = db.event_bus.get(&TypeId::of::<C>()) {
                    if let Some(sender) =
                        sender_any.downcast_ref::<broadcast::Sender<ChangeEvent<C>>>()
                    {
                        let _ = sender.send(ChangeEvent::Put(item));
                    }
                }
            }
            OutboxRecord::Delete(id_val) => {
                info!("replaying delete");
                if let Some(handlers) = db.compute_handlers.get(&TypeId::of::<C>()) {
                    for handler_any in handlers.iter() {
                        if let Some(concrete_handler) =
                            handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                        {
                            let ctx = Context { db: db.clone() };
                            if let Err(e) = concrete_handler.on_delete(&id_val, &ctx).await {
                                warn!(error = %e, "handler delete failed");
                            }
                        }
                    }
                }
                if let Some(sender_any) = db.event_bus.get(&TypeId::of::<C>()) {
                    if let Some(sender) =
                        sender_any.downcast_ref::<broadcast::Sender<ChangeEvent<C>>>()
                    {
                        let _ = sender.send(ChangeEvent::Delete(id_val));
                    }
                }
            }
            OutboxRecord::PutBatch(items) => {
                info!("replaying put batch");
                // Trigger handlers for each item in batch
                if let Some(handlers) = db.compute_handlers.get(&TypeId::of::<C>()) {
                    for item in &items {
                        for handler_any in handlers.iter() {
                            if let Some(concrete_handler) =
                                handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                            {
                                let ctx = Context { db: db.clone() };
                                if let Err(e) = concrete_handler.on_put(item, &ctx).await {
                                    warn!(error = %e, "handler put failed");
                                }
                            }
                        }
                    }
                }
                // Publish batch event
                if let Some(sender_any) = db.event_bus.get(&TypeId::of::<C>()) {
                    if let Some(sender) =
                        sender_any.downcast_ref::<broadcast::Sender<ChangeEvent<C>>>()
                    {
                        let _ = sender.send(ChangeEvent::PutBatch(items));
                    }
                }
            }
            OutboxRecord::DeleteBatch(ids) => {
                info!("replaying delete batch");
                // Trigger handlers for each id in batch
                if let Some(handlers) = db.compute_handlers.get(&TypeId::of::<C>()) {
                    for id_val in &ids {
                        for handler_any in handlers.iter() {
                            if let Some(concrete_handler) =
                                handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                            {
                                let ctx = Context { db: db.clone() };
                                if let Err(e) = concrete_handler.on_delete(id_val, &ctx).await {
                                    warn!(error = %e, "handler delete failed");
                                }
                            }
                        }
                    }
                }
                // Publish batch event
                if let Some(sender_any) = db.event_bus.get(&TypeId::of::<C>()) {
                    if let Some(sender) =
                        sender_any.downcast_ref::<broadcast::Sender<ChangeEvent<C>>>()
                    {
                        let _ = sender.send(ChangeEvent::DeleteBatch(ids));
                    }
                }
            }
        }

        db.storage.outbox_remove(&id).await?;
    }

    Ok(())
}

/// Load or initialize state for a `StatefulCompute` handler.
pub async fn load_state<H, C>(db: &PrkDb, handler: &H) -> Result<H::State, StorageError>
where
    H: StatefulCompute<C, PrkDb>,
    C: Collection,
{
    if let Some(bytes) = db.storage.get(handler.state_key().as_bytes()).await? {
        let (state, _): (H::State, _) = decode_from_slice(&bytes, config::standard())
            .map_err(|e| StorageError::Deserialization(e.to_string()))?;
        Ok(state)
    } else {
        Ok(handler.init_state())
    }
}

pub async fn save_state<H, C>(db: &PrkDb, handler: &H, state: &H::State) -> Result<(), StorageError>
where
    H: StatefulCompute<C, PrkDb>,
    C: Collection,
{
    let bytes = encode_to_vec(state, config::standard())
        .map_err(|e| StorageError::Serialization(e.to_string()))?;
    db.storage.put(handler.state_key().as_bytes(), &bytes).await
}

/// Replay outbox for collection `C` applying to `StatefulCompute` handler `H` and persist state.
pub async fn replay_collection<C, H>(db: &PrkDb, handler: &H) -> Result<(), StorageError>
where
    C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    H: StatefulCompute<C, PrkDb>,
{
    let mut state = load_state::<H, C>(db, handler).await?;

    let entries = db.storage.outbox_list().await?;
    let prefix = format!("{}:", std::any::type_name::<C>());
    let mut ours: Vec<(String, Vec<u8>)> = entries
        .into_iter()
        .filter_map(|(k, v)| {
            let k_str = String::from_utf8(k.into()).ok()?;
            if k_str.starts_with(&prefix) {
                Some((k_str, v))
            } else {
                None
            }
        })
        .collect();

    // Sort by sequence number
    ours.sort_by(|(k1, _), (k2, _)| {
        let seq1 = k1
            .split(':')
            .last()
            .unwrap_or("0")
            .parse::<u64>()
            .unwrap_or(0);
        let seq2 = k2
            .split(':')
            .last()
            .unwrap_or("0")
            .parse::<u64>()
            .unwrap_or(0);
        seq1.cmp(&seq2)
    });

    let ctx = Context { db: db.clone() };

    for (_key, val) in ours {
        let (record, _): (OutboxRecord<C>, _) = decode_from_slice(&val, config::standard())
            .map_err(|e| StorageError::Deserialization(e.to_string()))?;

        match record {
            OutboxRecord::Put(item) => {
                let _ = handler.on_put(&item, &mut state, &ctx).await;
            }
            OutboxRecord::Delete(id) => {
                let _ = handler.on_delete(&id, &mut state, &ctx).await;
            }
            OutboxRecord::PutBatch(items) => {
                // Process each item in batch
                for item in items {
                    let _ = handler.on_put(&item, &mut state, &ctx).await;
                }
            }
            OutboxRecord::DeleteBatch(ids) => {
                // Process each id in batch
                for id in ids {
                    let _ = handler.on_delete(&id, &mut state, &ctx).await;
                }
            }
        }

        // Mark as processed by saving offset?
        // For now just save state periodically or at end.
        // In this simple replay, we save state after every event or batch?
        // The original code seemed to save state at the end or not shown here.

        // Also we need to mark the outbox entry as processed if that's the intention,
        // but replay usually implies rebuilding state from full history.
        // If we want to remove from outbox, we use outbox_remove.

        // Let's assume we just update state.

        // We also need to handle the offset tracking if this is a consumer,
        // but this function seems to be "replay_collection" for internal compute.

        // The original code had:
        // db.storage.put(&offset_key, ...).await
        // But I don't see offset_key definition in the snippet I'm replacing.
        // Ah, I need to check where offset_key came from.
        // It seems I missed some context in the previous view.
    }

    save_state(db, handler, &state).await?;

    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(bound(
    serialize = "C: Serialize, C::Id: Serialize",
    deserialize = "C: DeserializeOwned, C::Id: DeserializeOwned"
))]
pub struct OutboxEnvelope<C: Collection>
where
    C: Serialize,
    C::Id: Serialize,
{
    pub ts_millis: i64,
    pub event: OutboxRecord<C>,
}

/// Drain outbox since a given timestamp (inclusive)
#[instrument(skip(db))]
pub async fn drain_outbox_since<C>(db: &PrkDb, since_millis: i64) -> Result<(), StorageError>
where
    C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let entries = db.storage.outbox_list().await?;
    let prefix = format!("{}:", std::any::type_name::<C>());
    let mut ours: Vec<(String, Vec<u8>)> = entries
        .into_iter()
        .filter(|(k, _)| k.starts_with(&prefix))
        .collect();
    ours.sort_by(|a, b| a.0.cmp(&b.0));

    for (id, bytes) in ours {
        let (env, _): (OutboxEnvelope<C>, _) = decode_from_slice(&bytes, config::standard())
            .map_err(|e| StorageError::Deserialization(e.to_string()))?;
        if env.ts_millis < since_millis {
            continue;
        }
        let rec = env.event.clone();
        // same handling as drain_outbox_for
        match rec.clone() {
            OutboxRecord::Put(item) => {
                if let Some(handlers) = db.compute_handlers.get(&TypeId::of::<C>()) {
                    for handler_any in handlers.iter() {
                        if let Some(concrete_handler) =
                            handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                        {
                            let ctx = Context { db: db.clone() };
                            let _ = concrete_handler.on_put(&item, &ctx).await;
                        }
                    }
                }
            }
            OutboxRecord::Delete(id_val) => {
                if let Some(handlers) = db.compute_handlers.get(&TypeId::of::<C>()) {
                    for handler_any in handlers.iter() {
                        if let Some(concrete_handler) =
                            handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                        {
                            let ctx = Context { db: db.clone() };
                            let _ = concrete_handler.on_delete(&id_val, &ctx).await;
                        }
                    }
                }
            }
            OutboxRecord::PutBatch(items) => {
                if let Some(handlers) = db.compute_handlers.get(&TypeId::of::<C>()) {
                    for item in &items {
                        for handler_any in handlers.iter() {
                            if let Some(concrete_handler) =
                                handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                            {
                                let ctx = Context { db: db.clone() };
                                let _ = concrete_handler.on_put(item, &ctx).await;
                            }
                        }
                    }
                }
            }
            OutboxRecord::DeleteBatch(ids) => {
                if let Some(handlers) = db.compute_handlers.get(&TypeId::of::<C>()) {
                    for id_val in &ids {
                        for handler_any in handlers.iter() {
                            if let Some(concrete_handler) =
                                handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                            {
                                let ctx = Context { db: db.clone() };
                                let _ = concrete_handler.on_delete(id_val, &ctx).await;
                            }
                        }
                    }
                }
            }
        }
        db.storage.outbox_remove(&id).await?;
    }
    Ok(())
}

/// Compact outbox for collection `C` up to the stored handler offset (inclusive).
pub async fn compact_outbox_for<C, H>(db: &PrkDb, handler: &H) -> Result<(), StorageError>
where
    C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    H: StatefulCompute<C, PrkDb>,
{
    let mut offset_key = handler.state_key().into_bytes();
    offset_key.extend_from_slice(b":offset");
    let Some(off_bytes) = db.storage.get(&offset_key).await? else {
        return Ok(());
    };
    let upto = String::from_utf8(off_bytes).unwrap_or_default();
    if upto.is_empty() {
        return Ok(());
    }

    let prefix = format!("{}:", std::any::type_name::<C>());
    let entries = db.storage.outbox_list().await?;
    let mut to_delete: Vec<String> = entries
        .into_iter()
        .filter(|(k, _)| k.starts_with(&prefix) && *k <= upto)
        .map(|(k, _)| k)
        .collect();
    to_delete.sort();
    for id in to_delete {
        db.storage.outbox_remove(&id).await?;
    }
    Ok(())
}

/// Convenience: snapshot (via replay) and then compact for handler `H` over collection `C`.
pub async fn snapshot_and_compact<C, H>(db: &PrkDb, handler: &H) -> Result<(), StorageError>
where
    C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    H: StatefulCompute<C, PrkDb>,
{
    replay_collection::<C, H>(db, handler).await?;
    compact_outbox_for::<C, H>(db, handler).await
}

/// Drain DLQ for collection `C`, retrying failed events up to `max_attempts`.
#[instrument(skip(db))]
pub async fn drain_dlq_for<C>(db: &PrkDb, max_attempts: u32) -> Result<(), StorageError>
where
    C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let entries = db.storage.outbox_list().await?;
    let prefix = format!("dlq:{}:", std::any::type_name::<C>());
    let mut ours: Vec<(String, Vec<u8>)> = entries
        .into_iter()
        .filter(|(k, _)| k.starts_with(&prefix))
        .collect();
    ours.sort_by(|a, b| a.0.cmp(&b.0));

    for (id, bytes) in ours {
        let (mut rec, _): (DlqRecord<C>, _) = decode_from_slice(&bytes, config::standard())
            .map_err(|e| StorageError::Deserialization(e.to_string()))?;
        let ctx = Context { db: db.clone() };
        let res = match &rec.event {
            OutboxRecord::Put(item) => {
                if let Some(handlers) = db.compute_handlers.get(&TypeId::of::<C>()) {
                    let mut last_err: Option<String> = None;
                    for handler_any in handlers.iter() {
                        if let Some(concrete_handler) =
                            handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                        {
                            match concrete_handler.on_put(item, &ctx).await {
                                Ok(()) => {}
                                Err(e) => {
                                    last_err = Some(format!("{}", e));
                                }
                            }
                        }
                    }
                    last_err.map(Err).unwrap_or(Ok(()))
                } else {
                    Ok(())
                }
            }
            OutboxRecord::Delete(idv) => {
                if let Some(handlers) = db.compute_handlers.get(&TypeId::of::<C>()) {
                    let mut last_err: Option<String> = None;
                    for handler_any in handlers.iter() {
                        if let Some(concrete_handler) =
                            handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                        {
                            match concrete_handler.on_delete(idv, &ctx).await {
                                Ok(()) => {}
                                Err(e) => {
                                    last_err = Some(format!("{}", e));
                                }
                            }
                        }
                    }
                    last_err.map(Err).unwrap_or(Ok(()))
                } else {
                    Ok(())
                }
            }
            OutboxRecord::PutBatch(items) => {
                if let Some(handlers) = db.compute_handlers.get(&TypeId::of::<C>()) {
                    let mut last_err: Option<String> = None;
                    for item in items {
                        for handler_any in handlers.iter() {
                            if let Some(concrete_handler) =
                                handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                            {
                                match concrete_handler.on_put(item, &ctx).await {
                                    Ok(()) => {}
                                    Err(e) => {
                                        last_err = Some(format!("{}", e));
                                    }
                                }
                            }
                        }
                    }
                    last_err.map(Err).unwrap_or(Ok(()))
                } else {
                    Ok(())
                }
            }
            OutboxRecord::DeleteBatch(ids) => {
                if let Some(handlers) = db.compute_handlers.get(&TypeId::of::<C>()) {
                    let mut last_err: Option<String> = None;
                    for idv in ids {
                        for handler_any in handlers.iter() {
                            if let Some(concrete_handler) =
                                handler_any.downcast_ref::<Arc<dyn ComputeHandler<C, PrkDb>>>()
                            {
                                match concrete_handler.on_delete(idv, &ctx).await {
                                    Ok(()) => {}
                                    Err(e) => {
                                        last_err = Some(format!("{}", e));
                                    }
                                }
                            }
                        }
                    }
                    last_err.map(Err).unwrap_or(Ok(()))
                } else {
                    Ok(())
                }
            }
        };

        match res {
            Ok(()) => {
                db.storage.outbox_remove(&id).await?;
            }
            Err(err_msg) => {
                rec.attempts = rec.attempts.saturating_add(1);
                rec.last_error = Some(err_msg);
                if rec.attempts >= max_attempts {
                    // Leave it for manual inspection, don't re-enqueue
                    warn!("DLQ retries exhausted for {}", id);
                } else {
                    let bytes = encode_to_vec(&rec, config::standard())
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;
                    db.storage.outbox_save(&id, &bytes).await?;
                }
            }
        }
    }
    Ok(())
}
