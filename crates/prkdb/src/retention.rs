//! Retention policies for automatic event cleanup
//!
//! This module provides time-based and size-based retention policies
//! to automatically clean up old events from the outbox.
//!
//! # Example
//!
//! ```rust,no_run
//! use prkdb::retention::{RetentionPolicy, RetentionConfig};
//! use std::time::Duration;
//!
//! let policy = RetentionPolicy::new(RetentionConfig {
//!     time_to_live: Some(Duration::from_secs(86400)), // 1 day
//!     max_events: Some(10000),
//!     min_events_to_keep: 1000,
//!     ..Default::default()
//! });
//! ```

use crate::db::PrkDb;
use crate::outbox::OutboxEnvelope;
use bincode::{config, serde::decode_from_slice};
use prkdb_core::collection::Collection;
use prkdb_core::error::StorageError;
use serde::{de::DeserializeOwned, Serialize};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Retention policy configuration
#[derive(Debug, Clone)]
pub struct RetentionConfig {
    /// Time-to-live for events (None = no time-based retention)
    pub time_to_live: Option<Duration>,
    
    /// Maximum number of events to keep (None = no size-based retention)
    pub max_events: Option<usize>,
    
    /// Minimum number of events to always keep, even if expired
    pub min_events_to_keep: usize,
    
    /// How often to run cleanup
    pub cleanup_interval: Duration,
    
    /// Maximum number of events to delete in one cleanup cycle
    pub max_delete_batch: usize,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            time_to_live: Some(Duration::from_secs(7 * 24 * 3600)), // 7 days
            max_events: Some(1_000_000),
            min_events_to_keep: 1000,
            cleanup_interval: Duration::from_secs(300), // 5 minutes
            max_delete_batch: 10000,
        }
    }
}

/// Retention policy for event cleanup
pub struct RetentionPolicy {
    config: RetentionConfig,
}

impl RetentionPolicy {
    /// Create a new retention policy
    pub fn new(config: RetentionConfig) -> Self {
        Self { config }
    }

    /// Apply retention policy to a collection
    pub async fn apply<C>(&self, db: &PrkDb) -> Result<RetentionStats, StorageError>
    where
        C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let collection_name = std::any::type_name::<C>();
        let entries = db.storage.outbox_list().await?;
        let prefix = format!("{}:", collection_name);
        
        // Filter entries for this collection
        let mut matching: Vec<(String, Vec<u8>, i64, u64)> = entries
            .into_iter()
            .filter(|(id, _)| id.starts_with(&prefix))
            .filter_map(|(id, bytes)| {
                // Decode envelope to get timestamp
                let envelope: OutboxEnvelope<C> = match decode_from_slice(&bytes, config::standard()) {
                    Ok((env, _)) => env,
                    Err(_) => return None,
                };
                
                // Extract sequence number from ID format: "type_name:00000000000000000123"
                let parts: Vec<&str> = id.split(':').collect();
                let seq = if parts.len() >= 2 {
                    parts[1].parse::<u64>().ok()?
                } else {
                    return None;
                };
                
                Some((id, bytes, envelope.ts_millis, seq))
            })
            .collect();
        
        // Sort by sequence number (oldest first)
        matching.sort_by_key(|(_, _, _, seq)| *seq);
        
        let total_events = matching.len();
        let mut to_delete = Vec::new();
        let now_millis = chrono::Utc::now().timestamp_millis();
        
        // Apply time-based retention
        if let Some(ttl) = self.config.time_to_live {
            let ttl_millis = ttl.as_millis() as i64;
            let cutoff_time = now_millis - ttl_millis;
            
            for (id, _, ts, _) in &matching {
                if *ts < cutoff_time {
                    to_delete.push(id.clone());
                }
            }
        }
        
        // Apply size-based retention
        if let Some(max_events) = self.config.max_events {
            if total_events > max_events {
                let excess = total_events - max_events;
                for (id, _, _, _) in matching.iter().take(excess) {
                    if !to_delete.contains(id) {
                        to_delete.push(id.clone());
                    }
                }
            }
        }
        
        // Ensure we keep minimum events
        let events_after_deletion = total_events.saturating_sub(to_delete.len());
        if events_after_deletion < self.config.min_events_to_keep {
            let to_keep = self.config.min_events_to_keep.saturating_sub(events_after_deletion);
            to_delete.truncate(to_delete.len().saturating_sub(to_keep));
        }
        
        // Limit batch size
        to_delete.truncate(self.config.max_delete_batch);
        
        // Delete events
        let deleted_count = to_delete.len();
        for id in &to_delete {
            db.storage.outbox_remove(id).await?;
        }
        
        info!(
            collection = %collection_name,
            total = total_events,
            deleted = deleted_count,
            remaining = total_events - deleted_count,
            "Applied retention policy"
        );
        
        Ok(RetentionStats {
            total_events,
            deleted_events: deleted_count,
            remaining_events: total_events - deleted_count,
        })
    }

    /// Spawn a background task that periodically applies retention
    pub fn spawn_cleanup_task<C>(
        self,
        db: PrkDb,
    ) -> JoinHandle<()>
    where
        C: Collection + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        C::Id: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let interval = self.config.cleanup_interval;
        
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            
            loop {
                ticker.tick().await;
                
                match self.apply::<C>(&db).await {
                    Ok(stats) => {
                        info!(
                            deleted = stats.deleted_events,
                            remaining = stats.remaining_events,
                            "Retention cleanup completed"
                        );
                    }
                    Err(e) => {
                        warn!(error = %e, "Retention cleanup failed");
                    }
                }
            }
        })
    }
}

/// Statistics from retention policy application
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionStats {
    /// Total events before cleanup
    pub total_events: usize,
    /// Number of events deleted
    pub deleted_events: usize,
    /// Number of events remaining
    pub remaining_events: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::InMemoryAdapter;
    use prkdb_macros::Collection;

    #[derive(Collection, Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
    struct TestEvent {
        #[id]
        id: u32,
        value: String,
    }

    #[test]
    fn retention_config_defaults() {
        let config = RetentionConfig::default();
        assert!(config.time_to_live.is_some());
        assert!(config.max_events.is_some());
        assert_eq!(config.min_events_to_keep, 1000);
        assert_eq!(config.cleanup_interval, Duration::from_secs(300));
    }


    #[tokio::test]
    async fn retention_policy_no_deletion_when_under_limit() {
        let db = PrkDb::builder()
            .with_storage(InMemoryAdapter::new())
            .register_collection::<TestEvent>()
            .build()
            .unwrap();

        // Add a few events
        let collection = db.collection::<TestEvent>();
        for i in 0..5 {
            collection
                .put(TestEvent {
                    id: i,
                    value: format!("test{}", i),
                })
                .await
                .unwrap();
        }

        let policy = RetentionPolicy::new(RetentionConfig {
            time_to_live: None,
            max_events: Some(100),
            min_events_to_keep: 0,
            ..Default::default()
        });

        let stats = policy.apply::<TestEvent>(&db).await.unwrap();
        assert_eq!(stats.deleted_events, 0);
        assert_eq!(stats.remaining_events, stats.total_events);
    }

    #[tokio::test]
    async fn retention_policy_size_based() {
        let db = PrkDb::builder()
            .with_storage(InMemoryAdapter::new())
            .register_collection::<TestEvent>()
            .build()
            .unwrap();

        // Add many events
        let collection = db.collection::<TestEvent>();
        for i in 0..20 {
            collection
                .put(TestEvent {
                    id: i,
                    value: format!("test{}", i),
                })
                .await
                .unwrap();
        }

        let policy = RetentionPolicy::new(RetentionConfig {
            time_to_live: None,
            max_events: Some(10),
            min_events_to_keep: 0,
            cleanup_interval: Duration::from_secs(60),
            max_delete_batch: 100,
        });

        let stats = policy.apply::<TestEvent>(&db).await.unwrap();
        assert_eq!(stats.total_events, 20);
        assert_eq!(stats.deleted_events, 10);
        assert_eq!(stats.remaining_events, 10);
    }

    #[tokio::test]
    async fn retention_policy_respects_min_events() {
        let db = PrkDb::builder()
            .with_storage(InMemoryAdapter::new())
            .register_collection::<TestEvent>()
            .build()
            .unwrap();

        // Add events
        let collection = db.collection::<TestEvent>();
        for i in 0..15 {
            collection
                .put(TestEvent {
                    id: i,
                    value: format!("test{}", i),
                })
                .await
                .unwrap();
        }

        let policy = RetentionPolicy::new(RetentionConfig {
            time_to_live: None,
            max_events: Some(5),
            min_events_to_keep: 10, // Should keep at least 10
            cleanup_interval: Duration::from_secs(60),
            max_delete_batch: 100,
        });

        let stats = policy.apply::<TestEvent>(&db).await.unwrap();
        assert!(stats.remaining_events >= 10);
    }
}