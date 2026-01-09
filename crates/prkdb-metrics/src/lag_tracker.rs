//! Consumer lag tracking

use dashmap::DashMap;
use std::sync::Arc;

/// Consumer lag tracker
pub struct ConsumerLagTracker {
    /// Latest offsets per partition (collection:partition -> offset)
    latest_offsets: Arc<DashMap<String, u64>>,
    /// Consumer offsets (group:collection:partition -> offset)
    consumer_offsets: Arc<DashMap<String, u64>>,
}

impl ConsumerLagTracker {
    pub fn new() -> Self {
        Self {
            latest_offsets: Arc::new(DashMap::new()),
            consumer_offsets: Arc::new(DashMap::new()),
        }
    }

    /// Update the latest offset for a partition
    pub fn update_latest_offset(&self, collection: &str, partition: u32, offset: u64) {
        let key = format!("{}:{}", collection, partition);
        self.latest_offsets.insert(key, offset);

        crate::exporter::PARTITION_RECORDS
            .with_label_values(&[collection, &partition.to_string()])
            .set(offset as f64);
    }

    /// Update consumer offset
    pub fn update_consumer_offset(
        &self,
        group_id: &str,
        collection: &str,
        partition: u32,
        offset: u64,
    ) {
        let key = format!("{}:{}:{}", group_id, collection, partition);
        self.consumer_offsets.insert(key, offset);

        crate::exporter::CONSUMER_OFFSET
            .with_label_values(&[group_id, collection, &partition.to_string()])
            .set(offset as f64);

        // Calculate and update lag
        if let Some(lag) = self.calculate_lag(group_id, collection, partition) {
            crate::exporter::CONSUMER_LAG
                .with_label_values(&[group_id, collection, &partition.to_string()])
                .set(lag as f64);
        }
    }

    /// Calculate lag for a consumer
    pub fn calculate_lag(&self, group_id: &str, collection: &str, partition: u32) -> Option<u64> {
        let latest_key = format!("{}:{}", collection, partition);
        let consumer_key = format!("{}:{}:{}", group_id, collection, partition);

        let latest = self.latest_offsets.get(&latest_key)?;
        let consumer = self.consumer_offsets.get(&consumer_key)?;

        Some(latest.saturating_sub(*consumer))
    }

    /// Get all lags for a consumer group
    pub fn get_group_lags(&self, group_id: &str) -> Vec<(String, u32, u64)> {
        let prefix = format!("{}:", group_id);
        self.consumer_offsets
            .iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .filter_map(|entry| {
                let parts: Vec<&str> = entry.key().split(':').collect();
                if parts.len() == 3 {
                    let collection = parts[1].to_string();
                    let partition: u32 = parts[2].parse().ok()?;
                    let lag = self.calculate_lag(group_id, &collection, partition)?;
                    Some((collection, partition, lag))
                } else {
                    None
                }
            })
            .collect()
    }
}

impl Default for ConsumerLagTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lag_tracker_creation() {
        let tracker = ConsumerLagTracker::new();
        assert!(tracker.latest_offsets.is_empty());
        assert!(tracker.consumer_offsets.is_empty());
    }

    #[test]
    fn test_update_latest_offset() {
        let tracker = ConsumerLagTracker::new();
        tracker.update_latest_offset("orders", 0, 100);
        tracker.update_latest_offset("orders", 1, 200);

        let key0 = "orders:0";
        let key1 = "orders:1";
        assert_eq!(tracker.latest_offsets.get(key0).map(|r| *r), Some(100));
        assert_eq!(tracker.latest_offsets.get(key1).map(|r| *r), Some(200));
    }

    #[test]
    fn test_update_consumer_offset() {
        let tracker = ConsumerLagTracker::new();
        tracker.update_consumer_offset("group1", "orders", 0, 50);
        tracker.update_consumer_offset("group1", "orders", 1, 150);

        let key0 = "group1:orders:0";
        let key1 = "group1:orders:1";
        assert_eq!(tracker.consumer_offsets.get(key0).map(|r| *r), Some(50));
        assert_eq!(tracker.consumer_offsets.get(key1).map(|r| *r), Some(150));
    }

    #[test]
    fn test_calculate_lag() {
        let tracker = ConsumerLagTracker::new();

        // Set latest offset
        tracker.update_latest_offset("orders", 0, 100);

        // Set consumer offset
        tracker.update_consumer_offset("group1", "orders", 0, 70);

        // Calculate lag
        let lag = tracker.calculate_lag("group1", "orders", 0);
        assert_eq!(lag, Some(30));
    }

    #[test]
    fn test_calculate_lag_no_lag() {
        let tracker = ConsumerLagTracker::new();

        tracker.update_latest_offset("orders", 0, 100);
        tracker.update_consumer_offset("group1", "orders", 0, 100);

        let lag = tracker.calculate_lag("group1", "orders", 0);
        assert_eq!(lag, Some(0));
    }

    #[test]
    fn test_calculate_lag_consumer_ahead() {
        let tracker = ConsumerLagTracker::new();

        tracker.update_latest_offset("orders", 0, 100);
        tracker.update_consumer_offset("group1", "orders", 0, 150);

        // Should saturate at 0, not underflow
        let lag = tracker.calculate_lag("group1", "orders", 0);
        assert_eq!(lag, Some(0));
    }

    #[test]
    fn test_calculate_lag_missing_data() {
        let tracker = ConsumerLagTracker::new();

        // No data set
        let lag = tracker.calculate_lag("group1", "orders", 0);
        assert_eq!(lag, None);

        // Only latest set
        tracker.update_latest_offset("orders", 0, 100);
        let lag = tracker.calculate_lag("group1", "orders", 0);
        assert_eq!(lag, None);
    }

    #[test]
    fn test_get_group_lags() {
        let tracker = ConsumerLagTracker::new();

        // Set up multiple partitions
        tracker.update_latest_offset("orders", 0, 100);
        tracker.update_latest_offset("orders", 1, 200);
        tracker.update_latest_offset("users", 0, 50);

        tracker.update_consumer_offset("group1", "orders", 0, 70);
        tracker.update_consumer_offset("group1", "orders", 1, 180);
        tracker.update_consumer_offset("group1", "users", 0, 40);

        let lags = tracker.get_group_lags("group1");

        assert_eq!(lags.len(), 3);
        assert!(lags.contains(&("orders".to_string(), 0, 30)));
        assert!(lags.contains(&("orders".to_string(), 1, 20)));
        assert!(lags.contains(&("users".to_string(), 0, 10)));
    }

    #[test]
    fn test_get_group_lags_empty() {
        let tracker = ConsumerLagTracker::new();
        let lags = tracker.get_group_lags("nonexistent");
        assert!(lags.is_empty());
    }

    #[test]
    fn test_multiple_consumer_groups() {
        let tracker = ConsumerLagTracker::new();

        tracker.update_latest_offset("orders", 0, 100);

        tracker.update_consumer_offset("group1", "orders", 0, 70);
        tracker.update_consumer_offset("group2", "orders", 0, 90);

        let lag1 = tracker.calculate_lag("group1", "orders", 0);
        let lag2 = tracker.calculate_lag("group2", "orders", 0);

        assert_eq!(lag1, Some(30));
        assert_eq!(lag2, Some(10));
    }
}
