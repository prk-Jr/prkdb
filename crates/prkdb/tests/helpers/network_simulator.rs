use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Network simulation for chaos testing
///
/// Allows intercepting RPC calls between nodes to simulate:
/// - Network partitions
/// - Packet delays
/// - Packet loss
#[derive(Clone)]
pub struct NetworkSimulator {
    rules: Arc<RwLock<Vec<NetworkRule>>>,
    config_path: Option<PathBuf>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum NetworkRule {
    /// Block all traffic between two nodes (bidirectional)
    Partition { node1: u64, node2: u64 },
    /// Add latency to traffic from src to dst
    Delay { src: u64, dst: u64, ms: u64 },
    /// Drop packets from src to dst with given probability (0.0-1.0)
    Drop { src: u64, dst: u64, rate: f64 },
}

impl NetworkSimulator {
    pub fn new(config_path: Option<PathBuf>) -> Self {
        Self {
            rules: Arc::new(RwLock::new(Vec::new())),
            config_path,
        }
    }

    async fn persist_rules(&self) {
        if let Some(path) = &self.config_path {
            let rules = self.rules.read().await;
            if let Ok(content) = serde_json::to_string(&*rules) {
                let _ = tokio::fs::write(path, content).await;
            }
        }
    }

    /// Add a network rule
    pub async fn add_rule(&self, rule: NetworkRule) {
        self.rules.write().await.push(rule);
        self.persist_rules().await;
    }

    /// Clear all rules
    #[allow(dead_code)]
    pub async fn clear_rules(&self) {
        self.rules.write().await.clear();
        self.persist_rules().await;
    }

    /// Create a network partition between two groups of nodes
    pub async fn partition(&self, group1: Vec<u64>, group2: Vec<u64>) {
        let mut rules = self.rules.write().await;
        for &n1 in &group1 {
            for &n2 in &group2 {
                rules.push(NetworkRule::Partition {
                    node1: n1,
                    node2: n2,
                });
            }
        }
        drop(rules); // Drop lock before persisting
        self.persist_rules().await;
    }

    /// Heal all partitions
    pub async fn heal_partitions(&self) {
        let mut rules = self.rules.write().await;
        rules.retain(|rule| !matches!(rule, NetworkRule::Partition { .. }));
        drop(rules);
        self.persist_rules().await;
    }

    /// Check if traffic from src to dst should be blocked
    pub async fn should_block(&self, src: u64, dst: u64) -> bool {
        let rules = self.rules.read().await;
        for rule in rules.iter() {
            match rule {
                NetworkRule::Partition { node1, node2 } => {
                    if (src == *node1 && dst == *node2) || (src == *node2 && dst == *node1) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    /// Get delay for traffic from src to dst
    pub async fn get_delay(&self, src: u64, dst: u64) -> Option<u64> {
        let rules = self.rules.read().await;
        for rule in rules.iter() {
            if let NetworkRule::Delay {
                src: rule_src,
                dst: rule_dst,
                ms,
            } = rule
            {
                if src == *rule_src && dst == *rule_dst {
                    return Some(*ms);
                }
            }
        }
        None
    }

    /// Check if packet from src to dst should be dropped
    #[allow(dead_code)]
    pub async fn should_drop(&self, src: u64, dst: u64) -> bool {
        let rules = self.rules.read().await;
        for rule in rules.iter() {
            if let NetworkRule::Drop {
                src: rule_src,
                dst: rule_dst,
                rate,
            } = rule
            {
                if src == *rule_src && dst == *rule_dst {
                    return rand::random::<f64>() < *rate;
                }
            }
        }
        false
    }
}

impl Default for NetworkSimulator {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_partition() {
        let sim = NetworkSimulator::new(None);

        // Initially, no blocking
        assert!(!sim.should_block(1, 2).await);

        // Add partition
        sim.partition(vec![1], vec![2, 3]).await;

        // Should block 1<->2 and 1<->3
        assert!(sim.should_block(1, 2).await);
        assert!(sim.should_block(2, 1).await);
        assert!(sim.should_block(1, 3).await);
        assert!(sim.should_block(3, 1).await);

        // But not 2<->3
        assert!(!sim.should_block(2, 3).await);

        // Heal partition
        sim.heal_partitions().await;
        assert!(!sim.should_block(1, 2).await);
    }

    #[tokio::test]
    async fn test_delay() {
        let sim = NetworkSimulator::new(None);

        // Add delay
        sim.add_rule(NetworkRule::Delay {
            src: 1,
            dst: 2,
            ms: 100,
        })
        .await;

        assert_eq!(sim.get_delay(1, 2).await, Some(100));
        assert_eq!(sim.get_delay(2, 1).await, None); // Only one direction
    }
}
