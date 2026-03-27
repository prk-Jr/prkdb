use anyhow::{Context, Result};
use prkdb::prelude::*;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// Configuration options for Raft cluster
#[derive(Clone, Debug)]
pub struct RaftOptions {
    pub node_id: u64,
    pub listen_addr: std::net::SocketAddr,
    pub peers: Vec<(u64, std::net::SocketAddr)>,
    pub num_partitions: usize,
}

/// Database connection manager that handles Sled's exclusive locking properly
pub struct DatabaseManager {
    connection: Arc<Mutex<Option<PrkDb>>>,
    database_path: String,
    raft_options: Option<RaftOptions>,
}

impl DatabaseManager {
    pub fn new(database_path: impl AsRef<Path>, raft_options: Option<RaftOptions>) -> Self {
        Self {
            connection: Arc::new(Mutex::new(None)),
            database_path: database_path.as_ref().to_string_lossy().to_string(),
            raft_options,
        }
    }

    /// Get a database connection with retry logic for lock contention
    pub async fn get_connection(&self) -> Result<PrkDb> {
        const MAX_RETRIES: usize = 3;
        const RETRY_DELAY: Duration = Duration::from_millis(100);

        for attempt in 0..MAX_RETRIES {
            match self.try_get_connection() {
                Ok(db) => return Ok(db),
                Err(e) if attempt < MAX_RETRIES - 1 => {
                    if e.to_string().contains("could not acquire lock")
                        || e.to_string().contains("Resource temporarily unavailable")
                    {
                        // Wait before retrying
                        tokio::time::sleep(RETRY_DELAY * (attempt as u32 + 1)).await;
                        continue;
                    } else {
                        return Err(e);
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Err(anyhow::anyhow!(
            "Failed to acquire database lock after {} attempts",
            MAX_RETRIES
        ))
    }

    fn try_get_connection(&self) -> Result<PrkDb> {
        let mut conn_guard = self.connection.lock().unwrap();

        if conn_guard.is_none() {
            // Check if Raft is enabled
            if let Some(raft_opts) = &self.raft_options {
                println!(
                    "🚀 Initializing PrkDB with Multi-Raft (Node ID: {})",
                    raft_opts.node_id
                );

                let mut nodes = raft_opts.peers.clone();
                if let Some(existing) = nodes
                    .iter_mut()
                    .find(|(node_id, _)| *node_id == raft_opts.node_id)
                {
                    existing.1 = raft_opts.listen_addr;
                } else {
                    nodes.push((raft_opts.node_id, raft_opts.listen_addr));
                }
                nodes.sort_by_key(|(node_id, _)| *node_id);

                let config = prkdb::raft::ClusterConfig {
                    local_node_id: raft_opts.node_id,
                    listen_addr: raft_opts.listen_addr,
                    nodes,
                    ..Default::default()
                };

                let db = PrkDb::new_multi_raft(
                    raft_opts.num_partitions,
                    config,
                    std::path::PathBuf::from(&self.database_path),
                )?;

                *conn_guard = Some(db);
            } else {
                let storage = prkdb_storage_sled::SledAdapter::open(&self.database_path)
                    .with_context(|| {
                        format!("Failed to open database at: {}", self.database_path)
                    })?;

                let db = PrkDb::builder()
                    .with_storage(storage)
                    .build()
                    .context("Failed to build PrkDb instance")?;

                *conn_guard = Some(db);
            }
        }

        Ok(conn_guard.as_ref().unwrap().clone())
    }

    /// Get direct access to the database instance (for gRPC server)
    pub async fn get_db_instance(&self) -> Result<PrkDb> {
        self.get_connection().await
    }

    /// Execute a read-only operation with proper error handling
    pub async fn with_read_only<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(PrkDb) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        match self.get_connection().await {
            Ok(db) => operation(db).await,
            Err(e) if e.to_string().contains("could not acquire lock") => {
                // For read-only operations, we can provide degraded functionality
                Err(anyhow::anyhow!(
                    "Database is currently busy. This may happen when another process is accessing the database. \
                    Try again in a moment or ensure no other PrkDB operations are running."
                ))
            }
            Err(e) => Err(e),
        }
    }

    /// Get storage adapter directly for simple scan operations
    pub async fn scan_storage(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let db = self.get_connection().await?;

        if let Some(pm) = &db.partition_manager {
            let mut entries = Vec::new();
            for partition_id in 0..pm.partition_count() {
                if let Some(storage) = pm.get_partition_storage(partition_id as u64) {
                    let mut partition_entries = storage.scan_prefix(b"").await?;
                    entries.append(&mut partition_entries);
                }
            }
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            return Ok(entries);
        }

        db.storage().scan_prefix(b"").await.map_err(|e| e.into())
    }

    pub fn schema_storage_path(&self) -> PathBuf {
        PathBuf::from(&self.database_path).join("schemas")
    }
}

/// Global database manager instance
static DB_MANAGER: OnceLock<DatabaseManager> = OnceLock::new();

/// Initialize the global database manager
pub fn init_database_manager(database_path: impl AsRef<Path>, raft_options: Option<RaftOptions>) {
    let _ = DB_MANAGER.set(DatabaseManager::new(database_path, raft_options));
}

/// Get the global database manager
pub fn get_database_manager() -> &'static DatabaseManager {
    DB_MANAGER.get().expect("Database manager not initialized")
}

/// Helper function to execute read-only database operations
pub async fn with_database_read<T, F, Fut>(operation: F) -> Result<T>
where
    F: FnOnce(PrkDb) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    get_database_manager().with_read_only(operation).await
}

/// Helper function to scan storage
pub async fn scan_storage() -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    get_database_manager().scan_storage().await
}

/// Helper function to get database instance directly
pub async fn get_db_instance() -> Result<PrkDb> {
    get_database_manager().get_db_instance().await
}
