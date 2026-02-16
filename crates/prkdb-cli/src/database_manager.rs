use anyhow::{Context, Result};
use prkdb::prelude::*;
use prkdb_storage_sled::SledAdapter;
use prkdb_types::storage::StorageAdapter;
use std::path::Path;
use std::sync::{Arc, Mutex};
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
    adapter: Arc<Mutex<Option<SledAdapter>>>,
    database_path: String,
    raft_options: Option<RaftOptions>,
}

impl DatabaseManager {
    pub fn new(database_path: impl AsRef<Path>, raft_options: Option<RaftOptions>) -> Self {
        Self {
            connection: Arc::new(Mutex::new(None)),
            adapter: Arc::new(Mutex::new(None)),
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

                let config = prkdb::raft::ClusterConfig {
                    local_node_id: raft_opts.node_id,
                    listen_addr: raft_opts.listen_addr,
                    nodes: raft_opts.peers.clone(),
                    ..Default::default()
                };

                let db = PrkDb::new_multi_raft(
                    raft_opts.num_partitions,
                    config,
                    std::path::PathBuf::from(&self.database_path),
                )?;

                *conn_guard = Some(db);
            } else {
                let storage = SledAdapter::open(&self.database_path).with_context(|| {
                    format!("Failed to open database at: {}", self.database_path)
                })?;

                *self.adapter.lock().unwrap() = Some(storage.clone());

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
        // Ensure initialized
        let _ = self.try_get_connection()?;

        let adapter = {
            let adapter_guard = self.adapter.lock().unwrap();
            adapter_guard.clone()
        };

        if let Some(adapter) = adapter {
            return adapter.scan_prefix(b"").await.map_err(|e| e.into());
        }

        Err(anyhow::anyhow!("Database adapter not available"))
    }
}

/// Global database manager instance
static mut DB_MANAGER: Option<DatabaseManager> = None;
static INIT: std::sync::Once = std::sync::Once::new();

/// Initialize the global database manager
pub fn init_database_manager(database_path: impl AsRef<Path>, raft_options: Option<RaftOptions>) {
    INIT.call_once(|| unsafe {
        DB_MANAGER = Some(DatabaseManager::new(database_path, raft_options));
    });
}

/// Get the global database manager
#[allow(static_mut_refs)]
pub fn get_database_manager() -> &'static DatabaseManager {
    unsafe {
        DB_MANAGER
            .as_ref()
            .expect("Database manager not initialized")
    }
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
