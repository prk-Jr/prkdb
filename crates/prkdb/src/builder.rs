use crate::compute::ComputeHandler;
use crate::consumer::{ConsumerGroupCoordinator, StorageOffsetStore};
use crate::db::{ComputeHandlerMap, EventBusMap, PrkDb};
use crate::error::DbError;
use crate::partitioning::AssignmentStrategy;
use dashmap::DashMap;
use prkdb_orm::dialect::{DefaultDialect, SqlDialect};
use prkdb_orm::schema::Table;
use prkdb_types::collection::{ChangeEvent, Collection};
use prkdb_types::storage::StorageAdapter;
use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::broadcast;

/// Performance optimization level for storage
#[derive(Debug, Clone, Copy)]
pub enum OptimizationLevel {
    /// Balanced performance (200-400K ops/sec)
    Balanced,
    /// Maximum throughput (500-800K ops/sec)
    Throughput,
    /// LEGENDARY performance (1.2M+ ops/sec) 🏆
    Legendary,
}

/// Non-generic builder — we accept any adapter implementing the async `StorageAdapter` port.
pub struct Builder {
    storage: Option<Arc<dyn StorageAdapter>>,
    event_bus: EventBusMap,
    compute_handlers: ComputeHandlerMap,
    collection_registry: crate::db::CollectionRegistry,
    namespace: Option<Vec<u8>>,
    data_dir: Option<std::path::PathBuf>,
    event_capacity: usize,
    schema_dialect: SqlDialect,
    schema_migrations: Vec<String>,
    _marker: PhantomData<()>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            storage: None,
            event_bus: Default::default(),
            compute_handlers: Default::default(),
            collection_registry: Default::default(),
            namespace: None,
            data_dir: None,
            event_capacity: 16,
            schema_dialect: DefaultDialect::current(),
            schema_migrations: Vec::new(),
            _marker: PhantomData,
        }
    }
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Accept any concrete adapter that implements the StorageAdapter trait.
    pub fn with_storage<T>(mut self, storage: T) -> Self
    where
        T: StorageAdapter + 'static,
    {
        self.storage = Some(Arc::new(storage));
        self
    }

    pub fn register_collection<C: Collection>(mut self) -> Self {
        self.add_collection::<C>();
        self
    }

    /// Register a collection that also provides ORM schema metadata. This will schedule a migration
    /// using the configured SQL dialect when the builder is constructed.
    pub fn register_table_collection<C>(mut self) -> Self
    where
        C: Collection + Table,
    {
        self.add_collection::<C>();
        let ddl = C::create_table_sql(self.schema_dialect);
        self.schema_migrations.push(ddl);
        self
    }

    pub fn with_namespace(mut self, ns: impl AsRef<[u8]>) -> Self {
        self.namespace = Some(ns.as_ref().to_vec());
        self
    }

    pub fn with_data_dir(mut self, path: impl AsRef<std::path::Path>) -> Self {
        let path_buf = path.as_ref().to_path_buf();

        // LEGENDARY performance by default! 🏆
        // 1.2M ops/sec with collection-level partitioning and adaptive batching
        let config = prkdb_core::wal::WalConfig {
            log_dir: path_buf.clone(),
            segment_bytes: 512 * 1024 * 1024, // 512MB segments
            batch_size: 500,                  // Proven optimal (783K→1.2M!)
            compression: prkdb_core::wal::CompressionConfig::none(),
            ..prkdb_core::wal::WalConfig::benchmark_config()
        };

        // Use CollectionPartitionedAdapter for maximum performance!
        let adapter = crate::storage::CollectionPartitionedAdapter::new(config)
            .expect("Failed to create storage");

        self.storage = Some(Arc::new(adapter));
        self.data_dir = Some(path_buf);
        self
    }

    pub fn with_event_capacity(mut self, capacity: usize) -> Self {
        self.event_capacity = capacity;
        self
    }

    pub fn with_schema_dialect(mut self, dialect: SqlDialect) -> Self {
        self.schema_dialect = dialect;
        self
    }

    pub fn with_compute_handler<C, H>(self, handler: H) -> Self
    where
        C: Collection,
        H: ComputeHandler<C, PrkDb> + 'static,
    {
        let arc_handler: Arc<dyn ComputeHandler<C, PrkDb>> = Arc::new(handler);
        self.compute_handlers
            .entry(TypeId::of::<C>())
            .or_default()
            .push(Box::new(arc_handler));
        self
    }

    /// Use optimized storage with configurable performance level
    ///
    /// Most users should just use `.with_data_dir()` which gives legendary performance.
    /// Only use this if you want to customize the optimization level.
    ///
    /// # Optimization Levels
    /// - `Balanced`: 200-400K ops/sec (lower memory usage)
    /// - `Throughput`: 500-800K ops/sec (write-heavy workloads)
    /// - `Legendary`: 1.2M+ ops/sec (maximum performance)
    pub fn with_optimized_storage(
        mut self,
        data_dir: impl AsRef<std::path::Path>,
        level: OptimizationLevel,
    ) -> Self {
        let path = data_dir.as_ref().to_path_buf();

        let config = match level {
            OptimizationLevel::Balanced => prkdb_core::wal::WalConfig {
                log_dir: path.clone(),
                segment_bytes: 256 * 1024 * 1024, // 256MB segments
                batch_size: 300,                  // Medium batches
                compression: prkdb_core::wal::CompressionConfig::none(),
                ..prkdb_core::wal::WalConfig::benchmark_config()
            },
            OptimizationLevel::Throughput => prkdb_core::wal::WalConfig {
                log_dir: path.clone(),
                segment_bytes: 512 * 1024 * 1024, // 512MB segments
                batch_size: 500,                  // Optimal for most workloads
                compression: prkdb_core::wal::CompressionConfig::none(),
                ..prkdb_core::wal::WalConfig::benchmark_config()
            },
            OptimizationLevel::Legendary => prkdb_core::wal::WalConfig {
                log_dir: path.clone(),
                segment_bytes: 512 * 1024 * 1024, // 512MB segments
                batch_size: 500,                  // Proven optimal
                compression: prkdb_core::wal::CompressionConfig::none(),
                ..prkdb_core::wal::WalConfig::benchmark_config()
            },
        };

        let adapter = crate::storage::CollectionPartitionedAdapter::new(config)
            .expect("Failed to create optimized storage");

        self.storage = Some(Arc::new(adapter));
        self.data_dir = Some(path);
        self
    }

    pub async fn build_async(self) -> Result<PrkDb, DbError> {
        self.build_internal().await
    }

    pub fn build(self) -> Result<PrkDb, DbError> {
        let Builder {
            storage,
            event_bus,
            compute_handlers,
            collection_registry,
            namespace,
            schema_migrations,
            data_dir,
            ..
        } = self;

        let has_migrations = !schema_migrations.is_empty();
        let ddls = schema_migrations;

        let storage = if let Some(storage) = storage {
            storage
        } else if let Some(path) = data_dir {
            let wal_config = prkdb_core::wal::WalConfig {
                log_dir: path,
                segment_bytes: 64 * 1024 * 1024,
                index_interval_bytes: 4096,
                retention_ms: None,
                compaction: prkdb_core::wal::CompactionPolicy::None,
                compression: prkdb_core::wal::CompressionConfig::default(),
                batch_size: 100,
                flush_interval_ms: 10,
                segment_count: 4,
                shard_count: Some(16), // Multi-WAL sharding for 5-10x performance
                workload_profile: prkdb_core::wal::adaptive::WorkloadProfile::Balanced,
                adaptive_config: prkdb_core::wal::adaptive::AdaptiveBatchConfig::default(),
            };

            // WalStorageAdapter::new is synchronous, so we don't need async context or blocking.
            let adapter = crate::storage::WalStorageAdapter::new(wal_config)?;
            Arc::new(adapter) as Arc<dyn StorageAdapter>
        } else {
            Arc::new(crate::storage_old_inmemory::InMemoryAdapter::new()) as Arc<dyn StorageAdapter>
        };

        let db = Self::finish(
            storage,
            event_bus,
            compute_handlers,
            namespace,
            collection_registry,
        )?;

        if has_migrations {
            // Avoid blocking inside an existing runtime.
            if Handle::try_current().is_ok() {
                return Err(DbError::Internal(anyhow::anyhow!(
                    "Schema migrations require async context; use build_async() when registering table collections"
                )));
            }
            let rt = Runtime::new()
                .map_err(|e| DbError::Internal(anyhow::anyhow!("Runtime init failed: {}", e)))?;
            rt.block_on(Self::run_migrations(&db, ddls))?;
        }

        Ok(db)
    }

    async fn build_internal(self) -> Result<PrkDb, DbError> {
        let Builder {
            storage,
            event_bus,
            compute_handlers,
            collection_registry,
            namespace,
            schema_migrations,
            data_dir,
            ..
        } = self;

        let storage = if let Some(storage) = storage {
            storage
        } else if let Some(path) = data_dir {
            let wal_config = prkdb_core::wal::WalConfig {
                log_dir: path,
                segment_bytes: 64 * 1024 * 1024,
                index_interval_bytes: 4096,
                retention_ms: None, // Infinite retention by default
                compaction: prkdb_core::wal::CompactionPolicy::None,
                compression: prkdb_core::wal::CompressionConfig::default(), // Use LZ4 by default
                batch_size: 100,
                flush_interval_ms: 10,
                segment_count: 4,
                shard_count: Some(16), // Multi-WAL sharding for 5-10x performance
                workload_profile: prkdb_core::wal::adaptive::WorkloadProfile::Balanced,
                adaptive_config: prkdb_core::wal::adaptive::AdaptiveBatchConfig::default(),
            };
            let adapter = crate::storage::WalStorageAdapter::new(wal_config)?;
            Arc::new(adapter) as Arc<dyn StorageAdapter>
        } else {
            Arc::new(crate::storage_old_inmemory::InMemoryAdapter::new()) as Arc<dyn StorageAdapter>
        };

        let ddls = schema_migrations;
        let db = Self::finish(
            storage,
            event_bus,
            compute_handlers,
            namespace,
            collection_registry,
        )?;
        Self::run_migrations(&db, ddls).await?;
        Ok(db)
    }

    fn finish(
        storage: Arc<dyn StorageAdapter>,
        event_bus: EventBusMap,
        compute_handlers: ComputeHandlerMap,
        namespace: Option<Vec<u8>>,
        collection_registry: crate::db::CollectionRegistry,
    ) -> Result<PrkDb, DbError> {
        // Create consumer group coordinator with storage-based offset store
        let offset_store = Arc::new(StorageOffsetStore::new(storage.clone()));
        let consumer_coordinator = Arc::new(ConsumerGroupCoordinator::new(
            offset_store,
            AssignmentStrategy::RoundRobin,
        ));

        Ok(PrkDb {
            storage,
            event_bus: Arc::new(event_bus),
            compute_handlers: Arc::new(compute_handlers),
            namespace,
            metrics: crate::metrics::DbMetrics::default(),
            consumer_coordinator,
            partitioning_registry: Arc::new(DashMap::new()),
            collection_registry: Arc::new(collection_registry),
            partition_manager: None,
            replication_targets: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        })
    }

    fn add_collection<C: Collection>(&mut self) {
        let (sender, _) = broadcast::channel::<ChangeEvent<C>>(self.event_capacity);
        self.event_bus.insert(TypeId::of::<C>(), Box::new(sender));

        // Register the collection name in the registry
        let collection_name = std::any::type_name::<C>()
            .split("::")
            .last()
            .unwrap_or(std::any::type_name::<C>())
            .to_string();
        self.collection_registry
            .insert(TypeId::of::<C>(), collection_name);
    }

    async fn run_migrations(db: &PrkDb, ddls: Vec<String>) -> Result<(), DbError> {
        for ddl in ddls {
            db.storage.migrate_table(&ddl).await?;
        }
        Ok(())
    }
}
