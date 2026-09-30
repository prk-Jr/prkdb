//! Storage backend for schema registry.
//!
//! Stores schemas in a separate metadata directory from user data.

use crate::error::{SchemaError, SchemaResult};
use crate::types::{Schema, SchemaInfo};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use tokio::fs;
use tracing::{debug, info, warn};

/// Storage interface for schema registry.
#[async_trait]
pub trait SchemaStorage: Send + Sync {
    /// Store a schema
    async fn put(&self, schema: &Schema) -> SchemaResult<()>;

    /// Get a specific schema version
    async fn get(&self, collection: &str, version: u32) -> SchemaResult<Option<Schema>>;

    /// Get the latest schema for a collection
    async fn get_latest(&self, collection: &str) -> SchemaResult<Option<Schema>>;

    /// List all schema infos
    async fn list(&self) -> SchemaResult<Vec<SchemaInfo>>;

    /// Get the next schema ID
    async fn next_schema_id(&self) -> SchemaResult<u32>;

    /// Get the next version for a collection
    async fn next_version(&self, collection: &str) -> SchemaResult<u32>;
}

/// In-memory schema storage (for testing and development).
pub struct InMemorySchemaStorage {
    /// Schemas by collection -> version -> schema
    schemas: RwLock<HashMap<String, HashMap<u32, Schema>>>,
    /// Next schema ID
    next_id: RwLock<u32>,
}

impl InMemorySchemaStorage {
    /// Create a new in-memory storage.
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            next_id: RwLock::new(1),
        }
    }
}

impl Default for InMemorySchemaStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SchemaStorage for InMemorySchemaStorage {
    async fn put(&self, schema: &Schema) -> SchemaResult<()> {
        let mut schemas = self
            .schemas
            .write()
            .map_err(|e| SchemaError::Storage(format!("Lock error: {}", e)))?;

        let collection_schemas = schemas.entry(schema.collection.clone()).or_default();
        collection_schemas.insert(schema.version, schema.clone());

        debug!(
            "Stored schema for '{}' version {}",
            schema.collection, schema.version
        );
        Ok(())
    }

    async fn get(&self, collection: &str, version: u32) -> SchemaResult<Option<Schema>> {
        let schemas = self
            .schemas
            .read()
            .map_err(|e| SchemaError::Storage(format!("Lock error: {}", e)))?;

        Ok(schemas
            .get(collection)
            .and_then(|versions| versions.get(&version).cloned()))
    }

    async fn get_latest(&self, collection: &str) -> SchemaResult<Option<Schema>> {
        let schemas = self
            .schemas
            .read()
            .map_err(|e| SchemaError::Storage(format!("Lock error: {}", e)))?;

        Ok(schemas.get(collection).and_then(|versions| {
            versions
                .iter()
                .max_by_key(|(v, _)| *v)
                .map(|(_, schema)| schema.clone())
        }))
    }

    async fn list(&self) -> SchemaResult<Vec<SchemaInfo>> {
        let schemas = self
            .schemas
            .read()
            .map_err(|e| SchemaError::Storage(format!("Lock error: {}", e)))?;

        let mut infos = Vec::new();
        for (collection, versions) in schemas.iter() {
            if let Some((latest_version, latest_schema)) = versions.iter().max_by_key(|(v, _)| *v) {
                let first_schema = versions
                    .values()
                    .min_by_key(|s| s.created_at)
                    .unwrap_or(latest_schema);

                infos.push(SchemaInfo {
                    collection: collection.clone(),
                    latest_version: *latest_version,
                    schema_id: latest_schema.schema_id,
                    compatibility: latest_schema.compatibility,
                    created_at: first_schema.created_at,
                    updated_at: latest_schema.created_at,
                });
            }
        }

        Ok(infos)
    }

    async fn next_schema_id(&self) -> SchemaResult<u32> {
        let mut next_id = self
            .next_id
            .write()
            .map_err(|e| SchemaError::Storage(format!("Lock error: {}", e)))?;
        let id = *next_id;
        *next_id += 1;
        Ok(id)
    }

    async fn next_version(&self, collection: &str) -> SchemaResult<u32> {
        let schemas = self
            .schemas
            .read()
            .map_err(|e| SchemaError::Storage(format!("Lock error: {}", e)))?;

        Ok(schemas
            .get(collection)
            .map(|versions| versions.keys().max().copied().unwrap_or(0) + 1)
            .unwrap_or(1))
    }
}

/// File-based schema storage (for production).
///
/// Persists schemas to disk with the following structure:
/// ```text
/// {base_path}/
/// ├── schemas.json          # Index file with all schema metadata
/// └── descriptors/
///     └── {collection}/
///         └── v{version}.binpb  # FileDescriptorProto bytes
/// ```
///
/// Every file is written atomically (temp file, fsync, rename, fsync of the
/// directory), descriptor before index, so a crash leaves at worst a descriptor
/// the index does not reference. [`FileSchemaStorage::load`] fails closed: an
/// index entry whose descriptor is missing or does not match its checksum is an
/// error, never an empty schema (SCH-02).
pub struct FileSchemaStorage {
    /// Base directory for schema storage
    base_path: PathBuf,
    /// In-memory cache (always kept in sync with disk)
    cache: InMemorySchemaStorage,
    /// Serializes `put`, so the overwrite check, the cache insert and the index
    /// snapshot/write happen as one step and index writes cannot reorder.
    write_lock: tokio::sync::Mutex<()>,
}

/// Suffix of the temporary file [`write_atomic`] renames into place.
const TMP_SUFFIX: &str = ".tmp";

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(TMP_SUFFIX);
    PathBuf::from(name)
}

/// Write `bytes` to `path` so that a crash leaves either the old file or the new
/// one: write `path.tmp`, `sync_all`, rename over `path`, then sync the parent
/// directory so the rename itself is durable.
async fn write_atomic(path: &Path, bytes: Vec<u8>) -> SchemaResult<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || write_atomic_blocking(&path, &bytes))
        .await
        .map_err(|e| SchemaError::Storage(format!("atomic write task failed: {e}")))?
}

fn write_atomic_blocking(path: &Path, bytes: &[u8]) -> SchemaResult<()> {
    use std::io::Write;

    let io_err = |what: &str, p: &Path, e: std::io::Error| {
        SchemaError::Storage(format!("{what} {}: {e}", p.display()))
    };
    let parent = path
        .parent()
        .ok_or_else(|| SchemaError::Storage(format!("{} has no parent", path.display())))?;
    std::fs::create_dir_all(parent).map_err(|e| io_err("cannot create directory", parent, e))?;

    let tmp = tmp_path_for(path);
    {
        let mut file = std::fs::File::create(&tmp).map_err(|e| io_err("cannot create", &tmp, e))?;
        file.write_all(bytes)
            .map_err(|e| io_err("cannot write", &tmp, e))?;
        file.sync_all()
            .map_err(|e| io_err("cannot sync", &tmp, e))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| io_err("cannot rename into", path, e))?;
    sync_dir(parent)
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> SchemaResult<()> {
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| SchemaError::Storage(format!("cannot sync directory {}: {e}", dir.display())))
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> SchemaResult<()> {
    // Directories cannot be opened for syncing on this platform; the rename is
    // as durable as the filesystem makes it.
    Ok(())
}

/// Remove `*.tmp` files left by a write that crashed before its rename. They are
/// never referenced: the rename is what publishes a file.
fn remove_stale_tmp_files(base: &Path) {
    let mut dirs = vec![base.to_path_buf()];
    if let Ok(entries) = std::fs::read_dir(base.join("descriptors")) {
        dirs.extend(
            entries
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| p.is_dir()),
        );
    }
    for dir in dirs {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for path in entries.filter_map(Result::ok).map(|e| e.path()) {
            let is_tmp = path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(TMP_SUFFIX));
            if is_tmp && path.is_file() {
                match std::fs::remove_file(&path) {
                    Ok(()) => info!("Removed stale temporary file {:?}", path),
                    Err(e) => warn!("Could not remove stale temporary file {:?}: {}", path, e),
                }
            }
        }
    }
}

impl FileSchemaStorage {
    /// Create a new file-based storage.
    pub fn new(base_path: PathBuf) -> Self {
        info!("Initializing schema storage at {:?}", base_path);
        Self {
            base_path,
            cache: InMemorySchemaStorage::new(),
            write_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Load schemas from disk into cache.
    ///
    /// Fails if an index entry's descriptor is missing, unreadable or does not
    /// match its recorded checksum: serving such a registry would report fewer
    /// schemas than were registered and let clients re-register over them.
    pub async fn load(&mut self) -> SchemaResult<()> {
        let base = self.base_path.clone();
        tokio::task::spawn_blocking(move || remove_stale_tmp_files(&base))
            .await
            .map_err(|e| SchemaError::Storage(format!("temp file cleanup failed: {e}")))?;

        let index_path = self.base_path.join("schemas.json");

        if !index_path.exists() {
            info!("No existing schema index found, starting fresh");
            return Ok(());
        }

        let content = fs::read_to_string(&index_path)
            .await
            .map_err(|e| SchemaError::Storage(format!("Failed to read index: {}", e)))?;

        let schemas: Vec<Schema> = serde_json::from_str(&content)
            .map_err(|e| SchemaError::Serialization(format!("Failed to parse index: {}", e)))?;

        info!("Loading {} schemas from disk", schemas.len());

        for schema in schemas {
            // Fail closed: a corrupt or tampered index must not let an unsafe
            // collection name reach `descriptor_path` (SCH-01). We refuse
            // rather than try to guess a safe interpretation: there is no
            // deployed data yet, so failing loudly is strictly better than
            // silently misreading the index.
            crate::names::validate_collection_name(&schema.collection).map_err(|_| {
                SchemaError::Storage(format!(
                    "corrupt schema index: invalid collection name {:?}; the registry was \
                     written by an older PrkDB that accepted unsafe names — remove or rename \
                     the entry in schemas.json",
                    crate::names::truncate_for_error(&schema.collection)
                ))
            })?;

            let descriptor = self.read_descriptor(&schema).await?;
            let loaded = Schema {
                descriptor,
                ..schema
            };
            self.cache.put(&loaded).await?;
        }

        // Update next_id based on loaded schemas
        let max_id = self
            .cache
            .list()
            .await?
            .iter()
            .map(|s| s.schema_id)
            .max()
            .unwrap_or(0);

        *self
            .cache
            .next_id
            .write()
            .map_err(|e| SchemaError::Storage(format!("Lock error: {}", e)))? = max_id + 1;

        Ok(())
    }

    /// Read and verify the descriptor an index entry refers to.
    async fn read_descriptor(&self, schema: &Schema) -> SchemaResult<Vec<u8>> {
        let path = self.descriptor_path(&schema.collection, schema.version);
        let bytes = match fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(SchemaError::Storage(format!(
                    "missing descriptor {} for {} v{}; restore it from backup or remove the \
                     entry from schemas.json",
                    path.display(),
                    schema.collection,
                    schema.version
                )));
            }
            Err(e) => {
                return Err(SchemaError::Storage(format!(
                    "cannot read descriptor {} for {} v{}: {e}",
                    path.display(),
                    schema.collection,
                    schema.version
                )));
            }
        };

        if let Some(expected) = schema.descriptor_crc32 {
            let actual = crc32fast::hash(&bytes);
            if actual != expected {
                return Err(SchemaError::DescriptorChecksumMismatch {
                    path: path.display().to_string(),
                    expected,
                    actual,
                });
            }
        }
        Ok(bytes)
    }

    /// Get the path to a schema descriptor file.
    fn descriptor_path(&self, collection: &str, version: u32) -> PathBuf {
        self.base_path
            .join("descriptors")
            .join(collection)
            .join(format!("v{}.binpb", version))
    }

    /// Save the index file (metadata without descriptors). Callers hold `write_lock`.
    async fn save_index(&self) -> SchemaResult<()> {
        // Collect data while holding lock, then release before async I/O
        let json = {
            let schemas = self
                .cache
                .schemas
                .read()
                .map_err(|e| SchemaError::Storage(format!("Lock error: {}", e)))?;

            // Flatten all schemas, excluding the descriptor bytes
            let all_schemas: Vec<Schema> = schemas
                .values()
                .flat_map(|versions| versions.values().cloned())
                .map(|mut s| {
                    // Don't store descriptor in index (stored separately)
                    s.descriptor = Vec::new();
                    s
                })
                .collect();

            serde_json::to_vec_pretty(&all_schemas)
                .map_err(|e| SchemaError::Serialization(format!("Failed to serialize: {}", e)))?
        }; // Lock is released here

        let index_path = self.base_path.join("schemas.json");
        write_atomic(&index_path, json).await?;

        debug!("Saved schema index to {:?}", index_path);
        Ok(())
    }

    /// Drop a cache entry whose index write failed, so memory matches disk.
    fn forget(&self, collection: &str, version: u32) -> SchemaResult<()> {
        let mut schemas = self
            .cache
            .schemas
            .write()
            .map_err(|e| SchemaError::Storage(format!("Lock error: {}", e)))?;
        if let Some(versions) = schemas.get_mut(collection) {
            versions.remove(&version);
            if versions.is_empty() {
                schemas.remove(collection);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SchemaStorage for FileSchemaStorage {
    async fn put(&self, schema: &Schema) -> SchemaResult<()> {
        crate::names::validate_collection_name(&schema.collection)?;

        let _guard = self.write_lock.lock().await;

        // Versions are immutable once stored: overwriting one would change what
        // readers of that version decode with.
        if self
            .cache
            .get(&schema.collection, schema.version)
            .await?
            .is_some()
        {
            return Err(SchemaError::VersionConflict {
                collection: schema.collection.clone(),
                version: schema.version,
            });
        }

        // Descriptor first, index second: a crash in between leaves an
        // unreferenced descriptor, never an index entry without one.
        let descriptor_path = self.descriptor_path(&schema.collection, schema.version);
        write_atomic(&descriptor_path, schema.descriptor.clone()).await?;
        debug!("Saved descriptor to {:?}", descriptor_path);

        let stored = Schema {
            descriptor_crc32: Some(crc32fast::hash(&schema.descriptor)),
            ..schema.clone()
        };
        self.cache.put(&stored).await?;

        if let Err(e) = self.save_index().await {
            self.forget(&schema.collection, schema.version)?;
            return Err(e);
        }

        Ok(())
    }

    async fn get(&self, collection: &str, version: u32) -> SchemaResult<Option<Schema>> {
        self.cache.get(collection, version).await
    }

    async fn get_latest(&self, collection: &str) -> SchemaResult<Option<Schema>> {
        self.cache.get_latest(collection).await
    }

    async fn list(&self) -> SchemaResult<Vec<SchemaInfo>> {
        self.cache.list().await
    }

    async fn next_schema_id(&self) -> SchemaResult<u32> {
        self.cache.next_schema_id().await
    }

    async fn next_version(&self, collection: &str) -> SchemaResult<u32> {
        self.cache.next_version(collection).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CompatibilityMode;
    use tempfile::TempDir;

    fn create_test_schema(collection: &str, version: u32) -> Schema {
        Schema {
            schema_id: version,
            collection: collection.to_string(),
            version,
            descriptor: vec![0x0a, 0x0b, 0x0c], // dummy proto bytes
            compatibility: CompatibilityMode::Backward,
            is_breaking: false,
            migration_id: None,
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            descriptor_crc32: None,
        }
    }

    #[tokio::test]
    async fn test_in_memory_storage_put_get() {
        let storage = InMemorySchemaStorage::new();
        let schema = create_test_schema("users", 1);

        storage.put(&schema).await.unwrap();

        let retrieved = storage.get("users", 1).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().collection, "users");
    }

    #[tokio::test]
    async fn test_in_memory_storage_get_latest() {
        let storage = InMemorySchemaStorage::new();

        storage.put(&create_test_schema("users", 1)).await.unwrap();
        storage.put(&create_test_schema("users", 2)).await.unwrap();
        storage.put(&create_test_schema("users", 3)).await.unwrap();

        let latest = storage.get_latest("users").await.unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().version, 3);
    }

    #[tokio::test]
    async fn test_in_memory_storage_list() {
        let storage = InMemorySchemaStorage::new();

        storage.put(&create_test_schema("users", 1)).await.unwrap();
        storage.put(&create_test_schema("orders", 1)).await.unwrap();

        let list = storage.list().await.unwrap();
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn test_in_memory_storage_versioning() {
        let storage = InMemorySchemaStorage::new();

        assert_eq!(storage.next_version("users").await.unwrap(), 1);

        storage.put(&create_test_schema("users", 1)).await.unwrap();
        assert_eq!(storage.next_version("users").await.unwrap(), 2);

        storage.put(&create_test_schema("users", 2)).await.unwrap();
        assert_eq!(storage.next_version("users").await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_file_storage_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileSchemaStorage::new(temp_dir.path().to_path_buf());

        let schema = create_test_schema("users", 1);
        storage.put(&schema).await.unwrap();

        // Verify files were created
        let index_path = temp_dir.path().join("schemas.json");
        assert!(index_path.exists());

        let descriptor_path = temp_dir.path().join("descriptors/users/v1.binpb");
        assert!(descriptor_path.exists());

        // Verify descriptor content
        let content = std::fs::read(&descriptor_path).unwrap();
        assert_eq!(content, vec![0x0a, 0x0b, 0x0c]);
    }

    #[tokio::test]
    async fn test_file_storage_load() {
        let temp_dir = TempDir::new().unwrap();

        // Write some schemas
        {
            let storage = FileSchemaStorage::new(temp_dir.path().to_path_buf());
            storage.put(&create_test_schema("users", 1)).await.unwrap();
            storage.put(&create_test_schema("users", 2)).await.unwrap();
            storage.put(&create_test_schema("orders", 1)).await.unwrap();
        }

        // Load in a new instance
        let mut storage = FileSchemaStorage::new(temp_dir.path().to_path_buf());
        storage.load().await.unwrap();

        // Verify
        let users_latest = storage.get_latest("users").await.unwrap();
        assert!(users_latest.is_some());
        assert_eq!(users_latest.unwrap().version, 2);

        let orders = storage.get("orders", 1).await.unwrap();
        assert!(orders.is_some());

        let list = storage.list().await.unwrap();
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn test_file_storage_schema_id_continuity() {
        let temp_dir = TempDir::new().unwrap();

        // Create and save some schemas
        {
            let storage = FileSchemaStorage::new(temp_dir.path().to_path_buf());
            let id1 = storage.next_schema_id().await.unwrap();
            let id2 = storage.next_schema_id().await.unwrap();

            let mut schema1 = create_test_schema("users", 1);
            schema1.schema_id = id1;
            let mut schema2 = create_test_schema("users", 2);
            schema2.schema_id = id2;

            storage.put(&schema1).await.unwrap();
            storage.put(&schema2).await.unwrap();
        }

        // Reload and verify ID continuity
        let mut storage = FileSchemaStorage::new(temp_dir.path().to_path_buf());
        storage.load().await.unwrap();

        let next_id = storage.next_schema_id().await.unwrap();
        assert_eq!(next_id, 3); // Should continue from 3
    }
}
