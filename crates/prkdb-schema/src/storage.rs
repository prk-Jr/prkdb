//! Storage backend for schema registry.
//!
//! Stores schemas in a separate metadata directory from user data.

use crate::error::{SchemaError, SchemaResult};
use crate::types::{Schema, SchemaInfo};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::fs;
use tracing::{debug, error, info, warn};

/// Storage interface for schema registry.
///
/// `'static` so the registry can finish a registration on a spawned task even when
/// the caller's future is dropped (see [`crate::SchemaRegistry::register`]).
#[async_trait]
pub trait SchemaStorage: Send + Sync + 'static {
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

fn lock_error(e: impl std::fmt::Display) -> SchemaError {
    SchemaError::Storage(format!("Lock error: {}", e))
}

impl InMemorySchemaStorage {
    /// Create a new in-memory storage.
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            next_id: RwLock::new(1),
        }
    }

    fn insert(&self, schema: &Schema) -> SchemaResult<()> {
        let mut schemas = self.schemas.write().map_err(lock_error)?;
        schemas
            .entry(schema.collection.clone())
            .or_default()
            .insert(schema.version, schema.clone());
        debug!(
            "Stored schema for '{}' version {}",
            schema.collection, schema.version
        );
        Ok(())
    }

    fn contains(&self, collection: &str, version: u32) -> SchemaResult<bool> {
        let schemas = self.schemas.read().map_err(lock_error)?;
        Ok(schemas
            .get(collection)
            .is_some_and(|versions| versions.contains_key(&version)))
    }

    /// Every stored schema without its descriptor bytes, as written to `schemas.json`.
    fn index_entries(&self) -> SchemaResult<Vec<Schema>> {
        let schemas = self.schemas.read().map_err(lock_error)?;
        Ok(schemas
            .values()
            .flat_map(|versions| versions.values().cloned())
            .map(|s| Schema {
                descriptor: Vec::new(),
                ..s
            })
            .collect())
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
        self.insert(schema)
    }

    async fn get(&self, collection: &str, version: u32) -> SchemaResult<Option<Schema>> {
        let schemas = self.schemas.read().map_err(lock_error)?;

        Ok(schemas
            .get(collection)
            .and_then(|versions| versions.get(&version).cloned()))
    }

    async fn get_latest(&self, collection: &str) -> SchemaResult<Option<Schema>> {
        let schemas = self.schemas.read().map_err(lock_error)?;

        Ok(schemas.get(collection).and_then(|versions| {
            versions
                .iter()
                .max_by_key(|(v, _)| *v)
                .map(|(_, schema)| schema.clone())
        }))
    }

    async fn list(&self) -> SchemaResult<Vec<SchemaInfo>> {
        let schemas = self.schemas.read().map_err(lock_error)?;

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
        let mut next_id = self.next_id.write().map_err(lock_error)?;
        let id = *next_id;
        *next_id += 1;
        Ok(id)
    }

    async fn next_version(&self, collection: &str) -> SchemaResult<u32> {
        let schemas = self.schemas.read().map_err(lock_error)?;

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
/// Every file is written atomically (unique temp file, fsync, rename, fsync of the
/// directory; newly created directories are fsynced into their parents), descriptor
/// before index, so a crash leaves at worst a descriptor the index does not
/// reference. [`FileSchemaStorage::load`] fails closed: an index entry whose
/// descriptor is missing or does not match its checksum is an error, never an empty
/// schema (SCH-02).
///
/// `put` runs to completion on a blocking thread while holding the write lock, so
/// dropping a `put` future cannot leave a write running outside the lock.
///
/// If the index has been renamed into place but its directory cannot be fsynced,
/// the new version is on disk but not known to be durable. The storage then keeps
/// the entry, returns the error, and refuses every later `put` until it is
/// reopened: continuing would stack further writes on a directory whose last
/// update may not survive a crash.
pub struct FileSchemaStorage {
    inner: Arc<FileInner>,
    /// Serializes `put`. Held (as an owned guard) by the blocking write itself.
    write_lock: Arc<tokio::sync::Mutex<()>>,
}

struct FileInner {
    /// Base directory for schema storage
    base_path: PathBuf,
    /// In-memory cache (always kept in sync with disk)
    cache: InMemorySchemaStorage,
    /// Set when a published index could not be made durable; see [`FileSchemaStorage`].
    poisoned: AtomicBool,
}

/// Suffix of the temporary files [`write_atomic`] renames into place; `load`
/// removes any left behind by a crash.
const TMP_SUFFIX: &str = ".tmp";

/// Distinguishes each temporary file this process creates.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique temporary path next to `path`: `{name}.{pid}.{n}.tmp`. Unique so that
/// two writes of the same file can never truncate each other's temp file.
fn tmp_path_for(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(format!(
        ".{}.{}{TMP_SUFFIX}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    PathBuf::from(name)
}

/// How far an atomic write got before failing.
enum WriteFailure {
    /// The target file is unchanged.
    NotPublished(SchemaError),
    /// The new file was renamed into place, but the directory fsync failed.
    PublishedNotDurable(SchemaError),
}

/// Errors from writes carry paths relative to the storage root: they can reach
/// clients through `register`, which should not learn server paths.
fn relative<'a>(base: &Path, path: &'a Path) -> std::path::Display<'a> {
    path.strip_prefix(base).unwrap_or(path).display()
}

/// `create_dir_all`, then fsync the parent of every directory it created, so the
/// new directory entries survive a crash along with the files inside them.
fn create_dir_all_durable(dir: &Path) -> std::io::Result<()> {
    let mut missing = Vec::new();
    let mut current = Some(dir);
    while let Some(d) = current {
        if d.as_os_str().is_empty() || d.exists() {
            break;
        }
        missing.push(d);
        current = d.parent();
    }
    std::fs::create_dir_all(dir)?;
    for created in missing.iter().rev() {
        if let Some(parent) = created.parent().filter(|p| !p.as_os_str().is_empty()) {
            sync_dir(parent)?;
        }
    }
    Ok(())
}

/// Write `bytes` to `path` so that a crash leaves either the old file or the new
/// one: write a unique temp file, `sync_all`, rename over `path`, then sync the
/// parent directory so the rename itself is durable.
fn write_atomic(base: &Path, path: &Path, bytes: &[u8]) -> Result<(), WriteFailure> {
    use std::io::Write;

    let fail = |what: &str, p: &Path, e: std::io::Error| {
        WriteFailure::NotPublished(SchemaError::Storage(format!(
            "{what} {}: {e}",
            relative(base, p)
        )))
    };
    let parent = path.parent().ok_or_else(|| {
        WriteFailure::NotPublished(SchemaError::Storage(format!(
            "{} has no parent directory",
            relative(base, path)
        )))
    })?;
    create_dir_all_durable(parent).map_err(|e| fail("cannot create directory", parent, e))?;

    let tmp = tmp_path_for(path);
    let written = std::fs::File::create(&tmp)
        .and_then(|mut file| {
            file.write_all(bytes)?;
            file.sync_all()
        })
        .and_then(|()| std::fs::rename(&tmp, path));
    if let Err(e) = written {
        let _ = std::fs::remove_file(&tmp);
        return Err(fail("cannot write", path, e));
    }

    sync_dir(parent).map_err(|e| {
        WriteFailure::PublishedNotDurable(SchemaError::Storage(format!(
            "cannot sync directory {}: {e}",
            relative(base, parent)
        )))
    })
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> std::io::Result<()> {
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

impl FileInner {
    fn descriptor_path(&self, collection: &str, version: u32) -> PathBuf {
        self.base_path
            .join("descriptors")
            .join(collection)
            .join(format!("v{}.binpb", version))
    }

    /// The whole of `put`, run on a blocking thread under the write lock.
    fn put_blocking(&self, schema: &Schema) -> SchemaResult<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(SchemaError::Storage(
                "schema storage refused the write: an earlier index update could not be \
                 made durable; restart the server"
                    .to_string(),
            ));
        }

        // Versions are immutable once stored: overwriting one would change what
        // readers of that version decode with.
        if self.cache.contains(&schema.collection, schema.version)? {
            return Err(SchemaError::VersionConflict {
                collection: schema.collection.clone(),
                version: schema.version,
            });
        }

        // Descriptor first, index second: a crash in between leaves an
        // unreferenced descriptor, never an index entry without one. A descriptor
        // that is published but not durable is harmless for the same reason.
        let descriptor_path = self.descriptor_path(&schema.collection, schema.version);
        match write_atomic(&self.base_path, &descriptor_path, &schema.descriptor) {
            Ok(()) => {}
            Err(WriteFailure::NotPublished(e) | WriteFailure::PublishedNotDurable(e)) => {
                return Err(e)
            }
        }
        debug!("Saved descriptor to {:?}", descriptor_path);

        let stored = Schema {
            descriptor_crc32: Some(crc32fast::hash(&schema.descriptor)),
            ..schema.clone()
        };

        // The index is serialized from the cache plus the new entry, and the cache
        // only learns the new version once the index lists it: readers never see a
        // version that a failed index write would then take back (and a later
        // registration would reuse with a different descriptor).
        let json = self.cache.index_entries().and_then(|mut entries| {
            entries.push(Schema {
                descriptor: Vec::new(),
                ..stored.clone()
            });
            serde_json::to_vec_pretty(&entries)
                .map_err(|e| SchemaError::Serialization(format!("Failed to serialize: {}", e)))
        });
        let index_path = self.base_path.join("schemas.json");
        let written = json
            .map_err(WriteFailure::NotPublished)
            .and_then(|json| write_atomic(&self.base_path, &index_path, &json));
        match written {
            Ok(()) => {
                debug!("Saved schema index to {:?}", index_path);
                self.cache.insert(&stored)
            }
            // The index on disk does not list this version, so memory must not either.
            Err(WriteFailure::NotPublished(e)) => Err(e),
            Err(WriteFailure::PublishedNotDurable(e)) => {
                // The index on disk does list it, so memory keeps it; stop writing.
                self.cache.insert(&stored)?;
                self.poisoned.store(true, Ordering::Release);
                error!(
                    "Schema index for '{}' v{} is published but not durable; refusing further \
                     schema writes until restart: {}",
                    schema.collection, schema.version, e
                );
                Err(e)
            }
        }
    }
}

impl FileSchemaStorage {
    /// Create a new file-based storage.
    pub fn new(base_path: PathBuf) -> Self {
        info!("Initializing schema storage at {:?}", base_path);
        Self {
            inner: Arc::new(FileInner {
                base_path,
                cache: InMemorySchemaStorage::new(),
                poisoned: AtomicBool::new(false),
            }),
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Create the storage root (and any missing ancestors) durably.
    pub fn create_base_dir(&self) -> SchemaResult<()> {
        create_dir_all_durable(&self.inner.base_path).map_err(|e| {
            SchemaError::Storage(format!(
                "cannot create schema directory {}: {e}",
                self.inner.base_path.display()
            ))
        })
    }

    /// Load schemas from disk into cache.
    ///
    /// Fails if an index entry's descriptor is missing, unreadable or does not
    /// match its recorded checksum: serving such a registry would report fewer
    /// schemas than were registered and let clients re-register over them.
    pub async fn load(&mut self) -> SchemaResult<()> {
        let base = self.inner.base_path.clone();
        tokio::task::spawn_blocking(move || remove_stale_tmp_files(&base))
            .await
            .map_err(|e| SchemaError::Storage(format!("temp file cleanup failed: {e}")))?;

        let index_path = self.inner.base_path.join("schemas.json");

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
            self.inner.cache.insert(&Schema {
                descriptor,
                ..schema
            })?;
        }

        // Update next_id based on loaded schemas
        let max_id = self
            .inner
            .cache
            .list()
            .await?
            .iter()
            .map(|s| s.schema_id)
            .max()
            .unwrap_or(0);

        *self.inner.cache.next_id.write().map_err(lock_error)? = max_id + 1;

        Ok(())
    }

    /// Read and verify the descriptor an index entry refers to. Only reached from
    /// `load`, whose errors go to the operator, so full paths are kept.
    async fn read_descriptor(&self, schema: &Schema) -> SchemaResult<Vec<u8>> {
        let path = self
            .inner
            .descriptor_path(&schema.collection, schema.version);
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
}

#[async_trait]
impl SchemaStorage for FileSchemaStorage {
    async fn put(&self, schema: &Schema) -> SchemaResult<()> {
        crate::names::validate_collection_name(&schema.collection)?;

        // The owned guard moves into the blocking task: if this future is dropped,
        // the write still finishes before the next `put` can start.
        let guard = self.write_lock.clone().lock_owned().await;
        let inner = self.inner.clone();
        let schema = schema.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            inner.put_blocking(&schema)
        })
        .await
        .map_err(|e| SchemaError::Storage(format!("schema write task failed: {e}")))?
    }

    async fn get(&self, collection: &str, version: u32) -> SchemaResult<Option<Schema>> {
        self.inner.cache.get(collection, version).await
    }

    async fn get_latest(&self, collection: &str) -> SchemaResult<Option<Schema>> {
        self.inner.cache.get_latest(collection).await
    }

    async fn list(&self) -> SchemaResult<Vec<SchemaInfo>> {
        self.inner.cache.list().await
    }

    async fn next_schema_id(&self) -> SchemaResult<u32> {
        self.inner.cache.next_schema_id().await
    }

    async fn next_version(&self, collection: &str) -> SchemaResult<u32> {
        self.inner.cache.next_version(collection).await
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
