//! Storage backend for schema registry.
//!
//! Stores schemas in a separate metadata directory from user data.

use crate::error::{SchemaError, SchemaResult};
use crate::types::{Schema, SchemaInfo};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
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
pub struct FileSchemaStorage {
    /// Base directory for schema storage
    base_path: PathBuf,
    /// In-memory cache (always kept in sync with disk)
    cache: InMemorySchemaStorage,
}

impl FileSchemaStorage {
    /// Create a new file-based storage.
    pub fn new(base_path: PathBuf) -> Self {
        info!("Initializing schema storage at {:?}", base_path);
        Self {
            base_path,
            cache: InMemorySchemaStorage::new(),
        }
    }

    /// Load schemas from disk into cache.
    pub async fn load(&mut self) -> SchemaResult<()> {
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
            // Load the descriptor from its file
            let descriptor_path = self.descriptor_path(&schema.collection, schema.version);

            let mut schema_with_descriptor = schema;
            if descriptor_path.exists() {
                match fs::read(&descriptor_path).await {
                    Ok(bytes) => {
                        schema_with_descriptor.descriptor = bytes;
                    }
                    Err(e) => {
                        warn!(
                            "Failed to load descriptor for {}:v{}: {}",
                            schema_with_descriptor.collection, schema_with_descriptor.version, e
                        );
                    }
                }
            }

            self.cache.put(&schema_with_descriptor).await?;
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

    /// Get the path to a schema descriptor file.
    fn descriptor_path(&self, collection: &str, version: u32) -> PathBuf {
        self.base_path
            .join("descriptors")
            .join(collection)
            .join(format!("v{}.binpb", version))
    }

    /// Save the index file (metadata without descriptors).
    async fn save_index(&self) -> SchemaResult<()> {
        // Collect data while holding lock, then release before async I/O
        let (json, index_path) = {
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

            let json = serde_json::to_string_pretty(&all_schemas)
                .map_err(|e| SchemaError::Serialization(format!("Failed to serialize: {}", e)))?;

            let index_path = self.base_path.join("schemas.json");
            (json, index_path)
        }; // Lock is released here

        // Now do async I/O without holding the lock
        if let Some(parent) = index_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| SchemaError::Storage(format!("Failed to create directory: {}", e)))?;
        }

        fs::write(&index_path, json)
            .await
            .map_err(|e| SchemaError::Storage(format!("Failed to write index: {}", e)))?;

        debug!("Saved schema index to {:?}", index_path);
        Ok(())
    }
}

#[async_trait]
impl SchemaStorage for FileSchemaStorage {
    async fn put(&self, schema: &Schema) -> SchemaResult<()> {
        // Save descriptor to file
        let descriptor_path = self.descriptor_path(&schema.collection, schema.version);

        if let Some(parent) = descriptor_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| SchemaError::Storage(format!("Failed to create directory: {}", e)))?;
        }

        fs::write(&descriptor_path, &schema.descriptor)
            .await
            .map_err(|e| SchemaError::Storage(format!("Failed to write descriptor: {}", e)))?;

        debug!("Saved descriptor to {:?}", descriptor_path);

        // Store in cache
        self.cache.put(schema).await?;

        // Update index file
        self.save_index().await?;

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
