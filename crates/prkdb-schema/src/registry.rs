//! Schema Registry - the main interface for schema operations.

use crate::compatibility::CompatibilityChecker;
use crate::error::{SchemaError, SchemaResult};
use crate::storage::SchemaStorage;
use crate::types::{CompatibilityMode, CompatibilityResult, Schema, SchemaInfo};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Schema Registry for PrkDB.
///
/// Stores and validates collection schemas for cross-language SDK support.
pub struct SchemaRegistry<S: SchemaStorage> {
    storage: Arc<S>,
}

impl<S: SchemaStorage> SchemaRegistry<S> {
    /// Create a new schema registry with the given storage backend.
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// Register a new schema for a collection.
    ///
    /// If the collection already has a schema, this will create a new version
    /// after checking compatibility.
    ///
    /// # Arguments
    /// * `collection` - Collection name
    /// * `schema_proto` - Serialized FileDescriptorProto bytes
    /// * `compatibility` - Compatibility mode to enforce
    /// * `migration_id` - Required if the change is breaking
    ///
    /// # Returns
    /// The registered schema with version info.
    pub async fn register(
        &self,
        collection: &str,
        schema_proto: Vec<u8>,
        compatibility: CompatibilityMode,
        migration_id: Option<String>,
    ) -> SchemaResult<Schema> {
        info!("Registering schema for collection '{}'", collection);

        // Check if there's an existing schema
        let existing = self.storage.get_latest(collection).await?;

        let (is_breaking, version) = if let Some(ref existing_schema) = existing {
            // Check compatibility with existing schema
            let compat_result = CompatibilityChecker::check(
                &existing_schema.descriptor,
                &schema_proto,
                compatibility,
            );

            if !compat_result.compatible {
                if compat_result.is_breaking && migration_id.is_none() {
                    return Err(SchemaError::BreakingChangeWithoutMigration {
                        collection: collection.to_string(),
                        reason: compat_result.errors.join("; "),
                    });
                }

                if !compat_result.is_breaking {
                    return Err(SchemaError::IncompatibleSchema {
                        collection: collection.to_string(),
                        errors: compat_result.errors,
                    });
                }
            }

            // Log warnings
            for warning in &compat_result.warnings {
                warn!("Schema warning for '{}': {}", collection, warning);
            }

            let next_version = self.storage.next_version(collection).await?;
            (compat_result.is_breaking, next_version)
        } else {
            // First schema for this collection
            (false, 1)
        };

        // Get next schema ID
        let schema_id = self.storage.next_schema_id().await?;

        // Create schema record
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let schema = Schema {
            schema_id,
            collection: collection.to_string(),
            version,
            descriptor: schema_proto,
            compatibility,
            is_breaking,
            migration_id,
            created_at: now,
        };

        // Store it
        self.storage.put(&schema).await?;

        info!(
            "Registered schema for '{}' version {} (id={}, breaking={})",
            collection, version, schema_id, is_breaking
        );

        Ok(schema)
    }

    /// Get a schema for a collection.
    ///
    /// # Arguments
    /// * `collection` - Collection name
    /// * `version` - Specific version, or None for latest
    pub async fn get(&self, collection: &str, version: Option<u32>) -> SchemaResult<Schema> {
        let schema = if let Some(v) = version {
            self.storage.get(collection, v).await?
        } else {
            self.storage.get_latest(collection).await?
        };

        schema.ok_or_else(|| {
            if let Some(v) = version {
                SchemaError::VersionNotFound {
                    collection: collection.to_string(),
                    version: v,
                }
            } else {
                SchemaError::NotFound {
                    collection: collection.to_string(),
                }
            }
        })
    }

    /// List all registered schemas.
    pub async fn list(&self) -> SchemaResult<Vec<SchemaInfo>> {
        self.storage.list().await
    }

    /// Check if a new schema is compatible with existing versions.
    pub async fn check_compatibility(
        &self,
        collection: &str,
        new_schema: &[u8],
    ) -> SchemaResult<CompatibilityResult> {
        let existing = self.storage.get_latest(collection).await?;

        match existing {
            Some(schema) => {
                let result = CompatibilityChecker::check(
                    &schema.descriptor,
                    new_schema,
                    schema.compatibility,
                );
                Ok(result)
            }
            None => {
                // No existing schema, new schema is always compatible
                Ok(CompatibilityResult::default())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::InMemorySchemaStorage;
    use prost::Message;
    use prost_types::FileDescriptorProto;

    fn create_test_registry() -> SchemaRegistry<InMemorySchemaStorage> {
        SchemaRegistry::new(Arc::new(InMemorySchemaStorage::new()))
    }

    fn create_empty_proto() -> Vec<u8> {
        FileDescriptorProto::default().encode_to_vec()
    }

    #[tokio::test]
    async fn test_register_first_schema() {
        let registry = create_test_registry();
        let proto = create_empty_proto();

        let schema = registry
            .register("users", proto, CompatibilityMode::Backward, None)
            .await
            .unwrap();

        assert_eq!(schema.collection, "users");
        assert_eq!(schema.version, 1);
        assert!(!schema.is_breaking);
    }

    #[tokio::test]
    async fn test_get_schema() {
        let registry = create_test_registry();
        let proto = create_empty_proto();

        registry
            .register("users", proto.clone(), CompatibilityMode::Backward, None)
            .await
            .unwrap();

        let schema = registry.get("users", None).await.unwrap();
        assert_eq!(schema.version, 1);

        let schema_v1 = registry.get("users", Some(1)).await.unwrap();
        assert_eq!(schema_v1.version, 1);
    }

    #[tokio::test]
    async fn test_schema_not_found() {
        let registry = create_test_registry();

        let result = registry.get("nonexistent", None).await;
        assert!(matches!(result, Err(SchemaError::NotFound { .. })));
    }

    #[tokio::test]
    async fn test_list_schemas() {
        let registry = create_test_registry();
        let proto = create_empty_proto();

        registry
            .register("users", proto.clone(), CompatibilityMode::Backward, None)
            .await
            .unwrap();
        registry
            .register("orders", proto, CompatibilityMode::Backward, None)
            .await
            .unwrap();

        let schemas = registry.list().await.unwrap();
        assert_eq!(schemas.len(), 2);
    }
}
