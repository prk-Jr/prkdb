//! Error types for the schema registry.

use thiserror::Error;

/// Schema registry errors.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// Schema not found for collection
    #[error("Schema not found for collection '{collection}'")]
    NotFound { collection: String },

    /// Schema version not found
    #[error("Schema version {version} not found for collection '{collection}'")]
    VersionNotFound { collection: String, version: u32 },

    /// Breaking change without migration
    #[error(
        "Breaking change detected for '{collection}': {reason}. Provide a migration_id to proceed."
    )]
    BreakingChangeWithoutMigration { collection: String, reason: String },

    /// Compatibility check failed
    #[error("Schema incompatible with '{collection}': {errors:?}")]
    IncompatibleSchema {
        collection: String,
        errors: Vec<String>,
    },

    /// Invalid schema descriptor
    #[error("Invalid schema descriptor: {0}")]
    InvalidDescriptor(String),

    /// Storage error
    #[error("Storage error: {0}")]
    Storage(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Collection already exists (for first registration)
    #[error("Collection '{0}' already has a schema registered")]
    AlreadyExists(String),
}

/// Result type for schema operations.
pub type SchemaResult<T> = Result<T, SchemaError>;
