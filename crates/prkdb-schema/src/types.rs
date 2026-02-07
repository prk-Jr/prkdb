//! Core types for the schema registry.

use serde::{Deserialize, Serialize};

/// Compatibility modes for schema evolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CompatibilityMode {
    /// No compatibility checking
    None,
    /// New schema can read old data (default, recommended)
    #[default]
    Backward,
    /// Old schema can read new data
    Forward,
    /// Both backward and forward compatible
    Full,
}

impl From<i32> for CompatibilityMode {
    fn from(value: i32) -> Self {
        match value {
            1 => CompatibilityMode::Backward,
            2 => CompatibilityMode::Forward,
            3 => CompatibilityMode::Full,
            _ => CompatibilityMode::None,
        }
    }
}

impl From<CompatibilityMode> for i32 {
    fn from(mode: CompatibilityMode) -> Self {
        match mode {
            CompatibilityMode::None => 0,
            CompatibilityMode::Backward => 1,
            CompatibilityMode::Forward => 2,
            CompatibilityMode::Full => 3,
        }
    }
}

/// Version information for a schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Auto-incremented version number
    pub version: u32,
    /// True if this version introduced a breaking change
    pub is_breaking: bool,
    /// Migration ID if breaking change (required for breaking changes)
    pub migration_id: Option<String>,
    /// Unix timestamp in milliseconds
    pub created_at: u64,
}

/// A registered schema for a collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Unique schema ID
    pub schema_id: u32,
    /// Collection name
    pub collection: String,
    /// Version of this schema
    pub version: u32,
    /// Serialized FileDescriptorProto bytes
    pub descriptor: Vec<u8>,
    /// Compatibility mode
    pub compatibility: CompatibilityMode,
    /// True if this version is a breaking change
    pub is_breaking: bool,
    /// Migration ID if breaking
    pub migration_id: Option<String>,
    /// Unix timestamp when created (ms)
    pub created_at: u64,
}

/// Summary information about a schema (for listing).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    /// Collection name
    pub collection: String,
    /// Latest version number
    pub latest_version: u32,
    /// Unique schema ID
    pub schema_id: u32,
    /// Compatibility mode
    pub compatibility: CompatibilityMode,
    /// When first created (ms)
    pub created_at: u64,
    /// When last updated (ms)
    pub updated_at: u64,
}

/// Result of a compatibility check.
#[derive(Debug, Clone)]
pub struct CompatibilityResult {
    /// True if compatible according to the mode
    pub compatible: bool,
    /// True if the change is considered breaking
    pub is_breaking: bool,
    /// List of compatibility errors
    pub errors: Vec<String>,
    /// List of warnings (non-blocking)
    pub warnings: Vec<String>,
}

impl Default for CompatibilityResult {
    fn default() -> Self {
        Self {
            compatible: true,
            is_breaking: false,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }
}
