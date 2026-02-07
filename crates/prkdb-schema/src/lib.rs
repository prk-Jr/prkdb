//! # prkdb-schema
//!
//! Schema Registry for PrkDB - enables cross-language SDK support with type-safe
//! schema definitions using Protocol Buffers.
//!
//! ## Features
//!
//! - **Schema Storage**: Store Protobuf `FileDescriptorProto` for each collection
//! - **Versioning**: Auto-increment versions with breaking change detection
//! - **Compatibility**: Forward, backward, and full compatibility checking
//! - **Migrations**: Require migration functions for breaking changes
//!
//! ## Usage
//!
//! ```rust,ignore
//! use prkdb_schema::{SchemaRegistry, CompatibilityMode};
//!
//! let registry = SchemaRegistry::new(storage);
//!
//! // Register a schema
//! let version = registry.register(
//!     "users",
//!     proto_bytes,
//!     CompatibilityMode::Backward,
//!     None,
//! ).await?;
//!
//! // Get latest schema
//! let schema = registry.get("users", None).await?;
//!
//! // Check compatibility before registering
//! let result = registry.check_compatibility("users", new_proto_bytes).await?;
//! ```

mod compatibility;
mod error;
mod registry;
mod storage;
mod types;

pub use compatibility::CompatibilityChecker;
pub use error::{SchemaError, SchemaResult};
pub use registry::SchemaRegistry;
pub use storage::{FileSchemaStorage, InMemorySchemaStorage, SchemaStorage};
pub use types::{CompatibilityMode, CompatibilityResult, Schema, SchemaInfo, SchemaVersion};
