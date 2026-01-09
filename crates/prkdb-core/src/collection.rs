use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::hash::Hash;

pub trait Collection: Serialize + DeserializeOwned + Clone + Send + Debug + Sync + 'static {
    type Id: Serialize + DeserializeOwned + Send + Sync + Debug + Clone + PartialEq + Eq + Hash;
    fn id(&self) -> &Self::Id;
}

/// Trait for versioned collections that support schema migration
///
/// # Example
/// ```ignore
/// #[derive(Collection)]
/// struct UserV2 {
///     #[id] id: u64,
///     name: String,
///     #[migrate(default = false)]  // Added in V2
///     premium: bool,
/// }
///
/// impl Versioned for UserV2 {
///     const VERSION: u32 = 2;
///     type PreviousVersion = UserV1;
///     
///     fn migrate(old: UserV1) -> Self {
///         Self { id: old.id, name: old.name, premium: false }
///     }
/// }
/// ```
pub trait Versioned: Collection {
    /// Current schema version
    const VERSION: u32;

    /// Previous version type (use Self if this is version 1)
    type PreviousVersion: Collection;

    /// Migrate from previous version to current
    fn migrate(old: Self::PreviousVersion) -> Self;
}

/// Schema version metadata stored with records
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct VersionedRecord<T> {
    /// Schema version when this record was written
    pub version: u32,
    /// The actual record data
    pub data: T,
}

impl<T> VersionedRecord<T> {
    pub fn new(version: u32, data: T) -> Self {
        Self { version, data }
    }
}

/// Validation error with field and message
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// Field that failed validation
    pub field: String,
    /// Error message
    pub message: String,
}

impl ValidationError {
    pub fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.message)
    }
}

impl std::error::Error for ValidationError {}

/// Trait for validatable collections
///
/// Implement this trait to add validation rules to your collection.
/// Validation is automatically run on insert/update operations.
///
/// # Example
/// ```ignore
/// impl Validatable for User {
///     fn validate(&self) -> Result<(), Vec<ValidationError>> {
///         let mut errors = Vec::new();
///         
///         if self.name.is_empty() {
///             errors.push(ValidationError::new("name", "cannot be empty"));
///         }
///         if self.age > 150 {
///             errors.push(ValidationError::new("age", "must be <= 150"));
///         }
///         if !self.email.contains('@') {
///             errors.push(ValidationError::new("email", "invalid format"));
///         }
///         
///         if errors.is_empty() { Ok(()) } else { Err(errors) }
///     }
/// }
/// ```
pub trait Validatable {
    /// Validate the record, returning errors if invalid
    fn validate(&self) -> Result<(), Vec<ValidationError>>;

    /// Check if record is valid (convenience method)
    fn is_valid(&self) -> bool {
        self.validate().is_ok()
    }
}

/// Trait for soft-deletable records
///
/// Records can be marked as deleted without actually removing them from storage.
/// Useful for audit trails, recovery, and compliance requirements.
///
/// # Example
/// ```ignore
/// #[derive(Collection)]
/// struct User {
///     #[id] id: u64,
///     name: String,
///     deleted_at: Option<u64>,  // Unix timestamp
/// }
///
/// impl SoftDeletable for User {
///     fn is_deleted(&self) -> bool {
///         self.deleted_at.is_some()
///     }
///     
///     fn mark_deleted(&mut self) {
///         self.deleted_at = Some(std::time::SystemTime::now()
///             .duration_since(std::time::UNIX_EPOCH)
///             .unwrap()
///             .as_secs());
///     }
///     
///     fn restore(&mut self) {
///         self.deleted_at = None;
///     }
/// }
/// ```
pub trait SoftDeletable {
    /// Check if record is soft-deleted
    fn is_deleted(&self) -> bool;

    /// Mark record as deleted
    fn mark_deleted(&mut self);

    /// Restore a soft-deleted record
    fn restore(&mut self);

    /// Check if record is active (not deleted)
    fn is_active(&self) -> bool {
        !self.is_deleted()
    }
}

/// Trait for timestamped records
///
/// Automatically tracks created_at and updated_at timestamps.
///
/// # Example
/// ```ignore
/// impl Timestamped for User {
///     fn created_at(&self) -> u64 { self.created_at }
///     fn updated_at(&self) -> u64 { self.updated_at }
///     fn set_created_at(&mut self, ts: u64) { self.created_at = ts; }
///     fn set_updated_at(&mut self, ts: u64) { self.updated_at = ts; }
/// }
/// ```
pub trait Timestamped {
    /// Get created timestamp (Unix seconds)
    fn created_at(&self) -> u64;

    /// Get last updated timestamp (Unix seconds)
    fn updated_at(&self) -> u64;

    /// Set created timestamp
    fn set_created_at(&mut self, timestamp: u64);

    /// Set updated timestamp  
    fn set_updated_at(&mut self, timestamp: u64);

    /// Touch the record (update updated_at to now)
    fn touch(&mut self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.set_updated_at(now);
    }

    /// Initialize timestamps (for new records)
    fn init_timestamps(&mut self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.set_created_at(now);
        self.set_updated_at(now);
    }
}

/// Trait for auditable records (tracks who made changes)
///
/// # Example
/// ```ignore
/// impl Auditable for Document {
///     fn created_by(&self) -> Option<&str> { self.created_by.as_deref() }
///     fn updated_by(&self) -> Option<&str> { self.updated_by.as_deref() }
///     fn set_created_by(&mut self, user: &str) { self.created_by = Some(user.to_string()); }
///     fn set_updated_by(&mut self, user: &str) { self.updated_by = Some(user.to_string()); }
/// }
/// ```
pub trait Auditable {
    /// Get user who created the record
    fn created_by(&self) -> Option<&str>;

    /// Get user who last updated the record
    fn updated_by(&self) -> Option<&str>;

    /// Set creating user
    fn set_created_by(&mut self, user: &str);

    /// Set updating user
    fn set_updated_by(&mut self, user: &str);
}

/// Wrapper that adds computed fields to a record
///
/// Computed fields are calculated at runtime and not stored in the database.
///
/// # Example
/// ```ignore
/// struct UserWithAge {
///     user: User,
///     age_in_days: u64,
/// }
///
/// let enriched: Vec<UserWithAge> = db.query::<User>()
///     .collect().await?
///     .into_iter()
///     .map(|user| UserWithAge {
///         age_in_days: (now - user.birth_date) / 86400,
///         user,
///     })
///     .collect();
/// ```
#[derive(Debug, Clone)]
pub struct WithComputed<T, C> {
    /// The original record
    pub record: T,
    /// Computed data
    pub computed: C,
}

impl<T, C> WithComputed<T, C> {
    pub fn new(record: T, computed: C) -> Self {
        Self { record, computed }
    }

    /// Map the computed value
    pub fn map_computed<C2, F: FnOnce(C) -> C2>(self, f: F) -> WithComputed<T, C2> {
        WithComputed {
            record: self.record,
            computed: f(self.computed),
        }
    }
}

/// Trait for lifecycle hooks on collections
///
/// Implement this trait to add callbacks that run before/after database operations.
///
/// # Example
/// ```ignore
/// impl Hooks for User {
///     fn before_insert(&mut self) -> Result<(), String> {
///         self.name = self.name.trim().to_string();
///         if self.name.is_empty() {
///             return Err("Name cannot be empty".to_string());
///         }
///         Ok(())
///     }
///     
///     fn after_insert(&self) {
///         println!("User {} created!", self.id);
///     }
///     
///     fn before_update(&mut self) -> Result<(), String> {
///         self.updated_count += 1;
///         Ok(())
///     }
/// }
/// ```
pub trait Hooks {
    /// Called before inserting a new record. Can modify the record or return error.
    fn before_insert(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Called after a record is successfully inserted.
    fn after_insert(&self) {}

    /// Called before updating a record. Can modify or reject.
    fn before_update(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Called after a record is updated.
    fn after_update(&self) {}

    /// Called before deleting a record. Return error to prevent deletion.
    fn before_delete(&self) -> Result<(), String> {
        Ok(())
    }

    /// Called after a record is deleted.
    fn after_delete(&self) {}
}

/// Represents a change to a collection item (for CDC / event sourcing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeEvent<C: Collection> {
    Put(C),
    Delete(C::Id),
    /// Batch put: multiple items inserted at once
    PutBatch(Vec<C>),
    /// Batch delete: multiple items deleted at once
    DeleteBatch(Vec<C::Id>),
}

impl<C: Collection> ChangeEvent<C> {
    pub fn unwrap_put(self) -> C {
        match self {
            ChangeEvent::Put(item) => item,
            _ => panic!("Expected ChangeEvent::Put, but got {:?}", self),
        }
    }
}
