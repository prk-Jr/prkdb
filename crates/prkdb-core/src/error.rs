use thiserror::Error;

/// Centralized error type for PrkDB
#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Storage error: {0}")]
    Storage(StorageError),

    #[error("Compute error: {0}")]
    Compute(ComputeError),

    #[error("Consumer error: {0}")]
    Consumer(ConsumerError),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Item not found")]
    NotFound,

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Operation timeout: {0}")]
    Timeout(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Storage-specific errors
#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum StorageError {
    #[error("Failed to access underlying store: {0}")]
    BackendError(String),

    #[error("Failed to serialize data: {0}")]
    Serialization(String),

    #[error("Failed to deserialize data: {0}")]
    Deserialization(String),

    #[error("Key not found")]
    NotFound,

    #[error("Transaction failed: {0}")]
    TransactionFailed(String),

    #[error("Replication failed: {0}")]
    Replication(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Data corruption detected: {0}")]
    Corruption(String),

    #[error("Recovery failed: {0}")]
    Recovery(String),

    #[error("Checksum mismatch: expected {expected}, found {found}")]
    ChecksumMismatch { expected: u32, found: u32 },

    #[error("Validation failed: {0}")]
    Validation(String),
}

/// Compute handler errors
#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum ComputeError {
    #[error("Handler failed: {0}")]
    Handler(String),

    #[error("State serialization failed: {0}")]
    StateSerialization(String),

    #[error("State deserialization failed: {0}")]
    StateDeserialization(String),

    #[error("Storage error: {0}")]
    Storage(String),
}

/// Consumer-specific errors
#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum ConsumerError {
    #[error("Failed to poll events: {0}")]
    PollFailed(String),

    #[error("Failed to commit offset: {0}")]
    CommitFailed(String),

    #[error("Failed to seek to offset: {0}")]
    SeekFailed(String),

    #[error("Invalid offset: {0}")]
    InvalidOffset(String),

    #[error("Consumer group error: {0}")]
    GroupError(String),

    #[error("Storage error: {0}")]
    Storage(String),
}

// Implement conversions from specific errors to Error
impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        Error::Storage(e)
    }
}

impl From<ComputeError> for Error {
    fn from(e: ComputeError) -> Self {
        Error::Compute(e)
    }
}

impl From<ConsumerError> for Error {
    fn from(e: ConsumerError) -> Self {
        Error::Consumer(e)
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Internal(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Internal(s.to_string())
    }
}

// Allow converting StorageError variants to Error::Serialization/Deserialization
impl StorageError {
    pub fn into_error(self) -> Error {
        match self {
            StorageError::Serialization(s) => Error::Serialization(s),
            StorageError::Deserialization(s) => Error::Deserialization(s),
            StorageError::NotFound => Error::NotFound,
            other => Error::Storage(other),
        }
    }
}

// Allow converting ConsumerError to Error with storage context
impl From<StorageError> for ConsumerError {
    fn from(e: StorageError) -> Self {
        ConsumerError::Storage(e.to_string())
    }
}

// Allow converting StorageError to ComputeError
impl From<StorageError> for ComputeError {
    fn from(e: StorageError) -> Self {
        ComputeError::Storage(e.to_string())
    }
}
