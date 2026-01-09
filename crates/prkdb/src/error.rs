use prkdb_core::error::{ComputeError, ConsumerError, StorageError};

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Item not found with the given ID")]
    NotFound,
    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
    #[error("Consumer error: {0}")]
    Consumer(#[from] ConsumerError),
    #[error("Compute error: {0}")]
    Compute(#[from] ComputeError),
}
