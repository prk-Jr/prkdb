//! Error types for PrkDB.
//!
//! This module provides a centralized error hierarchy that is shared across all PrkDB crates.

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

    #[error("Not leader. Leader is {leader_id:?}")]
    NotLeader { leader_id: Option<u64> },

    #[error("Internal error: {0}")]
    Internal(String),

    /// The write was accepted for publication and its outcome is **unknown**.
    ///
    /// # This is not a failure
    ///
    /// The write may still be published after this error is returned. Nothing about it has
    /// been rolled back, because there is nothing to roll back: the storage layer stopped
    /// being able to *say* whether the record reached the log, not the record from
    /// reaching it.
    ///
    /// A caller that reads this as "failed" and retries will double-write every request
    /// that was in flight when the writer stalled — trading a hang for silent duplication,
    /// which is the strictly worse bug. Retry only if the operation is idempotent, or read
    /// back and decide.
    ///
    /// # When it is returned
    ///
    /// Three places, all in the WAL write path (`docs/superpowers/specs/2026-08-11-wal-writer-liveness.md`):
    ///
    /// - the writer task exited — cleanly, by panic, or by cancellation — while writes
    ///   were queued (Part 1),
    /// - the stall watchdog observed no publication progress for longer than its bound and
    ///   discharged the queue (Part 2),
    /// - the caller's own bound on `rx.await` expired (Part 3).
    ///
    /// The third case is the one where "may still be published" is literally true: the
    /// batch is already with the writer and may land a moment later. The first two are
    /// reported the same way on purpose. Over-reporting uncertainty is safe; claiming a
    /// write failed when it committed is not, so the distinction is not worth the risk of
    /// getting it wrong at the one call site that matters.
    ///
    /// Deliberately **not** [`StorageError::Internal`]: `Internal` reads as a failure at
    /// every call site that already handles it, which is exactly the meaning this must not
    /// carry.
    #[error("Write not confirmed: {0} (the write may still be published — do not treat this as a failed write)")]
    WriteNotConfirmed(String),

    /// The write was **refused** before being queued, because the write path is at
    /// capacity.
    ///
    /// Unlike [`StorageError::WriteNotConfirmed`] this one is definite: nothing was
    /// enqueued, nothing will be published, and retrying after a delay is both safe and
    /// the intended response.
    ///
    /// It exists so that a stalled writer degrades into rejection rather than into
    /// unbounded buffering. Without it, Parts 1–3 of the liveness spec report the stall
    /// accurately right up until the process is killed for memory.
    #[error("Write rejected: {0}")]
    WriteBackpressure(String),

    /// The write was queued and then **discarded unwritten**, because the writer stopped.
    ///
    /// Definite, unlike [`StorageError::WriteNotConfirmed`]: these are writes still sitting
    /// in the accumulator when the watchdog fired, or refused outright once the writer had
    /// exited. Either way they were never handed to the writer, so nothing was appended to
    /// the WAL and nothing will be.
    ///
    /// **Retrying is safe and is the correct response.** That is the whole reason this is
    /// separate. Reporting these as `WriteNotConfirmed` would tell a caller the write "may
    /// still be published", and a caller who believes that will not retry — losing a write
    /// that is known to be lost. For keyed puts a retry is idempotent, so the risk here
    /// runs opposite to the double-write risk `WriteNotConfirmed` exists to avoid: there,
    /// over-reporting uncertainty is the safe direction; here, it is the lossy one.
    #[error("Write abandoned: {0} (the write was not persisted — retry it)")]
    WriteAbandoned(String),

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
    /// True when the write's outcome is unknown rather than known-bad.
    ///
    /// Call sites use this to keep the distinction rather than rediscovering it by
    /// matching: an unconfirmed write must not be retried non-idempotently, and must not
    /// be logged or reported with the word "failed".
    pub fn is_write_unconfirmed(&self) -> bool {
        matches!(self, StorageError::WriteNotConfirmed(_))
    }

    /// True when the write is **known** not to have been persisted.
    ///
    /// Distinct from [`Self::is_write_unconfirmed`] because the correct caller response
    /// differs: an abandoned write must be retried to avoid losing it, where an
    /// unconfirmed one may already have landed.
    pub fn is_write_abandoned(&self) -> bool {
        matches!(self, StorageError::WriteAbandoned(_))
    }

    /// True when the caller must not treat the write as durable — unknown *or* known-lost.
    ///
    /// The check for anything that would otherwise claim durability on the strength of "no
    /// error I recognise". Both variants deny it, for opposite reasons, and a call site
    /// that tests only one of them silently regains the bug when the other is returned.
    pub fn denies_durability(&self) -> bool {
        self.is_write_unconfirmed() || self.is_write_abandoned()
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_conversion() {
        let storage_err = StorageError::NotFound;
        let err: Error = storage_err.into();
        assert!(matches!(err, Error::Storage(StorageError::NotFound)));
    }

    #[test]
    fn test_storage_error_into_error() {
        let err = StorageError::Serialization("failed".to_string());
        let converted = err.into_error();
        assert!(matches!(converted, Error::Serialization(_)));
    }

    #[test]
    fn test_error_display() {
        let err = StorageError::BackendError("connection failed".to_string());
        assert_eq!(
            err.to_string(),
            "Failed to access underlying store: connection failed"
        );
    }

    /// Acceptance 3 of the WAL writer liveness spec, first half: the not-confirmed error
    /// is its own variant and says what it means.
    ///
    /// The assertion on the message is not decoration. This error crosses process
    /// boundaries as a string — logs, gRPC status messages, HTTP error bodies — and on the
    /// far side of that boundary the wording *is* the contract. A message that reads like
    /// a failure will be handled like one.
    #[test]
    fn write_not_confirmed_is_distinct_and_says_the_write_may_still_land() {
        let err = StorageError::WriteNotConfirmed("writer stalled".to_string());

        // Distinct from Internal, which every existing handler already reads as a failure.
        assert_ne!(err, StorageError::Internal("writer stalled".to_string()));
        assert!(!matches!(err, StorageError::Internal(_)));

        let rendered = err.to_string();
        assert!(
            rendered.contains("may still be published"),
            "message must state the write can still land, got: {rendered}"
        );
        assert!(
            !rendered.contains("failed") || rendered.contains("do not treat this as a failed"),
            "message must not read as a definite failure, got: {rendered}"
        );

        assert!(err.is_write_unconfirmed());
    }

    /// The other direction: nothing else is unconfirmed. A predicate that answers `true`
    /// for everything would pass the test above while telling call sites nothing.
    #[test]
    fn only_write_not_confirmed_is_unconfirmed() {
        for err in [
            StorageError::Internal("x".into()),
            StorageError::BackendError("x".into()),
            StorageError::WriteBackpressure("x".into()),
            StorageError::NotFound,
        ] {
            assert!(
                !err.is_write_unconfirmed(),
                "{err} must not be reported as unconfirmed"
            );
        }
    }

    /// Backpressure is the opposite claim to not-confirmed and must not be confusable with
    /// it: the write was refused, so retrying is safe.
    #[test]
    fn backpressure_is_a_definite_refusal() {
        let err = StorageError::WriteBackpressure("write queue full".to_string());
        assert!(!err.is_write_unconfirmed());
        assert!(err.to_string().contains("rejected"));
    }
}
