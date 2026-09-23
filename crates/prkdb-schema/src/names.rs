//! Collection names become directory names in `FileSchemaStorage`, so they are
//! restricted to a filesystem-safe allowlist (SCH-01).
//!
//! Windows reserved device names (`CON`, `NUL`, `COM1`, ...) are out of scope:
//! PrkDB targets Unix deployments.

use crate::compatibility::CompatibilityChecker;
use crate::error::{SchemaError, SchemaResult};

/// Maximum length, in bytes, of a valid collection name.
pub const MAX_COLLECTION_NAME_LEN: usize = 128;

/// Number of characters of an invalid/conflicting name to echo back in error
/// messages before truncating with an ellipsis.
const MAX_NAME_CHARS_IN_ERROR: usize = 64;

/// Validate that `name` is safe to use as a `FileSchemaStorage` directory
/// component: non-empty, at most [`MAX_COLLECTION_NAME_LEN`] bytes, not
/// starting with `.`, and drawn only from `[A-Za-z0-9_.-]`.
pub fn validate_collection_name(name: &str) -> SchemaResult<()> {
    let ok = !name.is_empty()
        && name.len() <= MAX_COLLECTION_NAME_LEN
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(SchemaError::InvalidCollectionName(truncate_for_error(name)))
    }
}

/// Validate that `bytes` decode to a non-trivial schema descriptor.
///
/// Rejects empty input outright, then reuses
/// [`CompatibilityChecker::decode_descriptor`] (which accepts either a
/// `FileDescriptorSet` or a bare `FileDescriptorProto`) and requires the
/// decoded descriptor to carry a `name` or at least one `message_type`, so a
/// technically-valid-but-empty descriptor cannot slip through.
pub(crate) fn validate_descriptor(bytes: &[u8]) -> SchemaResult<()> {
    if bytes.is_empty() {
        return Err(SchemaError::InvalidDescriptor(
            "descriptor is empty".to_string(),
        ));
    }

    let file =
        CompatibilityChecker::decode_descriptor(bytes).map_err(SchemaError::InvalidDescriptor)?;

    if file.name.is_none() && file.message_type.is_empty() {
        return Err(SchemaError::InvalidDescriptor(
            "descriptor has no file name and no message types".to_string(),
        ));
    }

    Ok(())
}

/// Truncate `name` to [`MAX_NAME_CHARS_IN_ERROR`] characters with a trailing
/// ellipsis, so untrusted (e.g. attacker-controlled, oversized) names cannot
/// blow up error messages or log lines.
pub(crate) fn truncate_for_error(name: &str) -> String {
    if name.chars().count() > MAX_NAME_CHARS_IN_ERROR {
        let truncated: String = name.chars().take(MAX_NAME_CHARS_IN_ERROR).collect();
        format!("{truncated}…")
    } else {
        name.to_string()
    }
}
