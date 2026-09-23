//! Collection names become directory names in FileSchemaStorage, so they are
//! restricted to a filesystem-safe allowlist (SCH-01).

use crate::error::{SchemaError, SchemaResult};

pub const MAX_COLLECTION_NAME_LEN: usize = 128;

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
        Err(SchemaError::InvalidCollectionName(name.to_string()))
    }
}

pub fn validate_descriptor(bytes: &[u8]) -> SchemaResult<()> {
    use prost::Message;
    use prost_types::{FileDescriptorProto, FileDescriptorSet};

    // Schemas may be encoded as either a `FileDescriptorSet` (as produced by
    // `#[derive(Collection)]`'s `schema_proto()`) or a bare `FileDescriptorProto`.
    // Mirror `CompatibilityChecker::decode_descriptor`'s dual-decode logic so
    // both encodings are accepted here.
    if let Ok(set) = FileDescriptorSet::decode(bytes) {
        if !set.file.is_empty() {
            return Ok(());
        }
    }

    FileDescriptorProto::decode(bytes)
        .map(|_| ())
        .map_err(|e| SchemaError::InvalidDescriptor(e.to_string()))
}
