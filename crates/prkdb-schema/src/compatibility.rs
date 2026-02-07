//! Schema compatibility checking.
//!
//! Detects breaking changes between schema versions.

use crate::types::{CompatibilityMode, CompatibilityResult};
use prost::Message;
use prost_types::{FileDescriptorProto, FileDescriptorSet};
use std::collections::HashSet;

/// Checks compatibility between schema versions.
pub struct CompatibilityChecker;

impl CompatibilityChecker {
    /// Check if a new schema is compatible with an existing schema.
    pub fn check(existing: &[u8], new: &[u8], mode: CompatibilityMode) -> CompatibilityResult {
        // For None mode, skip all checking
        if mode == CompatibilityMode::None {
            return CompatibilityResult::default();
        }

        let mut result = CompatibilityResult::default();

        // Parse descriptors
        let existing_desc = match Self::decode_descriptor(existing) {
            Ok(d) => d,
            Err(e) => {
                result.compatible = false;
                result
                    .errors
                    .push(format!("Failed to parse existing schema: {}", e));
                return result;
            }
        };

        let new_desc = match Self::decode_descriptor(new) {
            Ok(d) => d,
            Err(e) => {
                result.compatible = false;
                result
                    .errors
                    .push(format!("Failed to parse new schema: {}", e));
                return result;
            }
        };

        // Check based on compatibility mode
        match mode {
            CompatibilityMode::None => {
                // No checking, always compatible
            }
            CompatibilityMode::Backward => {
                Self::check_backward(&existing_desc, &new_desc, &mut result);
            }
            CompatibilityMode::Forward => {
                Self::check_forward(&existing_desc, &new_desc, &mut result);
            }
            CompatibilityMode::Full => {
                Self::check_backward(&existing_desc, &new_desc, &mut result);
                Self::check_forward(&existing_desc, &new_desc, &mut result);
            }
        }

        result
    }

    /// Helper to decode schema bytes as either FileDescriptorSet or FileDescriptorProto
    fn decode_descriptor(bytes: &[u8]) -> Result<FileDescriptorProto, String> {
        // Try decoding as FileDescriptorSet first
        if let Ok(set) = FileDescriptorSet::decode(bytes) {
            if let Some(file) = set.file.into_iter().next() {
                return Ok(file);
            }
        }

        // Fallback to FileDescriptorProto
        FileDescriptorProto::decode(bytes).map_err(|e| e.to_string())
    }

    /// Check backward compatibility (new schema can read old data).
    fn check_backward(
        existing: &FileDescriptorProto,
        new: &FileDescriptorProto,
        result: &mut CompatibilityResult,
    ) {
        // Get existing field numbers
        let existing_fields: HashSet<i32> = existing
            .message_type
            .iter()
            .flat_map(|m| m.field.iter().map(|f| f.number()))
            .collect();

        let new_fields: HashSet<i32> = new
            .message_type
            .iter()
            .flat_map(|m| m.field.iter().map(|f| f.number()))
            .collect();

        // Check for removed required fields
        for field_num in existing_fields.difference(&new_fields) {
            // Check if the removed field was required
            if let Some(field) = existing
                .message_type
                .iter()
                .flat_map(|m| m.field.iter())
                .find(|f| f.number() == *field_num)
            {
                let field_name = field.name();
                result.is_breaking = true;
                result.errors.push(format!(
                    "Field '{}' (number {}) was removed - breaking for backward compatibility",
                    field_name, field_num
                ));
            }
        }

        // Check for type changes on existing fields
        for msg in &new.message_type {
            for field in &msg.field {
                if let Some(old_msg) = existing
                    .message_type
                    .iter()
                    .find(|m| m.name() == msg.name())
                {
                    if let Some(old_field) =
                        old_msg.field.iter().find(|f| f.number() == field.number())
                    {
                        if old_field.r#type() != field.r#type() {
                            result.is_breaking = true;
                            result.errors.push(format!(
                                "Field '{}' type changed from {:?} to {:?} - breaking change",
                                field.name(),
                                old_field.r#type(),
                                field.r#type()
                            ));
                        }
                    }
                }
            }
        }

        // New fields are fine for backward compatibility (they'll have defaults)
        for field_num in new_fields.difference(&existing_fields) {
            if let Some(field) = new
                .message_type
                .iter()
                .flat_map(|m| m.field.iter())
                .find(|f| f.number() == *field_num)
            {
                result.warnings.push(format!(
                    "New field '{}' (number {}) added - ensure it has a default value",
                    field.name(),
                    field_num
                ));
            }
        }

        result.compatible = result.errors.is_empty();
    }

    /// Check forward compatibility (old schema can read new data).
    fn check_forward(
        existing: &FileDescriptorProto,
        new: &FileDescriptorProto,
        result: &mut CompatibilityResult,
    ) {
        let new_fields: HashSet<i32> = new
            .message_type
            .iter()
            .flat_map(|m| m.field.iter().map(|f| f.number()))
            .collect();

        let existing_fields: HashSet<i32> = existing
            .message_type
            .iter()
            .flat_map(|m| m.field.iter().map(|f| f.number()))
            .collect();

        // For forward compatibility, new required fields break things
        for field_num in new_fields.difference(&existing_fields) {
            if let Some(field) = new
                .message_type
                .iter()
                .flat_map(|m| m.field.iter())
                .find(|f| f.number() == *field_num)
            {
                // Note: In proto3, all fields are optional by default
                result.warnings.push(format!(
                    "New field '{}' (number {}) - old clients will ignore this",
                    field.name(),
                    field_num
                ));
            }
        }

        // If there are no errors from backward check, forward is compatible
        // (proto3 ignores unknown fields by default)
    }

    /// Quick check if a change is breaking (for auto-versioning).
    pub fn is_breaking(existing: &[u8], new: &[u8]) -> bool {
        let result = Self::check(existing, new, CompatibilityMode::Backward);
        result.is_breaking
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_schemas() {
        let empty = FileDescriptorProto::default();
        let bytes = empty.encode_to_vec();

        let result = CompatibilityChecker::check(&bytes, &bytes, CompatibilityMode::Backward);
        assert!(result.compatible);
        assert!(!result.is_breaking);
    }
}
