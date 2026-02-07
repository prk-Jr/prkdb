//! Schema types for cross-language SDK support.
//!
//! This module provides types that enable the `#[derive(Collection)]` macro
//! to generate Protobuf schema descriptors for use with the Schema Registry.

use crate::collection::Collection;

/// Trait for types that can provide their schema as a Protobuf descriptor.
///
/// This trait is automatically implemented by the `#[derive(Collection)]` macro
/// for types that have the `#[collection(proto = true)]` attribute.
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Collection, Serialize, Deserialize)]
/// #[collection(name = "users", proto = true)]
/// struct User {
///     #[key]
///     id: String,
///     
///     email: String,
///     
///     age: u32,
/// }
///
/// // Get the schema proto bytes for registration
/// let schema = User::schema_proto();
/// ```
pub trait ProtoSchema: Collection {
    /// Collection name for schema registry.
    fn collection_name() -> &'static str;

    /// Returns the field definitions for this type.
    fn field_definitions() -> &'static [FieldDef];

    /// Returns the schema as a serialized FileDescriptorProto.
    ///
    /// This can be registered with the Schema Registry via the
    /// `RegisterSchema` RPC.
    fn schema_proto() -> Vec<u8>;
}

/// Field definition for schema generation.
#[derive(Debug, Clone, Copy)]
pub struct FieldDef {
    /// Field name.
    pub name: &'static str,
    /// Protobuf field number (1-indexed, assigned in order).
    pub field_number: i32,
    /// Protobuf type for this field.
    pub proto_type: ProtoType,
    /// Whether this field is optional (wrapped in Option<T>).
    pub is_optional: bool,
    /// Whether this field is repeated (Vec<T>).
    pub is_repeated: bool,
}

/// Protobuf field types.
///
/// Maps to `google.protobuf.FieldDescriptorProto.Type` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ProtoType {
    /// double (f64)
    Double = 1,
    /// float (f32)
    Float = 2,
    /// int64 (i64)
    Int64 = 3,
    /// uint64 (u64)
    Uint64 = 4,
    /// int32 (i32)
    Int32 = 5,
    /// fixed64
    Fixed64 = 6,
    /// fixed32
    Fixed32 = 7,
    /// bool
    Bool = 8,
    /// string (String, &str)
    String = 9,
    /// Nested message (custom struct)
    Message = 11,
    /// bytes (Vec<u8>)
    Bytes = 12,
    /// uint32 (u32)
    Uint32 = 13,
    /// sfixed32
    Sfixed32 = 15,
    /// sfixed64
    Sfixed64 = 16,
    /// sint32
    Sint32 = 17,
    /// sint64
    Sint64 = 18,
}

impl ProtoType {
    /// Convert to the i32 value used by FieldDescriptorProto.
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

/// Convert a Rust type name to the corresponding ProtoType.
///
/// This is used by the macro for type mapping.
pub fn rust_type_to_proto(type_name: &str) -> ProtoType {
    match type_name {
        "String" | "&str" | "str" => ProtoType::String,
        "i32" => ProtoType::Int32,
        "i64" => ProtoType::Int64,
        "u32" => ProtoType::Uint32,
        "u64" => ProtoType::Uint64,
        "f32" => ProtoType::Float,
        "f64" => ProtoType::Double,
        "bool" => ProtoType::Bool,
        _ => ProtoType::Bytes, // Default: serialize as bytes
    }
}
