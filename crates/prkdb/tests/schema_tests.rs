//! Integration tests for the Schema Registry and ProtoSchema trait
//!
//! Tests the end-to-end flow of:
//! 1. ProtoSchema trait auto-implementation via #[derive(Collection)]
//! 2. Schema registration with the registry
//! 3. Schema retrieval and versioning
//! 4. Schema descriptor parsing

use prkdb_macros::Collection;
use prkdb_types::schema::{FieldDef, ProtoSchema, ProtoType};
use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────────────────────
// Test Models using #[derive(Collection)]
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct User {
    #[key]
    id: String,
    email: String,
    age: u32,
    active: bool,
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Product {
    #[key]
    sku: String,
    name: String,
    price: f64,
    quantity: u32,
    tags: Vec<String>,
    description: Option<String>,
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Order {
    #[key]
    order_id: u64,
    user_id: String,
    total: f64,
    items: Vec<String>,
    created_at: i64,
}

// ─────────────────────────────────────────────────────────────────────────────
// ProtoSchema Trait Tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_proto_schema_collection_name() {
    assert_eq!(User::collection_name(), "User");
    assert_eq!(Product::collection_name(), "Product");
    assert_eq!(Order::collection_name(), "Order");
}

#[test]
fn test_proto_schema_field_definitions_user() {
    let fields = User::field_definitions();

    assert_eq!(fields.len(), 4);

    // Check id field
    assert_eq!(fields[0].name, "id");
    assert_eq!(fields[0].field_number, 1);
    assert!(matches!(fields[0].proto_type, ProtoType::String));
    assert!(!fields[0].is_optional);
    assert!(!fields[0].is_repeated);

    // Check email field
    assert_eq!(fields[1].name, "email");
    assert_eq!(fields[1].field_number, 2);
    assert!(matches!(fields[1].proto_type, ProtoType::String));

    // Check age field
    assert_eq!(fields[2].name, "age");
    assert_eq!(fields[2].field_number, 3);
    assert!(matches!(fields[2].proto_type, ProtoType::Uint32));

    // Check active field
    assert_eq!(fields[3].name, "active");
    assert_eq!(fields[3].field_number, 4);
    assert!(matches!(fields[3].proto_type, ProtoType::Bool));
}

#[test]
fn test_proto_schema_field_definitions_product() {
    let fields = Product::field_definitions();

    assert_eq!(fields.len(), 6);

    // Check for repeated field (Vec<String>)
    let tags_field = fields.iter().find(|f| f.name == "tags").unwrap();
    assert!(
        tags_field.is_repeated,
        "Vec<String> should be marked as repeated"
    );

    // Check for optional field (Option<String>)
    let desc_field = fields.iter().find(|f| f.name == "description").unwrap();
    assert!(
        desc_field.is_optional,
        "Option<String> should be marked as optional"
    );

    // Check price field (f64 -> Double)
    let price_field = fields.iter().find(|f| f.name == "price").unwrap();
    assert!(matches!(price_field.proto_type, ProtoType::Double));
}

#[test]
fn test_proto_schema_field_definitions_order() {
    let fields = Order::field_definitions();

    assert_eq!(fields.len(), 5);

    // Check order_id (u64 -> Uint64)
    let order_id_field = fields.iter().find(|f| f.name == "order_id").unwrap();
    assert!(matches!(order_id_field.proto_type, ProtoType::Uint64));

    // Check created_at (i64 -> Int64)
    let created_at_field = fields.iter().find(|f| f.name == "created_at").unwrap();
    assert!(matches!(created_at_field.proto_type, ProtoType::Int64));
}

#[test]
fn test_proto_schema_generates_bytes() {
    let schema_bytes = User::schema_proto();

    // Should generate non-empty bytes
    assert!(
        !schema_bytes.is_empty(),
        "schema_proto() should not be empty"
    );

    // Minimum size: name_len(4) + "User"(4) + field_count(4) = 12 bytes minimum
    assert!(
        schema_bytes.len() >= 12,
        "schema_proto() should have minimum header size"
    );
}

#[test]
fn test_proto_schema_bytes_parseable() {
    let schema_bytes = User::schema_proto();

    // Parse the binary format we generate
    let mut cursor = 0;

    // Read message name length
    let name_len =
        u32::from_le_bytes(schema_bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;

    // Read message name
    let name = String::from_utf8_lossy(&schema_bytes[cursor..cursor + name_len]).to_string();
    cursor += name_len;
    assert_eq!(name, "User");

    // Read field count
    let field_count =
        u32::from_le_bytes(schema_bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;
    assert_eq!(field_count, 4);

    // Parse first field (id)
    let field_name_len =
        u32::from_le_bytes(schema_bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;
    let field_name =
        String::from_utf8_lossy(&schema_bytes[cursor..cursor + field_name_len]).to_string();
    cursor += field_name_len;
    assert_eq!(field_name, "id");

    let field_number = i32::from_le_bytes(schema_bytes[cursor..cursor + 4].try_into().unwrap());
    cursor += 4;
    assert_eq!(field_number, 1);

    let proto_type = i32::from_le_bytes(schema_bytes[cursor..cursor + 4].try_into().unwrap());
    cursor += 4;
    assert_eq!(proto_type, ProtoType::String.as_i32());

    let is_optional = schema_bytes[cursor] != 0;
    cursor += 1;
    assert!(!is_optional);

    let is_repeated = schema_bytes[cursor] != 0;
    assert!(!is_repeated);
}

// ─────────────────────────────────────────────────────────────────────────────
// Schema Registry Tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_schema_registry_register_and_get() {
    use prkdb_schema::{CompatibilityMode, InMemorySchemaStorage, SchemaRegistry};
    use std::sync::Arc;

    let storage = Arc::new(InMemorySchemaStorage::new());
    let registry = SchemaRegistry::new(storage);

    // Register User schema
    let schema_id = registry
        .register("users", User::schema_proto(), CompatibilityMode::None, None)
        .await
        .expect("Should register schema");

    assert!(schema_id.schema_id > 0);
    assert_eq!(schema_id.version, 1);

    // Retrieve schema
    let schema = registry
        .get("users", None)
        .await
        .expect("Should get schema");

    assert_eq!(schema.version, 1);
    assert_eq!(schema.descriptor, User::schema_proto());
}

#[tokio::test]
async fn test_schema_registry_versioning() {
    use prkdb_schema::{CompatibilityMode, InMemorySchemaStorage, SchemaRegistry};
    use std::sync::Arc;

    let storage = Arc::new(InMemorySchemaStorage::new());
    let registry = SchemaRegistry::new(storage);

    // Register v1
    let v1 = registry
        .register(
            "products",
            Product::schema_proto(),
            CompatibilityMode::None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(v1.version, 1);

    // Register v2 (same schema, new version)
    let v2 = registry
        .register(
            "products",
            Product::schema_proto(),
            CompatibilityMode::None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(v2.version, 2);

    // Get latest (should be v2)
    let latest = registry.get("products", None).await.unwrap();
    assert_eq!(latest.version, 2);

    // Get specific version (v1)
    let first = registry.get("products", Some(1)).await.unwrap();
    assert_eq!(first.version, 1);
}

#[tokio::test]
async fn test_schema_registry_list_schemas() {
    use prkdb_schema::{CompatibilityMode, InMemorySchemaStorage, SchemaRegistry};
    use std::sync::Arc;

    let storage = Arc::new(InMemorySchemaStorage::new());
    let registry = SchemaRegistry::new(storage);

    // Register multiple schemas
    registry
        .register("users", User::schema_proto(), CompatibilityMode::None, None)
        .await
        .unwrap();
    registry
        .register(
            "products",
            Product::schema_proto(),
            CompatibilityMode::None,
            None,
        )
        .await
        .unwrap();
    registry
        .register(
            "orders",
            Order::schema_proto(),
            CompatibilityMode::None,
            None,
        )
        .await
        .unwrap();

    // List all
    let list = registry.list().await.unwrap();
    assert_eq!(list.len(), 3);

    let collections: Vec<_> = list.iter().map(|s| s.collection.as_str()).collect();
    assert!(collections.contains(&"users"));
    assert!(collections.contains(&"products"));
    assert!(collections.contains(&"orders"));
}

#[tokio::test]
async fn test_schema_registry_not_found() {
    use prkdb_schema::{InMemorySchemaStorage, SchemaRegistry};
    use std::sync::Arc;

    let storage = Arc::new(InMemorySchemaStorage::new());
    let registry = SchemaRegistry::new(storage);

    let result = registry.get("nonexistent", None).await;
    assert!(result.is_err());
}

// ─────────────────────────────────────────────────────────────────────────────
// Type Mapping Tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_proto_type_as_i32() {
    // Verify ProtoType values match Protobuf spec
    assert_eq!(ProtoType::Double.as_i32(), 1);
    assert_eq!(ProtoType::Float.as_i32(), 2);
    assert_eq!(ProtoType::Int64.as_i32(), 3);
    assert_eq!(ProtoType::Uint64.as_i32(), 4);
    assert_eq!(ProtoType::Int32.as_i32(), 5);
    assert_eq!(ProtoType::Bool.as_i32(), 8);
    assert_eq!(ProtoType::String.as_i32(), 9);
    assert_eq!(ProtoType::Bytes.as_i32(), 12);
    assert_eq!(ProtoType::Uint32.as_i32(), 13);
}

#[test]
fn test_rust_type_to_proto_mapping() {
    use prkdb_types::schema::rust_type_to_proto;

    assert!(matches!(rust_type_to_proto("String"), ProtoType::String));
    assert!(matches!(rust_type_to_proto("&str"), ProtoType::String));
    assert!(matches!(rust_type_to_proto("i32"), ProtoType::Int32));
    assert!(matches!(rust_type_to_proto("i64"), ProtoType::Int64));
    assert!(matches!(rust_type_to_proto("u32"), ProtoType::Uint32));
    assert!(matches!(rust_type_to_proto("u64"), ProtoType::Uint64));
    assert!(matches!(rust_type_to_proto("f32"), ProtoType::Float));
    assert!(matches!(rust_type_to_proto("f64"), ProtoType::Double));
    assert!(matches!(rust_type_to_proto("bool"), ProtoType::Bool));
    // Unknown types default to Bytes
    assert!(matches!(rust_type_to_proto("CustomType"), ProtoType::Bytes));
}
