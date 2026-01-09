// Secondary Index Example
// Demonstrates PrkDB's macro-generated typed queries with actual storage
//
// Run: cargo run --release --example index_example

use prkdb::indexed_storage::IndexedWalStorage;
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_macros::Collection;
use prkdb_types::index::Indexed;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Example enum for indexed field
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Role {
    Admin,
    User,
    Guest,
}

/// User collection with indexed fields
#[derive(Collection, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct User {
    #[id]
    pub id: String,

    #[index]
    pub role: Role,

    #[index(unique)]
    pub email: String,

    pub name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!();
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║               PrkDB Secondary Indexes Example                 ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();

    // Create storage
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(WalStorageAdapter::new(WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    })?);
    let db = IndexedWalStorage::new(storage);

    // ═══════════════════════════════════════════════════════════════════════
    // Example 1: Macro-Generated Index Definitions
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  1. Macro-Generated Index Definitions");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let indexes = User::indexes();
    println!("\n  User::indexes() = [");
    for def in indexes {
        println!("    {{ field: \"{}\", unique: {} }}", def.field, def.unique);
    }
    println!("  ]");

    // Assertion: We should have 2 indexes
    assert_eq!(indexes.len(), 2, "Expected 2 indexes");
    assert_eq!(indexes[0].field, "role");
    assert!(!indexes[0].unique, "role should not be unique");
    assert_eq!(indexes[1].field, "email");
    assert!(indexes[1].unique, "email should be unique");
    println!("  ✅ Index assertions passed!");

    // Demonstrate type-safe field accessors
    println!("\n  Type-safe field accessors:");
    let fields = User::fields();
    println!("    User::fields().id = \"{}\"", fields.id);
    println!("    User::fields().role = \"{}\"", fields.role);
    println!("    User::fields().email = \"{}\"", fields.email);
    assert_eq!(fields.role, "role");
    assert_eq!(fields.email, "email");
    println!("  ✅ Field accessor assertions passed!");
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // Example 2: Insert Users
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  2. Insert Users (indexes auto-updated)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let users = vec![
        User {
            id: "1".into(),
            role: Role::Admin,
            email: "alice@example.com".into(),
            name: "Alice".into(),
        },
        User {
            id: "2".into(),
            role: Role::User,
            email: "bob@example.com".into(),
            name: "Bob".into(),
        },
        User {
            id: "3".into(),
            role: Role::Admin,
            email: "charlie@example.com".into(),
            name: "Charlie".into(),
        },
        User {
            id: "4".into(),
            role: Role::User,
            email: "diana@example.com".into(),
            name: "Diana".into(),
        },
        User {
            id: "5".into(),
            role: Role::Guest,
            email: "eve@example.com".into(),
            name: "Eve".into(),
        },
    ];

    for user in &users {
        db.insert(user).await?;
        println!("  Inserted: {} ({:?})", user.name, user.role);
    }
    println!("  ✅ 5 users inserted!");
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // Example 3: Query by Index
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  3. Query by Index (query_by)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Query admins
    let admins: Vec<User> = db.query_by("role", &Role::Admin).await?;
    println!(
        "\n  db.query_by(\"role\", Role::Admin) = {} users",
        admins.len()
    );
    for admin in &admins {
        println!("    → {} ({})", admin.name, admin.email);
    }
    assert_eq!(admins.len(), 2, "Expected 2 admins");
    assert!(
        admins.iter().any(|u| u.name == "Alice"),
        "Alice should be an admin"
    );
    assert!(
        admins.iter().any(|u| u.name == "Charlie"),
        "Charlie should be an admin"
    );
    println!("  ✅ Admin query assertions passed!");

    // Query users
    let regular_users: Vec<User> = db.query_by("role", &Role::User).await?;
    println!(
        "\n  db.query_by(\"role\", Role::User) = {} users",
        regular_users.len()
    );
    for user in &regular_users {
        println!("    → {} ({})", user.name, user.email);
    }
    assert_eq!(regular_users.len(), 2, "Expected 2 regular users");
    assert!(
        regular_users.iter().any(|u| u.name == "Bob"),
        "Bob should be a user"
    );
    assert!(
        regular_users.iter().any(|u| u.name == "Diana"),
        "Diana should be a user"
    );
    println!("  ✅ User query assertions passed!");

    // Query guests
    let guests: Vec<User> = db.query_by("role", &Role::Guest).await?;
    println!(
        "\n  db.query_by(\"role\", Role::Guest) = {} users",
        guests.len()
    );
    for guest in &guests {
        println!("    → {} ({})", guest.name, guest.email);
    }
    assert_eq!(guests.len(), 1, "Expected 1 guest");
    assert_eq!(guests[0].name, "Eve", "Eve should be a guest");
    println!("  ✅ Guest query assertions passed!");
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // Example 4: Query by Unique Index
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  4. Query by Unique Index (query_unique_by)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Find by email (unique)
    let alice: Option<User> = db.query_unique_by("email", &"alice@example.com").await?;
    println!("\n  db.query_unique_by(\"email\", \"alice@example.com\")");
    assert!(alice.is_some(), "Alice should be found by email");
    let alice = alice.unwrap();
    assert_eq!(alice.name, "Alice", "Found user should be Alice");
    assert_eq!(alice.id, "1", "Alice's ID should be 1");
    println!("    → Found: {} (id: {})", alice.name, alice.id);
    println!("  ✅ Unique query assertions passed!");

    // Non-existent email
    let nobody: Option<User> = db.query_unique_by("email", &"nobody@example.com").await?;
    println!("\n  db.query_unique_by(\"email\", \"nobody@example.com\")");
    assert!(
        nobody.is_none(),
        "Nobody should be found for non-existent email"
    );
    println!("    → Found: None");
    println!("  ✅ Non-existent query assertion passed!");
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // Example 5: Delete and Verify Index Updated
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  5. Delete Updates Index");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Count admins before
    let admins_before: Vec<User> = db.query_by("role", &Role::Admin).await?;
    println!("\n  Admins before delete: {}", admins_before.len());
    assert_eq!(admins_before.len(), 2, "Should have 2 admins before delete");

    // Delete Charlie (admin)
    let charlie = users.iter().find(|u| u.name == "Charlie").unwrap();
    db.delete(charlie).await?;
    println!("  Deleted: Charlie");

    // Count admins after
    let admins_after: Vec<User> = db.query_by("role", &Role::Admin).await?;
    println!("  Admins after delete: {}", admins_after.len());
    assert_eq!(admins_after.len(), 1, "Should have 1 admin after delete");
    assert_eq!(
        admins_after[0].name, "Alice",
        "Only Alice should remain as admin"
    );
    println!("  ✅ Delete and index update assertions passed!");
    println!();

    // Summary
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║              All Assertions Passed! ✅                        ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("  ✅ Index definitions correct (2 indexes: role, email)");
    println!("  ✅ Insert works (5 users inserted)");
    println!("  ✅ query_by(role, Admin) returns 2 users");
    println!("  ✅ query_by(role, User) returns 2 users");
    println!("  ✅ query_by(role, Guest) returns 1 user");
    println!("  ✅ query_unique_by(email) finds correct user");
    println!("  ✅ query_unique_by(non-existent) returns None");
    println!("  ✅ delete() correctly updates indexes");
    println!();

    Ok(())
}
