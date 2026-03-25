# Secondary Indexes

While key-value stores excel at fetching records by their primary ID, real-world application schemas often require lookup by entirely different fields (like finding a user by email, or fetching all orders within a specific date range).

PrkDB bridges the gap between raw bytes and high-level Query Engines by enabling declarative **Secondary Indexes** directly into your Rust struct definitions.

## The `#[index]` Macro

When defining your schema, simply attach the `#[index]` attribute to any struct fields you wish to query against later.

```rust
use prkdb::prelude::*;

#[derive(Collection, Clone, Serialize, Deserialize)]
struct User {
    #[key] // Primary Lookup ID
    pub id: String,
    
    #[index] // Range-Queryable Standard Index
    pub age: u32,
    
    #[index(unique)] // Strict Unique Constraints
    pub email: String,
}
```

Behind the scenes, PrkDB's `IndexedStorage` engine seamlessly intercepts insertions and maintains synchronized B-Tree lookup tables for these flagged fields.

## Querying Fields

Once indexed, you can leverage type-safe fluent builders or native query lookups across your collection. 

```rust
// Requires importing IndexedStorage extensions
let db = IndexedStorage::new(storage);

// Fetch a single unique record
let alice: Option<User> = db.query_unique_by("email", &"alice@example.com").await?;

// Perform range scans directly on the indexed tree
let adults: Vec<User> = db.query_range("age", &18, &65).await?;

// Utilize prefix scans for robust searching
let al_names: Vec<User> = db.query_prefix("name", "Al").await?;
```

Because these queries map directly to internal native trees, they are strictly `O(log N)` complexity, preventing costly linear scans across your dataset.

## Compound & Custom Indexes

If you need queries that check multiple fields simultaneously, you can generate Compound Indexes programmatically at runtime. 

```rust
// Declare a persistent compound index bridging 'role' and 'age'
db.create_compound_index::<User>("role_age", |u| {
    vec![u.role.clone(), u.age.to_string()]
}).await?;

// Fetch Users who are Admins AND aged precisely 30
let thirty_admins = db.query_compound::<User>("role_age", 
    vec!["Admin".into(), "30".into()]
).await?;
```

## Index Persistence

It's highly recommended to utilize PrkDB's background synchronization engine to flush Index tree states persistently to disk. Without this, your database will need to rebuild its entire index catalog synchronously at startup!

```rust
// Auto-sync in background thread every 30 seconds
db.start_auto_sync(Duration::from_secs(30)).await;
```
