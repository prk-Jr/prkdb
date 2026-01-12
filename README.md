# PrkDB

**A high-performance, Rust-native event streaming database**

[![Performance](https://img.shields.io/badge/Performance-7.3M%20ops%2Fsec-brightgreen)]()
[![Tests](https://img.shields.io/badge/Tests-30%2B%20passing-brightgreen)]()
[![Chaos Tests](https://img.shields.io/badge/Chaos%20Tests-14%20passing-brightgreen)]()
[![Rust](https://img.shields.io/badge/Rust-1.70+-orange)]()

## 🚀 Features

- **878K batch writes/sec** - Lock-free DashMap indexes + bulk WAL (76x improvement!)
- **894K queries/sec** - Lock-free indexed lookups
- **ACID Transactions** - Commit/rollback, savepoints, conflict detection
- **TTL/Expiration** - Auto-expire records after configurable duration
- **Secondary Indexes** - Type-safe queries with `#[index]` macro
- **Raft consensus** - Multi-node distributed replication with Pre-Vote protocol
- **Advanced Sharding** - Consistent hashing and range-based partitioning
- **Read Consistency Levels** - Linearizable, stale, and follower reads
- **Kafka-style consumers** - Consumer groups with offset tracking
- **ORM layer** - SQLite, PostgreSQL, MySQL support
- **Type-safe collections** - `#[derive(Collection)]` macro
- **Built-in monitoring** - Prometheus + Grafana dashboards
- **Checkpoint Recovery** - Fast startup with incremental WAL recovery

## 🧱 Modular Architecture

The database is now composed of loosely coupled crates, enabling lightweight clients and flexible deployments:

- **`prkdb-client`**: A stand-alone, smart client that routes requests to the correct partition leader. It depends only on `prkdb-proto` and `tonic`, making it perfect for building microservices that talk to PrkDB.
- **`prkdb-types`**: Pure data types and traits. Use this if you are building a storage adapter or plugin.
- **`prkdb-proto`**: The wire protocol definitions.


## Quick Start

```rust
use prkdb::{PrkDb, Collection};
use serde::{Deserialize, Serialize};

#[derive(Collection, Serialize, Deserialize)]
struct User {
    id: String,
    name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = PrkDb::builder()
        .with_data_dir("./data")
        .register_collection::<User>()
        .build()?;

    // Insert
    db.put(&User { id: "1".into(), name: "Alice".into() }).await?;

    // Query
    let user: Option<User> = db.get("1").await?;
    Ok(())
}
```

## Transactions

PrkDB supports ACID transactions with conflict detection and savepoints.

```rust
use prkdb::prelude::*;

// Basic transaction
let mut tx = storage.begin_transaction();
tx.put(b"key1", b"value1")?;
tx.put(b"key2", b"value2")?;
tx.commit().await?;  // Atomic commit

// Rollback
let mut tx = storage.begin_transaction();
tx.put(b"key1", b"bad_value")?;
tx.rollback();  // Discard all changes

// Savepoints (nested transactions)
let mut tx = storage.begin_transaction();
tx.put(b"order", b"pending")?;
tx.savepoint("sp1")?;
tx.put(b"payment", b"charged")?;
tx.rollback_to_savepoint("sp1")?;  // Undo payment, keep order

// Conflict detection (Serializable isolation)
let config = TransactionConfig {
    isolation_level: IsolationLevel::Serializable,
    ..Default::default()
};
let mut tx = storage.begin_transaction_with_config(config);
let _ = tx.get(b"key1").await?;  // Tracked for conflicts
// If another transaction modifies key1, commit() returns ConflictDetected
```

## TTL (Time-To-Live)

Automatic record expiration for caching, sessions, and temporary data.

```rust
use prkdb::prelude::*;
use std::time::Duration;

// Wrap storage with TTL support
let ttl_storage = TtlStorage::new(storage);

// Put with 1 hour TTL
ttl_storage.put_with_ttl(b"session:123", b"data", Duration::from_secs(3600)).await?;

// Get returns None if expired
let value = ttl_storage.get(b"session:123").await?;

// Check remaining TTL
let remaining = ttl_storage.ttl(b"session:123").await?;

// Manually expire
ttl_storage.expire(b"session:123").await?;

// Remove TTL (persist forever)
ttl_storage.persist(b"key").await?;
```

## Secondary Indexes

Type-safe queries on any field with `#[index]` attribute.

```rust
use prkdb::prelude::*;

#[derive(Collection, Clone, Serialize, Deserialize)]
struct User {
    #[key]
    pub id: String,
    
    #[index]
    pub age: u32,
    
    #[index(unique)]
    pub email: String,
}

// Query by indexed field
let admins: Vec<User> = db.query_by("role", &"admin").await?;
let alice: Option<User> = db.query_unique_by("email", &"a@b.com").await?;

// Range queries
let adults: Vec<User> = db.query_range("age", &18, &65).await?;
let al_names: Vec<User> = db.query_prefix("name", "Al").await?;

// Flexible closure filter (scans all, O(n))
let vip: Vec<User> = db.filter(|u: &User| u.age > 18 && u.orders > 100).await?;
```

### Batch Operations

```rust
db.insert_batch(&[user1, user2, user3]).await?;  // Returns count
db.delete_batch(&[user1, user2]).await?;
```

### Pagination

```rust
let page1: Vec<User> = db.paginate(10, 0).await?;   // limit=10, offset=0
let page2: Vec<User> = db.paginate(10, 10).await?;  // Next page
let filtered: Vec<User> = db.filter_paginated(|u| u.age > 18, 5, 0).await?;
```

### Aggregations

```rust
let count = db.count::<User>().await?;
let total = db.sum(|u: &User| u.orders).await?;
let avg = db.avg(|u: &User| u.age as f64).await?;
let min = db.min(|u: &User| u.age).await?;
let max = db.max(|u: &User| u.age).await?;
let adults = db.count_where(|u: &User| u.age >= 18).await?;
```

### Generic Storage Support

```rust
// Works with any StorageAdapter
let indexed = IndexedStorage::new(any_storage_adapter);
```

### Watch/Subscribe (Real-time Changes)

```rust
// Subscribe to changes
let mut rx = db.watch();
tokio::spawn(async move {
    while let Ok(event) = rx.recv().await {
        match event {
            ChangeEvent::Inserted { collection, id, .. } => println!("New!"),
            ChangeEvent::Deleted { collection, id } => println!("Gone!"),
        }
    }
});
```

### Compound Indexes

```rust
// Create compound index at runtime
db.create_compound_index::<User>("role_age", |u| {
    vec![u.role.clone(), u.age.to_string()]
}).await?;

// Query by multiple fields
let admins_30 = db.query_compound::<User>("role_age", 
    vec!["Admin".into(), "30".into()]
).await?;
```

### Index Persistence

```rust
// Load indexes from disk on startup (or create fresh if not found)
let db = IndexedStorage::load_from(storage, "./data/indexes.db").await?;

// Save before shutdown or periodically
db.save_indexes("./data/indexes.db").await?;

// Get index stats
let stats = db.index_stats().await;  // BTreeMap<collection, count>

// Auto-sync in background (recommended!)
db.start_auto_sync(Duration::from_secs(30)).await;
// Indexes saved automatically every 30 seconds

// On shutdown
db.stop_auto_sync();
```

### QueryBuilder DSL

Type-safe, fluent query API with macro-generated field methods:

```rust
// Fluent query building with type-safe field methods
let users = db.query::<User>()
    .where_role_eq("admin")           // Generated method
    .where_age_gt(18)                 // Generated method
    .filter(|u| u.verified)           // Generic closure
    .order_by(|u| u.created_at)
    .take(10)
    .collect().await?;

// Projection to different type
let summaries = db.query::<User>()
    .where_role_eq("admin")
    .select(|u| UserSummary { 
        id: u.id, 
        name: u.name.clone() 
    })
    .collect().await?;
```

Generated methods by type:
- **String**: `where_{field}_eq`, `where_{field}_contains`, `where_{field}_starts_with`
- **Numeric**: `where_{field}_eq`, `where_{field}_gt/lt/gte/lte`
- **Boolean**: `where_{field}_eq`, `where_{field}_is_true/is_false`

### Advanced Aggregations

```rust
// Get unique values
let roles = db.query::<User>().distinct(|u| u.role.clone()).await?;

// Group by key
let by_role = db.query::<User>().group_by(|u| u.role.clone()).await?;

// Sum/Count by group
let salaries = db.query::<User>()
    .sum_by(|u| u.dept.clone(), |u| u.salary).await?;
let counts = db.query::<User>().count_by(|u| u.role.clone()).await?;
```

### Full-Text Search

```rust
// Create text index
db.create_text_index::<User, _>("bio", |u| &u.bio).await?;

// Search with ranked results
let users = db.search::<User>("bio", "rust async developer").await?;
// Results ranked by number of matching tokens
```

### Query Plan Explain

```rust
let plan = db.query::<User>()
    .filter(|u| u.age > 18)
    .take(10)
    .explain();

println!("{}", plan);
// Query Plan for User
// ├─ Filters: 1
// ├─ Uses Index: No (full scan)
// ├─ Ordering: No
// ├─ Limit: 10
// └─ Full scan → Apply 1 filters → Return results
```

### Transactions

```rust
// Start a transaction
let mut tx = db.transaction();

// Buffer operations
tx.insert(&user1)?;
tx.insert(&user2)?;
tx.delete(&old_user)?;

// Commit all atomically
tx.commit().await?;

// OR: Rollback to discard all
// tx.rollback();
```

### Schema Migrations

```rust
// Versioned collections support migration
impl Versioned for UserV2 {
    const VERSION: u32 = 2;
    type PreviousVersion = UserV1;
    
    fn migrate(old: UserV1) -> Self {
        Self { id: old.id, name: old.name, premium: false }
    }
}
```

### Validators

```rust
// Define validation rules
impl Validatable for User {
    fn validate(&self) -> Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();
        if self.name.is_empty() {
            errors.push(ValidationError::new("name", "cannot be empty"));
        }
        if !self.email.contains('@') {
            errors.push(ValidationError::new("email", "invalid format"));
        }
        if errors.is_empty() { Ok(()) } else { Err(errors) }
    }
}

// Auto-validate on insert
db.insert_validated(&user).await?;
```

### Cursor Pagination

```rust
// First page
let (users, next_cursor) = db.query::<User>()
    .order_by(|u| u.id)
    .paginate(10, None).await?;

// Next page
if let Some(cursor) = next_cursor {
    let (more, _) = db.query::<User>()
        .order_by(|u| u.id)
        .paginate(10, Some(cursor)).await?;
}

// Or use after() directly
let page2 = db.query::<User>()
    .after(&last_id)
    .take(10)
    .collect().await?;
```

### LRU Cache

```rust
use prkdb::cache::LruCache;

let cache = LruCache::<u64, User>::new(1000);
cache.put(user_id, user);

if let Some(user) = cache.get(&user_id) {
    // Cache hit
}

let stats = cache.stats();
println!("{:.1}% utilized", stats.utilization());
```

### Relationship Loading

```rust
// User has many Orders (eager load, avoids N+1)
let users_with_orders = db.query::<User>()
    .collect_with::<Order, _, _>(
        |user| user.id,          // parent key
        |order| order.user_id    // foreign key
    ).await?;  // Vec<(User, Vec<Order>)>

// Order belongs to User
let orders_with_user = db.query::<Order>()
    .collect_with_one::<User, _>(|order| order.user_id)
    .await?;  // Vec<(Order, Option<User>)>
```

### Upsert and Update

```rust
// Upsert: Insert or update if exists
db.upsert(&user).await?;  // Returns true if updated, false if inserted

// Update: Modify existing record with closure
db.update::<User, _>(&user_id, |u| {
    u.name = "New Name".to_string();
    u.age += 1;
}).await?;

// Check existence
if db.exists::<User>(&user_id).await? { ... }
```

### Soft Delete

```rust
// Mark as deleted (keeps data)
db.soft_delete::<User>(&user_id).await?;

// Query only active records
let active = db.query_active::<User>().await?;

// Restore a soft-deleted record
db.restore::<User>(&user_id).await?;
```

### Audit Logging (Timestamps)

```rust
impl Timestamped for User {
    fn created_at(&self) -> u64 { self.created_at }
    fn updated_at(&self) -> u64 { self.updated_at }
    fn set_created_at(&mut self, ts: u64) { self.created_at = ts; }
    fn set_updated_at(&mut self, ts: u64) { self.updated_at = ts; }
}

// Auto-set timestamps on insert
db.insert_timestamped(&mut user).await?;

// Auto-update on upsert
db.upsert_timestamped(&mut user).await?;
```

### Computed Fields

```rust
// Add computed fields to query results
let users_with_age = db.query::<User>()
    .with_computed(|user| (now - user.birth_date) / 86400)
    .await?;  // Vec<WithComputed<User, u64>>

for item in users_with_age {
    println!("{}: {} days old", item.record.name, item.computed);
}
```

### Index Statistics

```rust
// Get stats for a collection
let stats = db.collection_stats::<User>().await;
println!("User Index: {}", stats);
// "Fields: 3 | Values: 100 | Entries: 150 | Compound: 0 | Text: 1"

// Get stats for all collections
let all_stats = db.all_collection_stats().await;
```

### Lifecycle Hooks

```rust
impl Hooks for User {
    fn before_insert(&mut self) -> Result<(), String> {
        self.name = self.name.trim().to_string();  // Normalize
        Ok(())
    }
    fn after_insert(&self) {
        log::info!("User {} created", self.id);
    }
}

// Auto-run hooks on insert/delete
db.insert_with_hooks(&mut user).await?;
db.delete_with_hooks(&user).await?;
```

### Rate Limiting

```rust
use prkdb::rate_limit::RateLimiter;

let limiter = RateLimiter::per_second(100);  // 100 ops/sec

// Wait for permission before operation
limiter.acquire().await;
db.insert(&record).await?;

// Or try without waiting
if limiter.try_acquire().await {
    db.insert(&record).await?;
}
```

### Snapshot and Clone

```rust
// Create snapshot for backup
let snapshot: Vec<User> = db.snapshot::<User>().await?;

// Clone to another storage
db.clone_to::<User>(&backup_db).await?;
```

### Find Operations

```rust
// Find first matching record
let admin = db.find_one::<User, _>(|u| u.role == "admin").await?;

// Find all matching records
let admins = db.find_all::<User, _>(|u| u.role == "admin").await?;
```

### Bulk Conditional Operations

```rust
// Delete all matching records
let deleted = db.delete_where::<User, _>(|u| !u.active).await?;

// Update all matching records
let updated = db.update_where::<User, _, _>(
    |u| !u.verified,
    |u| u.verified = true
).await?;
```

### More Aggregations

```rust
// Find min/max by field
let youngest = db.query::<User>().min_by(|u| u.age).await?;
let oldest = db.query::<User>().max_by(|u| u.age).await?;

// Calculate average
let avg_age = db.query::<User>().avg_by(|u| u.age as f64).await?;

// Boolean checks
let has_admin = db.query::<User>().any(|u| u.role == "admin").await?;
let all_active = db.query::<User>().all(|u| u.active).await?;
```

### More Query Helpers

```rust
// Extract single field (like SQL SELECT column)
let names: Vec<String> = db.query::<User>().pluck(|u| u.name.clone()).await?;

// Partition into matching/non-matching
let (active, inactive) = db.query::<User>().partition(|u| u.active).await?;

// Custom fold/reduce
let total_salary = db.query::<User>().fold(0.0, |acc, u| acc + u.salary).await?;

// Random sample
let sample = db.query::<User>().sample(5).await?;  // 5 random users

// Get last record
let last = db.query::<User>().last().await?;

// Take/skip while condition
let early = db.query::<User>().take_while(|u| u.id < 100).await?;
```

### Advanced Query Helpers

```rust
// Process in chunks/batches
let chunks = db.query::<User>().chunks(100).await?;  // Vec<Vec<User>>

// Add index to records
let indexed = db.query::<User>().enumerate().await?;  // Vec<(usize, User)>

// Remove consecutive duplicates
let deduped = db.query::<User>()
    .order_by(|u| u.role.clone())
    .dedup_by_key(|u| u.role.clone()).await?;

// Join with another collection
let joined = db.query::<Order>()
    .join_with(&users, |o| o.user_id, |u| u.id)
    .await?;  // Vec<(Order, Option<User>)>
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         PrkDB                                │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ Collections │  │  Consumers  │  │    ORM Layer        │ │
│  │  (Typed)    │  │  (Groups)   │  │ (SQL Integration)   │ │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘ │
│         │                │                     │            │
│  ┌──────▼─────────────────▼─────────────────────▼─────────┐ │
│  │                    Storage Layer                        │ │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌───────────┐  │ │
│  │  │   WAL   │  │  Sled   │  │ SQLite  │  │ Segmented │  │ │
│  │  │ (mmap)  │  │         │  │         │  │    Log    │  │ │
│  │  └─────────┘  └─────────┘  └─────────┘  └───────────┘  │ │
│  └────────────────────────────────────────────────────────┘ │
│                           │                                  │
│  ┌────────────────────────▼────────────────────────────────┐│
│  │                   Raft Consensus                         ││
│  │  ┌────────┐  ┌────────┐  ┌────────┐                     ││
│  │  │ Node 1 │◄─┤ Node 2 │◄─┤ Node 3 │  (gRPC)             ││
│  │  └────────┘  └────────┘  └────────┘                     ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

## Distributed Cluster

### One-Command Start
```bash
./scripts/start_cluster.sh
```

### Manual Setup
```bash
# Terminal 1
cargo run --release --example raft_node -- \
  --node-id 1 --listen 127.0.0.1:50051 \
  --peers 2=127.0.0.1:50052,3=127.0.0.1:50053

# Terminal 2
cargo run --release --example raft_node -- \
  --node-id 2 --listen 127.0.0.1:50052 \
  --peers 1=127.0.0.1:50051,3=127.0.0.1:50053

# Terminal 3
cargo run --release --example raft_node -- \
  --node-id 3 --listen 127.0.0.1:50053 \
  --peers 1=127.0.0.1:50051,2=127.0.0.1:50052
```

### Read Consistency Levels

```rust
use prkdb::raft::rpc::ReadMode;

// Linearizable (default) - Always reads from leader
let value = db.get(key).await?;

// Stale read - Fast local read (may be stale)
let value = db.get_local(key).await?;

// Follower read - Linearizable from any node
let value = db.get_follower_read(key).await?;
```

### Sharding Strategies

```rust
use prkdb::raft::{ConsistentHashRing, RangePartitioner, PartitionStrategy};

// Consistent hashing (default) - Minimal data movement on rebalance
let ring = ConsistentHashRing::new(3, 150); // 3 partitions, 150 virtual nodes
let partition = ring.get_partition(key);

// Range partitioning - For ordered access patterns
let mut partitioner = RangePartitioner::new(3);
let partition = partitioner.get_partition(key);

// Split hotspots
partitioner.split_partition(0, b"middle_key")?;
```

## Testing

### Benchmarks
```bash
cargo run --release --example kafka_comparison        # Full benchmark suite
cargo run --release --example ultra_performance       # 199K writes, 7.3M reads
cargo run --release --example max_performance         # Peak performance
cargo run --release --example streaming_benchmark     # Streaming throughput
cargo run --release --example transaction_example     # Transaction demo
cargo run --release --example ttl_example             # TTL/Expiration demo
cargo run --release --example index_example           # Secondary Indexes demo
```

### Chaos & Consistency Tests
```bash
./scripts/chaos_test.sh       # 8 resilience tests
./scripts/consistency_test.sh # 6 data durability tests
```

## Monitoring

```bash
docker compose -f docker/docker-compose.yml up -d
```

- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:8091

## CLI

```bash
prkdb collection list
prkdb consumer list
prkdb metrics
prkdb serve

# Data Operations
prkdb put user:101 '{"name": "Alice"}'
prkdb get user:101
prkdb delete user:101
prkdb batch-put data.txt --separator=,
```

## Crates

| Crate | Description |
|-------|-------------|
| `prkdb` | Main library |
| `prkdb-client` | **New** Lightweight smart client |
| `prkdb-types` | **New** Core domain types & traits |
| `prkdb-proto` | **New** gRPC definitions |
| `prkdb-core` | WAL, compression, replication |
| `prkdb-cli` | Command-line interface |
| `prkdb-macros` | `#[derive(Collection)]` |
| `prkdb-orm` | SQL ORM |
| `prkdb-metrics` | Prometheus |
| `prkdb-storage-*` | Storage backends |

## Benchmark Results

| Workload | Ops/sec | vs Kafka |
|----------|---------|----------|
| Consumer reads | **7.3M** | 14x faster 🏆 |
| Batch writes | **199K** | Kafka-competitive |
| Mixed 50/50 | **143K** | Competitive |
| Multi-threaded | **601K** | Matches top-end |

## Test Results

| Category | Status |
|----------|--------|
| Benchmarks | ✅ 7.3M reads/sec, 199K writes/sec |
| Partitioning | ✅ 1.56B ops/s (641ps/op) |
| Storage Backends | ✅ 8 tests |
| ORM Layer | ✅ 15 tests |
| CLI | ✅ 7 commands |
| Raft Cluster | ✅ 3 nodes + Pre-Vote |
| Sharding | ✅ 7 tests (ConsistentHash + Range) |
| Chaos Tests | ✅ 8 passed |
| Consistency Tests | ✅ 6 passed |

## License

MIT OR Apache-2.0