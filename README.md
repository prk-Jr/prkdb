# PrkDB

**A high-performance, Rust-native event streaming database**

[![Benchmarks](https://img.shields.io/badge/Benchmarks-Raw%20artifacts-blue)]()
[![Chaos Tests](https://img.shields.io/badge/Chaos%20Tests-19%20passing-blue)]()
[![Rust](https://img.shields.io/badge/Rust-1.70+-orange)]()

Docs: https://prk-jr.github.io/prkdb/

## 🚀 Features

- **High-throughput local writes** - mmap WAL + batch writes tuned for embedded and single-node deployments
- **Fast local replay** - optimized append/read paths for local streaming workloads
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
use prkdb::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Collection, Serialize, Deserialize)]
struct User {
    #[id]
    id: String,
    name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = PrkDb::builder()
        .with_data_dir("./data")
        .register_collection::<User>()
        .build()?;

    let users = db.collection::<User>();
    users
        .put(User {
            id: "1".into(),
            name: "Alice".into(),
        })
        .await?;

    let user = users.get(&"1".to_string()).await?;
    Ok(())
}
```

## 🌐 Cross-Language Clients (Codegen)

PrkDB includes a built-in schema registry and cross-language client generator for **TypeScript**, **Python**, and **Go**.

First, register your schema (via `#[derive(Collection)]` export or raw `.proto`):
```bash
export PRKDB_ADMIN_TOKEN=change-me
prkdb schema register --server http://127.0.0.1:50051 --collection users --proto user.proto
```

Then generate a strongly-typed client in your language of choice:
```bash
# Generate a TypeScript HTTP client
prkdb codegen --server http://127.0.0.1:50051 --lang typescript --collection users --out ./src/client

# Generate a Python client
prkdb codegen --server http://127.0.0.1:50051 --lang python --collection users --out ./app/models

# Generate a Go client
prkdb codegen --server http://127.0.0.1:50051 --lang go --collection users --out ./pkg/models
```

Generated clients target the HTTP API exposed by `prkdb-cli serve`, which defaults to `http://127.0.0.1:8080`. Use `http://127.0.0.1:50051` for schema/codegen gRPC commands against `prkdb-cli serve`, or `http://127.0.0.1:8080` when talking directly to `prkdb-server`.

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

## 📡 Watch/Subscribe API

Get real-time notifications when keys match a prefix change.

```rust
use tokio_stream::StreamExt;

// Subscribe to all keys starting with "user:"
let mut stream = client.watch(b"user:").await?;

while let Some(event) = stream.next().await {
    match event? {
        // PUT event has key and new value
        e if e.event_type == 0 => println!("Updated: {:?}", e.key),
        // DELETE event has key only
        e => println!("Deleted: {:?}", e.key),
    }
}
```

## 🛡️ Smart Client & Resilience

`prkdb-client` is built for production reliability:

- **Connection Pooling**: Maintains a pool of connections per node (default: 4) to maximize concurrency and prevent head-of-line blocking.
- **Health-Based Routing**: Automatically routes around unhealthy nodes. Tracks success/failure rates and avoids "zombie" leaders.
- **Retries with Backoff**: Configurable exponential backoff for transient failures.
- **Topology Awareness**: Caches cluster state (sharding, leadership) and lazily refreshes on errors.

```rust
let config = ClientConfig {
    max_retries: 5,
    max_connections_per_node: 8,
    unhealthy_threshold: 3,
    unhealthy_cooldown_secs: 30,
    ..Default::default()
};
let client = PrkDbClient::with_config(vec!["http://127.0.0.1:8080".into()], config).await?;
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

## 🛠️ Remote Admin

Manage partitions, consumers, and replication via gRPC.

```bash
# Set consumer group offset
prkdb reset-offset my-group --topic user-updates --to-datetime "2023-10-27T10:00:00Z"

# Add replication target (without full Raft reconfiguration)
prkdb replication add 10.0.0.2:9090

# Check health
prkdb status
```

### Admin RPCs

- `ResetConsumerOffset`: Rewind or skip message consumption.
- `StartReplication`: Direct log replication to non-voting followers.
- `StopReplication`: Stop replicating to a target.
- `Metadata`: Get full cluster topology.

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
NODE_ID=1 \
CLUSTER_NODES=1@127.0.0.1:8080,2@127.0.0.1:8081,3@127.0.0.1:8082 \
STORAGE_PATH=/tmp/prkdb/node1 \
cargo run --release -p prkdb --bin prkdb-server

# Terminal 2
NODE_ID=2 \
CLUSTER_NODES=1@127.0.0.1:8080,2@127.0.0.1:8081,3@127.0.0.1:8082 \
STORAGE_PATH=/tmp/prkdb/node2 \
cargo run --release -p prkdb --bin prkdb-server

# Terminal 3
NODE_ID=3 \
CLUSTER_NODES=1@127.0.0.1:8080,2@127.0.0.1:8081,3@127.0.0.1:8082 \
STORAGE_PATH=/tmp/prkdb/node3 \
cargo run --release -p prkdb --bin prkdb-server
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
let new_partition = partitioner.split_partition(b"middle_key".to_vec());
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

### Repository Status
```bash
cargo run -p xtask -- repo-status snapshot
cargo run -p xtask -- repo-status audit
cargo run -p xtask -- repo-status render
```

Use `cargo run -p xtask -- repo-status snapshot --fail-on-objective-drift` in CI when
objective roadmap/docs/contract drift should fail the workflow.

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
prkdb metrics show
prkdb serve

# Schema & Codegen
prkdb schema register --server http://127.0.0.1:50051 --collection users --proto schema.desc
prkdb schema list --server http://127.0.0.1:50051
prkdb codegen --server http://127.0.0.1:50051 --collection users --lang typescript --out ./client

# Data Operations
prkdb put user:101 '{"name": "Alice"}' --server http://127.0.0.1:50051
prkdb get user:101 --server http://127.0.0.1:50051
prkdb delete user:101 --server http://127.0.0.1:50051
prkdb batch-put data.txt --separator=, --server http://127.0.0.1:50051
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

## 🔬 Benchmarks

CI publishes raw benchmark artifacts for:
- local PrkDB storage-engine measurements
- single-broker Kafka perf-tool reference runs

Those numbers are useful for trend tracking inside this repo, but they are not an apples-to-apples database comparison. The PrkDB runs are local native benchmarks; the Kafka runs exercise a networked broker with the official perf tools. Treat them as separate reference points unless the methodology is explicitly aligned.

## 🐵 Chaos Engineering

PrkDB includes comprehensive chaos testing to ensure production reliability:

| Test Category | Tests | Coverage |
|---------------|-------|----------|
| **Distributed Raft** | 7 | Split-brain, leader crash, cascading failures |
| **Jepsen Consistency** | 6 | Linearizable register, bank transfers, monotonic reads |
| **Extended Chaos** | 6 | Asymmetric partition, rolling restart, message reorder |
| **Local Storage** | 4 | Delays, concurrent ops, memory pressure |
| **Disk Corruption** | 3 | Byte flip, truncation, header corruption |

```bash
# Run all consistency tests (runs in CI)
cargo test --test jepsen_consistency_tests
cargo test --test extended_chaos_tests

# Run Raft chaos tests (nightly CI)
cargo test --test raft_chaos_tests -- --ignored --nocapture

# Run corruption tests
cargo test --test corruption_tests -- --ignored --nocapture
```

**Chaos Monkey Results:**
- ✅ 99.4% write success rate during active chaos
- ✅ 100% data integrity after stabilization
- ✅ Survives up to 2 concurrent node failures (maintains quorum)

## Test Results

| Category | Status |
|----------|--------|
| Local Benchmarks | ✅ Raw PrkDB and Kafka reference artifacts published in CI |
| Chaos Engineering | ✅ 19 tests (Raft + Jepsen + Extended + Corruption) |
| Raft Cluster | ✅ 5-node chaos monkey with 99.4% success |
| Storage Backends | ✅ 8 tests |
| ORM Layer | ✅ 15 tests |
| Sharding | ✅ 7 tests (ConsistentHash + Range) |

## License

Apache-2.0
