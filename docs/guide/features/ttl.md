# Time-To-Live (TTL)

PrkDB includes a background expiration engine that allows you to attach **Time-To-Live (TTL)** durations to specific keys. When the duration expires, the database automatically deletes the key and its contents.

This is extremely useful when PrkDB serves as an intelligent caching layer, session store, or ephemeral pub-sub state backend.

## Setting TTLs

You can interact with PrkDB's TTL features by wrapping an implementation of the raw `StorageAdapter` inside the `TtlStorage` decorator.

```rust
use prkdb::prelude::*;
use std::time::Duration;

// Wrap an existing storage adapter with TTL support
let ttl_storage = TtlStorage::new(base_storage);

// Insert a session token that automatically vanishes in 1 hour (3600 seconds)
ttl_storage.put_with_ttl(
    b"session:token123", 
    b"user_data", 
    Duration::from_secs(3600)
).await?;
```

If a client attempts to fetch an expired key that the background sweeper hasn't garbage collected yet, `ttl_storage.get()` will safely intercept the read and return `None` as if it were truly deleted.

## Querying and Modifying TTLs

You can inspect the remaining life of an arbitrary key using `.ttl()`.

```rust
let remaining_time = ttl_storage.ttl(b"session:token123").await?;

if let Some(duration) = remaining_time {
    println!("Session expires in {} seconds", duration.as_secs());
}
```

If you wish to force an early expiration for an active key, you can manually expire it:

```rust
ttl_storage.expire(b"session:token123").await?;
```

If you wish to remove the expiration timer from a key entirely, so that it persists indefinitely:

```rust
// The token will never vanish!
ttl_storage.persist(b"session:token123").await?;
```
