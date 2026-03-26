# ACID Transactions

PrkDB supports fully atomic, consistent, isolated, and durable (ACID) transactions. You can safely bundle multiple read and write operations together to ensure they either all succeed or all fail as a single unit.

PrkDB transactions execute in **Serializable** isolation mode by default, meaning concurrent transactions will trigger conflict detection rather than risking dirty reads or write skews.

## Basic Transactions

To execute a series of writes atomically, begin a transaction from the storage engine (or `PrkDb` instance):

```rust
use prkdb::prelude::*;

// Begin an atomic transaction block
let mut tx = db.transaction();

tx.put(b"user:balance:101", b"500")?;
tx.put(b"user:balance:202", b"1500")?;

// Attempt to commit all changes atomically to the WAL
tx.commit().await?;
```

## Rollbacks and Error Recovery

If something goes wrong dynamically inside your application logic, you can easily discard the pending state mutations by calling `rollback()`. If a transaction object is dropped out of scope without `commit()` being explicitly called, the changes are automatically rolled back.

```rust
let mut tx = db.transaction();
tx.put(b"inventory:item123", b"0")?;

if out_of_stock {
    // Abandon changes!
    tx.rollback();
    return Err("Out of stock!");
}

tx.commit().await?;
```

## Savepoints (Nested Transactions)

For complex multi-stage workflows, you might want to try a series of operations and conditionally rollback a specific subset without abandoning the entire transaction chunk. PrkDB enables this via Named Savepoints.

```rust
let mut tx = db.transaction();

// Stage 1: Order creation
tx.put(b"order:999", b"pending")?;

// Create a savepoint before Stage 2
tx.savepoint("payment_started")?;

// Stage 2: Attempt Payment
tx.put(b"payment:order999", b"charged")?;

if payment_failed {
    // Oh no! The payment gateway rejected us.
    // Undo the payment modification, but keep the order "pending"
    tx.rollback_to_savepoint("payment_started")?;
}

// Commits the transaction (Order is saved, payment is effectively rolled back)
tx.commit().await?;
```

## Conflict Detection

When multiple clients attempt to modify the same keys concurrently under Serializable Isolation, PrkDB's storage engine tracks reads and writes.

If `Transaction B` commits a modification to a key that `Transaction A` previously read within its uncommitted transaction scope, `Transaction A` will receive a `ConflictDetected` error upon its deferred `commit()` call.

```rust
let config = TransactionConfig {
    isolation_level: IsolationLevel::Serializable,
    ..Default::default()
};

let mut tx = storage.begin_transaction_with_config(config);
let balance = tx.get(b"account_A").await?;

// If another transaction external to ours modifies "account_A" 
// while we are sleeping...
tokio::time::sleep(Duration::from_millis(50)).await;

tx.put(b"account_A", balance + 10)?;

// This commit will FAIL with ConflictDetected to prevent write-skew!
let result = tx.commit().await;
```
