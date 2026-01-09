// Transaction Example
// Demonstrates PrkDB's ACID transaction support
//
// Run: cargo run --release --example transaction_example

use prkdb::prelude::*;
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!();
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║                  PrkDB Transaction Example                    ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();

    // Create storage
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(WalStorageAdapter::new(WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    })?);

    // ═══════════════════════════════════════════════════════════════════════
    // Example 1: Basic Transaction
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Example 1: Basic Transaction");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let mut tx = storage.begin_transaction();
    tx.put(b"account:alice", b"100")?;
    tx.put(b"account:bob", b"200")?;

    // Read-your-writes: see uncommitted data
    let alice = tx.get(b"account:alice").await?;
    println!(
        "  Alice balance (in tx): {:?}",
        String::from_utf8_lossy(&alice.unwrap())
    );

    // Atomic commit
    let count = tx.commit().await?;
    println!("  Committed {} operations atomically ✅", count);
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // Example 2: Rollback
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Example 2: Rollback");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let mut tx = storage.begin_transaction();
    tx.put(b"account:alice", b"999999")?; // Suspicious transfer!
    tx.rollback(); // Abort

    // Alice's balance unchanged
    let alice = storage.get(b"account:alice").await?;
    println!(
        "  Alice balance after rollback: {:?}",
        String::from_utf8_lossy(&alice.unwrap())
    );
    println!("  Rollback preserved original data ✅");
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // Example 3: Savepoints (Nested Transactions)
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Example 3: Savepoints (Nested Transactions)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let mut tx = storage.begin_transaction();

    tx.put(b"order:1", b"pending")?;
    tx.savepoint("after_order_created")?;

    tx.put(b"order:1", b"processing")?;
    tx.put(b"payment:1", b"charged")?;

    // Oops, payment failed - rollback to savepoint
    tx.rollback_to_savepoint("after_order_created")?;

    // Order still exists as pending, payment was reverted
    let order = tx.get(b"order:1").await?;
    println!(
        "  Order status: {:?}",
        String::from_utf8_lossy(&order.unwrap())
    );

    tx.commit().await?;
    println!("  Savepoint rollback preserved partial work ✅");
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // Example 4: Conflict Detection (Serializable Isolation)
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Example 4: Conflict Detection (Serializable)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Create a serializable transaction
    let config = TransactionConfig {
        isolation_level: IsolationLevel::Serializable,
        ..Default::default()
    };
    let mut tx = storage.begin_transaction_with_config(config);

    // Read a value (tracked for conflict detection)
    let _alice = tx.get(b"account:alice").await?;

    // Concurrent writer modifies the same key
    storage.put(b"account:alice", b"50").await?;

    // Transaction tries to commit - conflict detected!
    tx.put(b"account:alice", b"150")?;
    let result = tx.commit().await;

    match result {
        Err(TransactionError::ConflictDetected { conflicting_keys }) => {
            println!("  Conflict detected on {} keys ✅", conflicting_keys.len());
            println!("  This means another transaction modified data we read!");
        }
        Ok(_) => println!("  No conflict (unexpected)"),
        Err(e) => println!("  Error: {}", e),
    }
    println!();

    // Summary
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║                    Transaction Features                       ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("  ✅ Atomic commit (all-or-nothing)");
    println!("  ✅ Rollback (abort transaction)");
    println!("  ✅ Read-your-writes (see uncommitted changes)");
    println!("  ✅ Savepoints (nested rollback)");
    println!("  ✅ Conflict detection (Serializable isolation)");
    println!("  ✅ Zero overhead for non-transactional operations");
    println!();

    Ok(())
}
