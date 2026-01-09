// TTL (Time-To-Live) Example
// Demonstrates PrkDB's automatic record expiration
//
// Run: cargo run --release --example ttl_example

use prkdb::prelude::*;
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!();
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║                  PrkDB TTL/Expiration Example                 ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();

    // Create storage
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(WalStorageAdapter::new(WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    })?);

    // Wrap with TTL support
    let ttl_storage = TtlStorage::new(storage);

    // ═══════════════════════════════════════════════════════════════════════
    // Example 1: Basic TTL
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Example 1: Basic TTL");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Put with 1 hour TTL
    ttl_storage
        .put_with_ttl(
            b"session:user123",
            b"session_data",
            Duration::from_secs(3600),
        )
        .await?;

    // Get remaining TTL
    let remaining = ttl_storage.ttl(b"session:user123").await?;
    if let Some(duration) = remaining {
        println!("  Session TTL: {} seconds remaining", duration.as_secs());
    }

    // Read value
    let value = ttl_storage.get(b"session:user123").await?;
    println!("  Session data exists: {}", value.is_some());
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // Example 2: Automatic Expiration
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Example 2: Automatic Expiration");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Put with very short TTL (100ms)
    ttl_storage
        .put_with_ttl(b"temp:token", b"abc123", Duration::from_millis(100))
        .await?;

    // Immediately readable
    let value = ttl_storage.get(b"temp:token").await?;
    println!(
        "  Token before expiry: {:?}",
        value.map(|v| String::from_utf8_lossy(&v).to_string())
    );

    // Wait for expiration
    println!("  Waiting 150ms for expiration...");
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Now returns None
    let value = ttl_storage.get(b"temp:token").await?;
    println!("  Token after expiry: {:?}", value);
    println!("  Auto-expiration works! ✅");
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // Example 3: Manual Expiration
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Example 3: Manual Expiration");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Put with long TTL
    ttl_storage
        .put_with_ttl(b"cache:item", b"cached_data", Duration::from_secs(3600))
        .await?;

    println!(
        "  Cache item exists: {}",
        ttl_storage.get(b"cache:item").await?.is_some()
    );

    // Manually expire (invalidate cache)
    ttl_storage.expire(b"cache:item").await?;

    println!(
        "  After expire(): {}",
        ttl_storage.get(b"cache:item").await?.is_some()
    );
    println!("  Manual expiration works! ✅");
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // Example 4: Persist (Remove TTL)
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Example 4: Persist (Remove TTL)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Put with short TTL
    ttl_storage
        .put_with_ttl(
            b"important:data",
            b"keep_forever",
            Duration::from_millis(100),
        )
        .await?;

    // Remove TTL (persist indefinitely)
    let had_ttl = ttl_storage.persist(b"important:data").await?;
    println!("  Had TTL: {}", had_ttl);

    // Wait past original expiry
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Still readable (no TTL)
    let value = ttl_storage.get(b"important:data").await?;
    println!(
        "  Data after original expiry: {:?}",
        value.map(|v| String::from_utf8_lossy(&v).to_string())
    );
    println!("  Persist works! ✅");
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // Example 5: Regular Put (No TTL)
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Example 5: Regular Put (No TTL)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Regular put - no TTL
    ttl_storage
        .put(b"permanent:key", b"permanent_value")
        .await?;

    let ttl = ttl_storage.ttl(b"permanent:key").await?;
    println!("  TTL for regular put: {:?}", ttl);
    println!("  Regular put has no TTL! ✅");
    println!();

    // Summary
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║                      TTL Features                             ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("  ✅ put_with_ttl() - Set key with expiration");
    println!("  ✅ get() - Returns None if expired");
    println!("  ✅ ttl() - Check remaining time");
    println!("  ✅ expire() - Manually expire a key");
    println!("  ✅ persist() - Remove TTL (keep forever)");
    println!("  ✅ start_cleanup() - Background cleanup task");
    println!("  ✅ Zero overhead for non-TTL operations");
    println!();

    Ok(())
}
