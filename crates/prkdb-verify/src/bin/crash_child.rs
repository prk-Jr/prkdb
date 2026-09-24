//! Task 1.10: crash-test child process.
//!
//! Writes `n` acknowledged puts to a `WalStorageAdapter` rooted at `dir`,
//! printing `ACK <i>` (flushed) to stdout right after each put returns
//! `Ok`, then sleeps forever so the parent test can `SIGKILL` it at a known
//! point and reopen the data directory to check which acknowledged keys
//! survived.
//!
//! `WalStorageAdapter::new` calls `block_in_place` internally, which panics
//! on a current-thread runtime — hence the default (multi-thread) flavor of
//! `#[tokio::main]`.

use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::io::Write;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(
        args.get(1)
            .ok_or_else(|| anyhow::anyhow!("usage: crash_child <dir> <n>"))?,
    );
    let n: u32 = args
        .get(2)
        .ok_or_else(|| anyhow::anyhow!("usage: crash_child <dir> <n>"))?
        .parse()?;

    let db = WalStorageAdapter::new(WalConfig {
        log_dir: dir,
        ..WalConfig::test_config()
    })?;

    let mut out = std::io::stdout();
    for i in 0..n {
        db.put(format!("k{i}").as_bytes(), b"v").await?;
        writeln!(out, "ACK {i}")?;
        out.flush()?;
    }

    // Wait to be SIGKILLed by the parent test; never returns on success.
    std::thread::sleep(std::time::Duration::from_secs(3600));
    Ok(())
}
