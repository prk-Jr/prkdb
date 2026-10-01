//! Task 1.10 (+ review follow-ups): crash-test child process.
//!
//! Writes `n` acknowledged puts to a `WalStorageAdapter` rooted at `dir`,
//! printing `ACK <i>` (flushed) right after each write is acknowledged,
//! then sleeps for an hour so the parent test can `SIGKILL` it at a known
//! point and reopen the data directory to see which acknowledged keys
//! survived.
//!
//! `WalConfig::test_config()` is `SyncMode::Durable`, so every `ACK` is
//! printed only after the write is fsynced (Task 2.8a); a process kill is the
//! weaker fault here, and power loss is covered in-process by
//! `tests/power_loss.rs`.
//!
//! Usage: `crash_child <dir> <n> [--many <batch_size>] [--value-bytes <n>] [--segment-bytes <n>]`
//!
//! - `--many <batch_size>`: write through `put_many` (one WAL frame per
//!   batch) in batches of this size instead of one `put` per key. `ACK <i>`
//!   for every key in a batch is only printed once that batch's `put_many`
//!   call has returned `Ok` — never while the batch is still in flight.
//! - `--value-bytes <n>`: value size per key in bytes (default 1, i.e. the
//!   original `b"v"`).
//! - `--segment-bytes <n>`: overrides `WalConfig::segment_bytes` (default
//!   comes from `WalConfig::test_config()`, currently 1 MiB). Tests that
//!   want the log to roll to new segments mid-run pass a size well below the
//!   total they write.

use anyhow::{bail, Context};
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::io::Write;
use std::path::PathBuf;

struct Args {
    dir: PathBuf,
    n: u32,
    many: Option<usize>,
    value_bytes: usize,
    segment_bytes: Option<u64>,
}

const USAGE: &str = "usage: crash_child <dir> <n> [--many <batch_size>] [--value-bytes <n>] \
                     [--segment-bytes <n>]";

fn parse_args() -> anyhow::Result<Args> {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    if raw.len() < 2 {
        bail!(USAGE);
    }
    let dir = PathBuf::from(&raw[0]);
    let n: u32 = raw[1].parse().context("parsing <n>")?;

    let mut many = None;
    let mut value_bytes = 1usize;
    let mut segment_bytes = None;

    let mut i = 2;
    while i < raw.len() {
        match raw[i].as_str() {
            "--many" => {
                let v = raw.get(i + 1).context("--many requires a value")?;
                many = Some(v.parse().context("parsing --many")?);
                i += 2;
            }
            "--value-bytes" => {
                let v = raw.get(i + 1).context("--value-bytes requires a value")?;
                value_bytes = v.parse().context("parsing --value-bytes")?;
                i += 2;
            }
            "--segment-bytes" => {
                let v = raw.get(i + 1).context("--segment-bytes requires a value")?;
                segment_bytes = Some(v.parse().context("parsing --segment-bytes")?);
                i += 2;
            }
            other => bail!("{USAGE}\nunrecognized argument: {other}"),
        }
    }

    Ok(Args {
        dir,
        n,
        many,
        value_bytes,
        segment_bytes,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = parse_args()?;

    let mut wal_config = WalConfig {
        log_dir: args.dir,
        ..WalConfig::test_config()
    };
    if let Some(segment_bytes) = args.segment_bytes {
        wal_config.segment_bytes = segment_bytes;
    }

    let db = WalStorageAdapter::new(wal_config)?;
    let value = vec![b'v'; args.value_bytes.max(1)];
    let mut out = std::io::stdout();

    match args.many {
        None => {
            for i in 0..args.n {
                db.put(format!("k{i}").as_bytes(), &value).await?;
                writeln!(out, "ACK {i}")?;
                out.flush()?;
            }
        }
        Some(batch_size) => {
            let batch_size = (batch_size.max(1) as u32).min(args.n.max(1));
            let mut start = 0u32;
            while start < args.n {
                let end = (start + batch_size).min(args.n);
                let items: Vec<(Vec<u8>, Vec<u8>)> = (start..end)
                    .map(|i| (format!("k{i}").into_bytes(), value.clone()))
                    .collect();
                // ACKs for this whole batch are only printed after `put_many`
                // returns `Ok` below — never while the batch is in flight.
                db.put_many(items).await?;
                for i in start..end {
                    writeln!(out, "ACK {i}")?;
                    out.flush()?;
                }
                start = end;
            }
        }
    }

    // Wait to be SIGKILLed by the parent test; never returns on success.
    std::thread::sleep(std::time::Duration::from_secs(3600));
    Ok(())
}
