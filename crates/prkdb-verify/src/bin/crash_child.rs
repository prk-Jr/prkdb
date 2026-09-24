//! Task 1.10 (+ review follow-ups): crash-test child process.
//!
//! Writes `n` acknowledged puts to a `WalStorageAdapter` rooted at `dir`,
//! printing `ACK <i>` (flushed) right after each write is acknowledged,
//! then sleeps for an hour so the parent test can `SIGKILL` it at a known
//! point and reopen the data directory to see which acknowledged keys
//! survived.
//!
//! Passing this does not imply fsync durability: the mmap WAL makes
//! acknowledged writes visible in the page cache, which survives a process
//! kill but not power loss (STO-02, Phase 2).
//!
//! `WalStorageAdapter::new` calls `block_in_place` internally, which panics
//! on a current-thread runtime — hence the default (multi-thread) flavor of
//! `#[tokio::main]`.
//!
//! Usage: `crash_child <dir> <n> [--many <batch_size>] [--value-bytes <n>] [--segments <n>]`
//!
//! - `--many <batch_size>`: write through `put_many` (the
//!   accumulator/flush-loop path — see `WalStorageAdapter::put_many` in
//!   `crates/prkdb/src/storage/wal_adapter.rs`, which routes through
//!   `enqueue_writes` into the `AdaptiveBatchAccumulator`) in batches of
//!   this size instead of one `put` per key. `ACK <i>` for every key in a
//!   batch is only printed once that batch's `put_many` call has returned
//!   `Ok` — never while the batch is still in flight.
//! - `--value-bytes <n>`: value size per key in bytes (default 1, i.e. the
//!   original `b"v"`).
//! - `--segments <n>`: overrides `WalConfig::segment_count` (default comes
//!   from `WalConfig::test_config()`, currently 4). Tests that want to
//!   force every key into a single mmap segment file — to reliably drive
//!   that segment past its `INITIAL_SEGMENT_SIZE` (64 MB, see
//!   `crates/prkdb-core/src/wal/mmap_log_segment.rs`) and exercise its
//!   resize path — pass `--segments 1`.

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
    segments: Option<usize>,
}

const USAGE: &str =
    "usage: crash_child <dir> <n> [--many <batch_size>] [--value-bytes <n>] [--segments <n>]";

fn parse_args() -> anyhow::Result<Args> {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    if raw.len() < 2 {
        bail!(USAGE);
    }
    let dir = PathBuf::from(&raw[0]);
    let n: u32 = raw[1].parse().context("parsing <n>")?;

    let mut many = None;
    let mut value_bytes = 1usize;
    let mut segments = None;

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
            "--segments" => {
                let v = raw.get(i + 1).context("--segments requires a value")?;
                segments = Some(v.parse().context("parsing --segments")?);
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
        segments,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = parse_args()?;

    let mut wal_config = WalConfig {
        log_dir: args.dir,
        ..WalConfig::test_config()
    };
    if let Some(segments) = args.segments {
        wal_config.segment_count = segments;
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
