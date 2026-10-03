//! WAL write-path comparison bench (Task 2.9; was the Task 2.1 spike).
//!
//! Measures put throughput and latency on the single ordered `Wal` and on the public
//! adapter path, per (writers, value size) cell, plus the device ceilings. Its raw output
//! is what `scripts/wal_fast_rule.py` reads; the decision record is
//! `docs/remediation/decisions/2026-09-24-single-log-spike.md`.
//!
//! Cells:
//! - `wal_durable`, `wal_fast`: `Wal::append` of a one-op `Batch`, encoded on the caller.
//! - `adapter_put`: `WalStorageAdapter::put`, the public write path (Fast; see below).
//! - `compaction_concurrent_put`: `adapter_put` on 1 MiB segments over a 1,000-key space
//!   per writer (so the log is mostly dead records), while `WalStorageAdapter::compact`
//!   runs back to back for the whole cell: put latency with compaction running (Task 2.15).
//!   `wal_fast_rule.py --compaction-tail` (the `probe-wal-bench` job) requires its p99 to
//!   stay within `COMPACTION_P99_K` times `adapter_put`'s, per cell.
//! - `model_memcpy_only`: MODEL, not product code — encode the same `Batch`, take one
//!   async mutex, memcpy into pre-faulted memory. A CPU/memory ceiling for a put.
//!
//! Stream cells (Task 2.15b.8, design note `2026-10-02-streaming-log-design.md` §11),
//! printed in a second table with a records/s column; `wal_fast_rule.py` does not parse
//! them (its row pattern is `<cell>/<w>w/<v>k`), so the rule's input is unchanged:
//! - `stream_append/{durable,fast}/{1,64}w/{1,64}k/b{1,100}`: `StreamLog::append` of 1 or
//!   100 keyed records per call. ops/s counts calls; records/s and MB/s count records.
//! - `stream_read/tail/1k`, `stream_read/cold/64k`: one reader, `read_from` with 1 MiB
//!   limits, cycling over a pre-filled stream; cold drops the page cache
//!   (`posix_fadvise(DONTNEED)`, Linux only; elsewhere cold equals tail and says so) at
//!   the start of every pass.
//! - `stream_append_retention/durable/64w/1k/b1`: as `stream_append` on 1 MiB segments
//!   with `max_bytes` = 8 MiB and `apply_retention` looping for the whole cell.
//! - A retention row without measured segment reclamation is explicitly T4 UNJUDGEABLE.
//! - T5 requires a separate matched 1 GiB stream/keyed reopen measurement in Step 2;
//!   this harness does not measure recovery.
//! - The ceiling block gains `pread 1 MiB` rows (page cache, and cold on Linux): the
//!   denominator for the read targets.
//!
//! The spike's `SingleLog` prototype, the old mmap WAL cell and the two-shard probe were
//! removed in Task 2.9 with the implementations they measured (D13). Old runs still print
//! the adapter cell as `current_adapter_put`; `wal_fast_rule.py` accepts either name.
//!
//! It lives in `crates/prkdb` rather than `crates/prkdb-core` because the adapter cell has
//! to go through `WalStorageAdapter`, and `prkdb-core` cannot depend on `prkdb`.
//!
//! Run: `cargo bench -p prkdb --bench wal_write_path`
//! Env: `SPIKE_WARMUP_MS` (default 1000), `SPIKE_MEASURE_MS` (default 3000),
//!      `SPIKE_FILTER` (substring of the cell name, e.g. `fast/64w`),
//!      `SPIKE_REPS` (default 1). (The names predate the rename; CI passes them.)

mod support {
    pub mod stream_measurement;
}

use prkdb::storage::WalStorageAdapter;
use prkdb::stream_log::{ReadLimits, Record, RetentionPolicy, StartAt, StreamLog};
use prkdb_core::vfs::{StdVfs, Vfs};
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::{CompressionConfig, SyncMode, Wal, WalConfig, WalOptions};
use prkdb_types::storage::StorageAdapter;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use support::stream_measurement::{
    records_for_append, stream_config, MeasurementWindow, RetentionCounts, StreamCounts,
};

enum Target {
    /// `WalStorageAdapter::put` — the public write path.
    Adapter(WalStorageAdapter),
    /// MODEL, not product code: encode, one async mutex, one memcpy into pre-faulted
    /// memory.
    MemcpyModel(tokio::sync::Mutex<(Vec<u8>, usize)>),
    /// The single ordered `Wal`. `put` encodes a one-op `Batch` on the caller, as the
    /// adapter does.
    Wal(Wal),
    /// `StreamLog::append` of `batch` keyed records per call.
    Stream { log: StreamLog, batch: usize },
}

const MODEL_BYTES: usize = 64 * 1024 * 1024;

fn encode_put(key: Vec<u8>, value: &[u8]) -> Vec<u8> {
    Batch {
        ops: vec![BatchOp::Put {
            key,
            value: value.to_vec(),
        }],
    }
    .encode(&CompressionConfig::none())
    .expect("batch encode")
}

impl Target {
    async fn put(&self, key: Vec<u8>, value: &[u8]) {
        match self {
            Target::Adapter(a) => {
                a.put(&key, value).await.expect("adapter put");
            }
            Target::MemcpyModel(m) => {
                let rec = encode_put(key, value);
                let mut g = m.lock().await;
                let (buf, pos) = &mut *g;
                if *pos + rec.len() > buf.len() {
                    *pos = 0;
                }
                buf[*pos..*pos + rec.len()].copy_from_slice(&rec);
                *pos += rec.len();
            }
            Target::Wal(wal) => {
                wal.append(encode_put(key, value), None)
                    .await
                    .expect("wal append");
            }
            Target::Stream { log, batch } => {
                let records = records_for_append(key, value, *batch);
                log.append(records).await.expect("stream append");
            }
        }
    }
}

struct CellResult {
    name: String,
    writers: usize,
    value_size: usize,
    ops: u64,
    secs: f64,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
}

fn ns(d: Duration) -> u64 {
    d.as_nanos() as u64
}

fn pct(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx] as f64 / 1000.0
}

async fn run_cell(
    name: String,
    target: Arc<Target>,
    writers: usize,
    value_size: usize,
    window: MeasurementWindow,
    key_space: Option<u64>,
) -> CellResult {
    let value: Arc<Vec<u8>> = Arc::new((0..value_size).map(|i| (i % 251) as u8).collect());

    let mut handles = Vec::with_capacity(writers);
    for w in 0..writers {
        let target = target.clone();
        let value = value.clone();
        handles.push(tokio::spawn(async move {
            let mut lat: Vec<u64> = Vec::with_capacity(1 << 16);
            let mut i: u64 = 0;
            loop {
                let t0 = Instant::now();
                if t0 >= window.end {
                    break;
                }
                let k = key_space.map_or(i, |n| i % n);
                let key = format!("w{w:03}_k{k:012}").into_bytes();
                i += 1;
                target.put(key, &value).await;
                if window.measures(t0) {
                    lat.push(ns(t0.elapsed()));
                }
            }
            lat
        }));
    }

    let mut all: Vec<u64> = Vec::new();
    for h in handles {
        all.extend(h.await.expect("writer task"));
    }
    // Ops are counted when they *start* inside the window, so the window is the divisor.
    let secs = window.measure.as_secs_f64();
    all.sort_unstable();
    CellResult {
        name,
        writers,
        value_size,
        ops: all.len() as u64,
        secs,
        p50_us: pct(&all, 0.50),
        p99_us: pct(&all, 0.99),
        p999_us: pct(&all, 0.999),
    }
}

fn load_avg() -> String {
    #[cfg(unix)]
    {
        let mut l = [0f64; 3];
        // SAFETY: valid pointer to 3 doubles.
        let n = unsafe { libc::getloadavg(l.as_mut_ptr(), 3) };
        if n == 3 {
            return format!("{:.2} {:.2} {:.2}", l[0], l[1], l[2]);
        }
    }
    "n/a".to_string()
}

/// The row format `scripts/wal_fast_rule.py` parses: cell, writers, value, ops/s, MB/s, p50,
/// p99 first (it reads ops/s and p99).
fn print_header() {
    println!("| cell | writers | value | ops/s | MB/s | p50 µs | p99 µs | p99.9 µs | load1 |");
    println!("|---|---|---|---|---|---|---|---|---|");
}

fn print_row(r: &CellResult) {
    let ops_s = r.ops as f64 / r.secs;
    let mb_s = ops_s * r.value_size as f64 / 1_000_000.0;
    println!(
        "| {} | {} | {} KiB | {:.0} | {:.1} | {:.1} | {:.1} | {:.1} | {} |",
        r.name,
        r.writers,
        r.value_size / 1024,
        ops_s,
        mb_s,
        r.p50_us,
        r.p99_us,
        r.p999_us,
        load_avg(),
    );
}

fn env_ms(key: &str, default: u64) -> Duration {
    Duration::from_millis(
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default),
    )
}

#[cfg(unix)]
fn plain_fsync(file: &std::fs::File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: `fsync` on a file descriptor owned by a live `File`.
    let rc = unsafe { libc::fsync(file.as_raw_fd()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn plain_fsync(file: &std::fs::File) -> io::Result<()> {
    file.sync_data()
}

/// Raw device ceiling: one thread, 1 MiB `pwrite`s, with and without a sync per write.
fn disk_ceiling(dir: &Path, measure: Duration) -> io::Result<()> {
    let vfs = StdVfs;
    vfs.create_dir_all(dir)?;
    let chunk = vec![0xabu8; 1024 * 1024];
    for (label, sync) in [
        ("pwrite 1 MiB, no sync", false),
        ("pwrite 1 MiB + sync_data", true),
    ] {
        let path = dir.join(format!("ceiling_{sync}.bin"));
        let f = vfs.create(&path)?;
        let start = Instant::now();
        let mut pos = 0u64;
        let mut n = 0u64;
        while start.elapsed() < measure && pos < 4 * 1024 * 1024 * 1024 {
            f.write_at(pos, &chunk)?;
            if sync {
                f.sync_data()?;
            }
            pos += chunk.len() as u64;
            n += 1;
        }
        let secs = start.elapsed().as_secs_f64();
        println!(
            "- disk ceiling, {label}: {:.0} MB/s ({:.0} writes/s)",
            n as f64 * chunk.len() as f64 / 1e6 / secs,
            n as f64 / secs
        );
        drop(f);
        vfs.remove(&path)?;
    }
    read_ceiling(dir, measure)?;
    // Sync latency for a tiny write, to show the per-commit floor.
    let path = dir.join("ceiling_small.bin");
    let f = vfs.create(&path)?;
    let raw = std::fs::OpenOptions::new().write(true).open(&path)?;
    for (label, plain) in [
        ("sync_data (F_FULLFSYNC on macOS)", false),
        ("plain fsync(2)", true),
    ] {
        let mut lat = Vec::new();
        let start = Instant::now();
        let mut pos = 0u64;
        while start.elapsed() < measure {
            f.write_at(pos, &chunk[..4096])?;
            let t = Instant::now();
            if plain {
                plain_fsync(&raw)?;
            } else {
                f.sync_data()?;
            }
            lat.push(ns(t.elapsed()));
            pos += 4096;
        }
        lat.sort_unstable();
        println!(
            "- 4 KiB write + {label}: p50 {:.0} µs, p99 {:.0} µs ({} samples)",
            pct(&lat, 0.5),
            pct(&lat, 0.99),
            lat.len()
        );
    }
    drop(f);
    vfs.remove(&path)?;
    Ok(())
}

/// Drops `path`'s cached pages so the next read comes from the device. Linux only; returns
/// whether it did anything. The file must be synced first (dirty pages are not dropped).
fn drop_page_cache(path: &Path) -> io::Result<bool> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::File::open(path)?;
        // SAFETY: `posix_fadvise` on a file descriptor owned by a live `File`.
        let rc = unsafe { libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        Ok(true)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path; // no portable equivalent; cold rows say so
        Ok(false)
    }
}

/// Sequential read ceiling: 1 MiB `pread`s over a 256 MiB synced file, from the page
/// cache, then (Linux) after dropping it before every pass.
fn read_ceiling(dir: &Path, measure: Duration) -> io::Result<()> {
    let vfs = StdVfs;
    let path = dir.join("ceiling_read.bin");
    let chunk = vec![0x5au8; 1024 * 1024];
    let size: u64 = 256 * 1024 * 1024;
    let f = vfs.create(&path)?;
    let mut pos = 0;
    while pos < size {
        f.write_at(pos, &chunk)?;
        pos += chunk.len() as u64;
    }
    f.sync_data()?;
    let mut buf = vec![0u8; chunk.len()];
    for cold in [false, true] {
        let start = Instant::now();
        let mut pos = 0u64;
        let mut n = 0u64;
        let mut dropped = cold && drop_page_cache(&path)?;
        while start.elapsed() < measure {
            if pos >= size {
                pos = 0;
                if cold {
                    dropped = drop_page_cache(&path)?;
                }
            }
            let got = f.read_at(pos, &mut buf)?;
            assert_eq!(got, buf.len(), "short read in the read ceiling");
            pos += got as u64;
            n += 1;
        }
        let secs = start.elapsed().as_secs_f64();
        let label = match (cold, dropped) {
            (false, _) => "pread 1 MiB, page cache".to_string(),
            (true, true) => "pread 1 MiB, cold (fadvise DONTNEED per pass)".to_string(),
            (true, false) => "pread 1 MiB, cold unavailable here (same as page cache)".to_string(),
        };
        println!(
            "- device: {label}: {:.0} MB/s",
            n as f64 * buf.len() as f64 / 1e6 / secs
        );
    }
    drop(f);
    vfs.remove(&path)?;
    Ok(())
}

fn print_stream_header() {
    println!();
    println!(
        "| cell | writers | value | ops/s | MB/s | p50 µs | p99 µs | p99.9 µs | records/s | load1 |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
}

/// Exact measured operation/record totals; MB/s counts record values.
fn print_stream_row(r: &CellResult, counts: StreamCounts) {
    let ops_s = counts.operations_per_second(r.secs);
    let rec_s = counts.records_per_second(r.secs);
    println!(
        "| {} | {} | {} KiB | {:.0} | {:.1} | {:.1} | {:.1} | {:.1} | {:.0} | {} |",
        r.name,
        r.writers,
        r.value_size / 1024,
        ops_s,
        counts.value_megabytes_per_second(r.value_size, r.secs),
        r.p50_us,
        r.p99_us,
        r.p999_us,
        rec_s,
        load_avg(),
    );
}

/// `stream_read/{tail,cold}/<v>k`: fills ~`fill` bytes, syncs, then one reader cycles
/// `read_from` with 1 MiB limits. ops are reads; the row's value column is the record
/// size and records/s counts records read.
async fn stream_read_cell(
    dir: &Path,
    value_size: usize,
    cold: bool,
    warmup: Duration,
    measure: Duration,
) -> (CellResult, StreamCounts, bool) {
    const FILL: usize = 256 * 1024 * 1024;
    let log = StreamLog::open(stream_config(dir, SyncMode::Fast))
        .await
        .expect("stream open");
    let per_append = (1024 * 1024 / value_size).max(1);
    let value = vec![0x42u8; value_size];
    let mut written = 0;
    while written < FILL {
        let records = (0..per_append)
            .map(|i| Record {
                key: Some(format!("k{i:06}").into_bytes()),
                value: value.clone(),
                headers: vec![],
            })
            .collect();
        log.append(records).await.expect("fill append");
        written += per_append * value_size;
    }
    log.sync().await.expect("fill sync");
    let segments: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
        .expect("read stream dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "wal"))
        .collect();
    let drop_all = |segments: &[std::path::PathBuf]| -> bool {
        let mut all = !segments.is_empty();
        for p in segments {
            all &= drop_page_cache(p).expect("posix_fadvise");
        }
        all
    };
    let limits = ReadLimits {
        max_records: usize::MAX,
        max_bytes: 1024 * 1024,
    };
    let start = Instant::now();
    let measure_start = start + warmup;
    let end = measure_start + measure;
    let mut lat = Vec::with_capacity(1 << 16);
    let mut records_read = 0usize;
    let mut dropped = cold && drop_all(&segments);
    let mut from = StartAt::Earliest;
    loop {
        let t0 = Instant::now();
        if t0 >= end {
            break;
        }
        let batch = log.read_from(from, limits).await.expect("stream read");
        if t0 >= measure_start {
            lat.push(ns(t0.elapsed()));
            records_read += batch.records.len();
        }
        from = if batch.records.is_empty() || batch.next >= log.next_offset() {
            if cold {
                dropped = drop_all(&segments);
            }
            StartAt::Earliest
        } else {
            StartAt::Offset(batch.next)
        };
    }
    log.close().expect("stream close");
    lat.sort_unstable();
    let kind = if cold { "cold" } else { "tail" };
    let r = CellResult {
        name: format!("stream_read/{kind}/{}k", value_size / 1024),
        writers: 1,
        value_size,
        ops: lat.len() as u64,
        secs: measure.as_secs_f64(),
        p50_us: pct(&lat, 0.50),
        p99_us: pct(&lat, 0.99),
        p999_us: pct(&lat, 0.999),
    };
    let counts = StreamCounts {
        operations: r.ops,
        records: records_read as u64,
    };
    (r, counts, !cold || dropped)
}

fn open_wal(dir: &Path, sync_mode: SyncMode) -> Target {
    let o = WalOptions {
        sync_mode,
        sync_interval: Duration::from_millis(10),
        segment_bytes: 256 * 1024 * 1024,
        max_batch_bytes: 16 * 1024 * 1024,
        max_queued_bytes: 64 * 1024 * 1024,
        front_release: prkdb_core::wal::FrontRelease::ElidedOnly,
        append_kind: prkdb_core::wal::frame::FrameKind::Batch,
        lsn_limit: None,
    };
    let (wal, _) = Wal::open(Arc::new(StdVfs), dir, o, 1, &mut |_, _, _| Ok(())).expect("wal open");
    Target::Wal(wal)
}

fn main() {
    // `cargo bench` passes `--bench`; ignore arguments.
    let warmup = env_ms("SPIKE_WARMUP_MS", 1000);
    let measure = env_ms("SPIKE_MEASURE_MS", 3000);
    let filter = std::env::var("SPIKE_FILTER").unwrap_or_default();
    let reps: usize = std::env::var("SPIKE_REPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    println!("# wal_write_path");
    println!(
        "- warm-up {} ms, measure {} ms, reps {reps}, tokio worker threads = {}",
        warmup.as_millis(),
        measure.as_millis(),
        std::thread::available_parallelism().map_or(0, |n| n.get())
    );
    println!("- load average at start: {}", load_avg());

    let base = tempfile::tempdir().expect("tempdir");
    if filter.is_empty() || filter.contains("ceiling") {
        disk_ceiling(&base.path().join("ceiling"), Duration::from_secs(2)).expect("ceiling");
    }

    print_header();
    let kinds = [
        "adapter_put",
        "compaction_concurrent_put",
        "model_memcpy_only",
        "wal_durable",
        "wal_fast",
    ];
    let mut cell_id = 0;
    for _ in 0..reps {
        for value_size in [1024usize, 64 * 1024] {
            for writers in [1usize, 8, 64] {
                for kind in kinds {
                    let name = format!("{kind}/{writers}w/{}k", value_size / 1024);
                    if !filter.is_empty() && !name.contains(&filter) {
                        continue;
                    }
                    cell_id += 1;
                    let dir = base.path().join(format!("cell_{cell_id}"));
                    let r = rt.block_on(async {
                        let target = match kind {
                            "model_memcpy_only" => Target::MemcpyModel(tokio::sync::Mutex::new((
                                vec![1u8; MODEL_BYTES],
                                0,
                            ))),
                            "wal_durable" => open_wal(&dir, SyncMode::Durable),
                            "wal_fast" => open_wal(&dir, SyncMode::Fast),
                            "compaction_concurrent_put" => {
                                let cfg = WalConfig {
                                    log_dir: dir.clone(),
                                    sync_mode: SyncMode::Fast,
                                    segment_bytes: 1024 * 1024,
                                    ..WalConfig::test_config()
                                };
                                Target::Adapter(WalStorageAdapter::new(cfg).expect("adapter"))
                            }
                            _ => {
                                // `adapter_put`: Fast, because the Linux adapter rule
                                // (Task 2.8d) compares against the pre-2.8a adapter, which
                                // never synced whatever its config said, and
                                // `test_config()` is Durable.
                                let cfg = WalConfig {
                                    log_dir: dir.clone(),
                                    sync_mode: SyncMode::Fast,
                                    ..WalConfig::test_config()
                                };
                                Target::Adapter(WalStorageAdapter::new(cfg).expect("adapter"))
                            }
                        };
                        let target = Arc::new(target);
                        let compacting = kind == "compaction_concurrent_put";
                        let compactor = match (&*target, compacting) {
                            (Target::Adapter(db), true) => {
                                let db = db.clone();
                                let until = Instant::now() + warmup + measure;
                                Some(tokio::spawn(async move {
                                    let mut runs = 0u64;
                                    while Instant::now() < until {
                                        db.compact().await.expect("compaction");
                                        runs += 1;
                                    }
                                    runs
                                }))
                            }
                            _ => None,
                        };
                        let key_space = compacting.then_some(1_000);
                        let r = run_cell(
                            name,
                            target.clone(),
                            writers,
                            value_size,
                            MeasurementWindow::new(Instant::now(), warmup, measure),
                            key_space,
                        )
                        .await;
                        if let Some(compactor) = compactor {
                            let runs = compactor.await.expect("compactor task");
                            println!("- {}: {runs} compaction runs during the cell", r.name);
                        }
                        drop(target);
                        r
                    });
                    print_row(&r);
                    let _ = std::fs::remove_dir_all(&dir);
                }
            }
        }
    }

    print_stream_header();
    for _ in 0..reps {
        for value_size in [1024usize, 64 * 1024] {
            for writers in [1usize, 64] {
                for (mode_name, mode) in [("durable", SyncMode::Durable), ("fast", SyncMode::Fast)]
                {
                    for batch in [1usize, 100] {
                        let name = format!(
                            "stream_append/{mode_name}/{writers}w/{}k/b{batch}",
                            value_size / 1024
                        );
                        if !filter.is_empty() && !name.contains(&filter) {
                            continue;
                        }
                        cell_id += 1;
                        let dir = base.path().join(format!("cell_{cell_id}"));
                        let r = rt.block_on(async {
                            let log = StreamLog::open(stream_config(&dir, mode))
                                .await
                                .expect("stream open");
                            let target = Arc::new(Target::Stream { log, batch });
                            run_cell(
                                name,
                                target,
                                writers,
                                value_size,
                                MeasurementWindow::new(Instant::now(), warmup, measure),
                                None,
                            )
                            .await
                        });
                        print_stream_row(
                            &r,
                            StreamCounts {
                                operations: r.ops,
                                records: r.ops * batch as u64,
                            },
                        );
                        let _ = std::fs::remove_dir_all(&dir);
                    }
                }
            }
        }

        let name = "stream_append_retention/durable/64w/1k/b1".to_string();
        if filter.is_empty() || name.contains(&filter) {
            cell_id += 1;
            let dir = base.path().join(format!("cell_{cell_id}"));
            let (r, counts) = rt.block_on(async {
                let mut cfg = stream_config(&dir, SyncMode::Durable);
                cfg.wal.segment_bytes = 1024 * 1024;
                cfg.retention = RetentionPolicy {
                    max_age: None,
                    max_bytes: Some(8 * 1024 * 1024),
                };
                let log = StreamLog::open(cfg).await.expect("stream open");
                let target = Arc::new(Target::Stream { log, batch: 1 });
                let window = MeasurementWindow::new(Instant::now(), warmup, measure);
                let retainer = {
                    let target = target.clone();
                    tokio::task::spawn_blocking(move || {
                        let Target::Stream { log, .. } = &*target else {
                            unreachable!("stream target")
                        };
                        let mut counts = RetentionCounts::default();
                        while Instant::now() < window.end {
                            let removed =
                                log.apply_retention().expect("retention").segments_removed;
                            counts.observe(window, Instant::now(), removed);
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        counts
                    })
                };
                let r = run_cell(name, target, 64, 1024, window, None).await;
                let counts = retainer.await.expect("retention task");
                (r, counts)
            });
            print_stream_row(
                &r,
                StreamCounts {
                    operations: r.ops,
                    records: r.ops,
                },
            );
            println!(
                "- {}: {} measured retention runs removed {} segments; {} warmup runs removed {} segments",
                r.name, counts.measured_runs, counts.measured_removed, counts.warmup_runs, counts.warmup_removed
            );
            if let Err(reason) = counts.validate() {
                println!(
                    "- T4 UNJUDGEABLE for {}: {reason}; this p99 row is not retention evidence",
                    r.name
                );
            }
            let _ = std::fs::remove_dir_all(&dir);
        }

        for (value_size, cold) in [(1024usize, false), (64 * 1024, true)] {
            let kind = if cold { "cold" } else { "tail" };
            let name = format!("stream_read/{kind}/{}k", value_size / 1024);
            if !filter.is_empty() && !name.contains(&filter) {
                continue;
            }
            cell_id += 1;
            let dir = base.path().join(format!("cell_{cell_id}"));
            let (r, counts, really_cold) =
                rt.block_on(stream_read_cell(&dir, value_size, cold, warmup, measure));
            print_stream_row(&r, counts);
            if !really_cold {
                println!(
                    "- {}: page cache could not be dropped here; equals tail",
                    r.name
                );
            }
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
