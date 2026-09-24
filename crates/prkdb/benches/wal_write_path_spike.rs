//! SPIKE (remediation Task 2.1) — single-log group-commit WAL vs the current write path.
//!
//! THROWAWAY. This file is a measurement instrument for one decision (spec §7 Phase 2
//! "2a. One WAL"), recorded in `docs/remediation/decisions/2026-09-24-single-log-spike.md`.
//! Nothing in the product links it; the `SingleLog` prototype below is deliberately not a
//! library module and must not be promoted as-is (no recovery, no reads, no error
//! propagation beyond the ack). Tasks 2.2–2.4 build the real thing.
//!
//! It lives in `crates/prkdb` rather than `crates/prkdb-core` because the comparison has
//! to go through the current path's public API (`WalStorageAdapter::put`), and
//! `prkdb-core` cannot depend on `prkdb`.
//!
//! Run: `cargo bench -p prkdb --bench wal_write_path_spike`
//! Env: `SPIKE_WARMUP_MS` (default 1000), `SPIKE_MEASURE_MS` (default 3000),
//!      `SPIKE_FILTER` (substring of the cell name, e.g. `fast/64w`),
//!      `SPIKE_REPS` (default 1).

use prkdb::storage::WalStorageAdapter;
use prkdb_core::vfs::{StdVfs, Vfs, VfsFile};
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::mmap_parallel_wal::MmapParallelWal;
use prkdb_core::wal::{
    CompressionConfig, LogOperation, LogRecord, SyncMode, Wal, WalConfig, WalOptions,
};
use prkdb_types::storage::StorageAdapter;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------------------
// SPIKE prototype: SingleLog
// ---------------------------------------------------------------------------------------

/// How the writer makes a group-commit batch durable before acking it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncPolicy {
    /// `VfsFile::sync_data` per batch, then ack. On macOS that is `F_FULLFSYNC`.
    Durable,
    /// Plain `fsync(2)` per batch, then ack. On macOS this does *not* flush the drive's
    /// write cache, so it approximates the cost of Linux `fdatasync` on a drive that
    /// honours flushes cheaply. Only here to make Linux-like cost estimable.
    DurablePlainFsync,
    /// Ack after `pwrite`; a background thread calls `sync_data` every 10 ms.
    Fast,
}

const SEGMENT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
const FAST_SYNC_INTERVAL: Duration = Duration::from_millis(10);
/// len u32 | crc32 u32 | offset u64
const FRAME_HEADER: usize = 16;

struct Pending {
    payload: Vec<u8>,
    done: oneshot::Sender<io::Result<u64>>,
}

/// Writer-thread counters, read by the driver as deltas over the measurement window.
#[derive(Default)]
struct WriterStats {
    batches: AtomicU64,
    records: AtomicU64,
    bytes: AtomicU64,
    /// Time blocked in `recv` waiting for the first record of a batch.
    idle_ns: AtomicU64,
    write_ns: AtomicU64,
    sync_ns: AtomicU64,
    /// Thread CPU time (CLOCK_THREAD_CPUTIME_ID), refreshed after every batch.
    cpu_ns: AtomicU64,
    max_batch: AtomicU64,
}

#[derive(Clone, Copy, Default)]
struct WriterSnapshot {
    batches: u64,
    records: u64,
    bytes: u64,
    idle_ns: u64,
    write_ns: u64,
    sync_ns: u64,
    cpu_ns: u64,
    max_batch: u64,
}

impl WriterStats {
    fn snapshot(&self) -> WriterSnapshot {
        WriterSnapshot {
            batches: self.batches.load(Ordering::Relaxed),
            records: self.records.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            idle_ns: self.idle_ns.load(Ordering::Relaxed),
            write_ns: self.write_ns.load(Ordering::Relaxed),
            sync_ns: self.sync_ns.load(Ordering::Relaxed),
            cpu_ns: self.cpu_ns.load(Ordering::Relaxed),
            max_batch: self.max_batch.load(Ordering::Relaxed),
        }
    }
}

/// One active segment file plus, for `DurablePlainFsync`, a std handle for `fsync(2)`.
struct Segment {
    file: Arc<dyn VfsFile>,
    raw: Option<std::fs::File>,
    pos: u64,
}

/// SPIKE: one globally ordered log, one writer thread, group commit.
struct SingleLog {
    tx: Option<mpsc::Sender<Pending>>,
    writer: Option<JoinHandle<()>>,
    syncer: Option<JoinHandle<()>>,
    stop_syncer: Arc<AtomicBool>,
    stats: Arc<WriterStats>,
}

impl SingleLog {
    fn open(vfs: Arc<dyn Vfs>, dir: &Path, policy: SyncPolicy) -> io::Result<Self> {
        vfs.create_dir_all(dir)?;
        let first = open_segment(vfs.as_ref(), dir, 0, policy)?;
        let current: Arc<Mutex<Arc<dyn VfsFile>>> = Arc::new(Mutex::new(first.file.clone()));
        let stats = Arc::new(WriterStats::default());
        let stop_syncer = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel::<Pending>();

        let syncer = if policy == SyncPolicy::Fast {
            let current = current.clone();
            let stop = stop_syncer.clone();
            Some(
                std::thread::Builder::new()
                    .name("spike-wal-syncer".into())
                    .spawn(move || {
                        while !stop.load(Ordering::Relaxed) {
                            std::thread::sleep(FAST_SYNC_INTERVAL);
                            let file = current.lock().expect("syncer lock").clone();
                            // SPIKE: a real implementation must surface this error and
                            // fail the log; here we only measure cost.
                            let _ = file.sync_data();
                        }
                    })?,
            )
        } else {
            None
        };

        let writer = {
            let stats = stats.clone();
            let dir = dir.to_path_buf();
            std::thread::Builder::new()
                .name("spike-wal-writer".into())
                .spawn(move || writer_loop(vfs, dir, policy, first, current, rx, stats))?
        };

        Ok(Self {
            tx: Some(tx),
            writer: Some(writer),
            syncer,
            stop_syncer,
            stats,
        })
    }

    /// Append one encoded record; resolves with its global offset once the batch that
    /// carried it is written (Fast) or synced (Durable).
    async fn append(&self, payload: Vec<u8>) -> io::Result<u64> {
        let (done, rx) = oneshot::channel();
        self.tx
            .as_ref()
            .expect("log is open")
            .send(Pending { payload, done })
            .map_err(|_| io::Error::other("writer gone"))?;
        rx.await
            .map_err(|_| io::Error::other("writer dropped ack"))?
    }

    fn stats(&self) -> WriterSnapshot {
        self.stats.snapshot()
    }
}

impl Drop for SingleLog {
    fn drop(&mut self) {
        self.tx.take();
        if let Some(w) = self.writer.take() {
            let _ = w.join();
        }
        self.stop_syncer.store(true, Ordering::Relaxed);
        if let Some(s) = self.syncer.take() {
            let _ = s.join();
        }
    }
}

fn segment_path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!("{index:020}.log"))
}

fn open_segment(vfs: &dyn Vfs, dir: &Path, index: u64, policy: SyncPolicy) -> io::Result<Segment> {
    let path = segment_path(dir, index);
    let file = vfs.create(&path)?;
    vfs.sync_dir(dir)?;
    let raw = if policy == SyncPolicy::DurablePlainFsync {
        Some(std::fs::OpenOptions::new().write(true).open(&path)?)
    } else {
        None
    };
    Ok(Segment { file, raw, pos: 0 })
}

fn sync_segment(seg: &Segment, policy: SyncPolicy) -> io::Result<()> {
    match policy {
        SyncPolicy::Durable => seg.file.sync_data(),
        SyncPolicy::DurablePlainFsync => plain_fsync(seg.raw.as_ref().expect("raw handle")),
        // Fast: the background syncer covers steady state; the writer syncs only on roll.
        SyncPolicy::Fast => Ok(()),
    }
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

#[cfg(unix)]
fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid out-pointer; CLOCK_THREAD_CPUTIME_ID is supported on Linux and macOS.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if rc != 0 {
        return 0;
    }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

#[cfg(not(unix))]
fn thread_cpu_ns() -> u64 {
    0
}

fn frame_into(buf: &mut Vec<u8>, offset: u64, payload: &[u8]) {
    let mut h = crc32fast::Hasher::new();
    h.update(&offset.to_le_bytes());
    h.update(payload);
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&h.finalize().to_le_bytes());
    buf.extend_from_slice(&offset.to_le_bytes());
    buf.extend_from_slice(payload);
}

fn ns(d: Duration) -> u64 {
    d.as_nanos() as u64
}

fn writer_loop(
    vfs: Arc<dyn Vfs>,
    dir: PathBuf,
    policy: SyncPolicy,
    mut seg: Segment,
    current: Arc<Mutex<Arc<dyn VfsFile>>>,
    rx: mpsc::Receiver<Pending>,
    stats: Arc<WriterStats>,
) {
    let mut next_offset: u64 = 1;
    let mut seg_index: u64 = 0;
    let mut batch: Vec<Pending> = Vec::with_capacity(1024);
    let mut buf: Vec<u8> = Vec::with_capacity(MAX_BATCH_BYTES + 128 * 1024);

    loop {
        let idle_start = Instant::now();
        let Ok(first) = rx.recv() else { break };
        stats
            .idle_ns
            .fetch_add(ns(idle_start.elapsed()), Ordering::Relaxed);

        // Group commit: everything already queued, up to a byte cap.
        let mut batch_bytes = first.payload.len() + FRAME_HEADER;
        batch.push(first);
        while batch_bytes < MAX_BATCH_BYTES {
            match rx.try_recv() {
                Ok(p) => {
                    batch_bytes += p.payload.len() + FRAME_HEADER;
                    batch.push(p);
                }
                Err(_) => break,
            }
        }

        buf.clear();
        let base = next_offset;
        for (i, p) in batch.iter().enumerate() {
            frame_into(&mut buf, base + i as u64, &p.payload);
        }

        let t_write = Instant::now();
        let mut result: io::Result<()> = Ok(());
        if seg.pos + buf.len() as u64 > SEGMENT_BYTES && seg.pos > 0 {
            // Roll: make the old segment durable in every mode, then switch.
            result = seg.file.sync_data().and_then(|()| {
                seg_index += 1;
                let new_seg = open_segment(vfs.as_ref(), &dir, seg_index, policy)?;
                *current.lock().expect("current lock") = new_seg.file.clone();
                seg = new_seg;
                Ok(())
            });
        }
        if result.is_ok() {
            result = seg.file.write_at(seg.pos, &buf);
        }
        stats
            .write_ns
            .fetch_add(ns(t_write.elapsed()), Ordering::Relaxed);

        if result.is_ok() {
            let t_sync = Instant::now();
            result = sync_segment(&seg, policy);
            stats
                .sync_ns
                .fetch_add(ns(t_sync.elapsed()), Ordering::Relaxed);
        }

        let n = batch.len() as u64;
        match result {
            Ok(()) => {
                seg.pos += buf.len() as u64;
                next_offset += n;
                for (i, p) in batch.drain(..).enumerate() {
                    let _ = p.done.send(Ok(base + i as u64));
                }
            }
            Err(e) => {
                // SPIKE: a real log would poison itself here (no later write may be
                // acked after a failed one). Measurement only.
                for p in batch.drain(..) {
                    let _ = p.done.send(Err(io::Error::new(e.kind(), e.to_string())));
                }
            }
        }

        stats.batches.fetch_add(1, Ordering::Relaxed);
        stats.records.fetch_add(n, Ordering::Relaxed);
        stats.bytes.fetch_add(buf.len() as u64, Ordering::Relaxed);
        stats.max_batch.fetch_max(n, Ordering::Relaxed);
        stats.cpu_ns.store(thread_cpu_ns(), Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------------------
// Targets and driver
// ---------------------------------------------------------------------------------------

enum Target {
    Single(SingleLog),
    /// Bottleneck probe only: two independent `SingleLog`s (Fast), writers routed by index.
    /// If this is much faster than one log at 64 writers, the single writer is the limit.
    TwoShards([SingleLog; 2]),
    /// `MmapParallelWal::append_batch(vec![record])` — exactly the WAL call
    /// `WalStorageAdapter::put` makes, without the adapter's index/cache/barrier work.
    Mmap(MmapParallelWal),
    /// `WalStorageAdapter::put` — the current public write path.
    Adapter(WalStorageAdapter),
    /// MODEL, not product code: the current mmap append with its every-10th-batch
    /// `msync(MS_ASYNC)` removed — record encode, one async mutex, one memcpy into
    /// pre-faulted memory. On Linux `MS_ASYNC` is close to a no-op, so this is a lower
    /// bound for what the current path costs there.
    MemcpyModel(tokio::sync::Mutex<(Vec<u8>, usize)>),
    /// The real `Wal` (Task 2.6), compared against the spike's `SingleLog` prototype
    /// above. `put` encodes a one-op `Batch` on the caller, as the future adapter will.
    Wal(Wal),
}

const MODEL_BYTES: usize = 64 * 1024 * 1024;

fn put_record(key: Vec<u8>, value: &[u8]) -> LogRecord {
    LogRecord::new(LogOperation::Put {
        collection: String::new(),
        id: key,
        data: value.to_vec(),
    })
}

impl Target {
    async fn put(&self, writer: usize, key: Vec<u8>, value: &[u8]) {
        match self {
            Target::TwoShards(logs) => {
                let payload = put_record(key, value).serialize_rkyv();
                logs[writer % 2]
                    .append(payload)
                    .await
                    .expect("shard append");
            }
            Target::Single(log) => {
                // Encoding happens on the caller, as it does for the mmap path.
                let payload = put_record(key, value).serialize_rkyv();
                log.append(payload).await.expect("single log append");
            }
            Target::Mmap(wal) => {
                wal.append_batch(vec![put_record(key, value)])
                    .await
                    .expect("mmap append");
            }
            Target::Adapter(a) => {
                a.put(&key, value).await.expect("adapter put");
            }
            Target::MemcpyModel(m) => {
                let mut rec = Vec::with_capacity(value.len() + 256);
                put_record(key, value).serialize_into_rkyv(&mut rec);
                let mut g = m.lock().await;
                let (buf, pos) = &mut *g;
                if *pos + rec.len() > buf.len() {
                    *pos = 0;
                }
                buf[*pos..*pos + rec.len()].copy_from_slice(&rec);
                *pos += rec.len();
            }
            Target::Wal(wal) => {
                let payload = Batch {
                    ops: vec![BatchOp::Put {
                        key,
                        value: value.to_vec(),
                    }],
                }
                .encode(&CompressionConfig::none())
                .expect("batch encode");
                wal.append(payload, None).await.expect("wal append");
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
    writer: Option<(WriterSnapshot, f64)>,
}

fn pct(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx] as f64 / 1000.0
}

fn delta(a: WriterSnapshot, b: WriterSnapshot) -> WriterSnapshot {
    WriterSnapshot {
        batches: b.batches - a.batches,
        records: b.records - a.records,
        bytes: b.bytes - a.bytes,
        idle_ns: b.idle_ns - a.idle_ns,
        write_ns: b.write_ns - a.write_ns,
        sync_ns: b.sync_ns - a.sync_ns,
        cpu_ns: b.cpu_ns.saturating_sub(a.cpu_ns),
        max_batch: b.max_batch,
    }
}

async fn run_cell(
    name: String,
    target: Arc<Target>,
    writers: usize,
    value_size: usize,
    warmup: Duration,
    measure: Duration,
) -> CellResult {
    let start = Instant::now();
    let measure_start = start + warmup;
    let end = measure_start + measure;
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
                if t0 >= end {
                    break;
                }
                let key = format!("w{w:03}_k{i:012}").into_bytes();
                i += 1;
                target.put(w, key, &value).await;
                if t0 >= measure_start {
                    lat.push(ns(t0.elapsed()));
                }
            }
            lat
        }));
    }

    let writer_stats = if let Target::Single(log) = target.as_ref() {
        tokio::time::sleep_until(measure_start.into()).await;
        let a = log.stats();
        let a_t = Instant::now();
        tokio::time::sleep_until(end.into()).await;
        let b = log.stats();
        Some((delta(a, b), a_t.elapsed().as_secs_f64()))
    } else {
        None
    };

    let mut all: Vec<u64> = Vec::new();
    for h in handles {
        all.extend(h.await.expect("writer task"));
    }
    // Ops are counted when they *start* inside the window, so the window is the divisor.
    let secs = measure.as_secs_f64();
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
        writer: writer_stats,
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

fn print_header() {
    println!(
        "| cell | writers | value | ops/s | MB/s | p50 µs | p99 µs | p99.9 µs | avg batch | max batch | writer idle % | writer write % | writer sync % | writer CPU % | load1 |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
}

fn print_row(r: &CellResult) {
    let ops_s = r.ops as f64 / r.secs;
    let mb_s = ops_s * r.value_size as f64 / 1_000_000.0;
    let (avg_batch, max_batch, idle, write, sync, cpu) = match &r.writer {
        Some((s, wall)) => {
            let wall_ns = wall * 1e9;
            (
                format!("{:.1}", s.records as f64 / s.batches.max(1) as f64),
                format!("{}", s.max_batch),
                format!("{:.0}", 100.0 * s.idle_ns as f64 / wall_ns),
                format!("{:.0}", 100.0 * s.write_ns as f64 / wall_ns),
                format!("{:.0}", 100.0 * s.sync_ns as f64 / wall_ns),
                format!("{:.0}", 100.0 * s.cpu_ns as f64 / wall_ns),
            )
        }
        None => Default::default(),
    };
    println!(
        "| {} | {} | {} KiB | {:.0} | {:.1} | {:.1} | {:.1} | {:.1} | {} | {} | {} | {} | {} | {} | {} |",
        r.name,
        r.writers,
        r.value_size / 1024,
        ops_s,
        mb_s,
        r.p50_us,
        r.p99_us,
        r.p999_us,
        avg_batch,
        max_batch,
        idle,
        write,
        sync,
        cpu,
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

    println!("# wal_write_path_spike");
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
        "single_log_durable",
        "single_log_durable_plain_fsync",
        "single_log_fast",
        "current_mmap_wal",
        "current_adapter_put",
        "model_memcpy_only",
        "two_shard_fast",
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
                            "single_log_durable" => Target::Single(
                                SingleLog::open(Arc::new(StdVfs), &dir, SyncPolicy::Durable)
                                    .expect("open"),
                            ),
                            "single_log_durable_plain_fsync" => Target::Single(
                                SingleLog::open(
                                    Arc::new(StdVfs),
                                    &dir,
                                    SyncPolicy::DurablePlainFsync,
                                )
                                .expect("open"),
                            ),
                            "two_shard_fast" => Target::TwoShards([
                                SingleLog::open(Arc::new(StdVfs), &dir.join("a"), SyncPolicy::Fast)
                                    .expect("open"),
                                SingleLog::open(Arc::new(StdVfs), &dir.join("b"), SyncPolicy::Fast)
                                    .expect("open"),
                            ]),
                            "single_log_fast" => Target::Single(
                                SingleLog::open(Arc::new(StdVfs), &dir, SyncPolicy::Fast)
                                    .expect("open"),
                            ),
                            "current_mmap_wal" => {
                                let cfg = WalConfig {
                                    log_dir: dir.clone(),
                                    ..WalConfig::test_config()
                                };
                                let n = cfg.segment_count;
                                Target::Mmap(
                                    MmapParallelWal::open_or_create(cfg, n)
                                        .await
                                        .expect("mmap wal"),
                                )
                            }
                            "model_memcpy_only" => Target::MemcpyModel(tokio::sync::Mutex::new((
                                vec![1u8; MODEL_BYTES],
                                0,
                            ))),
                            "wal_durable" => {
                                let o = WalOptions {
                                    sync_mode: SyncMode::Durable,
                                    sync_interval: Duration::from_millis(10),
                                    segment_bytes: 256 * 1024 * 1024,
                                    max_batch_bytes: 16 * 1024 * 1024,
                                    max_queued_bytes: 64 * 1024 * 1024,
                                };
                                let (wal, _) =
                                    Wal::open(Arc::new(StdVfs), &dir, o, 1, &mut |_, _, _| Ok(()))
                                        .expect("wal open");
                                Target::Wal(wal)
                            }
                            "wal_fast" => {
                                let o = WalOptions {
                                    sync_mode: SyncMode::Fast,
                                    sync_interval: Duration::from_millis(10),
                                    segment_bytes: 256 * 1024 * 1024,
                                    max_batch_bytes: 16 * 1024 * 1024,
                                    max_queued_bytes: 64 * 1024 * 1024,
                                };
                                let (wal, _) =
                                    Wal::open(Arc::new(StdVfs), &dir, o, 1, &mut |_, _, _| Ok(()))
                                        .expect("wal open");
                                Target::Wal(wal)
                            }
                            _ => {
                                let cfg = WalConfig {
                                    log_dir: dir.clone(),
                                    ..WalConfig::test_config()
                                };
                                Target::Adapter(WalStorageAdapter::new(cfg).expect("adapter"))
                            }
                        };
                        let target = Arc::new(target);
                        let r =
                            run_cell(name, target.clone(), writers, value_size, warmup, measure)
                                .await;
                        drop(target);
                        r
                    });
                    print_row(&r);
                    let _ = std::fs::remove_dir_all(&dir);
                }
            }
        }
    }
}
