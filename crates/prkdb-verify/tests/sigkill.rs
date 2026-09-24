//! Task 1.10 (+ review follow-ups): real-process SIGKILL crash checks.
//!
//! Spawns `crash_child` (`src/bin/crash_child.rs`), which writes acknowledged
//! puts to a WAL and prints `ACK <i>` after each one is durable. The parent
//! reads that stream, `SIGKILL`s the child at a chosen point (no graceful
//! shutdown, no chance to flush anything beyond what the WAL already made
//! durable), reopens the same data directory, and checks which acknowledged
//! keys survived.
//!
//! Unlike the in-process crash harness (`tests/harness.rs`), this exercises
//! a real OS-level process kill, which the in-process model can't reproduce:
//! there's no `Sut::crash()` call, no cooperating destructor, just a process
//! that stops existing mid-flight.
//!
//! Passing any test here does not imply fsync durability: the mmap WAL makes
//! acknowledged writes visible in the page cache, which survives a process
//! kill but not power loss (STO-02, Phase 2).
//!
//! Four tests, two axes:
//! - write path: one `put` per key, vs. `put_many` batches (the
//!   accumulator/flush-loop path).
//! - kill point: after the very last ack (`_survive_sigkill`), vs. at a
//!   random ack chosen mid-run while the child keeps writing behind it
//!   (`_survive_mid_stream_sigkill`). The mid-stream tests also use large
//!   enough values, in a single mmap segment, to force that segment past its
//!   initial capacity and exercise its resize path (see `MID_STREAM_*`
//!   below).

#![cfg(unix)]

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;

/// Puts for the two "run to completion, then kill" tests.
const NUM_PUTS: u32 = 200;
/// Batch size for the "run to completion" batched-path test.
const FULL_RUN_BATCH: usize = 20;

/// Puts, value size, and single-segment config for the two mid-stream-kill
/// tests. `MID_STREAM_VALUE_BYTES * MID_STREAM_N` (~100 MB) comfortably
/// exceeds `INITIAL_SEGMENT_SIZE` (64 MB, `mmap_log_segment.rs`) once routed
/// into a single segment (`--segments 1`), so a bit before 2/3 of the way
/// through the run the segment's `resize()` path fires; a random kill point
/// then lands before it most of the time and after it some of the time,
/// across enough runs. Values are large (1 MiB) specifically to keep the put
/// *count* small — this crosses the same 64 MB boundary as many small puts
/// would, but in ~100 calls instead of tens of thousands, which keeps the
/// test's wall-clock cost low and stable under a loaded CI runner.
const MID_STREAM_N: u32 = 100;
const MID_STREAM_VALUE_BYTES: usize = 1024 * 1024;
const MID_STREAM_BATCH: usize = 10;
const MID_STREAM_SEGMENTS: usize = 1;

const STDOUT_TIMEOUT: Duration = Duration::from_secs(60);

/// Ensures the child is killed even if an assertion below panics: a leaked
/// child process would otherwise keep sleeping (`crash_child` sleeps for an
/// hour after its last ack) and hold the temp dir's files open.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_child(dir: &Path, args: &[String]) -> KillOnDrop {
    let child = Command::new(env!("CARGO_BIN_EXE_crash_child"))
        .arg(dir.to_str().unwrap())
        .args(args)
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to spawn crash_child");
    KillOnDrop(child)
}

/// Reads a child's stdout on a dedicated thread, forwarding lines over a
/// channel: `BufRead::lines()` has no built-in timeout, so bounding the wait
/// happens on the receiving end (`wait_for_line`) instead of here.
fn spawn_stdout_reader(
    stdout: ChildStdout,
) -> (
    mpsc::Receiver<std::io::Result<String>>,
    std::thread::JoinHandle<()>,
) {
    let (tx, rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    (rx, handle)
}

/// Blocks until `target` is read from `rx`, `timeout` elapses (returns
/// `false`), or the child's stdout closes early (panics, including the
/// child's exit status so a crash isn't mistaken for a hang).
fn wait_for_line(
    rx: &mpsc::Receiver<std::io::Result<String>>,
    target: &str,
    timeout: Duration,
    child: &mut Child,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        match rx.recv_timeout(remaining) {
            Ok(Ok(line)) if line == target => return true,
            Ok(Ok(_)) => continue,
            Ok(Err(e)) => panic!("error reading crash_child stdout: {e}"),
            Err(mpsc::RecvTimeoutError::Timeout) => return false,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let status = child.try_wait();
                panic!(
                    "crash_child stdout closed before '{target}' appeared \
                     (child exit status: {status:?})"
                );
            }
        }
    }
}

/// SIGKILLs `child`, reaps it, and joins the reader thread only once the
/// process is confirmed gone — so the thread's own exit (on stdout EOF) is
/// never raced against.
fn kill_reap_and_join(child: &mut Child, reader_handle: std::thread::JoinHandle<()>) {
    child.kill().expect("failed to SIGKILL crash_child");
    child.wait().expect("failed to reap crash_child");
    reader_handle.join().ok();
}

async fn reopen(dir: &Path, segment_count: Option<usize>) -> WalStorageAdapter {
    let mut config = WalConfig {
        log_dir: dir.to_path_buf(),
        ..WalConfig::test_config()
    };
    if let Some(segment_count) = segment_count {
        config.segment_count = segment_count;
    }
    WalStorageAdapter::open_async(config)
        .await
        .expect("failed to reopen data dir after SIGKILL")
}

/// Returns the keys in `range` that are missing from `db`.
async fn missing_keys(db: &WalStorageAdapter, range: impl Iterator<Item = u32>) -> Vec<String> {
    let mut lost = Vec::new();
    for i in range {
        let key = format!("k{i}");
        match db.get(key.as_bytes()).await {
            Ok(Some(_)) => {}
            Ok(None) => lost.push(key),
            Err(e) => panic!("get({key}) failed after reopen: {e}"),
        }
    }
    lost
}

/// Picks a random key index in `0..n`, seeded so a failure's message
/// pinpoints the exact seed and index to reproduce it with.
fn pick_random_index(n: u32) -> (u64, u32) {
    let seed: u64 = rand::random();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    (seed, rng.gen_range(0..n))
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_writes_survive_sigkill() {
    let last_ack = format!("ACK {}", NUM_PUTS - 1);
    let dir = tempfile::tempdir().unwrap();

    let mut child = spawn_child(dir.path(), &[NUM_PUTS.to_string()]);
    let stdout = child.0.stdout.take().expect("child stdout not piped");
    let (rx, reader_handle) = spawn_stdout_reader(stdout);

    let saw_last_ack = wait_for_line(&rx, &last_ack, STDOUT_TIMEOUT, &mut child.0);
    assert!(
        saw_last_ack,
        "never saw '{last_ack}' from crash_child within {STDOUT_TIMEOUT:?} (hung or crashed early)"
    );

    // `Child::kill()` sends SIGKILL on Unix: no graceful shutdown, no final
    // flush beyond what was already durable when each `ACK` was printed.
    kill_reap_and_join(&mut child.0, reader_handle);

    let db = reopen(dir.path(), None).await;
    let lost = missing_keys(&db, 0..NUM_PUTS).await;
    assert!(
        lost.is_empty(),
        "{} of {NUM_PUTS} acknowledged keys lost after SIGKILL: {lost:?}",
        lost.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_batched_writes_survive_sigkill() {
    let last_ack = format!("ACK {}", NUM_PUTS - 1);
    let dir = tempfile::tempdir().unwrap();

    let mut child = spawn_child(
        dir.path(),
        &[
            NUM_PUTS.to_string(),
            "--many".to_string(),
            FULL_RUN_BATCH.to_string(),
        ],
    );
    let stdout = child.0.stdout.take().expect("child stdout not piped");
    let (rx, reader_handle) = spawn_stdout_reader(stdout);

    let saw_last_ack = wait_for_line(&rx, &last_ack, STDOUT_TIMEOUT, &mut child.0);
    assert!(
        saw_last_ack,
        "never saw '{last_ack}' from crash_child (batch={FULL_RUN_BATCH}) within \
         {STDOUT_TIMEOUT:?} (hung or crashed early)"
    );

    kill_reap_and_join(&mut child.0, reader_handle);

    let db = reopen(dir.path(), None).await;
    let lost = missing_keys(&db, 0..NUM_PUTS).await;
    assert!(
        lost.is_empty(),
        "{} of {NUM_PUTS} acknowledged batched (batch={FULL_RUN_BATCH}) keys lost after \
         SIGKILL: {lost:?}",
        lost.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_writes_survive_mid_stream_sigkill_single_puts() {
    let (seed, k) = pick_random_index(MID_STREAM_N);
    let target_ack = format!("ACK {k}");
    let dir = tempfile::tempdir().unwrap();

    let mut child = spawn_child(
        dir.path(),
        &[
            MID_STREAM_N.to_string(),
            "--value-bytes".to_string(),
            MID_STREAM_VALUE_BYTES.to_string(),
            "--segments".to_string(),
            MID_STREAM_SEGMENTS.to_string(),
        ],
    );
    let stdout = child.0.stdout.take().expect("child stdout not piped");
    let (rx, reader_handle) = spawn_stdout_reader(stdout);

    let saw_target = wait_for_line(&rx, &target_ack, STDOUT_TIMEOUT, &mut child.0);
    assert!(
        saw_target,
        "seed={seed} k={k}: never saw '{target_ack}' from crash_child within \
         {STDOUT_TIMEOUT:?} (hung or crashed early)"
    );

    // Kill immediately, mid-stream: the child keeps issuing puts behind
    // whatever `k` was picked, right up until this SIGKILL lands.
    kill_reap_and_join(&mut child.0, reader_handle);

    let db = reopen(dir.path(), Some(MID_STREAM_SEGMENTS)).await;
    let lost = missing_keys(&db, 0..=k).await;
    assert!(
        lost.is_empty(),
        "seed={seed} k={k}: {} of {} acknowledged keys (k0..=k{k}) lost after mid-stream \
         SIGKILL: {lost:?}",
        lost.len(),
        k + 1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_writes_survive_mid_stream_sigkill_batched_puts() {
    let (seed, k) = pick_random_index(MID_STREAM_N);
    let target_ack = format!("ACK {k}");
    let dir = tempfile::tempdir().unwrap();

    let mut child = spawn_child(
        dir.path(),
        &[
            MID_STREAM_N.to_string(),
            "--many".to_string(),
            MID_STREAM_BATCH.to_string(),
            "--value-bytes".to_string(),
            MID_STREAM_VALUE_BYTES.to_string(),
            "--segments".to_string(),
            MID_STREAM_SEGMENTS.to_string(),
        ],
    );
    let stdout = child.0.stdout.take().expect("child stdout not piped");
    let (rx, reader_handle) = spawn_stdout_reader(stdout);

    let saw_target = wait_for_line(&rx, &target_ack, STDOUT_TIMEOUT, &mut child.0);
    assert!(
        saw_target,
        "seed={seed} k={k} batch={MID_STREAM_BATCH}: never saw '{target_ack}' from crash_child \
         within {STDOUT_TIMEOUT:?} (hung or crashed early)"
    );

    kill_reap_and_join(&mut child.0, reader_handle);

    let db = reopen(dir.path(), Some(MID_STREAM_SEGMENTS)).await;
    let lost = missing_keys(&db, 0..=k).await;
    assert!(
        lost.is_empty(),
        "seed={seed} k={k} batch={MID_STREAM_BATCH}: {} of {} acknowledged batched keys \
         (k0..=k{k}) lost after mid-stream SIGKILL: {lost:?}",
        lost.len(),
        k + 1
    );
}
