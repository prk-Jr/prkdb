//! Task 1.10: real-process SIGKILL crash check.
//!
//! Spawns `crash_child` (`src/bin/crash_child.rs`), which writes `N`
//! acknowledged puts to a WAL and prints `ACK <i>` after each one. Once the
//! parent has seen `ACK <N-1>`, it `SIGKILL`s the child (no graceful
//! shutdown, no chance to flush anything beyond what the WAL already made
//! durable), reopens the same data directory, and asserts every
//! acknowledged key survived.
//!
//! Unlike the in-process crash harness (`tests/harness.rs`), this exercises
//! a real OS-level process kill, which the in-process model can't reproduce:
//! there's no `Sut::crash()` call, no cooperating destructor, just a process
//! that stops existing mid-flight.

#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

const NUM_PUTS: u32 = 200;
const LAST_ACK: &str = "ACK 199";
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

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_writes_survive_sigkill() {
    let dir = tempfile::tempdir().unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_crash_child"))
        .args([dir.path().to_str().unwrap(), &NUM_PUTS.to_string()])
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to spawn crash_child");
    let mut child = KillOnDrop(child);

    let stdout = child.0.stdout.take().expect("child stdout not piped");

    // Read stdout on a dedicated thread so a hung/misbehaving child can't
    // block the test past `STDOUT_TIMEOUT`: `BufRead::lines()` has no
    // built-in timeout, so we forward lines over a channel and bound the
    // wait on the receiving end instead.
    let (tx, rx) = mpsc::channel::<std::io::Result<String>>();
    let reader_handle = std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    let deadline = std::time::Instant::now() + STDOUT_TIMEOUT;
    let mut saw_last_ack = false;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match rx.recv_timeout(remaining) {
            Ok(Ok(line)) if line == LAST_ACK => {
                saw_last_ack = true;
                break;
            }
            Ok(Ok(_)) => continue,
            Ok(Err(e)) => panic!("error reading crash_child stdout: {e}"),
            Err(mpsc::RecvTimeoutError::Timeout) => break,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    // Drop the receiver's counterpart cleanly; the reader thread exits on
    // its own once the child's stdout closes (which SIGKILL below ensures).
    drop(reader_handle);

    assert!(
        saw_last_ack,
        "never saw '{LAST_ACK}' from crash_child within {STDOUT_TIMEOUT:?} (hung or crashed early)"
    );

    // `Child::kill()` sends SIGKILL on Unix: no graceful shutdown, no final
    // flush beyond what was already durable when each `ACK` was printed.
    child.0.kill().expect("failed to SIGKILL crash_child");
    child.0.wait().expect("failed to reap crash_child");

    let db = prkdb::storage::WalStorageAdapter::open_async(prkdb_core::wal::WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..prkdb_core::wal::WalConfig::test_config()
    })
    .await
    .expect("failed to reopen data dir after SIGKILL");

    use prkdb_types::storage::StorageAdapter;
    let mut lost = Vec::new();
    for i in 0..NUM_PUTS {
        let key = format!("k{i}");
        match db.get(key.as_bytes()).await {
            Ok(Some(_)) => {}
            Ok(None) => lost.push(key),
            Err(e) => panic!("get({key}) failed after reopen: {e}"),
        }
    }

    assert!(
        lost.is_empty(),
        "{} of {NUM_PUTS} acknowledged keys lost after SIGKILL: {lost:?}",
        lost.len()
    );
}
