//! Jepsen-style Consistency Checker
//!
//! Provides operation history recording and linearizability verification
//! for distributed consistency testing. Inspired by the Jepsen framework.

use prkdb::storage::WalStorageAdapter;
use prkdb::transaction::{IsolationLevel, Transaction, TransactionConfig, TransactionExt};
use prkdb_types::storage::StorageAdapter;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// The type of operation recorded in history
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpKind {
    Read,
    Write,
    CasSuccess,
    CasFail,
    TxnBegin,
    TxnCommit,
    TxnAbort,
}

/// Result of an operation
#[derive(Debug, Clone)]
pub enum OpResult {
    Ok(Option<Vec<u8>>),
    Err(String),
    Timeout,
}

/// A single recorded operation
#[derive(Debug, Clone)]
pub struct Operation {
    pub kind: OpKind,
    pub key: Vec<u8>,
    pub write_value: Option<Vec<u8>>,
    pub read_value: Option<Vec<u8>>,
    pub start_time: Instant,
    pub end_time: Instant,
    pub result: OpResult,
    pub client_id: u64,
}

/// Tracks operation history for linearizability analysis
#[derive(Clone)]
pub struct OperationHistory {
    ops: Arc<Mutex<Vec<Operation>>>,
}

impl Default for OperationHistory {
    fn default() -> Self {
        Self::new()
    }
}

impl OperationHistory {
    pub fn new() -> Self {
        Self {
            ops: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Record an operation
    pub fn record(&self, op: Operation) {
        self.ops.lock().unwrap().push(op);
    }

    /// Get all operations
    pub fn operations(&self) -> Vec<Operation> {
        self.ops.lock().unwrap().clone()
    }

    /// Check if history is linearizable for a single register
    ///
    /// A simple linearizability check: for each read, verify that the value
    /// read could have been written by a preceding write that completed
    /// before the read started, or by a concurrent write.
    /// Check whether the recorded history is linearizable.
    ///
    /// Delegates to the Wing & Gong search in [`super::wgl`]. The previous
    /// implementation asked only whether *some* write of the same value to the same key
    /// had started before the read ended — a condition any earlier write satisfies — so
    /// it could not fail on a stale read. See the meta-tests at the bottom of this file.
    pub fn is_linearizable(&self) -> LinearizabilityResult {
        let ops = self
            .ops
            .lock()
            .expect("history mutex is only poisoned if a recording thread panicked");

        match super::wgl::check(&ops) {
            super::wgl::Verdict::Linearizable => LinearizabilityResult::Linearizable,
            super::wgl::Verdict::NotLinearizable { reason } => {
                LinearizabilityResult::NotLinearizable { reason }
            }
            // Not answering is not the same as passing.
            super::wgl::Verdict::TooLarge { key, ops } => {
                LinearizabilityResult::NotLinearizable {
                    reason: format!(
                        "key {:?} has {} operations, above the {} the search can decide;                          shorten the history rather than trusting this result",
                        String::from_utf8_lossy(&key),
                        ops,
                        super::wgl::MAX_CHECKABLE_OPS
                    ),
                }
            }
        }
    }

    /// Check a custom invariant across all operations
    /// Returns (passed, failed_reason)
    pub fn check_invariant<F>(&self, checker: F) -> InvariantResult
    where
        F: Fn(&[Operation]) -> Result<(), String>,
    {
        let ops = self.ops.lock().unwrap();
        match checker(&ops) {
            Ok(()) => InvariantResult::Passed,
            Err(reason) => InvariantResult::Failed { reason },
        }
    }

    /// Check that reads are monotonic for each client
    /// (values read by a single client never go backwards)
    pub fn check_monotonic_reads(&self) -> InvariantResult {
        let ops = self.ops.lock().unwrap();

        // Group reads by client
        let mut client_reads: HashMap<u64, Vec<&Operation>> = HashMap::new();
        for op in ops.iter().filter(|o| o.kind == OpKind::Read) {
            client_reads.entry(op.client_id).or_default().push(op);
        }

        for (client_id, reads) in client_reads {
            let mut last_value: Option<Vec<u8>> = None;

            for read in reads {
                if let OpResult::Ok(Some(val)) = &read.result {
                    if let Some(prev) = &last_value {
                        // Simple comparison: if values are numeric, check ordering
                        if let (Ok(prev_num), Ok(curr_num)) =
                            (parse_u64_from_bytes(prev), parse_u64_from_bytes(val))
                        {
                            if curr_num < prev_num {
                                return InvariantResult::Failed {
                                    reason: format!(
                                        "Client {} saw non-monotonic values: {} -> {}",
                                        client_id, prev_num, curr_num
                                    ),
                                };
                            }
                        }
                    }
                    last_value = Some(val.clone());
                }
            }
        }

        InvariantResult::Passed
    }

    /// Clear history
    pub fn clear(&self) {
        self.ops.lock().unwrap().clear();
    }

    /// Get operation count
    pub fn len(&self) -> usize {
        self.ops.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.lock().unwrap().is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinearizabilityResult {
    Linearizable,
    NotLinearizable { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvariantResult {
    Passed,
    Failed { reason: String },
}

/// Helper to parse u64 from bytes (for counter tests)
fn parse_u64_from_bytes(bytes: &[u8]) -> Result<u64, ()> {
    if bytes.len() == 8 {
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    } else if let Ok(s) = std::str::from_utf8(bytes) {
        s.parse().map_err(|_| ())
    } else {
        Err(())
    }
}

/// Bank accounts backed by real storage, for transfer-invariant tests.
///
/// An earlier version held `Arc<Mutex<HashMap<String, i64>>>` and did all its work in
/// process memory. `transfer` mutated that map and `check_total_invariant` summed it, so
/// the test asserted that a mutex-guarded HashMap conserves a total — a property of
/// `std::sync::Mutex`, not of PrkDB. It exercised no storage, no transaction, and no
/// isolation level while being filed as a consistency test.
///
/// Every balance now lives in the storage adapter, transfers run inside a `Serializable`
/// transaction, and the invariant is computed by reading balances back out.
#[derive(Clone)]
pub struct BankAccounts {
    storage: Arc<WalStorageAdapter>,
    names: Vec<String>,
    initial_total: i64,
}

impl BankAccounts {
    /// Create `num_accounts` accounts, each holding `initial_balance`, persisted.
    pub async fn new(
        storage: Arc<WalStorageAdapter>,
        num_accounts: usize,
        initial_balance: i64,
    ) -> Self {
        let names: Vec<String> = (0..num_accounts).map(|i| format!("account_{i}")).collect();

        for name in &names {
            storage
                .put(name.as_bytes(), &initial_balance.to_le_bytes())
                .await
                .expect("seeding an account into fresh storage cannot fail");
        }

        Self {
            storage,
            names,
            initial_total: initial_balance * num_accounts as i64,
        }
    }

    /// Move `amount` between two accounts inside one serializable transaction.
    ///
    /// Returns `Err` on insufficient funds or on a write conflict. Both are expected
    /// under contention and neither breaks the invariant — a rejected transfer must
    /// leave both balances untouched, which is exactly what the test checks.
    pub async fn transfer(&self, from: &str, to: &str, amount: i64) -> Result<(), String> {
        let config = TransactionConfig {
            isolation_level: IsolationLevel::Serializable,
            ..Default::default()
        };
        let mut tx = self.storage.begin_transaction_with_config(config);

        let from_balance = read_balance(&mut tx, from).await?;
        if from_balance < amount {
            tx.rollback();
            return Err("Insufficient funds".to_string());
        }
        let to_balance = read_balance(&mut tx, to).await?;

        tx.put(
            from.as_bytes().to_vec(),
            (from_balance - amount).to_le_bytes(),
        )
        .map_err(|e| e.to_string())?;
        tx.put(to.as_bytes().to_vec(), (to_balance + amount).to_le_bytes())
            .map_err(|e| e.to_string())?;

        tx.commit().await.map_err(|e| e.to_string()).map(|_| ())
    }

    /// Read a balance straight from storage, outside any transaction.
    pub async fn get_balance(&self, account: &str) -> Option<i64> {
        self.storage
            .get(account.as_bytes())
            .await
            .ok()
            .flatten()
            .and_then(|b| b.try_into().ok())
            .map(i64::from_le_bytes)
    }

    /// Sum every balance **as stored** and compare against the seeded total.
    ///
    /// Money is neither created nor destroyed by any interleaving of transfers.
    pub async fn check_total_invariant(&self) -> InvariantResult {
        let mut total = 0i64;
        for name in &self.names {
            match self.get_balance(name).await {
                Some(v) => total += v,
                None => {
                    return InvariantResult::Failed {
                        reason: format!("account {name} vanished from storage"),
                    }
                }
            }
        }

        if total == self.initial_total {
            InvariantResult::Passed
        } else {
            InvariantResult::Failed {
                reason: format!(
                    "total balance mismatch: expected {}, storage holds {}",
                    self.initial_total, total
                ),
            }
        }
    }

    pub fn account_names(&self) -> Vec<String> {
        self.names.clone()
    }
}

/// Read an account balance inside a transaction, treating "absent" as zero.
async fn read_balance(tx: &mut Transaction, account: &str) -> Result<i64, String> {
    let raw = tx
        .get(account.as_bytes())
        .await
        .map_err(|e| e.to_string())?
        .unwrap_or_else(|| 0i64.to_le_bytes().to_vec());

    raw.try_into()
        .map(i64::from_le_bytes)
        .map_err(|_| format!("balance for {account} is not an 8-byte integer"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_linearizability_simple() {
        let history = OperationHistory::new();
        let now = Instant::now();
        // Simulate a tiny time progression for causality
        let later = now + std::time::Duration::from_nanos(1);

        // Write "hello" - completes before read
        history.record(Operation {
            kind: OpKind::Write,
            key: b"key1".to_vec(),
            write_value: Some(b"hello".to_vec()),
            read_value: None,
            start_time: now,
            end_time: now,
            result: OpResult::Ok(None),
            client_id: 1,
        });

        // Read "hello" - starts after write (valid linearization)
        history.record(Operation {
            kind: OpKind::Read,
            key: b"key1".to_vec(),
            write_value: None,
            read_value: Some(b"hello".to_vec()),
            start_time: later,
            end_time: later,
            result: OpResult::Ok(Some(b"hello".to_vec())),
            client_id: 2,
        });

        assert_eq!(
            history.is_linearizable(),
            LinearizabilityResult::Linearizable
        );
    }

    /// The invariant holds across real transactions against real storage. Before
    /// Task 6 this asserted the same thing about an in-process HashMap, which told us
    /// nothing about the database.
    #[tokio::test(flavor = "multi_thread")]
    async fn bank_invariant_holds_over_stored_balances() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            prkdb::storage::WalStorageAdapter::new(prkdb_core::wal::WalConfig {
                log_dir: dir.path().to_path_buf(),
                ..prkdb_core::wal::WalConfig::test_config()
            })
            .unwrap(),
        );
        let bank = BankAccounts::new(storage, 5, 100).await;

        bank.transfer("account_0", "account_1", 50).await.unwrap();
        bank.transfer("account_1", "account_2", 25).await.unwrap();

        assert_eq!(bank.check_total_invariant().await, InvariantResult::Passed);
        assert_eq!(bank.get_balance("account_0").await, Some(50));
        assert_eq!(bank.get_balance("account_1").await, Some(125));
        assert_eq!(bank.get_balance("account_2").await, Some(125));
    }

    /// A transfer that cannot be funded must leave both balances untouched.
    #[tokio::test(flavor = "multi_thread")]
    async fn rejected_transfer_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            prkdb::storage::WalStorageAdapter::new(prkdb_core::wal::WalConfig {
                log_dir: dir.path().to_path_buf(),
                ..prkdb_core::wal::WalConfig::test_config()
            })
            .unwrap(),
        );
        let bank = BankAccounts::new(storage, 2, 100).await;

        bank.transfer("account_0", "account_1", 500)
            .await
            .expect_err("500 is more than the 100 seeded");

        assert_eq!(bank.get_balance("account_0").await, Some(100));
        assert_eq!(bank.get_balance("account_1").await, Some(100));
        assert_eq!(bank.check_total_invariant().await, InvariantResult::Passed);
    }

    // ── Meta-tests: these test the checker, not the database ──────────────────
    //
    // Every consistency result this module produces is worth exactly as much as the
    // checker's ability to return NotLinearizable. Before trusting a single green run,
    // prove it can go red.

    /// Build an operation with an explicit real-time interval.
    fn op(
        kind: OpKind,
        key: &[u8],
        write_value: Option<&[u8]>,
        read_value: Option<&[u8]>,
        start: Instant,
        end: Instant,
        client_id: u64,
    ) -> Operation {
        let kind_for_result = kind.clone();
        Operation {
            kind,
            key: key.to_vec(),
            write_value: write_value.map(|v| v.to_vec()),
            read_value: read_value.map(|v| v.to_vec()),
            start_time: start,
            end_time: end,
            result: match (&kind_for_result, read_value) {
                (OpKind::Read, Some(v)) => OpResult::Ok(Some(v.to_vec())),
                (OpKind::Read, None) => OpResult::Ok(None),
                _ => OpResult::Ok(None),
            },
            client_id,
        }
    }

    /// A stale read is THE canonical linearizability violation: a read that returns an
    /// old value after a newer write has already completed in real time.
    ///
    /// W1 writes "v1" and completes at t1.
    /// W2 writes "v2", starting at t2 and completing at t3 — strictly after W1.
    /// R1 runs entirely at t4..t5, after W2 completed, and returns "v1".
    ///
    /// There is no total order consistent with real time that explains this: W2 must
    /// precede R1, so R1 must observe "v2". A checker that calls this linearizable
    /// cannot detect the violation it exists to detect.
    #[test]
    fn detects_stale_read_after_completed_write() {
        let history = OperationHistory::new();
        let t = |ms: u64| Instant::now() + Duration::from_millis(ms);
        let (t0, t1, t2, t3, t4, t5) = (t(0), t(10), t(20), t(30), t(40), t(50));

        history.record(op(OpKind::Write, b"k", Some(b"v1"), None, t0, t1, 1));
        history.record(op(OpKind::Write, b"k", Some(b"v2"), None, t2, t3, 1));
        history.record(op(OpKind::Read, b"k", None, Some(b"v1"), t4, t5, 2));

        match history.is_linearizable() {
            LinearizabilityResult::NotLinearizable { .. } => {}
            LinearizabilityResult::Linearizable => panic!(
                "checker reported a stale read as linearizable.\n\
                 W1=v1 completed at t1, W2=v2 completed at t3, R1 ran at t4..t5 and \
                 returned v1.\n\
                 No real-time-consistent order explains that, so the checker cannot \
                 detect violations."
            ),
        }
    }

    /// The opposite failure: a checker that rejects everything is as useless as one that
    /// accepts everything. A read overlapping an in-flight write may legitimately return
    /// either value — concurrency is not a violation.
    #[test]
    fn accepts_concurrent_read_returning_either_value() {
        let t = |ms: u64| Instant::now() + Duration::from_millis(ms);

        for observed in [b"v1".as_slice(), b"v2".as_slice()] {
            let history = OperationHistory::new();
            // W1 completes at t1. W2 runs t2..t5, overlapping R1 at t3..t4.
            history.record(op(OpKind::Write, b"k", Some(b"v1"), None, t(0), t(10), 1));
            history.record(op(OpKind::Write, b"k", Some(b"v2"), None, t(20), t(50), 1));
            history.record(op(
                OpKind::Read,
                b"k",
                None,
                Some(observed),
                t(30),
                t(40),
                2,
            ));

            assert_eq!(
                history.is_linearizable(),
                LinearizabilityResult::Linearizable,
                "a read overlapping an in-flight write may return {:?}; \
                 rejecting it makes the checker useless in the other direction",
                String::from_utf8_lossy(observed),
            );
        }
    }

    /// A read of a value nothing ever wrote is a violation regardless of timing.
    #[test]
    fn detects_read_of_never_written_value() {
        let history = OperationHistory::new();
        let t = |ms: u64| Instant::now() + Duration::from_millis(ms);

        history.record(op(OpKind::Write, b"k", Some(b"v1"), None, t(0), t(10), 1));
        history.record(op(
            OpKind::Read,
            b"k",
            None,
            Some(b"ghost"),
            t(20),
            t(30),
            2,
        ));

        assert!(
            matches!(
                history.is_linearizable(),
                LinearizabilityResult::NotLinearizable { .. }
            ),
            "reading a value no write ever produced must be rejected"
        );
    }
}
