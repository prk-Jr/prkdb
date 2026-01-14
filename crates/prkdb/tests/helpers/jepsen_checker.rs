//! Jepsen-style Consistency Checker
//!
//! Provides operation history recording and linearizability verification
//! for distributed consistency testing. Inspired by the Jepsen framework.

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
    pub fn is_linearizable(&self) -> LinearizabilityResult {
        let ops = self.ops.lock().unwrap();

        // Extract writes and reads
        let writes: Vec<_> = ops.iter().filter(|op| op.kind == OpKind::Write).collect();

        let reads: Vec<_> = ops.iter().filter(|op| op.kind == OpKind::Read).collect();

        // For each read, check if the read value is valid
        for read in &reads {
            let read_val = match &read.result {
                OpResult::Ok(Some(v)) => v.clone(),
                OpResult::Ok(None) => continue, // nil read is valid if no writes
                _ => continue,                  // Errors don't violate linearizability
            };

            // Find writes that could have produced this value
            let valid_write = writes.iter().any(|w| {
                // Write must have the same key
                if w.key != read.key {
                    return false;
                }

                // Write value must match read value
                let write_val = match &w.write_value {
                    Some(v) => v,
                    None => return false,
                };

                if write_val != &read_val {
                    return false;
                }

                // Write must have started before read ended (could be concurrent)
                // and either:
                // 1. Write completed before read started (definitely before)
                // 2. Write overlaps with read (concurrent, linearization possible)
                w.start_time < read.end_time
            });

            if !valid_write && !read_val.is_empty() {
                return LinearizabilityResult::NotLinearizable {
                    reason: format!(
                        "Read of key {:?} returned {:?} but no valid write found",
                        String::from_utf8_lossy(&read.key),
                        String::from_utf8_lossy(&read_val)
                    ),
                };
            }
        }

        LinearizabilityResult::Linearizable
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

/// Bank account for transfer invariant tests
#[derive(Clone)]
pub struct BankAccounts {
    accounts: Arc<Mutex<HashMap<String, i64>>>,
    initial_total: i64,
}

impl BankAccounts {
    pub fn new(num_accounts: usize, initial_balance: i64) -> Self {
        let mut accounts = HashMap::new();
        for i in 0..num_accounts {
            accounts.insert(format!("account_{}", i), initial_balance);
        }
        let initial_total = initial_balance * num_accounts as i64;
        Self {
            accounts: Arc::new(Mutex::new(accounts)),
            initial_total,
        }
    }

    pub fn transfer(&self, from: &str, to: &str, amount: i64) -> Result<(), String> {
        let mut accounts = self.accounts.lock().unwrap();

        let from_balance = accounts
            .get(from)
            .copied()
            .ok_or("From account not found")?;
        if from_balance < amount {
            return Err("Insufficient funds".to_string());
        }

        *accounts.get_mut(from).unwrap() -= amount;
        *accounts.get_mut(to).unwrap() += amount;

        Ok(())
    }

    pub fn get_balance(&self, account: &str) -> Option<i64> {
        self.accounts.lock().unwrap().get(account).copied()
    }

    pub fn check_total_invariant(&self) -> InvariantResult {
        let accounts = self.accounts.lock().unwrap();
        let total: i64 = accounts.values().sum();

        if total == self.initial_total {
            InvariantResult::Passed
        } else {
            InvariantResult::Failed {
                reason: format!(
                    "Total balance mismatch: expected {}, got {}",
                    self.initial_total, total
                ),
            }
        }
    }

    pub fn account_names(&self) -> Vec<String> {
        self.accounts.lock().unwrap().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_bank_invariant() {
        let bank = BankAccounts::new(5, 100);

        // Transfer between accounts
        bank.transfer("account_0", "account_1", 50).unwrap();
        bank.transfer("account_1", "account_2", 25).unwrap();

        assert_eq!(bank.check_total_invariant(), InvariantResult::Passed);
    }
}
