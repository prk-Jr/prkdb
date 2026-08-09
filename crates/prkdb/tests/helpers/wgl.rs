//! Wing & Gong linearizability checker.
//!
//! Replaces a "value provenance" check that could not fail on a stale read — it only
//! asked whether *some* write of the same value to the same key had started before the
//! read ended, which any earlier write trivially satisfies.
//!
//! # What linearizability means here
//!
//! A history is linearizable if every operation can be assigned a single instant — its
//! *linearization point* — inside its own `[start_time, end_time]` interval, such that
//! executing the operations in that order satisfies the sequential specification of the
//! object. For a register, that specification is: a read returns the value of the most
//! recent write.
//!
//! Two consequences worth stating, because the old checker got both wrong:
//!
//! - A read that overlaps an in-flight write may return **either** the old or the new
//!   value. Concurrency is not a violation.
//! - A read that returns an old value *after* a newer write has already completed in
//!   real time **is** a violation, because no valid assignment of linearization points
//!   can place that write after that read.
//!
//! # Algorithm
//!
//! Wing & Gong linear search with memoization. At each step, consider every operation
//! that could legally come next — those whose interval starts no later than the earliest
//! interval end among the remaining operations — apply it to the model state, and recurse.
//! Backtrack when a branch dead-ends.
//!
//! The search is exponential in the worst case, so histories are bounded by
//! [`MAX_CHECKABLE_OPS`]. A checker that hangs is a checker that gets deleted.
//!
//! Linearizability is composable per object, so histories are partitioned by key and each
//! key checked independently. That is both correct and a large practical speedup.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use super::jepsen_checker::{OpKind, OpResult, Operation};

/// Upper bound on operations per key. The search is exponential in the worst case;
/// beyond this the checker would hang rather than answer.
pub const MAX_CHECKABLE_OPS: usize = 200;

/// The model state of a single register: the value it currently holds.
type State = Option<Vec<u8>>;

/// One operation reduced to what the search needs.
#[derive(Clone, Debug)]
struct Entry {
    id: usize,
    start: Instant,
    end: Instant,
    /// `Write(value)` or `Read(observed)`. `Read(None)` is a read that saw nothing.
    action: Action,
    /// A timed-out or errored write may or may not have taken effect. The search must
    /// try both, otherwise it reports violations that are really just uncertainty.
    indeterminate: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Action {
    Write(Vec<u8>),
    Read(State),
}

/// Why a history could not be linearized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Linearizable,
    NotLinearizable {
        reason: String,
    },
    /// The history exceeded [`MAX_CHECKABLE_OPS`]. Not a pass — an admission that the
    /// question was not answered.
    TooLarge {
        key: Vec<u8>,
        ops: usize,
    },
}

/// Check a recorded history for linearizability, one key at a time.
pub fn check(ops: &[Operation]) -> Verdict {
    let mut by_key: HashMap<Vec<u8>, Vec<&Operation>> = HashMap::new();
    for op in ops {
        by_key.entry(op.key.clone()).or_default().push(op);
    }

    for (key, key_ops) in by_key {
        if key_ops.len() > MAX_CHECKABLE_OPS {
            return Verdict::TooLarge {
                key,
                ops: key_ops.len(),
            };
        }
        if let Some(reason) = check_one_key(&key, &key_ops) {
            return Verdict::NotLinearizable { reason };
        }
    }
    Verdict::Linearizable
}

/// Returns `Some(reason)` when this key's sub-history cannot be linearized.
fn check_one_key(key: &[u8], ops: &[&Operation]) -> Option<String> {
    let mut entries = Vec::new();
    for (id, op) in ops.iter().enumerate() {
        let indeterminate = matches!(op.result, OpResult::Err(_) | OpResult::Timeout);
        let action = match op.kind {
            OpKind::Write | OpKind::CasSuccess => match &op.write_value {
                Some(v) => Action::Write(v.clone()),
                // A write with no value carries no information about the register.
                None => continue,
            },
            OpKind::Read => match &op.result {
                OpResult::Ok(v) => Action::Read(v.clone()),
                // A read that errored or timed out observed nothing we can constrain.
                _ => continue,
            },
            // Transaction markers and failed CAS attempts do not change the register.
            _ => continue,
        };
        entries.push(Entry {
            id,
            start: op.start_time,
            end: op.end_time,
            action,
            indeterminate,
        });
    }

    if entries.is_empty() {
        return None;
    }

    let mut memo: HashSet<(Vec<usize>, State)> = HashSet::new();
    let remaining: Vec<usize> = (0..entries.len()).collect();

    if search(&entries, &remaining, &None, &mut memo) {
        None
    } else {
        Some(describe_failure(key, &entries))
    }
}

/// Wing & Gong linear search: try every operation that could legally come next.
fn search(
    entries: &[Entry],
    remaining: &[usize],
    state: &State,
    memo: &mut HashSet<(Vec<usize>, State)>,
) -> bool {
    if remaining.is_empty() {
        return true;
    }

    let key = (remaining.to_vec(), state.clone());
    if !memo.insert(key) {
        // This exact (remaining set, state) pair already failed.
        return false;
    }

    // An operation may only be linearized next if it starts no later than the earliest
    // end among the remaining ones — otherwise some operation would have to be ordered
    // after an operation that had already finished before it began.
    let earliest_end = remaining
        .iter()
        .map(|&i| entries[i].end)
        .min()
        .expect("remaining is non-empty");

    for (pos, &idx) in remaining.iter().enumerate() {
        let entry = &entries[idx];
        if entry.start > earliest_end {
            continue;
        }

        let mut rest = remaining.to_vec();
        rest.remove(pos);

        match &entry.action {
            Action::Write(v) => {
                // The write took effect.
                if search(entries, &rest, &Some(v.clone()), memo) {
                    return true;
                }
                // An indeterminate write may equally not have taken effect.
                if entry.indeterminate && search(entries, &rest, state, memo) {
                    return true;
                }
            }
            Action::Read(observed) => {
                if observed == state && search(entries, &rest, state, memo) {
                    return true;
                }
            }
        }
    }

    false
}

/// Build a message naming the most likely culprit: a read whose observed value no write
/// completing before it could have produced.
fn describe_failure(key: &[u8], entries: &[Entry]) -> String {
    let key_str = String::from_utf8_lossy(key);

    for entry in entries {
        let Action::Read(Some(observed)) = &entry.action else {
            continue;
        };
        // Was some other value written by an operation that completed before this read
        // began? If so, the read is stale.
        let superseded = entries
            .iter()
            .any(|w| matches!(&w.action, Action::Write(v) if v != observed) && w.end < entry.start);
        let produced_ever = entries
            .iter()
            .any(|w| matches!(&w.action, Action::Write(v) if v == observed));

        if !produced_ever {
            return format!(
                "key {key_str:?}: read returned {:?}, which no write ever produced",
                String::from_utf8_lossy(observed)
            );
        }
        if superseded {
            let newer: Vec<String> = entries
                .iter()
                .filter(|w| {
                    matches!(&w.action, Action::Write(v) if v != observed) && w.end < entry.start
                })
                .filter_map(|w| match &w.action {
                    Action::Write(v) => Some(String::from_utf8_lossy(v).into_owned()),
                    _ => None,
                })
                .collect();
            return format!(
                "key {key_str:?}: stale read returned {:?} after write(s) {:?} had already \
                 completed in real time — no ordering of linearization points explains it",
                String::from_utf8_lossy(observed),
                newer
            );
        }
    }

    format!("key {key_str:?}: no assignment of linearization points satisfies the history")
}
