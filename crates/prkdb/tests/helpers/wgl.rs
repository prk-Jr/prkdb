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

/// The operations as the checker saw them, in the order they began.
///
/// Printed on every failure because the heuristic above is a guess and this is not. A
/// linearizability failure that reaches CI once and never reproduces — which is exactly
/// what happened here — leaves nothing to work from but the message, and a message that
/// names the wrong operation is worse than no message. Timestamps are microseconds from
/// the first operation, which is what makes the real-time ordering constraints readable:
/// two operations that overlap can be linearized in either order, two that do not cannot.
fn render_history(entries: &[Entry]) -> String {
    let Some(origin) = entries.iter().map(|e| e.start).min() else {
        return String::from("  (no operations)");
    };

    let mut ordered: Vec<&Entry> = entries.iter().collect();
    ordered.sort_by_key(|e| (e.start, e.end));

    let mut out = String::from("  history (microseconds from the first operation):\n");
    for e in ordered {
        let what = match &e.action {
            Action::Write(v) => format!("write({})", String::from_utf8_lossy(v)),
            Action::Read(Some(v)) => format!("read -> {}", String::from_utf8_lossy(v)),
            Action::Read(None) => "read -> nothing".to_string(),
        };
        out.push_str(&format!(
            "    #{:<3} [{:>8} .. {:>8}] {}{}\n",
            e.id,
            e.start.duration_since(origin).as_micros(),
            e.end.duration_since(origin).as_micros(),
            what,
            if e.indeterminate {
                "  (indeterminate: may or may not have taken effect)"
            } else {
                ""
            }
        ));
    }
    out
}

/// Explain why this key's sub-history could not be linearized, and show the history.
///
/// Two jobs, because the first one can only ever be a guess. The search knows a valid
/// order does not exist; it does not know which operation is to blame, and naming one is a
/// heuristic. So the heuristic is stated conservatively and the history is printed
/// underneath it, which is the part that survives being wrong.
///
/// The previous version called a read stale whenever *any* write of a different value had
/// completed before it began. On this file's own workload — write v0, read, write v1,
/// read — that is true of every read after the first: the read of `v1` is preceded by the
/// completed write of `v0`. So it reported a correct read as the culprit, stopped there,
/// and hid whichever operation actually broke the history. It fired on the one CI failure
/// this checker has ever produced, and sent the investigation at the wrong operation.
///
/// A read is only *definitely* stale when the value it returned had already been
/// superseded in real time: some other write both completed before the read began and
/// began after every write that could have produced the observed value had finished. Then
/// no ordering can put the observed write last, and the read cannot be explained.
fn describe_failure(key: &[u8], entries: &[Entry]) -> String {
    let key_str = String::from_utf8_lossy(key);
    let history = render_history(entries);

    for entry in entries {
        let Action::Read(Some(observed)) = &entry.action else {
            continue;
        };

        let producers: Vec<&Entry> = entries
            .iter()
            .filter(|w| matches!(&w.action, Action::Write(v) if v == observed))
            .collect();

        if producers.is_empty() {
            return format!(
                "key {key_str:?}: read returned {:?}, which no write ever produced\n{history}",
                String::from_utf8_lossy(observed)
            );
        }

        // Writes that finished before this read started *and* started after every possible
        // producer of the observed value finished — so they are unambiguously later.
        let superseding: Vec<String> = entries
            .iter()
            .filter(|w| matches!(&w.action, Action::Write(v) if v != observed))
            .filter(|w| w.end < entry.start && producers.iter().all(|p| p.end < w.start))
            .filter_map(|w| match &w.action {
                Action::Write(v) => Some(String::from_utf8_lossy(v).into_owned()),
                _ => None,
            })
            .collect();

        if !superseding.is_empty() {
            return format!(
                "key {key_str:?}: stale read returned {:?} after write(s) {:?} had already \
                 superseded it in real time — no ordering of linearization points explains \
                 it\n{history}",
                String::from_utf8_lossy(observed),
                superseding
            );
        }
    }

    format!(
        "key {key_str:?}: no assignment of linearization points satisfies the history\n{history}"
    )
}
