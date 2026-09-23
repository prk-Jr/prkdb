//! Runs op sequences against a fresh SUT, checks after every restart op, and
//! shrinks failing sequences.
//!
//! SUT failures (a `put`/`delete`/`checkpoint` erroring, a `reopen`/`crash`
//! failing to come back up, or a post-restart `get` erroring or disagreeing
//! with the model) are *findings*: they're reported in [`Outcome`], not
//! propagated as `Err`. Only genuine harness bugs (e.g. `tempfile::tempdir()`
//! failing) are allowed to surface as `anyhow::Result::Err`.

use crate::checker::{check_durable, CheckOutcome, Mismatch};
use crate::model::Model;
use crate::ops::{generate, Op, Profile};
use crate::sut::Sut;
use std::future::Future;

/// What happened when running a sequence of ops against a SUT.
#[derive(Debug)]
pub enum Outcome {
    /// The whole sequence ran clean. `checks` is the total number of key
    /// comparisons performed across every restart-triggered check (see
    /// [`Report::checks`]).
    Pass { checks: usize },
    /// A post-restart check found a value that disagreed with the model.
    Mismatch {
        /// Index into the op sequence of the restart (`Reopen`/`Crash`) whose
        /// check produced this mismatch, or `ops.len()` for the implicit
        /// trailing reopen every sequence gets.
        at: usize,
        /// The restart op that preceded (and triggered) this check.
        after: Op,
        mismatch: Mismatch,
    },
    /// A SUT call itself returned an error: a put/delete/checkpoint failed, a
    /// reopen/crash failed to bring the SUT back up, or a post-restart get
    /// errored.
    SutError {
        /// Index into the op sequence of the op that errored, or `ops.len()`
        /// for the implicit trailing reopen.
        at: usize,
        /// The op that errored.
        op: Op,
        error: String,
    },
}

impl Outcome {
    /// True if `self` and `other` are the same kind of finding: for
    /// `Mismatch`, that also requires the same key. Used by the minimizer to
    /// confirm a shrunk sequence still reproduces the *same* bug rather than
    /// a different one.
    pub fn same_kind(&self, other: &Outcome) -> bool {
        match (self, other) {
            (Outcome::Mismatch { mismatch: a, .. }, Outcome::Mismatch { mismatch: b, .. }) => {
                a.key == b.key
            }
            (Outcome::SutError { .. }, Outcome::SutError { .. }) => true,
            _ => false,
        }
    }

    pub fn is_pass(&self) -> bool {
        matches!(self, Outcome::Pass { .. })
    }
}

/// A minimized failing sequence.
#[derive(Debug)]
pub struct Failure {
    pub seed: u64,
    /// The finding this (minimized) sequence reproduces. Same kind as the
    /// original failure found by [`run_seeds`]; carries the op index and, for
    /// a mismatch, the preceding restart op (see [`Outcome`]).
    pub outcome: Outcome,
    /// The minimized op sequence that still reproduces `outcome`.
    pub ops: Vec<Op>,
    /// Length of the original, un-minimized sequence (for reporting the
    /// shrink ratio).
    pub original_len: usize,
}

/// Summary of a `run_seeds` run.
#[derive(Debug, Default)]
pub struct Report {
    pub seeds: u64,
    /// Total number of key comparisons performed across every check, summed
    /// over every seed that passed. A `Report` with `checks == 0` after at
    /// least one seed ran is a vacuous run and should not be treated as a
    /// green result.
    pub checks: usize,
    pub failure: Option<Failure>,
}

/// Runs `restart` (a `Reopen` or `Crash` op) and, if it succeeds, checks the
/// SUT against `model`. Returns the number of keys compared on success, or
/// the `Outcome` finding on failure. `idx` is the op's index in the sequence
/// (or `ops.len()` for the implicit trailing reopen), used to label findings.
async fn restart_and_check(
    sut: &mut dyn Sut,
    restart: &Op,
    model: &Model,
    idx: usize,
) -> Result<usize, Box<Outcome>> {
    let result = match restart {
        Op::Reopen => sut.reopen().await,
        Op::Crash => sut.crash().await,
        other => unreachable!("restart_and_check called with non-restart op {other:?}"),
    };
    if let Err(e) = result {
        return Err(Box::new(Outcome::SutError {
            at: idx,
            op: restart.clone(),
            error: e.to_string(),
        }));
    }
    match check_durable(model, sut).await {
        CheckOutcome::Ok { compared } => Ok(compared),
        CheckOutcome::Mismatch { mismatch, .. } => Err(Box::new(Outcome::Mismatch {
            at: idx,
            after: restart.clone(),
            mismatch,
        })),
        CheckOutcome::SutError { error, .. } => Err(Box::new(Outcome::SutError {
            at: idx,
            op: restart.clone(),
            error,
        })),
    }
}

/// Runs `ops` against `sut`, checking after every restart (and once more
/// after an implicit trailing reopen, so every sequence verifies at least
/// once). Returns the resulting [`Outcome`] — SUT/model failures are findings
/// carried in `Ok`, not `Err`. `Err` is reserved for genuine harness bugs.
pub async fn run_ops(sut: &mut dyn Sut, ops: &[Op]) -> anyhow::Result<Outcome> {
    let mut model = Model::default();
    let mut checks = 0;

    for (idx, op) in ops.iter().enumerate() {
        match op {
            Op::Put(k, v) => match sut.put(k, v).await {
                Ok(()) => model.put(k.clone(), v.clone()),
                Err(e) => {
                    return Ok(Outcome::SutError {
                        at: idx,
                        op: op.clone(),
                        error: e.to_string(),
                    })
                }
            },
            Op::Delete(k) => match sut.delete(k).await {
                Ok(()) => model.delete(k),
                Err(e) => {
                    return Ok(Outcome::SutError {
                        at: idx,
                        op: op.clone(),
                        error: e.to_string(),
                    })
                }
            },
            Op::Checkpoint => {
                if let Err(e) = sut.checkpoint().await {
                    return Ok(Outcome::SutError {
                        at: idx,
                        op: op.clone(),
                        error: e.to_string(),
                    });
                }
            }
            Op::Reopen | Op::Crash => match restart_and_check(sut, op, &model, idx).await {
                Ok(n) => checks += n,
                Err(outcome) => return Ok(*outcome),
            },
        }
    }

    match restart_and_check(sut, &Op::Reopen, &model, ops.len()).await {
        Ok(n) => Ok(Outcome::Pass { checks: checks + n }),
        Err(outcome) => Ok(*outcome),
    }
}

/// Runs `seeds` seeded sequences (each `len` ops long) against fresh SUTs,
/// stopping at the first finding and minimizing it. Equivalent to
/// `run_seeds_with(.., repro_attempts: 1)`.
pub async fn run_seeds<F, Fut, S>(
    make: F,
    first_seed: u64,
    seeds: u64,
    len: usize,
    profile: Profile,
) -> anyhow::Result<Report>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<S>>,
    S: Sut,
{
    run_seeds_with(make, first_seed, seeds, len, profile, 1).await
}

/// Like [`run_seeds`], but requires `repro_attempts` consecutive
/// reproductions of the same finding before the minimizer accepts a shrink —
/// useful when the SUT (or fault injection under it) is nondeterministic.
pub async fn run_seeds_with<F, Fut, S>(
    make: F,
    first_seed: u64,
    seeds: u64,
    len: usize,
    profile: Profile,
    repro_attempts: usize,
) -> anyhow::Result<Report>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<S>>,
    S: Sut,
{
    let mut report = Report::default();
    for seed in first_seed..first_seed.saturating_add(seeds) {
        let ops = generate(seed, len, profile);
        let mut sut = make().await?;
        report.seeds += 1;
        match run_ops(&mut sut, &ops).await? {
            Outcome::Pass { checks } => report.checks += checks,
            outcome => {
                let original_len = ops.len();
                let (ops, outcome) = minimize(&make, ops, outcome, repro_attempts.max(1)).await?;
                report.failure = Some(Failure {
                    seed,
                    outcome,
                    ops,
                    original_len,
                });
                return Ok(report);
            }
        }
    }
    Ok(report)
}

/// Greedy one-at-a-time deletion: keep removing ops while the *same* failure
/// (same `Outcome` kind, same key for a mismatch) still reproduces. A
/// candidate that passes, fails differently, or can't even be run (a harness
/// error constructing a fresh SUT) is treated as "not reproduced" and the op
/// is kept. Never re-runs just to rebuild the result: the outcome from the
/// last accepted (or, if none was ever accepted, the original) reproduction
/// is what gets returned.
async fn minimize<F, Fut, S>(
    make: &F,
    ops: Vec<Op>,
    outcome: Outcome,
    repro_attempts: usize,
) -> anyhow::Result<(Vec<Op>, Outcome)>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<S>>,
    S: Sut,
{
    let mut best_ops = ops;
    let mut best_outcome = outcome;
    let mut i = 0;
    while i < best_ops.len() {
        let mut candidate = best_ops.clone();
        candidate.remove(i);
        match reproduces(make, &candidate, &best_outcome, repro_attempts).await {
            Some(reproduced) => {
                best_ops = candidate;
                best_outcome = reproduced;
                // Don't advance `i`: retry shrinking at the same position.
            }
            None => i += 1,
        }
    }
    Ok((best_ops, best_outcome))
}

/// Runs `ops` up to `attempts` times, requiring every attempt to reproduce
/// the same failure kind as `original`. Returns the last reproduction's
/// `Outcome` on success (all `attempts` reproduced), or `None` if any attempt
/// passed, failed differently, or couldn't be run at all.
async fn reproduces<F, Fut, S>(
    make: &F,
    ops: &[Op],
    original: &Outcome,
    attempts: usize,
) -> Option<Outcome>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<S>>,
    S: Sut,
{
    let mut last = None;
    for _ in 0..attempts {
        let mut sut = make().await.ok()?;
        match run_ops(&mut sut, ops).await {
            Ok(outcome) if outcome.same_kind(original) => last = Some(outcome),
            _ => return None,
        }
    }
    last
}
