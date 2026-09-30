//! Runs op sequences against a fresh SUT, checks after every restart op, and
//! shrinks failing sequences.
//!
//! SUT failures (a `put`/`delete`/`checkpoint` erroring, a `reopen`/`crash`
//! failing to come back up, or a post-restart `get` erroring or disagreeing
//! with the model) are *findings*: they're reported in [`Outcome`], not
//! propagated as `Err`. Only genuine harness bugs (e.g. `tempfile::tempdir()`
//! failing) are allowed to surface as `anyhow::Result::Err` — and so is a SUT
//! returning [`Unsupported`], which means the profile asked for an op this SUT
//! cannot do: a misconfigured run, not a bug in the SUT.

use crate::checker::{check_durable, CheckOutcome, Mismatch};
use crate::model::{Mode, Model, Value};
use crate::ops::{generate, Op, Profile, OP_KIND_NAMES};
use crate::sut::{Sut, Unsupported};
use std::collections::BTreeMap;
use std::future::Future;

/// Executed ops per kind name (see [`Op::kind_name`]).
pub type OpCounts = BTreeMap<&'static str, u64>;

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

/// The presence shape of an acceptable set: (some candidate is "absent",
/// some candidate is "present"). Compared as a set so that a shrink which
/// changes how many prefixes were pending (and thus how many candidates there
/// are) is still recognized as the same bug.
fn acceptable_shape(acceptable: &[Option<Value>]) -> (bool, bool) {
    (
        acceptable.iter().any(Option::is_none),
        acceptable.iter().any(Option::is_some),
    )
}

impl Outcome {
    /// True if `self` and `other` are the same kind of finding: for
    /// `Mismatch`, that also requires the same key AND the same shape (was
    /// the key present in the model vs. the SUT, before vs. after, and the
    /// same presence shape of the acceptable set) — a missing-vs-present
    /// mismatch is a different bug from a present-vs-different-value one,
    /// even on the same key. For `SutError`, that also requires the same op
    /// discriminant (a `Put` failing is a different bug from a `Reopen`
    /// failing, even though both are `SutError`s). Used by the minimizer to
    /// confirm a shrunk sequence still reproduces the *same* bug rather than
    /// a different one.
    pub fn same_kind(&self, other: &Outcome) -> bool {
        match (self, other) {
            (Outcome::Mismatch { mismatch: a, .. }, Outcome::Mismatch { mismatch: b, .. }) => {
                a.key == b.key
                    && (a.expected.is_some(), a.actual.is_some())
                        == (b.expected.is_some(), b.actual.is_some())
                    && acceptable_shape(&a.acceptable) == acceptable_shape(&b.acceptable)
            }
            (Outcome::SutError { op: a, .. }, Outcome::SutError { op: b, .. }) => {
                std::mem::discriminant(a) == std::mem::discriminant(b)
            }
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
    /// original failure found by [`run`]; carries the op index and, for
    /// a mismatch, the preceding restart op (see [`Outcome`]).
    pub outcome: Outcome,
    /// The minimized op sequence that still reproduces `outcome`.
    pub ops: Vec<Op>,
    /// Length of the original, un-minimized sequence (for reporting the
    /// shrink ratio).
    pub original_len: usize,
}

/// Summary of a [`run`].
#[derive(Debug, Default)]
pub struct Report {
    pub seeds: u64,
    /// Total number of key comparisons performed across every check, summed
    /// over every seed that passed. A `Report` with `checks == 0` after at
    /// least one seed ran is a vacuous run and should not be treated as a
    /// green result.
    pub checks: usize,
    /// Ops executed per kind across every seed run (not counting the
    /// minimizer's re-runs), including each sequence's implicit trailing
    /// reopen as `"Reopen"`. A kind the profile enables but that shows 0 here
    /// was never exercised, so a green run says nothing about it.
    pub op_counts: OpCounts,
    pub failure: Option<Failure>,
}

impl Report {
    /// Op kinds `profile` enables that never executed in this run. Non-empty
    /// means the run is vacuous for those kinds.
    pub fn missing_op_kinds(&self, profile: Profile) -> Vec<&'static str> {
        profile
            .op_kinds()
            .into_iter()
            .filter(|kind| self.op_counts.get(kind).copied().unwrap_or(0) == 0)
            .collect()
    }

    /// `op_counts` as `Put:12,Delete:4,…`, in canonical kind order.
    pub fn format_op_counts(&self) -> String {
        OP_KIND_NAMES
            .iter()
            .filter_map(|kind| self.op_counts.get(kind).map(|n| format!("{kind}:{n}")))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Everything a [`run`] needs besides the SUT factory.
#[derive(Debug, Clone)]
pub struct RunConfig {
    pub first_seed: u64,
    pub seeds: u64,
    /// Ops per generated sequence.
    pub ops: usize,
    pub profile: Profile,
    pub mode: Mode,
    /// Consecutive reproductions of the same finding the minimizer requires
    /// before accepting a shrink (values below 1 are treated as 1).
    pub repro_attempts: usize,
}

/// Turns a SUT error from executing `op` into either a finding or, if the SUT
/// reported the op [`Unsupported`], a harness error.
fn sut_failure(e: anyhow::Error, idx: usize, op: &Op) -> anyhow::Result<Outcome> {
    if let Some(unsupported) = e.downcast_ref::<Unsupported>() {
        anyhow::bail!(
            "profile mismatch at op {idx} ({}): {unsupported}; choose a profile this SUT implements",
            op.kind_name()
        );
    }
    Ok(Outcome::SutError {
        at: idx,
        op: op.clone(),
        error: e.to_string(),
    })
}

/// Runs `restart` (a `Reopen` or `Crash` op) and, if it succeeds, checks the
/// SUT against `model`. Returns `Ok(Ok(compared))` on success, `Ok(Err(..))`
/// with the finding on failure, and `Err` for a harness error. `idx` is the
/// op's index in the sequence (or `ops.len()` for the implicit trailing
/// reopen), used to label findings.
async fn restart_and_check(
    sut: &mut dyn Sut,
    restart: &Op,
    model: &Model,
    idx: usize,
) -> anyhow::Result<Result<usize, Box<Outcome>>> {
    let result = match restart {
        Op::Reopen => sut.reopen().await,
        Op::Crash => sut.crash().await,
        other => unreachable!("restart_and_check called with non-restart op {other:?}"),
    };
    if let Err(e) = result {
        return Ok(Err(Box::new(sut_failure(e, idx, restart)?)));
    }
    Ok(match check_durable(model, sut).await {
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
    })
}

/// Runs `ops` against `sut`, checking after every restart (and once more
/// after an implicit trailing reopen, so every sequence verifies at least
/// once). Returns the resulting [`Outcome`] — SUT/model failures are findings
/// carried in `Ok`, not `Err`. `Err` is reserved for harness errors,
/// including a SUT reporting an op [`Unsupported`].
pub async fn run_ops(sut: &mut dyn Sut, ops: &[Op], mode: Mode) -> anyhow::Result<Outcome> {
    run_ops_counted(sut, ops, mode, &mut OpCounts::new()).await
}

/// [`run_ops`], adding every executed op (including the implicit trailing
/// reopen) to `counts`.
async fn run_ops_counted(
    sut: &mut dyn Sut,
    ops: &[Op],
    mode: Mode,
    counts: &mut OpCounts,
) -> anyhow::Result<Outcome> {
    let mut model = Model::default();
    let mut checks = 0;

    for (idx, op) in ops.iter().enumerate() {
        *counts.entry(op.kind_name()).or_default() += 1;
        match op {
            // In Durable mode every ack is durable, so the full state stays
            // the only candidate: acked mutations simply join `pending`.
            Op::Put(k, v) => match sut.put(k, v).await {
                Ok(()) => model.put(k.clone(), v.clone()),
                Err(e) => return sut_failure(e, idx, op),
            },
            Op::Delete(k) => match sut.delete(k).await {
                Ok(()) => model.delete(k),
                Err(e) => return sut_failure(e, idx, op),
            },
            Op::Checkpoint => match sut.checkpoint().await {
                Ok(()) => model.mark_durable(),
                Err(e) => return sut_failure(e, idx, op),
            },
            Op::Reopen | Op::Crash => match restart_and_check(sut, op, &model, idx).await? {
                Ok(n) => {
                    checks += n;
                    // A clean reopen flushes everything. A process exit loses
                    // nothing that was written, but in Fast mode unsynced
                    // writes are still not durable against a later power loss.
                    if matches!(op, Op::Reopen) || mode == Mode::Durable {
                        model.mark_durable();
                    }
                }
                Err(outcome) => return Ok(*outcome),
            },
        }
    }

    *counts.entry(Op::Reopen.kind_name()).or_default() += 1;
    match restart_and_check(sut, &Op::Reopen, &model, ops.len()).await? {
        Ok(n) => Ok(Outcome::Pass { checks: checks + n }),
        Err(outcome) => Ok(*outcome),
    }
}

/// Runs `seeds` seeded sequences (each `len` ops long) in Durable mode against
/// fresh SUTs, stopping at the first finding and minimizing it. Equivalent to
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
    let config = RunConfig {
        first_seed,
        seeds,
        ops: len,
        profile,
        mode: Mode::Durable,
        repro_attempts,
    };
    run(make, &config).await
}

/// Runs `config.seeds` seeded sequences against fresh SUTs, stopping at the
/// first finding and minimizing it.
pub async fn run<F, Fut, S>(make: F, config: &RunConfig) -> anyhow::Result<Report>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<S>>,
    S: Sut,
{
    if config.mode == Mode::Fast {
        anyhow::bail!("Fast mode arrives with PowerLoss in Task 2.10b");
    }
    let mut report = Report::default();
    let end = config.first_seed.saturating_add(config.seeds);
    for seed in config.first_seed..end {
        let ops = generate(seed, config.ops, config.profile);
        let mut sut = make().await?;
        report.seeds += 1;
        match run_ops_counted(&mut sut, &ops, config.mode, &mut report.op_counts).await? {
            Outcome::Pass { checks } => report.checks += checks,
            outcome => {
                let original_len = ops.len();
                let attempts = config.repro_attempts.max(1);
                let (ops, outcome) = minimize(&make, ops, outcome, config.mode, attempts).await?;
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
    mode: Mode,
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
        match reproduces(make, &candidate, &best_outcome, mode, repro_attempts).await {
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
    mode: Mode,
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
        match run_ops(&mut sut, ops, mode).await {
            Ok(outcome) if outcome.same_kind(original) => last = Some(outcome),
            _ => return None,
        }
    }
    last
}
