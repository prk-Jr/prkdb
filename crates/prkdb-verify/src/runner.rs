//! Runs op sequences against a fresh SUT, checks after every restart op, and
//! shrinks failing sequences.

use crate::checker::{check_durable, Mismatch};
use crate::model::Model;
use crate::ops::{generate, Op, Profile};
use crate::sut::Sut;
use std::future::Future;

pub const KEY_SPACE: u8 = 16;

#[derive(Debug)]
pub struct Failure {
    pub seed: u64,
    pub ops: Vec<Op>,
    pub mismatch: Mismatch,
}

#[derive(Debug, Default)]
pub struct Report {
    pub seeds: u64,
    pub checks: usize,
    pub failure: Option<Failure>,
}

/// Returns Ok(checks) or the mismatch.
pub async fn run_ops(sut: &mut dyn Sut, ops: &[Op]) -> anyhow::Result<Result<usize, Mismatch>> {
    let mut model = Model::default();
    let mut checks = 0;
    for op in ops {
        match op {
            Op::Put(k, v) => {
                sut.put(k, v).await?;
                model.put(k.clone(), v.clone());
            }
            Op::Delete(k) => {
                sut.delete(k).await?;
                model.delete(k);
            }
            Op::Checkpoint => sut.checkpoint().await?,
            Op::Reopen | Op::Crash => {
                if matches!(op, Op::Reopen) {
                    sut.reopen().await?
                } else {
                    sut.crash().await?
                }
                let (n, m) = check_durable(&model, sut, KEY_SPACE).await?;
                checks += n;
                if let Some(m) = m {
                    return Ok(Err(m));
                }
            }
        }
    }
    // Final check after a clean reopen, so every sequence verifies at least once.
    sut.reopen().await?;
    let (n, m) = check_durable(&model, sut, KEY_SPACE).await?;
    Ok(match m {
        Some(m) => Err(m),
        None => Ok(checks + n),
    })
}

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
    let mut report = Report::default();
    for seed in first_seed..first_seed + seeds {
        let ops = generate(seed, len, profile);
        let mut sut = make().await?;
        report.seeds += 1;
        match run_ops(&mut sut, &ops).await? {
            Ok(n) => report.checks += n,
            Err(_) => {
                let (ops, mismatch) = minimize(&make, ops).await?;
                report.failure = Some(Failure {
                    seed,
                    ops,
                    mismatch,
                });
                return Ok(report);
            }
        }
    }
    Ok(report)
}

/// Greedy one-at-a-time deletion: keep removing ops while the failure persists.
async fn minimize<F, Fut, S>(make: &F, mut ops: Vec<Op>) -> anyhow::Result<(Vec<Op>, Mismatch)>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<S>>,
    S: Sut,
{
    let mut i = 0;
    while i < ops.len() {
        let mut candidate = ops.clone();
        candidate.remove(i);
        let mut sut = make().await?;
        if run_ops(&mut sut, &candidate).await?.is_err() {
            ops = candidate;
        } else {
            i += 1;
        }
    }
    let mut sut = make().await?;
    let mismatch = run_ops(&mut sut, &ops)
        .await?
        .expect_err("minimized sequence still fails");
    Ok((ops, mismatch))
}
