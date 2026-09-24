//! verify [--profile blocking|discovery] [--seed N] [--seed-offset N] [--seeds K] [--ops M] [--mode durable]

use anyhow::{bail, Result};
use prkdb_verify::ops::Profile;
use prkdb_verify::runner::{run_seeds, Outcome, Report};
use prkdb_verify::sut::WalSut;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut profile = Profile::Blocking;
    let (mut first, mut seeds, mut ops) = (0u64, 200u64, 80usize);
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let mut val = || {
            it.next()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("{a} needs a value"))
        };
        match a.as_str() {
            "--profile" => {
                profile = Profile::parse(&val()?).ok_or_else(|| anyhow::anyhow!("bad profile"))?
            }
            "--seed" => {
                first = val()?.parse()?;
                seeds = 1;
            }
            "--seed-offset" => first = val()?.parse()?,
            "--seeds" => seeds = val()?.parse()?,
            "--ops" => ops = val()?.parse()?,
            "--mode" => {
                if val()? != "durable" {
                    bail!("only durable mode exists until Phase 2a");
                }
            }
            other => bail!("unknown argument {other}"),
        }
    }
    let name = if profile == Profile::Blocking {
        "blocking"
    } else {
        "discovery"
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let report: Report = rt.block_on(run_seeds(WalSut::new, first, seeds, ops, profile))?;
    println!(
        "profile={name} seeds={} checks={}",
        report.seeds, report.checks
    );
    if report.failure.is_none() && report.checks == 0 {
        bail!("vacuous run: no checks compared");
    }
    if let Some(f) = report.failure {
        eprintln!(
            "FAILED seed={} (replay: cargo xtask verify --profile {name} --seed {} --ops {ops})",
            f.seed, f.seed
        );
        match &f.outcome {
            Outcome::Mismatch {
                at,
                after,
                mismatch,
            } => {
                eprintln!("mismatch at op {at} (after {after:?}):");
                eprintln!("  key:      {:?}", mismatch.key);
                eprintln!("  expected: {:?}", mismatch.expected);
                eprintln!("  actual:   {:?}", mismatch.actual);
            }
            Outcome::SutError { at, op, error } => {
                eprintln!("sut error at op {at}:");
                eprintln!("  op:    {op:?}");
                eprintln!("  error: {error}");
            }
            Outcome::Pass { .. } => unreachable!("a Pass outcome is never a failure"),
        }
        eprintln!(
            "minimized ops ({} of {} original):",
            f.ops.len(),
            f.original_len
        );
        eprintln!("{:#?}", f.ops);
        bail!("harness failure");
    }
    Ok(())
}
