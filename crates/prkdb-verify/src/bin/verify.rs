//! `verify` — model-based crash/restart harness driver (spec §7 Phase 1).
//!
//! Usage:
//!   verify [--profile blocking|discovery] [--seed N] [--seed-offset N] [--seeds K] [--ops M] [--mode durable] [--help]
//!
//! Flags (when a flag is given more than once, the later occurrence wins):
//!   --profile <blocking|discovery>  op-generation profile (default: blocking)
//!   --seed N                        run exactly one seed N; equivalent to `--seed-offset N --seeds 1`
//!   --seed-offset N                 first seed to run (default: 0)
//!   --seeds K                       number of seeds to run (default: 200, must be >= 1)
//!   --ops M                         ops per generated sequence (default: 80, must be >= 1)
//!   --mode durable                  the only mode that exists until Phase 2a
//!   --help, -h                      print this message and exit

use anyhow::{bail, Context, Result};
use prkdb_verify::ops::Profile;
use prkdb_verify::runner::{run_seeds, Outcome, Report};
use prkdb_verify::sut::WalSut;

const USAGE: &str = "verify [--profile blocking|discovery] [--seed N] [--seed-offset N] [--seeds K] [--ops M] [--mode durable] [--help]

Flags (later flags override earlier ones):
  --profile <blocking|discovery>  op-generation profile (default: blocking)
  --seed N                        run exactly one seed N (equivalent to --seed-offset N --seeds 1)
  --seed-offset N                 first seed to run (default: 0)
  --seeds K                       number of seeds to run (default: 200, must be >= 1)
  --ops M                         ops per generated sequence (default: 80, must be >= 1)
  --mode durable                  the only mode that exists until Phase 2a
  --help, -h                      print this message and exit";

fn parse_u64(flag: &str, raw: &str) -> Result<u64> {
    raw.parse::<u64>()
        .with_context(|| format!("{flag}: expected a non-negative integer"))
}

fn parse_usize(flag: &str, raw: &str) -> Result<usize> {
    raw.parse::<usize>()
        .with_context(|| format!("{flag}: expected a non-negative integer"))
}

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
            "--help" | "-h" => {
                println!("{USAGE}");
                return Ok(());
            }
            "--profile" => {
                let v = val()?;
                profile = Profile::parse(&v).ok_or_else(|| {
                    anyhow::anyhow!("bad profile {v:?} (expected blocking|discovery)")
                })?;
            }
            "--seed" => {
                first = parse_u64(a, &val()?)?;
                seeds = 1;
            }
            "--seed-offset" => first = parse_u64(a, &val()?)?,
            "--seeds" => seeds = parse_u64(a, &val()?)?,
            "--ops" => ops = parse_usize(a, &val()?)?,
            "--mode" => {
                if val()? != "durable" {
                    bail!("only durable mode exists until Phase 2a");
                }
            }
            other => bail!("unknown argument {other}"),
        }
    }
    if seeds == 0 {
        bail!("--seeds must be at least 1");
    }
    if ops == 0 {
        bail!("--ops must be at least 1");
    }

    let name = profile.as_str();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let report: Report = rt.block_on(run_seeds(WalSut::new, first, seeds, ops, profile))?;

    if let Some(f) = report.failure {
        let seeds_passed = report.seeds - 1;
        let checks_passed = report.checks;
        eprintln!(
            "FAILED profile={name} seed={} seeds_passed={seeds_passed} checks_passed={checks_passed}",
            f.seed
        );
        eprintln!(
            "replay: cargo xtask verify --profile {name} --seed {} --ops {ops}",
            f.seed
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
        for (i, op) in f.ops.iter().enumerate() {
            eprintln!("  {i:>3}: {op:?}");
        }
        bail!("harness failure");
    }

    println!(
        "profile={name} seeds={} checks={}",
        report.seeds, report.checks
    );
    if report.checks == 0 {
        bail!("vacuous run: no checks compared");
    }
    Ok(())
}
