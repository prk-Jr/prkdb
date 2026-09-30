//! `verify` — model-based crash/restart harness driver (spec §7).
//!
//! Usage:
//!   verify [--profile core|blocking|discovery] [--seed N] [--seed-offset N] [--seeds K] [--ops M] [--mode durable] [--help]
//!
//! Flags (when a flag is given more than once, the later occurrence wins):
//!   --profile <core|blocking|discovery>  op-generation profile (default: blocking)
//!   --seed N                        run exactly one seed N; equivalent to `--seed-offset N --seeds 1`
//!   --seed-offset N                 first seed to run (default: 0)
//!   --seeds K                       number of seeds to run (default: 200, must be >= 1)
//!   --ops M                         ops per generated sequence (default: 80, must be >= 1)
//!   --mode durable                  the only mode until Fast arrives with PowerLoss (Task 2.10b)
//!   --help, -h                      print this message and exit
//!
//! A green run prints `profile=<p> mode=<m> seeds=<n> checks=<c> ops=<Put:…,Delete:…,…>`
//! and still fails as vacuous if no key was compared or if any op kind the
//! profile enables never executed.

use anyhow::{bail, Context, Result};
use prkdb_verify::model::Mode;
use prkdb_verify::ops::Profile;
use prkdb_verify::runner::{run, Failure, Outcome, Report, RunConfig};
use prkdb_verify::sut::WalSut;

const USAGE: &str = "verify [--profile core|blocking|discovery] [--seed N] [--seed-offset N] [--seeds K] [--ops M] [--mode durable] [--help]

Flags (later flags override earlier ones):
  --profile <core|blocking|discovery>  op-generation profile (default: blocking)
                                  core: the frozen Phase 1 table; blocking: the gate;
                                  discovery: everything implemented (findings, not gates)
  --seed N                        run exactly one seed N (equivalent to --seed-offset N --seeds 1)
  --seed-offset N                 first seed to run (default: 0)
  --seeds K                       number of seeds to run (default: 200, must be >= 1)
  --ops M                         ops per generated sequence (default: 80, must be >= 1)
  --mode durable                  the only mode until Fast arrives with PowerLoss (Task 2.10b)
  --help, -h                      print this message and exit

A green run prints `profile=<p> mode=<m> seeds=<n> checks=<c> ops=<Put:..,Delete:..,..>`
and fails as vacuous if no key was compared or any op kind the profile enables never ran.";

fn parse_u64(flag: &str, raw: &str) -> Result<u64> {
    raw.parse::<u64>()
        .with_context(|| format!("{flag}: expected a non-negative integer"))
}

fn parse_usize(flag: &str, raw: &str) -> Result<usize> {
    raw.parse::<usize>()
        .with_context(|| format!("{flag}: expected a non-negative integer"))
}

/// Parses the command line. `Ok(None)` means `--help` was printed.
fn parse_args(args: &[String]) -> Result<Option<RunConfig>> {
    let mut config = RunConfig {
        first_seed: 0,
        seeds: 200,
        ops: 80,
        profile: Profile::Blocking,
        mode: Mode::Durable,
        repro_attempts: 1,
    };
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
                return Ok(None);
            }
            "--profile" => {
                let v = val()?;
                config.profile = Profile::parse(&v).ok_or_else(|| {
                    anyhow::anyhow!("bad profile {v:?} (expected core|blocking|discovery)")
                })?;
            }
            "--seed" => {
                config.first_seed = parse_u64(a, &val()?)?;
                config.seeds = 1;
            }
            "--seed-offset" => config.first_seed = parse_u64(a, &val()?)?,
            "--seeds" => config.seeds = parse_u64(a, &val()?)?,
            "--ops" => config.ops = parse_usize(a, &val()?)?,
            "--mode" => {
                if Mode::parse(&val()?) != Some(Mode::Durable) {
                    bail!(
                        "only durable mode exists until Fast arrives with PowerLoss (Task 2.10b)"
                    );
                }
            }
            other => bail!("unknown argument {other}"),
        }
    }
    if config.seeds == 0 {
        bail!("--seeds must be at least 1");
    }
    if config.ops == 0 {
        bail!("--ops must be at least 1");
    }
    Ok(Some(config))
}

fn print_failure(config: &RunConfig, report: &Report, f: &Failure) {
    let name = config.profile.as_str();
    let mode = config.mode.as_str();
    eprintln!(
        "FAILED profile={name} mode={mode} seed={} seeds_passed={} checks_passed={} ops={}",
        f.seed,
        report.seeds - 1,
        report.checks,
        report.format_op_counts()
    );
    eprintln!(
        "replay: cargo xtask verify --profile {name} --mode {mode} --seed {} --ops {}",
        f.seed, config.ops
    );
    match &f.outcome {
        Outcome::Mismatch {
            at,
            after,
            mismatch,
        } => {
            eprintln!("mismatch at op {at} (after {after:?}):");
            eprintln!("  key:        {:?}", mismatch.key);
            eprintln!("  expected:   {:?}", mismatch.expected);
            eprintln!("  acceptable: {:?}", mismatch.acceptable);
            eprintln!("  actual:     {:?}", mismatch.actual);
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
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(config) = parse_args(&args)? else {
        return Ok(());
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let report: Report = rt.block_on(run(WalSut::new, &config))?;

    if let Some(f) = &report.failure {
        print_failure(&config, &report, f);
        bail!("harness failure");
    }

    println!(
        "profile={} mode={} seeds={} checks={} ops={}",
        config.profile.as_str(),
        config.mode.as_str(),
        report.seeds,
        report.checks,
        report.format_op_counts()
    );
    if report.checks == 0 {
        bail!("vacuous run: no checks compared");
    }
    let missing = report.missing_op_kinds(config.profile);
    if !missing.is_empty() {
        bail!("vacuous for {}: never executed", missing.join(", "));
    }
    Ok(())
}
