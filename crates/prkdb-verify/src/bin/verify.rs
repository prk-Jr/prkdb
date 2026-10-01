//! `verify` — model-based crash/restart harness driver (spec §7).
//!
//! Usage:
//!   verify [--profile core|blocking|discovery] [--mode durable|fast] [--sut fault|std]
//!          [--seed N] [--seed-offset N] [--seeds K] [--ops M] [--help]
//!
//! Flags (when a flag is given more than once, the later occurrence wins):
//!   --profile <core|blocking|discovery>  op-generation profile (default: blocking)
//!   --mode <durable|fast>           the WAL's sync mode (default: durable); fast needs --sut fault
//!   --sut <fault|std>               the WAL adapter on the simulated FaultFs (can lose power)
//!                                   or on the real filesystem (cannot); default: fault for
//!                                   blocking/discovery, std for core
//!   --seed N                        run exactly one seed N; equivalent to `--seed-offset N --seeds 1`
//!   --seed-offset N                 first seed to run (default: 0)
//!   --seeds K                       number of seeds to run (default: 200, must be >= 1)
//!   --ops M                         ops per generated sequence (default: 80, must be >= 1)
//!   --help, -h                      print this message and exit
//!
//! A green run prints `profile=<p> mode=<m> seeds=<n> checks=<c> ops=<Put:…,Delete:…,…>`
//! (plus ` rolls=<r>`, the WAL segment rolls, on `--sut fault`) and still fails as
//! vacuous if no key was compared, if any op kind the profile enables never
//! executed, or if a `fault` run never rolled a segment.

use anyhow::{bail, Context, Result};
use prkdb_verify::model::Mode;
use prkdb_verify::ops::Profile;
use prkdb_verify::runner::{run, Failure, Outcome, Report, RunConfig};
use prkdb_verify::sut::{FaultSut, WalSut};

const USAGE: &str = "verify [--profile core|blocking|discovery] [--mode durable|fast] [--sut fault|std] [--seed N] [--seed-offset N] [--seeds K] [--ops M] [--help]

Flags (later flags override earlier ones):
  --profile <core|blocking|discovery>  op-generation profile (default: blocking)
                                  core: the frozen Phase 1 table; blocking: the gate
                                  (core + PowerLoss); discovery: everything implemented
                                  (findings, not gates)
  --mode <durable|fast>           the WAL's sync mode (default: durable); fast needs --sut fault
  --sut <fault|std>               fault: the WAL adapter on the simulated FaultFs, which can
                                  lose power; std: on the real filesystem, which cannot
                                  (default: fault for blocking/discovery, std for core)
  --seed N                        run exactly one seed N (equivalent to --seed-offset N --seeds 1)
  --seed-offset N                 first seed to run (default: 0)
  --seeds K                       number of seeds to run (default: 200, must be >= 1)
  --ops M                         ops per generated sequence (default: 80, must be >= 1)
  --help, -h                      print this message and exit

A green run prints `profile=<p> mode=<m> seeds=<n> checks=<c> ops=<Put:..,Delete:..,..>`
(plus ` rolls=<r>` on --sut fault) and fails as vacuous if no key was compared, any op
kind the profile enables never ran, or a fault run never rolled a WAL segment.";

/// Which storage the harness drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SutKind {
    /// `FaultSut`: the WAL adapter on `FaultFs` (supports `PowerLoss`).
    Fault,
    /// `WalSut`: the WAL adapter on the real filesystem.
    Std,
}

impl SutKind {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "fault" => Some(Self::Fault),
            "std" => Some(Self::Std),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Fault => "fault",
            Self::Std => "std",
        }
    }

    fn default_for(profile: Profile) -> Self {
        match profile {
            Profile::Core => Self::Std,
            Profile::Blocking | Profile::Discovery => Self::Fault,
        }
    }
}

/// A parsed command line.
struct Args {
    config: RunConfig,
    sut: SutKind,
}

fn parse_u64(flag: &str, raw: &str) -> Result<u64> {
    raw.parse::<u64>()
        .with_context(|| format!("{flag}: expected a non-negative integer"))
}

fn parse_usize(flag: &str, raw: &str) -> Result<usize> {
    raw.parse::<usize>()
        .with_context(|| format!("{flag}: expected a non-negative integer"))
}

/// Parses the command line. `Ok(None)` means `--help` was printed.
fn parse_args(args: &[String]) -> Result<Option<Args>> {
    let mut config = RunConfig {
        first_seed: 0,
        seeds: 200,
        ops: 80,
        profile: Profile::Blocking,
        mode: Mode::Durable,
        repro_attempts: 1,
    };
    let mut sut = None;
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
            "--mode" => {
                let v = val()?;
                config.mode = Mode::parse(&v)
                    .ok_or_else(|| anyhow::anyhow!("bad mode {v:?} (expected durable|fast)"))?;
            }
            "--sut" => {
                let v = val()?;
                sut = Some(
                    SutKind::parse(&v)
                        .ok_or_else(|| anyhow::anyhow!("bad sut {v:?} (expected fault|std)"))?,
                );
            }
            "--seed" => {
                config.first_seed = parse_u64(a, &val()?)?;
                config.seeds = 1;
            }
            "--seed-offset" => config.first_seed = parse_u64(a, &val()?)?,
            "--seeds" => config.seeds = parse_u64(a, &val()?)?,
            "--ops" => config.ops = parse_usize(a, &val()?)?,
            other => bail!("unknown argument {other}"),
        }
    }
    if config.seeds == 0 {
        bail!("--seeds must be at least 1");
    }
    if config.ops == 0 {
        bail!("--ops must be at least 1");
    }
    let sut = sut.unwrap_or_else(|| SutKind::default_for(config.profile));
    if sut == SutKind::Std && config.mode == Mode::Fast {
        // `WalSut` always runs the WAL in Durable mode and cannot lose power,
        // so a Fast run on it would check nothing Fast-specific.
        bail!("--mode fast needs --sut fault");
    }
    Ok(Some(Args { config, sut }))
}

fn print_failure(args: &Args, report: &Report, f: &Failure) {
    let config = &args.config;
    let name = config.profile.as_str();
    let mode = config.mode.as_str();
    let sut = args.sut.as_str();
    eprintln!(
        "FAILED profile={name} mode={mode} sut={sut} seed={} seeds_passed={} checks_passed={} ops={}",
        f.seed,
        report.seeds - 1,
        report.checks,
        report.format_op_counts()
    );
    eprintln!(
        "replay: cargo xtask verify --profile {name} --mode {mode} --sut {sut} --seed {} --ops {}",
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
            if mismatch.no_prefix_fits() {
                eprintln!(
                    "  (this value is acceptable on its own, but no single prefix of the \
                     acknowledged writes explains the whole snapshot: a later write \
                     survived while an earlier one was lost)"
                );
            }
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
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some(args) = parse_args(&argv)? else {
        return Ok(());
    };
    let config = &args.config;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let mode = config.mode;
    let report: Report = match args.sut {
        SutKind::Fault => rt.block_on(run(|| FaultSut::new(mode), config))?,
        SutKind::Std => rt.block_on(run(WalSut::new, config))?,
    };

    if let Some(f) = &report.failure {
        print_failure(&args, &report, f);
        bail!("harness failure");
    }

    let rolls = report
        .segment_rolls
        .map(|n| format!(" rolls={n}"))
        .unwrap_or_default();
    println!(
        "profile={} mode={} seeds={} checks={} ops={}{rolls}",
        config.profile.as_str(),
        config.mode.as_str(),
        report.seeds,
        report.checks,
        report.format_op_counts()
    );
    if report.checks == 0 {
        bail!("vacuous run: no checks compared");
    }
    if report.segment_rolls == Some(0) {
        bail!("vacuous for segment rolls: no sequence ever crossed a segment boundary");
    }
    let missing = report.missing_op_kinds(config.profile);
    if !missing.is_empty() {
        bail!("vacuous for {}: never executed", missing.join(", "));
    }
    Ok(())
}
