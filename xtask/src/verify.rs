//! `cargo xtask verify ...` forwards to the prkdb-verify binary so xtask stays light.
//!
//! Usage (see `cargo xtask verify --help` for every flag):
//!   cargo xtask verify [--profile core|blocking|discovery] [--mode durable|fast]
//!                      [--sut fault|std] [--seed N] [--seed-offset N] [--seeds K] [--ops M]
//!
//! `--sut` defaults to `fault` (the WAL adapter on the simulated FaultFs, which can lose
//! power) for blocking/discovery and `std` (the real filesystem) for core; `--mode fast`
//! needs `--sut fault`.
//!
//! A green run prints `profile=<p> mode=<m> seeds=<n> checks=<c> ops=<Put:…,Delete:…,…>`
//! and fails as vacuous if no key was compared or any op kind the profile enables never ran.
//!
//! Builds and runs in debug mode by default for fast iteration; set
//! `PRKDB_VERIFY_RELEASE=1` to build/run in release mode (e.g. for large seed
//! counts, or the pre-push harness gate).

use anyhow::Result;

pub fn run(args: &[&str]) -> Result<()> {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let release = std::env::var_os("PRKDB_VERIFY_RELEASE").is_some_and(|v| v == "1");

    let mut cmd_args = vec!["run", "-p", "prkdb-verify", "--bin", "verify"];
    if release {
        cmd_args.push("--release");
    }
    cmd_args.push("--");

    let status = std::process::Command::new(cargo)
        .args(cmd_args)
        .args(args)
        .status()?;
    if status.success() {
        Ok(())
    } else {
        // Propagate the child's own exit code; it already printed its error.
        std::process::exit(status.code().unwrap_or(1));
    }
}
