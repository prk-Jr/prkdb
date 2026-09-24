//! `cargo xtask verify ...` forwards to the prkdb-verify binary so xtask stays light.
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
