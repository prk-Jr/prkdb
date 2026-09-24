//! `cargo xtask verify ...` forwards to the prkdb-verify binary so xtask stays light.

use anyhow::{bail, Result};

pub fn run(args: &[&str]) -> Result<()> {
    let status = std::process::Command::new(env!("CARGO"))
        .args([
            "run",
            "--release",
            "-q",
            "-p",
            "prkdb-verify",
            "--bin",
            "verify",
            "--",
        ])
        .args(args)
        .status()?;
    if !status.success() {
        bail!("verify failed ({status})");
    }
    Ok(())
}
