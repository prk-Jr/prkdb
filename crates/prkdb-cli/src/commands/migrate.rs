//! `prkdb-cli migrate --data-dir <dir>`: upgrade a data directory to this build's format,
//! offline (spec D4). The registry is empty in format 2, so today this reports what the
//! directory is and what, if anything, can be done.

use clap::Args;
use prkdb::storage::format::{read_format, unsupported_format, FORMAT_VERSION};
use prkdb::storage::migrations::plan;
use std::path::PathBuf;

#[derive(Args, Clone, Debug)]
pub struct MigrateArgs {
    /// Data directory to migrate. Must not be open in a running process.
    #[arg(long)]
    pub data_dir: PathBuf,
    /// List the migrations that would run, without running them.
    #[arg(long)]
    pub dry_run: bool,
}

pub fn handle_migrate(args: MigrateArgs) -> anyhow::Result<()> {
    let dir = &args.data_dir;
    if !dir.is_dir() {
        anyhow::bail!("data directory {} does not exist", dir.display());
    }

    let found = match read_format(dir)? {
        Some(marker) => marker.format,
        None if std::fs::read_dir(dir)?.next().is_none() => {
            println!(
                "data directory {} is empty; it is created at format {FORMAT_VERSION} when \
                 first opened. Nothing to migrate.",
                dir.display()
            );
            return Ok(());
        }
        // Format 1 had no marker.
        None => 1,
    };

    if found == FORMAT_VERSION {
        println!(
            "data directory {} is at format {FORMAT_VERSION}; no migrations available for \
             format {FORMAT_VERSION}",
            dir.display()
        );
        return Ok(());
    }
    if found > FORMAT_VERSION {
        return Err(unsupported_format(dir, found).into());
    }

    let chain = plan(found)?;
    if chain.is_empty() {
        anyhow::bail!(
            "no migrations available for format {found} → {FORMAT_VERSION}; format {found} \
             directories cannot be converted by this version. See docs/guide/upgrade."
        );
    }
    for step in &chain {
        println!(
            "format {} → {}: {}",
            step.from(),
            step.to(),
            step.description()
        );
        if !args.dry_run {
            step.run(dir)?;
        }
    }
    if args.dry_run {
        println!("dry run: nothing was changed");
    } else {
        println!(
            "data directory {} is now at format {FORMAT_VERSION}",
            dir.display()
        );
    }
    Ok(())
}
