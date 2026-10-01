//! `prkdb-cli migrate --data-dir <dir>`: upgrade a data directory to this build's format,
//! offline (spec D4). The registry is empty in format 2, so today this reports what the
//! directory is and what, if anything, can be done.
//!
//! # Containers
//!
//! Some layouts put several data directories under one root that has no `FORMAT` of its
//! own: the multi-raft `STORAGE_PATH` (`meta/`, `partition_<n>/`, plus `schemas/`, which is
//! not a data directory). Such a root is not format 1. `migrate` reports it as a
//! container and migrates **every** data directory in it, in name order, reporting each;
//! it succeeds only if all of them end at the current format. A data directory that
//! cannot be migrated does not stop the others from being reported (each migration is
//! all-or-nothing on its own directory, per `Migration::run`).
//!
//! The pre-D11 optimized-storage layout (`collections/<name>/` under a root with no
//! `FORMAT`) is **not** a container: since Task 2.9b that root is one data directory, and
//! opening it refuses it as format 1, so `migrate` reports format 1 too.
//!
//! # Locking
//!
//! Each data directory is migrated under its data-directory lock (STO-10), the one every
//! open takes, so `migrate` refuses a directory a live database holds, and no database
//! can open one while it is being migrated. A directory that is not writable (read-only
//! media, permissions) cannot hold `LOCK`: it is still reported, and a `--dry-run` still
//! lists its plan, but a migration that would write is refused.

use clap::Args;
use prkdb::storage::format::{detect_format, read_format, unsupported_format, FORMAT_VERSION};
use prkdb::storage::lock::lock_data_dir_unless_read_only;
use prkdb::storage::migrations::plan;
use prkdb_core::vfs::StdVfs;
use std::path::{Path, PathBuf};

#[derive(Args, Clone, Debug)]
pub struct MigrateArgs {
    /// Data directory to migrate. Refused while a running process has it open.
    #[arg(long)]
    pub data_dir: PathBuf,
    /// List the migrations that would run, without running them.
    #[arg(long)]
    pub dry_run: bool,
}

pub fn handle_migrate(args: MigrateArgs) -> anyhow::Result<()> {
    let root = &args.data_dir;
    if !root.is_dir() {
        anyhow::bail!("data directory {} does not exist", root.display());
    }

    let members = container_members(root)?;
    if members.is_empty() {
        return migrate_one(root, args.dry_run);
    }

    println!(
        "{} is a container, not a data directory: it holds {} data directories; \
         migrating each",
        root.display(),
        members.len()
    );
    let failures: Vec<String> = members
        .iter()
        .filter_map(|dir| migrate_one(dir, args.dry_run).err())
        .map(|e| e.to_string())
        .collect();
    if failures.is_empty() {
        return Ok(());
    }
    anyhow::bail!(
        "{} of {} data directories under {} could not be migrated:\n{}",
        failures.len(),
        members.len(),
        root.display(),
        failures.join("\n")
    )
}

/// The data directories under `root` if it is a container (see the module docs), sorted;
/// empty if `root` is a data directory itself (it has `FORMAT`, or no known members).
fn container_members(root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    if read_format(root)?.is_some() {
        return Ok(Vec::new());
    }
    let mut members = Vec::new();
    for path in subdirs(root)? {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name == "meta" || name.starts_with("partition_") || read_format(&path)?.is_some() {
            members.push(path);
        }
    }
    members.sort();
    Ok(members)
}

fn subdirs(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            out.push(path);
        }
    }
    Ok(out)
}

/// Migrates one data directory.
fn migrate_one(dir: &Path, dry_run: bool) -> anyhow::Result<()> {
    // Held until this directory is done; `LOCK` does not count as data below. `None`: the
    // directory is not writable (read-only media, permissions), so only a report or a dry
    // run can proceed; a real migration is refused below.
    let lock = lock_data_dir_unless_read_only(&StdVfs, dir)?;
    if lock.is_none() {
        println!(
            "data directory {} is not writable; inspecting it read-only, without its lock",
            dir.display()
        );
    }
    // `None`: empty (ignoring `lost+found` and dotfiles). Format 1 had no marker, so a
    // non-empty directory without one is `Some(1)`.
    let Some(found) = detect_format(&StdVfs, dir)? else {
        println!(
            "data directory {} is empty; it is created at format {FORMAT_VERSION} when \
             first opened. Nothing to migrate.",
            dir.display()
        );
        return Ok(());
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
    if lock.is_none() && !dry_run {
        anyhow::bail!(
            "data directory {} is at format {found} and needs migrating, but it is not \
             writable, so its lock cannot be taken; make it writable (or use --dry-run)",
            dir.display()
        );
    }

    // Never empty here: `plan` returns an empty chain only for the current format, and
    // names the gap otherwise. Format 1's error is "no migrations available for format 1".
    let chain =
        plan(found).map_err(|e| anyhow::anyhow!("data directory {}: {e}", dir.display()))?;
    for step in &chain {
        println!(
            "{}: format {} → {}: {}",
            dir.display(),
            step.from(),
            step.to(),
            step.description()
        );
        if !dry_run {
            step.run(dir)?;
        }
    }
    if dry_run {
        println!("dry run: {} was not changed", dir.display());
    } else {
        println!(
            "data directory {} is now at format {FORMAT_VERSION}",
            dir.display()
        );
    }
    Ok(())
}
