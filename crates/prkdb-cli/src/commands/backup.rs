//! Offline backup and restore.
//!
//! # Verification
//!
//! `backup` writes a sidecar manifest next to the archive recording its length and
//! SHA-256, and `restore` checks both before writing anything. Restoring a corrupt archive
//! into an empty directory and reporting success is the worst outcome a backup tool has,
//! because it is discovered only when the backup is needed.
//!
//! ## Why a sidecar rather than a trailer
//!
//! The archive is `header_len | header | entries…`, with the entry region optionally
//! gzipped and read until EOF. An uncompressed archive has no end marker, so a checksum
//! appended to the file would be consumed as another entry by `SnapshotReader`. Putting it
//! in the header is not possible either: the digest is only known once every entry has
//! been written, and the header is written first.
//!
//! The cost is that the manifest can be separated from its archive. `restore` handles
//! that by treating a missing manifest as a warning rather than an error — archives
//! written before manifests existed must still restore — and a *present but mismatched*
//! manifest as fatal.

use crate::Cli;
use clap::Args;
use prkdb::storage::snapshot::{CompressionType, SnapshotReader};
use prkdb::PrkDb;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// Bumped when the manifest's meaning changes, not when a field is added.
const MANIFEST_VERSION: u32 = 1;

#[derive(Args, Clone, Debug)]
pub struct BackupArgs {
    /// Output snapshot file path
    #[arg(short, long)]
    pub output: PathBuf,

    /// Compression type (none, gzip)
    #[arg(short, long, default_value = "gzip")]
    pub compression: String,
}

#[derive(Args, Clone, Debug)]
pub struct RestoreArgs {
    /// Input snapshot file path
    #[arg(short, long)]
    pub input: PathBuf,

    /// Target data directory (must be empty or output to new dir)
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Overforce overwrite if directory exists and is not empty
    #[arg(long)]
    pub force: bool,

    /// Restore without checking the archive against its manifest.
    ///
    /// Exists for recovering what can be recovered from a damaged archive. It is not a
    /// way to silence a mismatch: a mismatch means the archive is not what was backed up.
    #[arg(long)]
    pub skip_verify: bool,
}

/// Records what a backup produced, so a restore can tell whether it still has it.
#[derive(Debug, Serialize, Deserialize)]
pub struct BackupManifest {
    pub manifest_version: u32,
    /// File name only. The manifest travels with the archive, so an absolute path here
    /// would break the moment the pair is moved — which is what backups are for.
    pub archive: String,
    pub bytes: u64,
    pub sha256: String,
    pub entries: u64,
    pub max_offset: u64,
    pub compression: String,
    pub created_at: u64,
}

/// Conventional sidecar path: `snapshot.bin` -> `snapshot.bin.manifest`.
pub fn manifest_path(archive: &Path) -> PathBuf {
    let mut name = archive.as_os_str().to_os_string();
    name.push(".manifest");
    PathBuf::from(name)
}

/// Streams the file rather than reading it whole: a backup can be far larger than RAM,
/// and a verification step that OOMs on big archives protects only small ones.
fn hash_file(path: &Path) -> anyhow::Result<(u64, String)> {
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    let mut total = 0u64;

    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as u64;
    }

    Ok((total, format!("{:x}", hasher.finalize())))
}

/// Handle offline backup (direct storage access)
pub async fn handle_backup(args: BackupArgs, cli: &Cli) -> anyhow::Result<()> {
    tracing::info!(
        "Starting offline backup using database at {:?}",
        cli.database
    );

    // 1. Open database (embedded mode)
    // Use Builder to construct PrkDb with WAL storage
    let db = PrkDb::builder()
        .with_data_dir(&cli.database)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))?;

    // 2. Parse compression
    let compression = match args.compression.to_lowercase().as_str() {
        "gzip" => CompressionType::Gzip,
        "none" => CompressionType::None,
        _ => anyhow::bail!("Invalid compression type. Supported: gzip, none"),
    };

    // 3. Take snapshot
    let offset = db
        .take_snapshot(&args.output, compression)
        .await
        .map_err(|e| anyhow::anyhow!("Snapshot failed: {}", e))?;

    // 4. Record what was written, reading the entry count back from the archive itself
    //    rather than from what we intended to write. A manifest derived from intent would
    //    agree with a truncated archive.
    let entries = SnapshotReader::open(&args.output)
        .map_err(|e| anyhow::anyhow!("Failed to reopen archive to build its manifest: {}", e))?
        .header
        .index_entries;

    let (bytes, sha256) = hash_file(&args.output)?;
    let manifest = BackupManifest {
        manifest_version: MANIFEST_VERSION,
        archive: args
            .output
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
        bytes,
        sha256: sha256.clone(),
        entries,
        max_offset: offset,
        compression: args.compression.to_lowercase(),
        created_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };

    let manifest_file = manifest_path(&args.output);
    std::fs::write(&manifest_file, serde_json::to_vec_pretty(&manifest)?)?;

    println!("Backup successful!");
    println!("Output: {:?}", args.output);
    println!("Max Offset: {}", offset);
    println!("Entries: {}", entries);
    println!("SHA-256: {}", sha256);
    println!("Manifest: {:?}", manifest_file);

    Ok(())
}

/// Checks an archive against its manifest.
///
/// A missing manifest is a warning: archives predating manifests must still restore. A
/// manifest that disagrees with the archive is fatal, because the only honest thing to
/// say about such an archive is that it is not the one that was backed up.
fn verify_against_manifest(archive: &Path) -> anyhow::Result<()> {
    let manifest_file = manifest_path(archive);
    if !manifest_file.exists() {
        eprintln!(
            "⚠️  No manifest at {:?}; restoring without verification.",
            manifest_file
        );
        return Ok(());
    }

    let manifest: BackupManifest = serde_json::from_slice(&std::fs::read(&manifest_file)?)
        .map_err(|e| anyhow::anyhow!("Manifest {:?} is not readable: {}", manifest_file, e))?;

    if manifest.manifest_version > MANIFEST_VERSION {
        anyhow::bail!(
            "Manifest version {} is newer than this binary understands ({}). Refusing to \
             verify an archive written by a later format.",
            manifest.manifest_version,
            MANIFEST_VERSION
        );
    }

    let (bytes, sha256) = hash_file(archive)?;

    // Length is checked first purely so a truncated archive reports the useful error
    // rather than an opaque digest mismatch.
    if bytes != manifest.bytes {
        anyhow::bail!(
            "Archive {:?} is {} bytes; its manifest records {}. The archive is truncated \
             or was replaced. Use --skip-verify to restore it anyway.",
            archive,
            bytes,
            manifest.bytes
        );
    }

    if sha256 != manifest.sha256 {
        anyhow::bail!(
            "Archive {:?} does not match its manifest checksum.\n  expected {}\n  actual   {}\n\
             The archive is corrupt. Use --skip-verify to restore what can be read.",
            archive,
            manifest.sha256,
            sha256
        );
    }

    println!(
        "✅ Archive verified against manifest ({} entries, {} bytes)",
        manifest.entries, manifest.bytes
    );
    Ok(())
}

pub async fn handle_restore(args: RestoreArgs) -> anyhow::Result<()> {
    tracing::info!("Starting restore to {:?}", args.data_dir);

    // 1. Check directory
    if args.data_dir.exists() && args.data_dir.read_dir()?.next().is_some() && !args.force {
        anyhow::bail!(
            "Target directory {:?} is not empty. Use --force to overwrite.",
            args.data_dir
        );
    }

    // 2. Verify before touching the target. Discovering corruption halfway through a
    //    --force restore would leave the target holding a partial database.
    if args.skip_verify {
        eprintln!("⚠️  --skip-verify: restoring without checking the archive's manifest.");
    } else {
        verify_against_manifest(&args.input)?;
    }

    // 3. Open snapshot reader
    // Uses prkdb::storage::snapshot::SnapshotReader
    let mut reader = SnapshotReader::open(&args.input)
        .map_err(|e| anyhow::anyhow!("Failed to open snapshot: {}", e))?;

    println!("Snapshot Info:");
    println!("  Version: {}", reader.header.version);
    println!("  Entries: {}", reader.header.index_entries);
    println!("  Max Offset: {}", reader.header.max_offset);

    // 4. Open new DB at target location
    let db = PrkDb::builder()
        .with_data_dir(&args.data_dir)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create database: {}", e))?;

    // 5. Replay entries
    let mut count = 0;
    while let Some((key, val)) = reader.next_entry().map_err(|e| anyhow::anyhow!(e))? {
        db.put(&key, &val)
            .await
            .map_err(|e| anyhow::anyhow!("Restore put failed: {}", e))?;
        count += 1;
        if count % 10000 == 0 {
            tracing::info!("Restored {} entries...", count);
        }
    }

    // Force flush/checkpoint if possible via db API?
    // Currently no explicit flush in PrkDb public API, but storage usually flushes on close/write.
    // Since we just did Puts, they are WAL appended.

    println!("Restore complete! Restored {} entries.", count);

    Ok(())
}
