//! Fixed-count stream partitions beneath a container directory without a WAL.

use super::manifest::{StreamManifest, StreamManifestError};
use super::{AppendAck, Record, StreamConfig, StreamLog};
use crate::catalog::Catalog;
use crate::storage::lock::lock_data_dir;
use crate::storage::wal_adapter::wal_err;
use prkdb_core::vfs::{create_dir_all_durable, LockGuard, OpenMode, StdVfs, Vfs};
use prkdb_core::wal::segment::{parse_segment_file_name, scan_segment_flow};
use prkdb_types::error::StorageError;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// How an append chooses a partition. Key routing hashes raw bytes with SeaHash,
/// independent of Rust Hash trait framing, platform width and toolchain.
#[derive(Debug, Clone, Copy)]
pub enum Route<'a> {
    Partition(u32),
    RoundRobin,
    Key(&'a [u8]),
}

/// A stream container whose partition count is persisted in `STREAM`.
///
/// The root's final path component is a user stream name and must satisfy
/// [`Catalog::validate_name`]. Each partition holds its own WAL and directory lock.
pub struct PartitionedStream {
    // Drop streams before releasing the container lock.
    streams: Vec<StreamLog>,
    round_robin: AtomicU32,
    _lock: Box<dyn LockGuard>,
}

fn io_error(path: &Path, e: std::io::Error) -> StorageError {
    StorageError::Internal(format!("{}: {e}", path.display()))
}

fn corruption(path: &Path, message: impl std::fmt::Display) -> StorageError {
    StorageError::Corruption(format!("{}: {message}", path.display()))
}

fn read_manifest(vfs: &dyn Vfs, path: &Path) -> Result<StreamManifest, StorageError> {
    let file = vfs
        .open(path, OpenMode::Read)
        .map_err(|e| io_error(path, e))?;
    let len = file.len().map_err(|e| io_error(path, e))?;
    if !(24..=4096).contains(&len) {
        return Err(corruption(
            path,
            "STREAM manifest length must be between 24 and 4096 bytes",
        ));
    }
    let mut bytes = vec![0; len as usize];
    let mut read = 0;
    while read < bytes.len() {
        let n = file
            .read_at(read as u64, &mut bytes[read..])
            .map_err(|e| io_error(path, e))?;
        if n == 0 {
            return Err(corruption(path, "short STREAM manifest read"));
        }
        read += n;
    }
    if file.len().map_err(|e| io_error(path, e))? != len {
        return Err(corruption(path, "STREAM manifest changed during read"));
    }
    StreamManifest::decode(&bytes).map_err(|e| match e {
        StreamManifestError::UnsupportedVersion { found, supported } => {
            let creator = if found < supported { "an older" } else { "a newer" };
            StorageError::UnsupportedFormat(format!(
                "stream container {} was created by {creator} PrkDB (STREAM version {found}); \
                 this build reads STREAM version {supported}. Routing schemes cannot be changed on open",
                path.display()
            ))
        }
        e => corruption(path, e),
    })
}

fn has_frames(vfs: &dyn Vfs, dir: &Path) -> Result<bool, StorageError> {
    let mut segments: Vec<_> = vfs
        .read_dir(dir)
        .map_err(|e| io_error(dir, e))?
        .into_iter()
        .filter_map(|path| {
            let lsn = path
                .file_name()?
                .to_str()
                .and_then(parse_segment_file_name)?;
            Some((lsn, path))
        })
        .collect();
    segments.sort_unstable_by_key(|(lsn, _)| *lsn);
    for (index, (lsn, path)) in segments.iter().enumerate() {
        let file = vfs
            .open(path, OpenMode::Read)
            .map_err(|e| io_error(path, e))?;
        // WAL recovery can finish an interrupted header creation only in the final
        // segment. Classify it without repair; a nonfinal empty file stays corrupt.
        if index + 1 == segments.len() && file.is_empty().map_err(|e| io_error(path, e))? {
            continue;
        }
        let scan = scan_segment_flow(file.as_ref(), path, *lsn, &mut |_, _, _| {
            Ok(ControlFlow::Break(()))
        })
        .map_err(wal_err)?;
        if scan.stopped_by_visitor {
            return Ok(true);
        }
        if let Some((offset, fault)) = scan.stopped {
            return Err(corruption(
                path,
                format!("cannot prove an interrupted partition empty: {fault:?} at {offset}"),
            ));
        }
    }
    Ok(false)
}

fn incomplete(nonempty: &[PathBuf]) -> StorageError {
    StorageError::Validation(format!(
        "STREAM manifest is missing, but these partition directories contain frames: {}",
        nonempty
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

impl PartitionedStream {
    pub async fn open(
        root: &Path,
        partitions: u32,
        cfg: StreamConfig,
    ) -> Result<Self, StorageError> {
        Self::open_with_vfs(Arc::new(StdVfs), root, partitions, cfg).await
    }

    /// Open through the filesystem seam, including manifest publication and reads.
    pub async fn open_with_vfs(
        vfs: Arc<dyn Vfs>,
        root: &Path,
        partitions: u32,
        cfg: StreamConfig,
    ) -> Result<Self, StorageError> {
        let name = root.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
            StorageError::Validation("stream root must have a valid stream name".into())
        })?;
        Catalog::validate_name(name)?;
        if partitions == 0 {
            return Err(StorageError::Validation(
                "stream must have at least one partition".into(),
            ));
        }
        let root = root.to_path_buf();
        let (lock, creating, existing_locks) = {
            let vfs = vfs.clone();
            let root = root.clone();
            tokio::task::spawn_blocking(move || {
                create_dir_all_durable(vfs.as_ref(), &root)
                    .map_err(|e| io_error(&root, e))?;
                let lock = lock_data_dir(vfs.as_ref(), &root)?;
                let entries = vfs.read_dir(&root).map_err(|e| io_error(&root, e))?;
                for path in &entries {
                    if path.file_name().is_some_and(|n| n == "FORMAT") || path.extension().is_some_and(|e| e == "wal") {
                        return Err(StorageError::Validation(format!("stream container {} must have no FORMAT or WAL; found {}", root.display(), path.display())));
                    }
                }
                let manifest_path = root.join("STREAM");
                let creating = !vfs.exists(&manifest_path).map_err(|e| io_error(&manifest_path, e))?;
                let mut existing_locks = Vec::new();
                if creating {
                    let mut nonempty = Vec::new();
                    for dir in entries {
                        if dir.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with("partition_")) {
                            let guard = lock_data_dir(vfs.as_ref(), &dir)?;
                            if has_frames(vfs.as_ref(), &dir)? { nonempty.push(dir.clone()); }
                            existing_locks.push((dir, guard));
                        }
                    }
                    if !nonempty.is_empty() { return Err(incomplete(&nonempty)); }
                } else {
                    let manifest = read_manifest(vfs.as_ref(), &manifest_path)?;
                    if manifest.partitions != partitions {
                        return Err(StorageError::Validation(format!("{} has {} durable partitions; requested {partitions}; repartitioning is unsupported", root.display(), manifest.partitions)));
                    }
                    for p in 0..partitions {
                        let dir = root.join(format!("partition_{p}"));
                        if !vfs.exists(&dir).map_err(|e| io_error(&dir, e))? {
                            return Err(corruption(&manifest_path, format!("missing partition directory {}", dir.display())));
                        }
                    }
                }
                Ok((lock, creating, existing_locks))
            }).await.map_err(|e| StorageError::Internal(format!("partitioned stream open task: {e}")))??
        };
        let mut existing_locks = existing_locks;
        let mut streams = Vec::new();
        for p in 0..partitions {
            let path = root.join(format!("partition_{p}"));
            if let Some(i) = existing_locks.iter().position(|(dir, _)| dir == &path) {
                drop(existing_locks.swap_remove(i));
            }
            let mut partition_cfg = cfg.clone();
            partition_cfg.path = path.clone();
            partition_cfg.wal.log_dir = path;
            streams.push(StreamLog::open_with_vfs(vfs.clone(), partition_cfg).await?);
        }
        if creating {
            // A standalone StreamLog may have appended between the preflight lock
            // and open. Recheck under the locks owned by the opened stream handles.
            let (streams, lock, _existing_locks) = tokio::task::spawn_blocking(move || {
                let mut nonempty = Vec::new();
                for p in 0..partitions {
                    let dir = root.join(format!("partition_{p}"));
                    if has_frames(vfs.as_ref(), &dir)? {
                        nonempty.push(dir);
                    }
                }
                if !nonempty.is_empty() {
                    return Err(incomplete(&nonempty));
                }
                let tmp = root.join("STREAM.tmp");
                let path = root.join("STREAM");
                let bytes = StreamManifest::current(partitions)
                    .encode()
                    .map_err(|e| corruption(&path, e))?;
                let file = vfs.create(&tmp).map_err(|e| io_error(&tmp, e))?;
                file.write_at(0, &bytes).map_err(|e| io_error(&tmp, e))?;
                file.sync_data().map_err(|e| io_error(&tmp, e))?;
                drop(file);
                vfs.rename(&tmp, &path).map_err(|e| io_error(&path, e))?;
                vfs.sync_dir(&root).map_err(|e| io_error(&root, e))?;
                // Keep locks through blocking I/O, even if the caller cancels.
                Ok((streams, lock, existing_locks))
            })
            .await
            .map_err(|e| {
                StorageError::Internal(format!("partitioned stream manifest task: {e}"))
            })??;
            return Ok(Self {
                streams,
                round_robin: AtomicU32::new(0),
                _lock: lock,
            });
        }
        Ok(Self {
            streams,
            round_robin: AtomicU32::new(0),
            _lock: lock,
        })
    }

    pub async fn append(
        &self,
        route: Route<'_>,
        records: Vec<Record>,
    ) -> Result<(u32, AppendAck), StorageError> {
        let n = self.partitions();
        let p = match route {
            Route::Partition(p) if p < n => p,
            Route::Partition(p) => {
                return Err(StorageError::Validation(format!(
                    "partition {p} is outside stream's {n} partitions"
                )))
            }
            Route::Key(key) => (seahash::hash(key) % u64::from(n)) as u32,
            Route::RoundRobin => self
                .round_robin
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |p| {
                    Some(if p + 1 == n { 0 } else { p + 1 })
                })
                .expect("round-robin update always supplies a value"),
        };
        Ok((p, self.streams[p as usize].append(records).await?))
    }

    pub fn partition(&self, p: u32) -> Option<&StreamLog> {
        self.streams.get(p as usize)
    }
    pub fn partitions(&self) -> u32 {
        self.streams.len() as u32
    }

    /// Sync and close every partition, returning the first close error, if any.
    pub fn close(self) -> Result<(), StorageError> {
        let mut result = Ok(());
        for stream in self.streams {
            if let Err(error) = stream.close() {
                if result.is_ok() {
                    result = Err(error);
                }
            }
        }
        result
    }
}
