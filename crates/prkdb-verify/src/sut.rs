//! Drives the real embedded storage through its public API.

use crate::faultfs::{FaultFs, Tear};
use crate::model::{Key, Mode, Value};
use prkdb::storage::config::StorageConfig;
use prkdb::storage::WalStorageAdapter;
use prkdb_core::vfs::Vfs;
use prkdb_core::wal::segment::parse_segment_file_name;
use prkdb_core::wal::{SyncMode, WalConfig};
use prkdb_types::storage::StorageAdapter;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Returned by `Sut` methods a SUT does not implement. The runner treats it as
/// a harness error (the profile asked for an op this SUT cannot do), never as
/// a finding. New `Sut` methods get default bodies returning this, so adding
/// an op never breaks existing implementations.
#[derive(Debug)]
pub struct Unsupported(pub &'static str);

impl std::fmt::Display for Unsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SUT does not support {}", self.0)
    }
}

impl std::error::Error for Unsupported {}

#[async_trait::async_trait]
pub trait Sut: Send {
    async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()>;
    async fn delete(&mut self, k: &Key) -> anyhow::Result<()>;
    async fn get(&mut self, k: &Key) -> anyhow::Result<Option<Value>>;
    async fn reopen(&mut self) -> anyhow::Result<()>;
    async fn crash(&mut self) -> anyhow::Result<()>;
    async fn checkpoint(&mut self) -> anyhow::Result<()>;
    /// Runs a WAL compaction to completion (Task 2.15).
    async fn compact(&mut self) -> anyhow::Result<()> {
        Err(Unsupported("compact").into())
    }
    /// Cuts power (everything not synced may be lost or torn per `tear`,
    /// with `fault_seed` driving the fault injector's choices), then reopens.
    /// Only SUTs on a simulated filesystem can do this.
    async fn power_loss(&mut self, _tear: Tear, _fault_seed: u64) -> anyhow::Result<()> {
        Err(Unsupported("power_loss").into())
    }
    /// How many of the most recently acknowledged mutations the SUT itself
    /// says are not yet durable (`0` = everything acknowledged is durable).
    /// The runner moves every older pending mutation into the model's durable
    /// state after each op, so the Fast check's lower bound follows the SUT's
    /// own sync points (close, open, segment roll) and not only the ones the
    /// model can see. `None` (the default): the SUT cannot tell, and only
    /// `Reopen`/`Checkpoint` (and, in Durable mode, every ack) mark durability.
    fn unsynced_acked(&self) -> Option<u64> {
        None
    }
    /// The number of WAL segment files, if the SUT can tell. Lets a run prove
    /// that it crossed segment boundaries (see `Report::segment_rolls`).
    fn segment_count(&self) -> Option<usize> {
        None
    }
}

pub struct WalSut {
    dir: PathBuf,
    _tmp: tempfile::TempDir,
    db: Option<WalStorageAdapter>,
}

fn config(dir: &std::path::Path) -> WalConfig {
    WalConfig {
        log_dir: dir.to_path_buf(),
        ..WalConfig::test_config()
    }
}

impl WalSut {
    pub async fn new() -> anyhow::Result<Self> {
        let tmp = tempfile::tempdir()?;
        let dir = tmp.path().to_path_buf();
        let db = WalStorageAdapter::new(config(&dir))?;
        Ok(Self {
            dir,
            _tmp: tmp,
            db: Some(db),
        })
    }
    fn db(&self) -> &WalStorageAdapter {
        self.db.as_ref().expect("open")
    }
    async fn open(&mut self) -> anyhow::Result<()> {
        self.db = Some(WalStorageAdapter::open_async(config(&self.dir)).await?);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Sut for WalSut {
    async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()> {
        Ok(self.db().put(k, v).await?)
    }
    async fn delete(&mut self, k: &Key) -> anyhow::Result<()> {
        Ok(self.db().delete(k).await?)
    }
    async fn get(&mut self, k: &Key) -> anyhow::Result<Option<Value>> {
        Ok(self.db().get(k).await?)
    }
    async fn reopen(&mut self) -> anyhow::Result<()> {
        self.db().flush().await?;
        self.db = None;
        self.open().await
    }
    async fn crash(&mut self) -> anyhow::Result<()> {
        // In-process "crash": drop without an explicit flush. Drop now closes the
        // log, which syncs; this is a clean process exit, and power loss is
        // `PowerLoss` from Task 2.10b. Real process-kill coverage comes from the
        // SIGKILL test (Task 1.10); do not read a green run as more.
        self.db = None;
        self.open().await
    }
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        self.db().flush().await?;
        Ok(self.db().save_checkpoint_async().await?)
    }
    async fn compact(&mut self) -> anyhow::Result<()> {
        self.db().compact().await?;
        Ok(())
    }
}

/// Where `FaultSut` keeps its log on the simulated filesystem.
const FAULT_LOG_DIR: &str = "/db/wal";

/// Segment size for `FaultSut`. A harness frame is ~50 bytes (17-byte frame
/// header plus a one-op batch), so a 512-byte segment holds ~9 frames after
/// its 24-byte header: a default 80-op sequence rolls several times, and
/// recovery and power loss keep crossing segment boundaries.
const FAULT_SEGMENT_BYTES: u64 = 512;

/// The WAL adapter on [`FaultFs`] (Durable or Fast), for power-loss testing.
///
/// In Fast mode the periodic sync timer is pinned off (an hour), so nothing
/// depends on a timer firing and every seed is deterministic. The remaining
/// syncs (explicit flushes, and the WAL's own on open, close and segment roll)
/// are reported to the runner through [`Sut::unsynced_acked`], from the
/// adapter's `max_offset()` (last acknowledged LSN) and `durable_lsn()`. That
/// mapping holds because every `put`/`delete` writes exactly one frame and
/// nothing else writes frames in these profiles; `put`/`delete` check it and
/// fail loudly if it ever stops holding.
pub struct FaultSut {
    fs: FaultFs,
    mode: Mode,
    db: Option<WalStorageAdapter>,
}

impl FaultSut {
    pub async fn new(mode: Mode) -> anyhow::Result<Self> {
        let fs = FaultFs::new();
        fs.mkdir_durable(Path::new("/db"))?;
        let mut sut = Self { fs, mode, db: None };
        sut.open().await?;
        Ok(sut)
    }

    fn config(&self) -> StorageConfig {
        let (sync_mode, sync_interval_ms) = match self.mode {
            Mode::Durable => (SyncMode::Durable, WalConfig::test_config().sync_interval_ms),
            // Explicit syncs only: deterministic (see the type docs).
            Mode::Fast => (SyncMode::Fast, 3_600_000),
        };
        StorageConfig {
            wal: WalConfig {
                log_dir: PathBuf::from(FAULT_LOG_DIR),
                segment_bytes: FAULT_SEGMENT_BYTES,
                sync_mode,
                sync_interval_ms,
                ..WalConfig::test_config()
            },
            ..StorageConfig::default()
        }
    }

    async fn open(&mut self) -> anyhow::Result<()> {
        let config = self.config();
        let vfs = Arc::new(self.fs.clone());
        let db = tokio::task::spawn_blocking(move || WalStorageAdapter::open_with_vfs(config, vfs))
            .await??;
        self.db = Some(db);
        Ok(())
    }

    fn db(&self) -> &WalStorageAdapter {
        self.db.as_ref().expect("open")
    }

    /// Fails unless the mutation just acknowledged wrote exactly one frame:
    /// [`Sut::unsynced_acked`] counts frames, and the model counts mutations.
    fn one_frame_since(&self, before: u64) -> anyhow::Result<()> {
        let after = self.db().max_offset();
        if after != before + 1 {
            anyhow::bail!(
                "FaultSut frame accounting broke: one mutation moved the log from LSN \
                 {before} to {after}; unsynced_acked would mislead the checker"
            );
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Sut for FaultSut {
    async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()> {
        let before = self.db().max_offset();
        self.db().put(k, v).await?;
        self.one_frame_since(before)
    }
    async fn delete(&mut self, k: &Key) -> anyhow::Result<()> {
        let before = self.db().max_offset();
        self.db().delete(k).await?;
        self.one_frame_since(before)
    }
    async fn get(&mut self, k: &Key) -> anyhow::Result<Option<Value>> {
        Ok(self.db().get(k).await?)
    }
    async fn reopen(&mut self) -> anyhow::Result<()> {
        self.db().flush().await?;
        self.db = None;
        self.open().await
    }
    async fn crash(&mut self) -> anyhow::Result<()> {
        // A process exit: written data stays in FaultFs's page cache (and the
        // drop closes the log, which syncs). Power loss is `power_loss`.
        self.db = None;
        self.open().await
    }
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        Ok(self.db().save_checkpoint_async().await?)
    }
    async fn compact(&mut self) -> anyhow::Result<()> {
        self.db().compact().await?;
        Ok(())
    }
    async fn power_loss(&mut self, tear: Tear, fault_seed: u64) -> anyhow::Result<()> {
        // The loss comes first: the adapter's handles are stale afterwards, so
        // the sync its drop attempts fails harmlessly and cannot make anything
        // durable after the fact.
        self.fs
            .power_loss(&mut ChaCha8Rng::seed_from_u64(fault_seed), tear);
        self.db = None;
        self.open().await
    }
    fn unsynced_acked(&self) -> Option<u64> {
        // Read durable first: it only grows, so a sync landing between the two
        // loads can only make the answer larger (conservative), never smaller.
        let db = self.db.as_ref()?;
        let durable = db.durable_lsn();
        Some(db.max_offset().saturating_sub(durable))
    }
    fn segment_count(&self) -> Option<usize> {
        let entries = self.fs.read_dir(Path::new(FAULT_LOG_DIR)).ok()?;
        Some(
            entries
                .iter()
                .filter_map(|p| p.file_name()?.to_str())
                .filter(|name| parse_segment_file_name(name).is_some())
                .count(),
        )
    }
}
