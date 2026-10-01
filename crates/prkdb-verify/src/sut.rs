//! Drives the real embedded storage through its public API.

use crate::faultfs::{FaultFs, Tear};
use crate::model::{Key, Mode, Value};
use prkdb::storage::config::StorageConfig;
use prkdb::storage::WalStorageAdapter;
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
    /// Cuts power (everything not synced may be lost or torn per `tear`,
    /// with `fault_seed` driving the fault injector's choices), then reopens.
    /// Only SUTs on a simulated filesystem can do this.
    async fn power_loss(&mut self, _tear: Tear, _fault_seed: u64) -> anyhow::Result<()> {
        Err(Unsupported("power_loss").into())
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
        Ok(self.db().save_checkpoint()?)
    }
}

/// Where `FaultSut` keeps its log on the simulated filesystem.
const FAULT_LOG_DIR: &str = "/db/wal";

/// The WAL adapter on [`FaultFs`] (Durable or Fast), for power-loss testing.
///
/// In Fast mode the periodic sync timer is pinned off (an hour), so the only
/// syncs are the ones the model sees (`Reopen`, `Checkpoint`) plus the WAL's
/// own syncs on open, close and segment roll. Those extra syncs only make more
/// data durable than the model assumes, so the checker's lower bound stays
/// conservative: it can miss a lost write the WAL had synced on its own, but
/// never reports a false finding. And nothing depends on a timer firing, so
/// every seed is deterministic.
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
                // Small segments roll often, so recovery crosses segment
                // boundaries and a power loss can land right after a roll.
                segment_bytes: 16 * 1024,
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
}

#[async_trait::async_trait]
impl Sut for FaultSut {
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
        // A process exit: written data stays in FaultFs's page cache (and the
        // drop closes the log, which syncs). Power loss is `power_loss`.
        self.db = None;
        self.open().await
    }
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        Ok(self.db().save_checkpoint()?)
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
}
