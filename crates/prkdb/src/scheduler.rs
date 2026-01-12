use crate::compute::StatefulCompute;
use crate::outbox::snapshot_and_compact;
use crate::PrkDb;
use prkdb_types::collection::Collection;
use rand::{thread_rng, Rng};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::info;

pub fn spawn_snapshot_job<C, H>(
    db: PrkDb,
    handler: H,
    interval: Duration,
    jitter: Duration,
) -> JoinHandle<()>
where
    C: Collection + serde::de::DeserializeOwned + serde::Serialize + Clone + Send + Sync + 'static,
    C::Id: serde::de::DeserializeOwned + serde::Serialize + Clone + Send + Sync + 'static,
    H: StatefulCompute<C, PrkDb> + Send + Sync + 'static,
{
    tokio::spawn(async move {
        loop {
            let j = thread_rng().gen_range(Duration::from_millis(0)..=jitter);
            tokio::time::sleep(interval + j).await;
            if let Err(e) = snapshot_and_compact::<C, _>(&db, &handler).await {
                info!(error = %e, "snapshot_and_compact failed");
            }
        }
    })
}
