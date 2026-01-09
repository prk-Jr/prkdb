// PERFORMANCE OPTIMIZATION: Use mimalloc for 10-15% faster allocations
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod batch_accumulator;
pub mod builder;
pub mod cache; // LRU cache layer
pub mod client;
pub mod collection_handle;
pub mod compute; // Stub module
pub mod consumer;
pub mod dashboard;
pub mod db;
mod error;
pub mod indexed_storage; // Secondary index support
pub mod joins;
mod metrics;
pub mod observability;
pub mod outbox;
pub mod partitioning;
pub mod pipeline;
pub mod prometheus_metrics; // Prometheus metrics for Raft and system health
pub mod raft;
pub mod rate_limit; // Rate limiting for operations
pub mod replication;
pub mod scheduler;
pub mod storage;
mod storage_old_inmemory; // Renamed from storage.rs to allow storage/ directory
pub mod streaming;
pub mod transaction;
pub mod ttl;
pub mod windowing;

pub use db::PrkDb;
pub use error::DbError;

pub mod prelude {
    pub use crate::builder::Builder;
    pub use crate::compute::{ComputeHandler, StatefulCompute};
    pub use crate::consumer::*;
    pub use crate::db::PrkDb;
    pub use crate::indexed_storage::{ChangeEvent, IndexedStorage, IndexedWalStorage};
    pub use crate::partitioning::{DefaultPartitioner, Partitioner};
    pub use crate::pipeline::{Pipeline, PipelineMetrics, Sink, Source};
    pub use crate::transaction::{
        IsolationLevel, Transaction, TransactionConfig, TransactionError, TransactionExt,
        TransactionStatus,
    };
    pub use crate::ttl::TtlStorage;
    pub use prkdb_core::collection::Collection;
    pub use prkdb_core::index::{IndexDef, Indexed};
    pub use prkdb_core::storage::StorageAdapter;
    pub use prkdb_macros::Collection;
}

/// Doc example: stateful compute with replay
///
/// ```rust
/// use prkdb::prelude::*;
/// use prkdb::outbox::replay_collection;
/// use prkdb::compute::{Context, StatefulCompute};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
/// struct E { #[id] id: u64, v: i32 }
///
/// #[derive(Serialize, Deserialize, Default)]
/// struct S { sum: i32 }
///
/// struct H;
///
/// #[async_trait::async_trait]
/// impl StatefulCompute<E, PrkDb> for H {
///   type State = S;
///   fn state_key(&self) -> String { "ex:sum".to_string() }
///   fn init_state(&self) -> Self::State { S::default() }
///   async fn on_put(&self, e: &E, s: &mut S, _ctx: &Context<PrkDb>) -> Result<(), prkdb_core::error::ComputeError> { s.sum += e.v; Ok(()) }
///   async fn on_delete(&self, _id: &u64, _s: &mut S, _ctx: &Context<PrkDb>) -> Result<(), prkdb_core::error::ComputeError> { Ok(()) }
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let db = PrkDb::builder().with_storage(prkdb::storage::InMemoryAdapter::new()).register_collection::<E>().build().unwrap();
/// let c = db.collection::<E>();
/// c.put(E { id: 1, v: 2 }).await.unwrap();
/// c.put(E { id: 2, v: 3 }).await.unwrap();
/// let h = H;
/// replay_collection::<E, _>(&db, &h).await.unwrap();
/// // state persisted under key; this is just a compilation example
/// # }
/// ```
pub use crate::storage::InMemoryAdapter;

#[cfg(test)]
mod tests {
    use crate::storage::InMemoryAdapter;
    use prkdb_core::storage::StorageAdapter;

    #[tokio::test]
    async fn in_memory_adapter_put_get() {
        let adapter = InMemoryAdapter::new();
        adapter.put(b"key1", b"value1").await.unwrap();
        let val = adapter.get(b"key1").await.unwrap().unwrap();
        assert_eq!(val, b"value1");
    }
}
