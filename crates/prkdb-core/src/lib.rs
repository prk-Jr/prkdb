//! # prkdb-core
//!
//! Core implementation crate for PrkDB.
//!
//! This crate provides the implementation of storage, WAL, replication, and other
//! core functionality. It builds on top of `prkdb-types` which provides the
//! foundational domain types and traits.
//!
//! ## Re-exports from prkdb-types
//!
//! For backward compatibility, this crate re-exports all types from `prkdb-types`.
//! New code should prefer importing directly from `prkdb-types` when possible.

// Implementation modules
pub mod batch_config;
pub mod batching;
pub mod buffer_pool; // Phase 5.2: Buffer pooling for serialization
pub mod replication;
pub mod serialization;
pub mod wal;

// Re-export prkdb-types modules for usage within prkdb-core only if needed,
// but we are removing public re-exports as part of cleanup.

pub mod compute {
    //! Compute handler types.
    //! Note: The compute traits depend on Collection which now comes from prkdb-types.
    use prkdb_types::collection::Collection;
    use prkdb_types::error::ComputeError;

    use async_trait::async_trait;
    use serde::{de::DeserializeOwned, Serialize};

    pub struct Context<S> {
        pub db: S,
    }

    #[async_trait]
    pub trait ComputeHandler<T: Collection, S: 'static>: Send + Sync {
        async fn on_put(&self, item: &T, ctx: &Context<S>) -> Result<(), ComputeError>;
        async fn on_delete(&self, id: &T::Id, ctx: &Context<S>) -> Result<(), ComputeError>;
    }

    /// Stateful compute handler: maintains and persists its own state and supports replay.
    #[async_trait]
    pub trait StatefulCompute<T: Collection, S: 'static>: Send + Sync {
        type State: Serialize + DeserializeOwned + Send + Sync + 'static;

        /// Key used to persist state bytes (namespaced by type and this handler).
        fn state_key(&self) -> Vec<u8>;

        /// Initialize a fresh state when none is persisted yet.
        fn init_state(&self) -> Self::State;

        async fn on_put(
            &self,
            item: &T,
            state: &mut Self::State,
            ctx: &Context<S>,
        ) -> Result<(), ComputeError>;
        async fn on_delete(
            &self,
            id: &T::Id,
            state: &mut Self::State,
            ctx: &Context<S>,
        ) -> Result<(), ComputeError>;
    }
}
