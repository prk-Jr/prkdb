use crate::collection::Collection;
use crate::error::ComputeError;
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
