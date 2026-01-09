use async_trait::async_trait;
use prkdb_core::collection::Collection;
use prkdb_core::error::ComputeError;
use serde::{de::DeserializeOwned, Serialize};

pub struct Context<Db> {
    pub db: Db,
}

#[async_trait]
pub trait ComputeHandler<C: Collection, Db>: Send + Sync {
    async fn on_put(&self, item: &C, ctx: &Context<Db>) -> Result<(), ComputeError>;
    async fn on_delete(&self, id: &C::Id, ctx: &Context<Db>) -> Result<(), ComputeError>;
}

#[async_trait]
pub trait StatefulCompute<C: Collection, Db>: Send + Sync {
    type State: Serialize + DeserializeOwned + Send + Sync;

    fn init_state(&self) -> Self::State;
    fn state_key(&self) -> String;

    async fn on_put(
        &self,
        item: &C,
        state: &mut Self::State,
        ctx: &Context<Db>,
    ) -> Result<(), ComputeError>;
    async fn on_delete(
        &self,
        id: &C::Id,
        state: &mut Self::State,
        ctx: &Context<Db>,
    ) -> Result<(), ComputeError>;
}

// Default implementations for unit type
#[async_trait]
impl<C: Collection, Db: Send + Sync> ComputeHandler<C, Db> for () {
    async fn on_put(&self, _item: &C, _ctx: &Context<Db>) -> Result<(), ComputeError> {
        Ok(())
    }
    async fn on_delete(&self, _id: &C::Id, _ctx: &Context<Db>) -> Result<(), ComputeError> {
        Ok(())
    }
}

#[async_trait]
impl<C: Collection, Db: Send + Sync> StatefulCompute<C, Db> for () {
    type State = ();

    fn init_state(&self) -> Self::State {
        ()
    }
    fn state_key(&self) -> String {
        "stub".to_string()
    }

    async fn on_put(
        &self,
        _item: &C,
        _state: &mut Self::State,
        _ctx: &Context<Db>,
    ) -> Result<(), ComputeError> {
        Ok(())
    }
    async fn on_delete(
        &self,
        _id: &C::Id,
        _state: &mut Self::State,
        _ctx: &Context<Db>,
    ) -> Result<(), ComputeError> {
        Ok(())
    }
}
