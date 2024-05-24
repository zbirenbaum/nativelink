use std::sync::Arc;
use std::time::SystemTime;
use tonic::async_trait;
use redis_macros::{FromRedisValue, ToRedisArgs};
use futures::{Stream, StreamExt};
use nativelink_util::action_messages::{ActionResult, ActionStage, ActionState, Id};
use redis::aio::{MultiplexedConnection, PubSub};
use redis::{Client, Connection, Pipeline, ToRedisArgs};
use nativelink_error::Error;
use tokio::sync::watch;
use serde::{Serialize, Deserialize};

use crate::type_wrappers::{OperationStageFlags, RedisDigestHasherFunc, RedisActionInfo, RedisPlatformProperties, RedisPlatformPropertyValue};
use crate::operation_state_manager::{ActionStateResult, MatchingEngineStateManager, OperationFilter};

pub struct RedisActionState {
    client: Client,
    inner: RedisActionStateImpl,
}

#[derive(Clone, Serialize, Deserialize, FromRedisValue, ToRedisArgs)]
pub struct RedisActionStateImpl {
    id: Id,
    stage_flag: OperationStageFlags,
    result: Option<ActionResult>,
}

impl TryFrom<RedisActionStateImpl> for ActionState {
    type Error = Error;
    fn try_from(value: RedisActionStateImpl) -> Result<Self, Self::Error> {
        Ok(ActionState {
            id: value.id,
            stage: value.stage_flag.to_action_stage(value.result)?
        })
    }
}

impl RedisActionState {
    async fn subscribe<'a>(
        &'a self,
        client: &'a Client
    ) -> Result<watch::Receiver<Arc<ActionState>>, nativelink_error::Error> {
        let mut sub = client.get_async_pubsub().await?;
        let sub_channel = format!("{}:*", &self.inner.id.unique_qualifier.action_name());
        // Subscribe to action name: any completed operation can return status
        sub.subscribe(sub_channel).await.unwrap();
        let mut stream = sub.into_on_message();
        let action_state: ActionState = self.inner.clone().try_into()?;
        // let arc_action_state: Arc<ActionState> = Arc::new();
        // This hangs forever atm
        let (tx, rx) = tokio::sync::watch::channel(Arc::new(action_state));
        // Hand tuple of rx and future to pump the rx
        // Note: nativelink spawn macro name field doesn't accept variables so for now we have to use this to avoid conflicts.
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            let closed_fut = tx.closed();
            tokio::pin!(closed_fut);

            loop {
                tokio::select! {
                    msg = stream.next() => {
                        println!("got message");

                        let state: RedisActionStateImpl = msg.unwrap().get_payload().unwrap();
                        let finished = state.stage_flag.has_action_result();
                        let value: Arc<ActionState> = Arc::new(state.try_into().unwrap());
                        if tx.send(value).is_err() {
                            println!("Error sending value");
                            return;
                        }
                        if finished {
                            return;
                        }
                    }
                    _  = &mut closed_fut => {
                        println!("Future closed");
                        return
                    }
                }

            }
        });
        Ok(rx)
    }
}

#[async_trait]
impl ActionStateResult for RedisActionState {
    fn as_state(&self) -> Result<Arc<ActionState>, Error> {
        Ok(Arc::new(self.inner.clone().try_into()?))
    }
    async fn as_receiver(&self) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        self.subscribe(&self.client).await
    }
}

pub struct RedisManager {
    pub client: redis::Client,
}

impl RedisManager {
    pub fn new(url: String) -> Self {
        Self { client: Client::open(url).unwrap() }
    }

    async fn get_async_pubsub(&self) -> Result<PubSub, Error> {
        Ok(self.client.get_async_pubsub().await?)
    }

    // These getters avoid mapping errors everywhere and just use the ? operator.
    async fn get_multiplex_connection(&self) -> Result<MultiplexedConnection, Error> {
        Ok(self.client.get_multiplexed_tokio_connection().await?)
    }

    // These getters avoid mapping errors everywhere and just use the ? operator.
    fn get_connection(&self) -> Result<Connection, Error> {
        Ok(self.client.get_connection()?)
    }
}

#[async_trait]
impl MatchingEngineStateManager for RedisManager {
    /// Returns a stream of operations that match the filter.
    async fn filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Box<dyn Stream<Item = dyn ActionStateResult>> {
        let mut con = self.get_multiplex_connection().await.unwrap();
        let mut time_filter_keys: Vec<(String, SystemTime)> = Vec::new();
        todo!()

    }


    /// Update that state of an operation.
    async fn update_operation(
        &self,
        operation_id: Id,
        worker_id: Option<Id>,
        action_stage: ActionStage,
    ) -> Result<(), Error> {
        todo!()
    }

    /// Remove an operation from the state manager.
    /// It is important to use this function to remove operations
    /// that are no longer needed to prevent memory leaks.
    async fn remove_operation(
        &self,
        operation_id: Id,
    ) -> Result<(), Error> {
        todo!()
    }
}
