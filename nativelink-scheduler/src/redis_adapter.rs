#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]
use std::ops::Deref;
use std::sync::Arc;

use crate::scheduler_state::{ActionFields, ActionMaps, ActionSchedulerStateStore};
use crate::worker::WorkerUpdate;
use async_trait::async_trait;
use futures::future::Ready;
use futures::{Future, FutureExt, StreamExt};
use nativelink_error::{make_input_err, Code, Error, ResultExt};
use nativelink_proto::google::longrunning::{operation, Operation};
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionName, ActionResult, ActionStage, ActionState, OperationId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::platform_properties::{self, PlatformProperties, PlatformPropertyValue};
use prost::Message;
use redis::aio::{MultiplexedConnection, PubSub};
use redis::{
    AsyncCommands, AsyncIter, Client, Commands, Connection, FromRedisValue, Pipeline, RedisError,
    RedisResult, ToRedisArgs,
};
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use uuid::Uuid;

pub struct RedisAdapter {
    pub client: redis::Client,
}

impl RedisAdapter {
    pub fn new(url: String) -> Self {
        Self {
            client: Client::open(url).unwrap(),
        }
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
impl ActionSchedulerStateStore for RedisAdapter {
    fn dec_action_attempts(&self, operation_id: &OperationId) -> Result<u64, Error> {
        let key = ActionFields::Attempts;
        let mut con = self.get_connection()?;
        // let mut con = &self.().await?;
        let (attempts,): (u64,) = redis::transaction(&mut con, &[key.clone()], |con, pipe| {
            let old_val: isize = con.hget(&key, &operation_id)?;
            pipe.set(&key, old_val - 1).ignore().get(&key).query(con)
        })?;
        Ok(attempts)
    }

    fn inc_action_attempts(&self, operation_id: &OperationId) -> Result<u64, Error> {
        let key = ActionFields::Attempts;
        let mut con = self.get_connection()?;
        // let mut con = &self.().await?;
        let (attempts,): (u64,) = redis::transaction(&mut con, &[key.clone()], |con, pipe| {
            let old_val: isize = con.hget(&key, &operation_id)?;
            pipe.set(&key, old_val + 1).ignore().get(&key).query(con)
        })?;
        Ok(attempts)
    }

    async fn set_action_assignment(
        &self,
        _operation_id: OperationId,
        _assigned: bool,
    ) -> Result<(), Error> {
        let mut con = self.get_multiplex_connection().await?;
        // let fut = {
        //     if assigned {
        //         return con.hset(ActionStage, operation_id, ActionStage::Executing)
        //     else {
        //         return con.hdel(ActionStage, operation_id, ActionStage::Queued)
        //     }
        // }
        Ok(())
    }

    async fn subscribe<'a>(
        &'a self,
        key: &'a str,
        initial_state: Option<Arc<ActionState>>,
    ) -> Result<watch::Receiver<Arc<ActionState>>, nativelink_error::Error> {
        let mut sub = self.get_async_pubsub().await?;
        let id = OperationId::try_from(key)?;
        let state = {
            match initial_state {
                Some(v) => v,
                None => Arc::new(self.get_action_state(id).await?),
            }
        };
        // sub.subscribe(&key).await.unwrap();
        sub.subscribe(&key).await.unwrap();
        let mut stream = sub.into_on_message();
        // This hangs forever atm
        let (tx, rx) = tokio::sync::watch::channel(state);
        println!("recv count {:?}", tx.receiver_count());
        // Hand tuple of rx and future to pump the rx
        tokio::spawn(async move {
            let closed_fut = tx.closed();
            tokio::pin!(closed_fut);

            loop {
                tokio::select! {
                    msg = stream.next() => {
                        println!("got message");

                        let state: ActionState = msg.unwrap().get_payload().unwrap();
                        let finished = state.stage.is_finished();
                        let value = Arc::new(state);
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
        // rx.mark_changed();
        Ok(rx)
    }

    async fn get_operation_id_for_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Result<OperationId, Error> {
        let name = ActionName::from(unique_qualifier);
        let mut con = self.get_multiplex_connection().await?;
        let id = con.get::<ActionName, OperationId>(name).await?;
        Ok(id)
    }

    async fn update_action_stages(
        &self,
        operations: &[(OperationId, ActionStage)],
    ) -> Result<(), Error> {
        // Currently not removing from queued when assigned and set to executing
        let mut con = self.get_multiplex_connection().await?;
        let mut pipe = Pipeline::new();
        pipe.atomic()
            .hset_multiple(ActionFields::Stage, operations)
            .query_async(&mut con)
            .await?;
        for (id, _) in operations.iter() {
            // self.publish_action_state(*id).await?;
            self.publish_action_state(*id).await?;
        }
        Ok(())
    }

    async fn get_action_state(&self, id: OperationId) -> Result<ActionState, Error> {
        let mut con = self.get_multiplex_connection().await?;
        let mut pipe = Pipeline::new();
        let (stage, action_digest) = pipe
            .atomic()
            .hget(ActionFields::Stage, id)
            .hget(ActionFields::Digest, id)
            .query_async(&mut con)
            .await?;
        Ok(ActionState {
            operation_id: id,
            stage,
            action_digest,
        })
    }
    async fn publish_action_state(&self, id: OperationId) -> Result<(), Error> {
        let mut con = self.get_multiplex_connection().await?;
        // let keys = vec![ActionFields::Stage(id), ActionFields::Digest, id];
        let stage = con.hget(ActionFields::Stage, id).await?;
        let action_digest = con.hget(ActionFields::Digest, id).await?;
        // let (stage, action_digest) = con.mget(keys).await?;
        let action_state = ActionState {
            operation_id: id,
            stage,
            action_digest,
        };
        let var = id.to_string();
        println!("{:?}", var);
        let res = con.publish(var, action_state).await?;
        println!("Published inner: {:?}", res);
        Ok(())
    }

    async fn create_new_action(&self, action_info: &ActionInfo) -> Result<OperationId, Error> {
        let mut con = self.get_multiplex_connection().await?;
        let id = OperationId::new();
        let name = ActionName::from(action_info);
        let mut pipe = Pipeline::new();
        let (_, id): (redis::Value, OperationId) = pipe
            .atomic()
            .set(&name, id)
            .hset(ActionFields::Info, id, action_info)
            .ignore()
            .hset(
                ActionFields::Digest,
                id,
                action_info.unique_qualifier.digest,
            )
            .ignore()
            .hset(ActionFields::Stage, id, ActionStage::Queued)
            .ignore()
            .hset(ActionFields::Name, id, &name)
            .ignore()
            .hset(ActionFields::Attempts, id, 0)
            .ignore()
            .zadd(ActionMaps::Queued, id, action_info.priority)
            .ignore()
            .get(&name)
            .query_async(&mut con)
            .await?;
        Ok(id)
    }

    async fn find_action_by_hash_key(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Result<Option<watch::Receiver<Arc<ActionState>>>, Error> {
        let mut con = self.get_multiplex_connection().await?;
        let name = ActionName::from(unique_qualifier);
        let Some(operation_id) = con.get::<&ActionName, Option<OperationId>>(&name).await? else {
            return Ok(None);
        };
        let v = self
            .subscribe(&operation_id.to_string(), None)
            .await
            .unwrap();
        Ok(Some(v))
    }

    async fn get_action_info_for_actions(
        &self,
        actions: &[OperationId],
    ) -> Result<Vec<(OperationId, ActionInfo)>, Error> {
        if actions.is_empty() {
            return Ok(Vec::new());
        }
        let mut con = self.get_multiplex_connection().await?;
        println!("{:?}", actions);
        let infos: Vec<ActionInfo> = con.hget(ActionFields::Info, actions).await?;
        // clone here so that we aren't dealing with lifetime issues for POC
        Ok(std::iter::zip(actions.to_owned(), infos).collect())
    }

    async fn get_queued_actions(&self) -> Result<Vec<OperationId>, Error> {
        let mut con = self.get_multiplex_connection().await?;
        Ok(con.zrange(ActionMaps::Queued, 0, -1).await?)
    }

    async fn add_actions_to_queue(
        &self,
        actions_with_priority: &[(OperationId, i32)],
    ) -> Result<(), Error> {
        let mut con = self.get_multiplex_connection().await?;
        Ok(con
            .zadd_multiple(ActionMaps::Queued, actions_with_priority)
            .await?)
    }

    async fn remove_actions_from_queue(&self, actions: &[OperationId]) -> Result<(), Error> {
        let mut con = self.get_multiplex_connection().await?;
        Ok(con.zrem(ActionMaps::Queued, actions).await?)
    }
    // Return the action stage here.
    // If the stage is ActionStage::Completed the callee
    // should request the stored result directly.
    // Otherwise, the callee should call get_action_subscriber
    // and listen to it for updates to find out when its state changes
    async fn add_or_merge_action(
        &self,
        action_info: &ActionInfo,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        let mut con = self.get_multiplex_connection().await?;
        let name = ActionName::from(action_info);
        let existing_id: Option<OperationId> = con.hget(ActionFields::Id, &name).await?;
        let (operation_id, stage) = match existing_id {
            Some(id) => {
                let stage: ActionStage = con.hget(ActionFields::Stage, id).await?;
                if stage == ActionStage::Queued {
                    // Update priority if new priority is higher
                    redis::cmd("ZADD")
                        .arg(ActionMaps::Queued)
                        .arg("LT")
                        .arg(action_info.priority)
                        .arg(id)
                        .query_async(&mut con)
                        .await?
                }
                (id, stage)
            }
            None => (
                self.create_new_action(action_info).await?,
                ActionStage::Queued,
            ),
        };
        let state = Arc::new(ActionState {
            action_digest: *action_info.digest(),
            operation_id,
            stage,
        });
        let v = self
            .subscribe(&operation_id.to_string(), Some(state))
            .await
            .unwrap();
        Ok(v)
    }
}
