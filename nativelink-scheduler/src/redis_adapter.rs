#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]
use std::ops::Deref;
use std::sync::Arc;

use futures::future::Ready;
use futures::{Future, FutureExt, StreamExt};
use nativelink_error::{make_input_err, Code, ResultExt, Error};
use nativelink_proto::google::longrunning::{operation, Operation};
use nativelink_util::common::DigestInfo;
use nativelink_util::platform_properties::{self, PlatformProperties, PlatformPropertyValue};
use prost::Message;
use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey, ActionName, ActionResult, ActionStage, ActionState, OperationId, WorkerId };
use redis::aio::{MultiplexedConnection, PubSub};
use redis::{ AsyncCommands, AsyncIter, Client, Commands, Connection, FromRedisValue, Pipeline, RedisError, RedisResult, ToRedisArgs };
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use uuid::Uuid;
use crate::scheduler_state::{ActionFields, ActionMaps, ActionSchedulerStateStore, WorkerFields, WorkerSchedulerStateStore};
use crate::worker::WorkerUpdate;
use async_trait::async_trait;

pub struct RedisAdapter {
    pub client: redis::Client,
}

impl RedisAdapter {
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
impl WorkerSchedulerStateStore for RedisAdapter {
    // Create a worker Id and map it to the name in the config. Worker can then always resolve its id

    async fn get_actions_running_on_worker(
        &self,
        worker_id: &WorkerId
    ) -> Result<Vec<OperationId>, Error> {
        let mut con = self.get_multiplex_connection().await?;
        Ok(con.smembers(WorkerFields::RunningOperations(worker_id.to_owned())).await?)
    }

    async fn assign_actions_to_worker(
        &self,
        worker_id: &WorkerId,
        operation_ids: Vec<OperationId>
    ) -> Result<Vec<OperationId>, Error> {
        let mut con = self.get_multiplex_connection().await?;
        let mut pipe = Pipeline::new();
        let action_kv: Vec<(ActionMaps, &WorkerId)> = operation_ids.iter().map(|id| {
            (ActionMaps::Assigned(*id), worker_id)
        }).collect();
        pipe
            .atomic()
            .sadd(WorkerFields::RunningOperations(*worker_id), operation_ids)
            .mset(&action_kv);
        Ok(pipe.query_async(&mut con).await?)
    }

    async fn remove_actions_from_worker(
        &self,
        worker_id: &WorkerId,
        operation_ids: Vec<OperationId>,
    ) -> Result<(), Error> {
        let mut con = self.get_multiplex_connection().await?;
        let mut pipe = Pipeline::new();
        let action_k: Vec<ActionMaps> = operation_ids.iter().map(|id| {
            ActionMaps::Assigned(*id)
        }).collect();
        pipe
            .atomic()
            .srem(WorkerFields::RunningOperations(*worker_id), operation_ids)
            .del(&action_k);
        Ok(pipe.query_async(&mut con).await?)
    }

    async fn get_worker_running_action(
        &self,
        operation_id: &OperationId
    ) -> Result<Option<WorkerId>, Error> {
        let mut con = self.get_multiplex_connection().await?;
        Ok(con.get(ActionMaps::Assigned(*operation_id)).await?)
    }
}

#[async_trait]
impl ActionSchedulerStateStore for RedisAdapter {
    fn inc_action_attempts(
        &self,
        operation_id: &OperationId
    ) -> Result<u64, Error> {
        let key = ActionFields::Attempts(*operation_id);
        let mut con = self.get_connection()?;
        // let mut con = &self.().await?;
        let (attempts,) : (u64,) = redis::transaction(&mut con, &[key.clone()], |con, pipe| {
            let old_val : isize = con.get(&key)?;
            pipe
                .set(&key, old_val + 1).ignore()
                .get(&key).query(con)
        })?;
        Ok(attempts)
    }

    //TODO: This really needs to be atomic and in a tx but key watching is a mess
    async fn complete_action(
        &self,
        operation_id: &OperationId,
        result: ActionResult
    ) -> Result<(), Error> {
        let mut con = self.get_multiplex_connection().await?;
        // let mut con = &self.().await?;
        if let Some(worker_id) = self.get_worker_running_action(operation_id).await? {
            self.remove_actions_from_worker(&worker_id, vec![*operation_id]).await?;
        }
        // self.update_action_stage(operation_id, ActionStage::Completed(result)).await?;

        let mut pipe = Pipeline::new();
        // This is really inefficient but for POC don't need to keep track of which state its in.
        pipe
            .zrem(ActionMaps::Queued, operation_id)
            .del(ActionMaps::Assigned(*operation_id))
            .set(ActionMaps::Completed(*operation_id), result)
            .query_async(&mut con).await?;
        Ok(())

    }

    async fn subscribe<'a>(
        &'a self,
        key: &'a str,
        initial_state: Option<Arc<ActionState>>
    ) -> Result<watch::Receiver<Arc<ActionState>>, nativelink_error::Error> {
        let mut sub = self.get_async_pubsub().await?;
        let id = OperationId::try_from(key)?;
        let state = {
            match initial_state {
                Some(v) => v,
                None => Arc::new(self.get_action_state(id).await?)
            }
        };
        sub.subscribe(&key).await.unwrap();
        let mut stream = sub.into_on_message();
        // This hangs forever atm
        let (tx, mut rx) = tokio::sync::watch::channel(state);
        // Hand tuple of rx and future to pump the rx
        tokio::spawn(async move {
            let closed_fut = tx.closed();
            tokio::pin!(closed_fut);

            loop {
                tokio::select! {
                    msg = stream.next() => {
                        let state: ActionState = msg.unwrap().get_payload().unwrap();
                        let value = Arc::new(state);
                        if tx.send(value).is_err() {
                            return
                        }
                    }
                    _  = &mut closed_fut => {
                        println!("Future closed");
                        return
                    }
                }

            }
        });
        rx.mark_changed();
        Ok(rx)
    }

    async fn get_operation_id_for_action(
        &self,
        unique_qualifier: &ActionInfoHashKey
    ) -> Result<OperationId, Error> {
        let name = ActionName::from(unique_qualifier);
        let mut con = self.get_multiplex_connection().await?;
        let id = con.get::<ActionName, OperationId>(name).await?;
        Ok(id)
    }

    async fn update_action_stage(
        &self,
        worker_id: Option<WorkerId>,
        id: OperationId,
        stage: ActionStage,
    ) -> Result<(), Error> {
        // Currently not removing from queued when assigned and set to executing
        let mut con = self.get_multiplex_connection().await?;
        let mut pipe = Pipeline::new();
        pipe
            .atomic()
            .set(ActionFields::Stage(id), stage);
        if let Some(worker) = worker_id {
            pipe
                .sadd(WorkerFields::RunningOperations(worker), id)
                .set(ActionMaps::Assigned(id), worker);
        }
        else {
            // If there was a worker but the action is now unassigned, then remove the worker
            let maybe_worker_id = con.get(ActionMaps::Assigned(id)).await?;
            if let Some(prev_worker) = maybe_worker_id {
                pipe
                    .srem(WorkerFields::RunningOperations(prev_worker), id)
                    .del(ActionMaps::Assigned(id));
            }
        }
        pipe.query_async(&mut con).await?;
        let res = self.publish_action_state(id).await?;
        println!("Published outer {:?}", res);
        Ok(())
    }

    async fn get_action_state(&self, id: OperationId) -> Result<ActionState, Error> {
        let mut con = self.get_multiplex_connection().await?;
        let keys = vec![ActionFields::Stage(id), ActionFields::Digest(id)];
        let (stage, action_digest) = con.mget(keys).await?;
        Ok(ActionState {
            operation_id: id,
            stage,
            action_digest,
        })

    }
    async fn publish_action_state(&self, id: OperationId) -> Result<(), Error> {
        let mut con = self.get_multiplex_connection().await?;
        // let keys = vec![ActionFields::Stage(id), ActionFields::Digest(id)];
        let stage = con.get(ActionFields::Stage(id)).await?;
        let action_digest = con.get(ActionFields::Digest(id)).await?;
        // let (stage, action_digest) = con.mget(keys).await?;
        let action_state = ActionState {
            operation_id: id,
            stage,
            action_digest,
        };
        let res = con.publish(id, action_state).await?;
        println!("Published inner: {:?}", res);
        Ok(())

    }

    async fn create_new_action(
        &self,
        action_info: &ActionInfo
    ) -> Result<OperationId, Error> {
        let mut con = self.get_multiplex_connection().await?;
        let id = OperationId::new();
        let name = ActionName::from(action_info);
        let is_single = ActionName::to_redis_args(&name).is_single_arg();
        println!("{:?}", is_single);

        let mut pipe = Pipeline::new();
        let (_, id): (redis::Value, OperationId) = pipe
            .atomic()
            .set(&name, id)
            .set(ActionFields::Info(id), action_info).ignore()
            .set(ActionFields::Digest(id), action_info.unique_qualifier.digest).ignore()
            .set(ActionFields::Stage(id), ActionStage::Queued).ignore()
            .set(ActionFields::Name(id), &name).ignore()
            .set(ActionFields::Attempts(id), 0).ignore()
            .zadd(ActionMaps::Queued, id, action_info.priority).ignore()
            .get(&name)
            .query_async(&mut con)
            .await?;
        Ok(id)
    }

    async fn find_action_by_hash_key(
        &self,
        unique_qualifier: &ActionInfoHashKey
    ) -> Result<Option<watch::Receiver<Arc<ActionState>>>, Error> {
        let mut con = self.get_multiplex_connection().await?;
        let name = ActionName::from(unique_qualifier);
        let Some(operation_id) = con.get::<&ActionName, Option<OperationId>>(&name).await? else {
            return Ok(None)
        };
        let v = self.subscribe(&operation_id.to_string(), None).await.unwrap();
        Ok(Some(v))
    }

    async fn get_action_info_for_actions(
        &self,
        actions: Vec<OperationId>
    ) -> Result<Vec<ActionInfo>, Error> {
        let mut con = self.get_multiplex_connection().await?;
        Ok(con.mget(actions).await?)

    }

    // returns operation id and priority tuples
    async fn get_next_n_queued_actions(
        &self,
        num_actions: u64
    ) -> Result<Vec<(OperationId, i32)>, Error> {
        let mut con = self.get_multiplex_connection().await?;
        Ok(con.zrevrange(ActionMaps::Queued, 0, isize::try_from(num_actions).unwrap()).await?)
    }

    async fn remove_actions_from_queue(
        &self,
        actions: Vec<OperationId>
    ) -> Result<(), Error> {
        let mut con = self.get_multiplex_connection().await?;
        Ok(con.zrem(ActionMaps::Queued, &actions).await?)
    }


    async fn add_actions_to_queue(
        &self,
        actions_with_priority: Vec<(OperationId, i32)>
    ) -> Result<(), Error> {
        let mut con = self.get_multiplex_connection().await?;
        Ok(con.zadd_multiple(ActionMaps::Queued, &actions_with_priority).await?)
    }

    // Return the action stage here.
    // If the stage is ActionStage::Completed the callee
    // should request the stored result directly.
    // Otherwise, the callee should call get_action_subscriber
    // and listen to it for updates to find out when its state changes
    async fn add_or_merge_action(
        &self,
        action_info: &ActionInfo
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        let mut con = self.get_multiplex_connection().await?;
        let name = ActionName::from(action_info);
        let existing_id: Option<OperationId> = con.get(&name).await?;
        let (operation_id, stage) = match existing_id {
            Some(id) => {
                let stage: ActionStage = con.get(ActionFields::Stage(id)).await?;
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
            },
            None => {
                (self.create_new_action(action_info).await?, ActionStage::Queued)
            }
        };
        let state = Arc::new(ActionState {
            action_digest: *action_info.digest(),
            operation_id,
            stage
        });
        let v = self.subscribe(&operation_id.to_string(), Some(state)).await.unwrap();
        Ok(v)
    }
}
