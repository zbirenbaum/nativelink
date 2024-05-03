#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]
use std::sync::Arc;

use futures::{Future, StreamExt};
use nativelink_error::{make_input_err, Code, ResultExt, Error};
use nativelink_proto::google::longrunning::Operation;
use nativelink_util::common::DigestInfo;
use nativelink_util::platform_properties::{self, PlatformProperties, PlatformPropertyValue};
use prost::Message;
use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey, ActionName, ActionStage, ActionState, OperationId, WorkerId };
use redis::aio::{MultiplexedConnection, PubSub};
use redis::{ AsyncCommands, AsyncIter, Client, FromRedisValue, Pipeline, RedisError, RedisResult, ToRedisArgs };
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use uuid::Uuid;

pub struct Subscriber {
    redis_sub: PubSub,
    channel: String,
}

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize, PartialEq)]
enum PlatformPropertyRedisKey {
    Exact(String, PlatformPropertyValue),
    Priority(String, PlatformPropertyValue),
    Minimum(String),
}
#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize, PartialEq)]
enum WorkerFields {
    Workers,
    // takes the key and value of the property and adds the worker to the list
    RunningOperations(WorkerId),
    IsPaused(WorkerId),
    IsDraining(WorkerId),
    PlatformProperties(PlatformPropertyRedisKey),

}

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize, PartialEq)]
enum ActionFields {
    Digest(OperationId),
    Name(OperationId),
    Stage(OperationId),
    Attempts(OperationId),
    LastError(OperationId),
    Info(OperationId),
    AssignedWorker(OperationId)
}

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize, PartialEq)]
enum ActionMaps {
    // Sorted set of <OperationId, Priority>
    Queued,
    // <OperationId, WorkerId>
    Assigned,
    // <OperationId,
    // [stage | operation_id | action_digest],
    // ActionState>
}


pub struct RedisAdapter {
    url: String
}

impl RedisAdapter {
    // pub async fn get_queued_actions(&self) -> Result<Vec<OperationId>> {
    //     let mut con = self.get_multiplex_connection().await.unwrap();
    //     let jobs: redis::AsyncIter<OperationId> = con.zscan(ActionMaps::Queued).await?;
    //     todo!()
    // }

    pub async fn subscribe<'a>(&'a self, key: &'a str) -> Result<watch::Receiver<Arc<ActionState>>, nativelink_error::Error>
    where
        // T: Send + Sync + 'static,
        // for <'b> F: Fn(&redis::Value) -> T + Send + 'static
    {
        let mut sub = self.get_async_pubsub().await?;
        let id = OperationId::try_from(key)?;
        let state = self.get_action_state(id).await?;
        sub.subscribe(&key).await.unwrap();
        let mut stream = sub.into_on_message();
        // This hangs forever atm
        let (tx, rx) = tokio::sync::watch::channel(Arc::new(state));
        let mut rx_clone = rx.clone();
        // Hand tuple of rx and future to pump the rx
        tokio::spawn(async move {
            let closed_fut = tx.closed();
            tokio::pin!(closed_fut);
            // let mut stream = sub.into_on_message();

            loop {
                tokio::select! {
                    msg = stream.next() => {
                        let state: ActionState = msg.unwrap().get_payload().unwrap();
                        let value = Arc::new(state);
                        rx_clone.mark_changed();
                        // let value = pred(msg.unwrap().get_payload_bytes());
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
        Ok(rx)
    }

    pub fn new(url: String) -> Self {
        Self { url }
    }

    pub async fn get_async_pubsub(&self) -> RedisResult<PubSub> {
        let client = redis::Client::open(self.url.clone())?;
        client.get_async_pubsub().await
    }

    // These getters avoid mapping errors everywhere and just use the ? operator.
    pub async fn get_multiplex_connection(&self) -> RedisResult<MultiplexedConnection> {
        let client = redis::Client::open(self.url.clone())?;
        client.get_multiplexed_async_connection().await
    }

    pub async fn get_operation_id_for_action(
        &self,
        unique_qualifier: &ActionInfoHashKey
    ) -> Result<OperationId, Error> {
        let name = ActionName::from(unique_qualifier);
        let mut con = self.get_multiplex_connection().await?;
        let id = con.get::<ActionName, OperationId>(name).await?;
        Ok(id)
    }
    pub async fn get_worker_actions(
        &self,
        worker_id: &WorkerId
    ) -> Result<Vec<OperationId>, Error> {
        let mut con = self.get_multiplex_connection().await?;
        Ok(con.smembers(WorkerFields::RunningOperations(worker_id.to_owned())).await?)
    }

    pub async fn update_action_stage(
        &self,
        worker_id: Option<WorkerId>,
        id: OperationId,
        stage: ActionStage,
    ) -> Result<(), Error> {
        let mut con = self.get_multiplex_connection().await?;
        let mut pipe = Pipeline::new();
        pipe
            .atomic()
            .set(ActionFields::Stage(id), stage).ignore()
            .get(ActionFields::Stage(id))
            .get(ActionFields::Digest(id));
        if let Some(worker) = worker_id {
            pipe
                .sadd(WorkerFields::RunningOperations(worker), id).ignore()
                .set(ActionFields::AssignedWorker(id), worker).ignore();
        }
        let (stage, digest): (ActionStage, DigestInfo) = pipe.query_async(&mut con).await?;

        let state = ActionState {
            operation_id: id,
            stage,
            action_digest: digest
        };
        let _ = con.publish::<OperationId, ActionState, u128>(id, state).await?;
        Ok(())
    }

    pub async fn get_action_state(&self, id: OperationId) -> Result<ActionState, Error> {
        let mut con = self.get_multiplex_connection().await?;
        let keys = vec![ActionFields::Stage(id), ActionFields::Digest(id)];
        let (stage, action_digest) = con.mget(keys).await?;
        Ok(ActionState {
            operation_id: id,
            stage,
            action_digest,
        })

    }
    pub async fn publish_action_state(&self, id: OperationId) -> RedisResult<()> {
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
        con.publish(id, action_state).await?;
        Ok(())

    }

    pub async fn create_new_action(
        &self,
        action_info: &ActionInfo
    ) -> Result<OperationId, Error> {
        let mut con = self.get_multiplex_connection().await?;
        let id = OperationId::new();
        let name = ActionName::from(action_info);

        let mut pipe = Pipeline::new();
        let st: String = pipe
            .atomic()
            .set(&name, id).ignore()
            .set(ActionFields::Info(id), action_info).ignore()
            .set(ActionFields::Digest(id), action_info.unique_qualifier.digest).ignore()
            .set(ActionFields::Stage(id), ActionStage::Queued).ignore()
            .set(ActionFields::Name(id), &name).ignore()
            .set(ActionFields::Attempts(id), 0).ignore()
            .zadd(ActionMaps::Queued, id, action_info.priority).ignore()
            .get(&name)
            .get(ActionFields::Stage(id))
            .query_async(&mut con)
            .await?;

        OperationId::try_from(st)
    }

    pub async fn find_action_by_hash_key(
        &self,
        unique_qualifier: &ActionInfoHashKey
    ) -> RedisResult<Option<watch::Receiver<Arc<ActionState>>>> {
        let mut con = self.get_multiplex_connection().await?;
        let name = ActionName::from(unique_qualifier);
        let Some(operation_id) = con.get::<&ActionName, Option<OperationId>>(&name).await? else {
            return Ok(None)
        };
        let v = self.subscribe(&operation_id.to_string()).await.unwrap();
        Ok(Some(v))
    }

    pub async fn get_action_info_for_actions(
        &self,
        actions: &Vec<OperationId>
    ) -> RedisResult<Vec<ActionInfo>> {
        let mut con = self.get_multiplex_connection().await?;
        con.mget(actions).await

    }

    // returns operation id and priority tuples
    pub async fn get_next_n_queued_actions(
        &self,
        num_actions: u64
    ) -> RedisResult<Vec<(ActionInfo, i32)>> {
        let mut con = self.get_multiplex_connection().await?;
       con.zrevrange(ActionMaps::Queued, 0, isize::try_from(num_actions).unwrap()).await
    }

    pub async fn remove_actions_from_queue(
        &self,
        actions: Vec<OperationId>
    ) -> RedisResult<()> {
        let mut con = self.get_multiplex_connection().await?;
        con.zrem(ActionMaps::Queued, &actions).await
    }


    pub async fn enqueue_actions(
        &self,
        actions_with_priority: Vec<(OperationId, i32)>
    ) -> RedisResult<()> {
        let mut con = self.get_multiplex_connection().await?;
        con.zadd_multiple(ActionMaps::Queued, &actions_with_priority).await
    }

    // Return the action stage here.
    // If the stage is ActionStage::Completed the callee
    // should request the stored result directly.
    // Otherwise, the callee should call get_action_subscriber
    // and listen to it for updates to find out when its state changes
    pub async fn add_or_merge_action(
        &self,
        action_info: &ActionInfo
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        let mut con = self.get_multiplex_connection().await?;
        let name = ActionName::from(action_info);
        let existing_id: Option<OperationId> = con.get(&name).await?;

        // con.set(ActionFields::Stage(existing_id.unwrap()), ActionStage::Queued).await?;
        // let stage: ActionStage = con.get(ActionFields::Stage(existing_id.unwrap())).await?;
        // println!("{:?}", stage);
        let (operation_id, _stage) = match existing_id {
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
        let v = self.subscribe(&operation_id.to_string()).await.unwrap();
        Ok(v)
    }
}
