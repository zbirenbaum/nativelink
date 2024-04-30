use std::sync::Arc;

use futures::{Future, StreamExt};
use nativelink_error::{make_input_err, Code, ResultExt};
use nativelink_proto::google::longrunning::Operation;
use nativelink_util::platform_properties::{self, PlatformProperties, PlatformPropertyValue};
use prost::Message;
use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey, ActionName, ActionStage, ActionState, OperationId };
use redis::aio::{MultiplexedConnection, PubSub};
use redis::{ AsyncCommands, AsyncIter, Client, FromRedisValue, Pipeline, RedisError, RedisResult };
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use uuid::Uuid;
use crate::worker::WorkerId;

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
    PlatformProperties(OperationId)
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
    client: Client,
}

impl RedisAdapter {
    // pub async fn get_queued_actions(&self) -> Result<Vec<OperationId>> {
    //     let mut con = self.get_multiplex_connection().await.unwrap();
    //     let jobs: redis::AsyncIter<OperationId> = con.zscan(ActionMaps::Queued).await?;
    //     todo!()
    // }

    pub async fn subscribe<'a, T, F>(&'a self, key: &'a str, pred: F) -> Result<watch::Receiver<T>, nativelink_error::Error>
    where
        T: Send + Sync + 'static,
        for <'b> F: Fn(&'b [u8]) -> T + Send + 'static
    {
        let mut sub = self.get_async_pubsub().await.unwrap();
        sub.subscribe(&key).await.unwrap();
        let mut stream = sub.into_on_message();
        let Some(msg) = stream.next().await else {
            return Err(make_input_err!("failed to get initial state"));
        };
        let (tx, rx) = tokio::sync::watch::channel(pred(msg.get_payload_bytes()));
        // Hand tuple of rx and future to pump the rx
        tokio::spawn(async move {
            let closed_fut = tx.closed();
            tokio::pin!(closed_fut);

            loop {
                tokio::select! {
                    msg = stream.next() => {
                        let value = pred(msg.unwrap().get_payload_bytes());
                        if tx.send(value).is_err() {
                            return
                        }
                    }
                    _  = &mut closed_fut => { return }
                }

            }
        });
        Ok(rx)
    }

    pub fn new(url: String) -> Self {
        Self {
            client: redis::Client::open(url).expect("Could not connect to db"),
        }
    }

    pub async fn get_client(&self) -> Client {
        self.client.clone()
    }

    pub async fn get_async_pubsub(&self) -> RedisResult<PubSub> {
        self.client.get_async_pubsub().await
    }

    // These getters avoid mapping errors everywhere and just use the ? operator.
    pub async fn get_multiplex_connection(&self) -> RedisResult<MultiplexedConnection> {
        let client = self.client.clone();
        client.get_multiplexed_async_connection().await
    }

    pub async fn get_pubsub(&self) -> RedisResult<PubSub> {
        let client = self.client.clone();
        client.get_async_pubsub().await
    }

    pub async fn update_action_stage(
        &self,
        action_info: ActionInfo,
        stage: ActionStage
    ) -> RedisResult<()> {
        let mut con = self.get_multiplex_connection().await?;
        let name = ActionName::from(&action_info);
        let id: OperationId = con.get::<ActionName, OperationId>(name).await.unwrap();
        let mut pipe = Pipeline::new();
        let stage = pipe
            .atomic()
            .set(ActionFields::Stage(id), stage)
            .get(ActionFields::Stage(id))
            .query_async(&mut con)
            .await?;
        con.publish::<OperationId, ActionState, ActionState>(id, stage).await;
        Ok(())
    }

    pub async fn publish_action_state(&self, id: OperationId) -> RedisResult<()> {
        let mut con = self.get_multiplex_connection().await?;
        let keys = vec![ActionFields::Stage(id), ActionFields::Digest(id)];
        let (stage, action_digest) = con.mget(keys).await?;
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
    ) -> RedisResult<OperationId> {
        let mut con = self.get_multiplex_connection().await?;
        let id = OperationId(Uuid::new_v4().as_u128());
        let name = ActionName::from(action_info);
        let platform_properties: Vec<(&String, &PlatformPropertyValue)> = action_info
            .platform_properties
            .properties
            .iter()
            .collect();

        let mut pipe = Pipeline::new();
        pipe
            .atomic()
            .set(&name, &id.to_string())
            .set(ActionFields::Info(id), action_info)
            .set(ActionFields::Digest(id), action_info.unique_qualifier.digest)
            .set(ActionFields::Stage(id), ActionStage::Queued)
            .set(ActionFields::Name(id), &name)
            .set(ActionFields::Attempts(id), 0)
            .hset_multiple(ActionFields::PlatformProperties(id), &platform_properties)
            .zadd(ActionMaps::Queued, &id, action_info.priority)
            .get(&name)
            .query_async(&mut con)
            .await
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

        // replace stage with state -> lets you add more metadata
        let cb = |data: &[u8]| {
            let op = Operation::decode(data);
            // let op = op.map(Arc::new);
            let op = op.unwrap();
            Arc::new(ActionState::try_from(op).unwrap())
        };
        let v = self.subscribe(&operation_id.to_string(), cb).await.unwrap();
        self.publish_action_state(operation_id).await;
        Ok(Some(v))
    }

    // Return the action stage here.
    // If the stage is ActionStage::Completed the callee
    // should request the stored result directly.
    // Otherwise, the callee should call get_action_subscriber
    // and listen to it for updates to find out when its state changes
    pub async fn add_or_merge_action(
        &self,
        action_info: &ActionInfo
    ) -> RedisResult<watch::Receiver<Arc<ActionState>>> {
        let mut con = self.get_multiplex_connection().await?;
        let name = ActionName::from(action_info);
        let existing_id: Option<OperationId> = con.get(&name).await?;
        let operation_id = match existing_id {
            Some(id) => {
                let stage: ActionStage = con.hget(id, ActionFields::Stage(id)).await?;
                if stage == ActionStage::Queued {
                    // Update priority if new priority is higher
                    redis::cmd("ZADD")
                        .arg(ActionMaps::Queued)
                        .arg("LT")
                        .arg(action_info.priority)
                        .arg(&id)
                        .query_async(&mut con)
                        .await?
                }
                id
            },
            None => {
                self.create_new_action(&action_info).await?
            }
        };
        // replace stage with state -> lets you add more metadata
        let cb = |data: &[u8]| {
            let op = Operation::decode(data);
            // let op = op.map(Arc::new);
            let op = op.unwrap();
            Arc::new(ActionState::try_from(op).unwrap())
        };
        let v = self.subscribe(&operation_id.to_string(), cb).await.unwrap();
        self.publish_action_state(operation_id).await;
        Ok(v)
    }
}
