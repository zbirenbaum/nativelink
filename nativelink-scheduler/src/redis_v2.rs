use std::process::Output;

use futures::{Future, FutureExt};
use nativelink_error::{make_err, Code, Error};
use nativelink_proto::build::bazel::remote::execution::v2::ActionResult;
use nativelink_util::action_messages::{ActionInfo, ActionName, ActionStage};
use redis::aio::{MultiplexedConnection, PubSub};
use redis::{pipe, AsyncCommands, Client, Pipeline, RedisResult, ToRedisArgs };
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use tracing::subscriber;
use uuid::Uuid;
use crate::worker::WorkerId;

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize)]
pub struct ActionId (pub String);
impl std::fmt::Display for ActionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/*
Action flow:
    1. Action is submitted
    2. Return a subscriber which will receive the result on completion
        - subscribe_to_action(ActionUniqueId)
        - channel: action:state:${ActionUniqueId}
    3. Check ExistingActions for ActionUniqueId matching EncodedActionName
        - If not present:
            1. Add to ExistingActions
            2. Push to PendingActionsQueue
            3. Set ActionStageMap(ActionUniqueId) to ActionStage::Pending
    4. Get the action stage
        - ActionStageMap(ActionUniqueId)
    5. Publish the stage from 4
        - publish_to_action(action:state:${ActionUniqueId}, action_stage)
    6. whenever update_action_state(ActionInfoHashKey) is called:
        - get the encoded db key: ActionInfoHashKey::into<EncodedActionName>
        - find the ActionUniqueId for said key
            - if it doesn't exist:

                action was dropped and needs to be retried
        - publish the action stage to the action channel using the ActionUniqueId
*/

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize, PartialEq)]
enum WorkerMaps {
    Workers,

    PlatformProperties,
}

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize, PartialEq)]
enum ActionMaps {
    // Sorted set of <Id, Priority>
    Queued,
    // <Id, WorkerId>
    Assigned,
    // <Id, ActionResult>
    Completed,
    // <Id, ActionStage>
    Stage,
    // Queued, Assigned, or Completed
    Info,
}

pub struct RedisAdapter {
    client: Client,
}

impl RedisAdapter {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    // These getters avoid mapping errors everywhere and just use the ? operator.
    pub async fn get_multiplex_connection(&self) -> RedisResult<MultiplexedConnection> {
        self.client.get_multiplexed_tokio_connection().await
    }

    pub async fn get_pubsub(&self) -> RedisResult<PubSub> {
        self.client.get_async_pubsub().await
    }

    pub async fn subscribe_to_action(&self, id: ActionId) -> RedisResult<PubSub> {
        let mut pubsub = self.get_pubsub().await?;
        pubsub.subscribe(id).await?;
        Ok(pubsub)
    }

    pub async fn assign_action_to_worker(
        &self,
    ) -> RedisResult<()> {
        let mut con = self.get_multiplex_connection().await?;
        let queue_len: u128 = con.zcard(ActionMaps::Queued).await?;
        if queue_len == 0 {
            return Ok(())
        }
        let mut queue_iter: redis::AsyncIter<ActionId> = con.zscan(ActionMaps::Queued).await?;
        con.hgetall(WorkerMaps::PlatformProperties)

        // let workers = redis::RedisFuture<Output =
        Ok(())
    }
    pub async fn update_action_stage(
        &self,
        action_info: ActionInfo
    ) -> RedisResult<()> {
        let mut con = self.get_multiplex_connection().await?;
        let name = ActionName::from(&action_info);

        Ok(())
    }

    // Return the action stage here.
    // If the stage is ActionStage::Completed the callee
    // should request the stored result directly.
    // Otherwise, the callee should call get_action_subscriber
    // and listen to it for updates to find out when its state changes
    pub async fn add_or_merge_action(
        &self,
        action_info: ActionInfo
    ) -> RedisResult<ActionStage> {
        let mut con = self.get_multiplex_connection().await?;
        let name = ActionName::from(&action_info);
        let existing_id: Option<ActionId> = con.get(&name).await?;
        match existing_id {
            Some(id) => {
                let stage: ActionStage = con.hget(ActionMaps::Stage, &id).await?;
                if stage == ActionStage::Queued {
                    redis::cmd("ZADD LT")
                        .arg(ActionMaps::Queued)
                        .arg(&id)
                        .arg(action_info.priority)
                        .query_async(&mut con)
                        .await?;
                }
                Ok(stage)
            },
            None => {
                // If the action doesn't exist, initialize all its fields and return a subsriber to its state;
                let id = ActionId(Uuid::new_v4().to_string());
                let mut pipe = Pipeline::new();
                pipe
                    // Associate name to id.
                    .set(&name, &id).ignore()
                    // Associate &id to &name.
                    .set(&id, &name).ignore()
                    // Add action_info to the ActionInfo map
                    .hset(ActionMaps::Info, &id, &action_info)
                    // Set the action stage to queued.
                    .hset(ActionMaps::Stage, &id, ActionStage::Queued)
                    // Add &id to sorted set of priority.
                    .zadd(ActionMaps::Queued, &id, action_info.priority)
                    .get(&name).query_async(&mut con)
                    .await?;
                Ok(ActionStage::Queued)
            }
        }
    }
}
