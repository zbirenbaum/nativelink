use std::sync::Arc;

use futures::{Future, StreamExt};
use std::collections::HashMap;
use nativelink_config::schedulers::PropertyType;
use nativelink_error::{make_input_err, Code, ResultExt};
use nativelink_proto::google::longrunning::Operation;
use nativelink_util::platform_properties::{self, PlatformProperties, PlatformPropertyValue};
use prost::Message;
use nativelink_util::action_messages::{OperationId, ActionInfo, ActionName, ActionStage, ActionState };
use redis::aio::{MultiplexedConnection, PubSub};
use redis::{ AsyncCommands, Client, FromRedisValue, Pipeline, RedisError, RedisResult };
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use nativelink_error::Error;
use uuid::Uuid;

use crate::platform_property_manager::{self, PlatformPropertyManager};
use crate::worker::WorkerId;
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

pub struct Subscriber {
    redis_sub: PubSub,
    channel: String,
}

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize, PartialEq)]
pub enum Identifier {
    Worker(WorkerId),
    Operation(OperationId),
}

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize, PartialEq)]
enum PlatformPropertyRedisKey {
    Exact(String, PlatformPropertyValue),
    Priority(String, PlatformPropertyValue),
    Minimum(String),
}
#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize, PartialEq)]
enum WorkerMaps {
    Workers,
    // takes the key and value of the property and adds the worker to the list
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

const KNOWN_PROPERTIES: &str = "known_properties";

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


fn parse_property_value(
    value: &str,
    property_type: PropertyType
) -> Result<PlatformPropertyValue, Error> {
    match property_type {
        PropertyType::minimum => Ok(PlatformPropertyValue::Minimum(
            value.parse::<u64>().err_tip_with_code(|e| {
                (
                    Code::InvalidArgument,
                    format!("Cannot convert to platform property to u64: {value} - {e}"),
                )
            })?,
        )),
        PropertyType::exact => Ok(PlatformPropertyValue::Exact(value.to_string())),
        PropertyType::priority => Ok(PlatformPropertyValue::Priority(value.to_string())),
    }
}

pub struct RedisAdapter {
    client: Client,
    known_properties: HashMap<String, PropertyType>
}

// One call to get all worker IDs
// Create a pipeline which queuries all of those workers in one go

// SCAN 0 MATCH "key:value:*" COUNT 1000
// platform_properties {
//     exact {
//          property:propertyvalue:*
//          HMAP{
//              key: WorkerId: field: property, value: propertyvalue
//          }
//          workerId: {
//
//          }
//         key:value:workerId
//     },
//     // Sorted set
//     // return all worker ids with score for key >= minimum
//     minimum {
//         key: property_key,
//         value: score, workerId
//     }
// }
impl RedisAdapter {
    /// Given a specific key and value, returns the translated `PlatformPropertyValue`. This will
    /// automatically convert any strings to the type-value pairs of `PlatformPropertyValue` based
    /// on the configuration passed into the `PlatformPropertyManager` constructor.
    fn make_prop_value(&self, key: &str, value: &str) -> Result<PlatformPropertyValue, Error> {
        if let Some(prop_type) = self.known_properties.get(key) {
            return match prop_type {
                PropertyType::minimum => Ok(PlatformPropertyValue::Minimum(
                    value.parse::<u64>().err_tip_with_code(|e| {
                        (
                            Code::InvalidArgument,
                            format!("Cannot convert to platform property to u64: {value} - {e}"),
                        )
                    })?,
                )),
                PropertyType::exact => Ok(PlatformPropertyValue::Exact(value.to_string())),
                PropertyType::priority => Ok(PlatformPropertyValue::Priority(value.to_string())),
            };
        }
        Err(make_input_err!("Unknown platform property '{}'", key))
    }

    async fn set_worker_platform_properties(
        &self,
        worker_id: WorkerId,
        platform_properties: Vec<(&str, &str)>,
    ) {
        let mut pipe = &mut Pipeline::new();
        for (key, check_value) in platform_properties.iter() {
            pipe = match self.make_prop_value(key, check_value).unwrap() {
                PlatformPropertyValue::Exact(v) => {
                    pipe.lpush(
                        WorkerMaps::PlatformProperties(
                            PlatformPropertyRedisKey::Exact(
                                key.to_string(),
                                PlatformPropertyValue::Exact(v)
                            )
                        ),
                        worker_id
                    )
                },
                PlatformPropertyValue::Priority(v) => {
                    pipe.lpush(
                        WorkerMaps::PlatformProperties(
                            PlatformPropertyRedisKey::Priority(
                                key.to_string(),
                                PlatformPropertyValue::Priority(v)
                            )
                        ),
                        worker_id
                    )
                },
                PlatformPropertyValue::Minimum(v) => {
                    pipe.zadd(
                        WorkerMaps::PlatformProperties(
                            PlatformPropertyRedisKey::Minimum(key.to_string())
                        ),
                        worker_id,
                        v
                    )
                },
                PlatformPropertyValue::Unknown(_) => {
                    pipe
                }
            };
        };
    }
    async fn set_known_properties(
        &self,
        known_properties: Vec<(&str, PropertyType)>,
    ) -> RedisResult<()> {
        todo!()
        // let mut con = self.get_multiplex_connection().await?;
        // con.zscan_match(key, pattern)
        // con.hset_multiple(key, items)
    }

    pub async fn add_action_state(&self, state: ActionState) -> RedisResult<()> {
        todo!()
        // let mut con = self.get_multiplex_connection().await.unwrap();
        // let pipe = Pipeline::new();
        // pipe.atomic()
    }
    pub async fn get_queued_actions(&self) {
        // let mut con = self.get_multiplex_connection().await.unwrap();
        // let jobs = con.sscan(ActionMaps::Queued).await.into_iter();
        todo!()
    }
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

    pub fn new(url: String, known_properties: HashMap<String, PropertyType>) -> Self {
        Self {
            client: redis::Client::open(url).expect("Could not connect to db"),
            known_properties
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

    pub async fn assign_action_to_worker(
        &self,
    ) -> RedisResult<()> {
        let mut con = self.get_multiplex_connection().await?;
        let queue_len: u128 = con.zcard(ActionMaps::Queued).await?;
        if queue_len == 0 {
            return Ok(())
        }
        // let mut queue_iter: redis::AsyncIter<OperationId> = con.zscan(ActionMaps::Queued).await?;
        // con.hgetall(WorkerMaps::PlatformProperties)

        // let workers = redis::RedisFuture<Output =
        Ok(())
    }


    pub async fn update_action_state(
        &self,
        action_info: ActionInfo,
        stage: ActionStage
    ) -> RedisResult<()> {
        let mut con = self.get_multiplex_connection().await?;
        let name = ActionName::from(&action_info);
        let id: OperationId = con.get::<ActionName, OperationId>(name).await.unwrap();

        // con.publish::<OperationId, ActionState, ActionState>(id, stage).await;
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
        self.publish_action_state(operation_id);
        let v = self.subscribe(&operation_id.to_string(), cb).await.unwrap();
        Ok(v)
    }
}
