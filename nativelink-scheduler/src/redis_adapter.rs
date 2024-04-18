use futures::Future;
use nativelink_error::{make_err, Code, Error};
use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey, EncodedActionName};
use redis::aio::MultiplexedConnection;
use redis::{
    AsyncCommands, Client, FromRedisValue, Pipeline, RedisResult, ToRedisArgs
};
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::redis_adapter_helpers::{ActionFingerprint, UniqueId, ActionUniqueId};

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize)]

/*
Action flow:
    1. Action is submitted
    2. return a subscriber which will receive the result on completion
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
enum RedisLookup {
    // Hashmap<EncodedActionName, ActionUniqueId>
    ExistingActions,
    // List of actions waiting to run
    PendingActionsQueue,
    // Hashmap<ActionUniqueId, ActionInfo>
    QueuedActionsMap,
    // Hashmap<ActionUniqueId, ActionInfo>
    ActiveActionsMap,
    // List of ActionUniqueId corresponding to completed actions.
    CompletedActionsList,
    // Hashmap<ActionUniqueId, ActionResult>
    CompletedActionMap,
    // Hashmap<ActionUniqueId, ActionStage>
    ActionStageMap,
    // Number of times retry has been attempted for an action
    // K: ${ActionUniqueId}:attempts, V: number
    ActionRetries
}

impl RedisLookup {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ExistingActions => "actions:existing",
            Self::PendingActionsQueue => "actions:queued:list",
            Self::QueuedActionsMap => "actions:queued:map",
            Self::ActiveActionsMap => "actions:active:map",
            Self::CompletedActionsList => "actions:completed:list",
            Self::CompletedActionMap => "actions:completed:map",
            Self::ActionStageMap => "actions:stage:map",
            Self::ActionRetries => "actions:retries"
        }
    }
}

fn add_action<'a, K: ToRedisArgs + Serialize + Send + Sync + Clone>(
    connection: &'a mut MultiplexedConnection,
    pipe: &'a mut Pipeline,
    key: K
) -> impl Future<Output = RedisResult<()>> + 'a {
    pipe.atomic()
        .hdel("action_queue", &key)
        .hset("active", &key, &key)
        .ignore()
        .query_async(connection)
}
fn append_enqueue<'a, K: ToRedisArgs + Serialize + Send + Sync + Clone>(
    connection: &'a mut MultiplexedConnection,
    pipe: &'a mut Pipeline,
    key: K
) -> impl Future<Output = RedisResult<()>> + 'a {
    pipe.atomic()
        .hdel("action_queue", &key)
        .hset("active", &key, &key)
        .ignore()
        .query_async(connection)
}

#[derive(Debug, PartialEq, Serialize, Deserialize, FromRedisValue, ToRedisArgs)]
struct Container<T: ToRedisArgs + Serialize + Send + Sync + Clone> {
    inner: T,
}

impl<T: ToRedisArgs + Serialize + Clone + Send + Sync> Container<T> {
    pub async fn new(v: T) -> Self {
        Self { inner: v }
    }
}
pub struct RedisAdapter {
    client: Client,
}

impl RedisAdapter {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    // pub async async fn async_set<K: ToRedisArgs + FromRedisValue + Serialize + Clone + Send + Sync, V: ToRedisArgs + FromRedisValue + Serialize + Clone + Send + Sync>(&self, key: K, value: V) -> RedisResult<u128> {
    //     let connection_manager = &mut self.client.get_multiplexed_tokio_connection().await?;
    //     connection_manager.set(Container::new(key), Container::new(value)).await
    // }

    pub async fn lpush<
        K: ToRedisArgs + Serialize + Send + Sync + Clone,
        V: ToRedisArgs + Serialize + Send + Sync + Clone,
    >(
        &self,
        key: K,
        value: V,
    ) -> Result<u128, Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => con
                .lpush(key, value)
                .await
                .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call lpush")),
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn lpop<
        K: ToRedisArgs + Serialize + Send + Sync + Clone + Debug,
        V: FromRedisValue + DeserializeOwned,
    >(
        &self,
        key: K,
        num: Option<NonZeroUsize>,
    ) -> Result<V, Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => con
                .lpop(key, num)
                .await
                .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call lpop")),
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn get<
        K: ToRedisArgs + Serialize + Send + Sync + Clone,
        V: FromRedisValue + DeserializeOwned,
    >(
        &self,
        key: K,
    ) -> Result<V, Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => con
                .get(key)
                .await
                .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call get")),
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn set<
        K: ToRedisArgs + Serialize + Send + Sync + Clone,
        V: ToRedisArgs + Serialize + Send + Sync + Clone,
    >(
        &self,
        key: K,
        value: V,
    ) -> Result<u128, Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => con
                .set(key, value)
                .await
                .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call set")),
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn hget<
        K: ToRedisArgs + Serialize + Send + Sync + Clone,
        V: FromRedisValue + DeserializeOwned,
    >(
        &self,
        table: String,
        key: K,
    ) -> Result<V, Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => con
                .hget(table, key)
                .await
                .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call get")),
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn del<K: ToRedisArgs + Serialize + Send + Sync + Clone>(
        &self,
        key: K,
    ) -> Result<u128, Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => con
                .del(key)
                .await
                .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call del")),
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn enqueue<K: ToRedisArgs + Serialize + Send + Sync + Clone>(
        &self,
        key: K,
    ) -> Result<(), Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => {
                let mut pipe = redis::pipe();
                pipe.atomic()
                    .hset("queued", &key, &key)
                    .ignore()
                    .query_async(&mut con)
                    .await
                    .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call set"))
            }
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn make_active<K: ToRedisArgs + Serialize + Send + Sync + Clone>(
        &self,
        key: K,
    ) -> Result<(), Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => {
                let mut pipe = redis::pipe();
                pipe.atomic()
                    .hdel("queued", &key)
                    .hset("active", &key, &key)
                    .ignore()
                    .query_async(&mut con)
                    .await
                    .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call set"))
            }
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn add_new_action(&self, action_info: Arc<ActionInfo>) -> Result<(), Error> {
        let action_fingerprint = ActionFingerprint::new(&action_info.unique_qualifier);
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => {
                let mut pipe = redis::pipe();
                pipe.atomic()
                    .hset(
                        "id_action_name_map",
                        &action_fingerprint.get_unique_id(),
                        &action_fingerprint.get_encoded_action_name()
                    )
                    .hset(
                        "action_name_id_map",
                        &action_fingerprint.get_encoded_action_name(),
                        &action_fingerprint.get_unique_id()
                    )
                    .ignore()
                    .query_async(&mut con)
                    .await
                    .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call set"))
            }
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }

    }

    // We need to be able to check the values of individual platform properties.
    // We also need to be able to find all the relevant platform properties for
    // a given job or worker.
    // The way to do this is to generate keys based on the following:
    // UniqueId: the WorkerId or hex encoded ActionInfoHashKey hash
    // the platform properties
    // a list containing the platform property keys
    // pub async fn store_platform_properties<id: UniqueId, platform_properties:
}
