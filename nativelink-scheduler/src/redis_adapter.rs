use futures::Future;
use nativelink_error::{make_err, Code, Error};
use nativelink_util::action_messages::{ActionInfo, EncodedActionName};
use nativelink_util::platform_properties::PlatformProperties;
use redis::aio::{MultiplexedConnection, PubSub};
use redis::{AsyncCommands, Client, ConnectionLike, FromRedisValue, Pipeline, RedisResult, ToRedisArgs };
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use tokio::net::unix::pipe;
use std::borrow::Borrow;
use std::sync::Arc;
use crate::redis_adapter_helpers::{ActionIdentifier, ActionUniqueId};

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

// You need to make an action info map with all these values unfortunately...
// ID is stored with the key name and no path, only inserted for a new action
const ACTION_MAP: &str = "action";
const ACTION_NAME: &str = "{}:name";
const ACTION_INFO: &str = "{}:info";
const ACTION_STAGE: &str = "{}:stage";
const ACTION_RESULT: &str = "{}:result";
const ACTION_PRIORITY: &str = "{}:priority";
const ACTION_QUEUE: &str = "queue";

fn format_lookup(path: &str, id: &ActionUniqueId) -> String {
    return format!("{id}{path}")
}

pub struct RedisAdapter {
    client: Client,
}

async fn find_existing_action_id(con: &mut MultiplexedConnection, name: &EncodedActionName) -> RedisResult<Option<ActionUniqueId>> {
    con.hget(ACTION_MAP, name).await
}
// to do handle existing
async fn merge_action(con: &mut MultiplexedConnection, action_info: &ActionInfo) -> Result<(), Error> {
    let mut pipe = Pipeline::new();
    let key = "the_key";
    // let (new_val,) : (isize,) = redis::transaction(&mut con, &[key], |con, pipe| {
    //     let old_val : isize = con.get(key).await?;
    //     pipe
    //         .set(key, old_val + 1).ignore()
    //         .get(key).query(con)
    // })?;
    if action_info.priority > info.priority {
        pipe.clear();
        pipe.zadd(ACTION_QUEUE, member, score);
        info.priority = action_info.priority;
    }
}

async fn update_action_priority(con: &mut MultiplexedConnection, new_priority: i32) {

}

async fn add_action(con: &mut MultiplexedConnection, action_info: ActionInfo) -> RedisResult<()> {
    let id = ActionUniqueId(uuid::Uuid::new_v4().into());
    let name = EncodedActionName::from(&action_info.unique_qualifier);
    let mut pipe = Pipeline::new();
    if let Some(existing_id) = con.hget::<Option<ActionUniqueId>>(ACTION_MAP, name).await? {
        let current_priority: i32 = con.hget(ACTION_MAP, format_lookup(ACTION_PRIORITY, &id)).await?;
        if existing_info.priority < action_info.priority {
        }
    } else {
        pipe.atomic()
            // Associate the name with id.
            .hset(ACTION_MAP, &name, &id)
            // Associate the ID with name.
            .hset(ACTION_MAP, format_lookup(ACTION_NAME, &id), name)
            // Associate the ActionInfo with the ID.
            .hset(ACTION_MAP, format_lookup(ACTION_INFO, &id), &action_info)
            .hset(ACTION_MAP, format_lookup(ACTION_PRIORITY, &id), &action_info.priority)
            .query_async(con)
            .await?
    };

    // pipe
    //     .set(key, old_val + 1).ignore()
    //     .get(key).query(con)
    // })?;
}
impl RedisAdapter {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    // These getters avoid mapping errors everywhere and just use the ? operator.
    pub async fn get_multiplex_connection(&self) -> Result<MultiplexedConnection, Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(con) => Ok(con),
            Err(_) => Err(make_err!(Code::Internal, "Failed to connect to redis"))
        }
    }

    pub async fn get_pubsub(&self) -> Result<PubSub, Error> {
        match self.client.get_async_pubsub().await {
            Ok(con) => Ok(con),
            Err(_) => Err(make_err!(Code::Internal, "Failed to connect to redis"))
        }
    }
    pub async fn subscribe_to_action(&self, id: ActionUniqueId) -> Result<PubSub, Error> {
        let mut pubsub = self.get_pubsub().await?;
        pubsub.subscribe(id).await.map_err(|e| {
            make_err!(Code::Internal, "Could not subscribe to channel with error: {}", e.to_string())
        })?;
        Ok(pubsub)
    }
// Action flow:
//     1. Action is submitted
//     2. Return a subscriber which will receive the result on completion
//         - subscribe_to_action(ActionUniqueId)
//         - channel: action:state:${ActionUniqueId}
    // pub async async fn async_set<K: ToRedisArgs + FromRedisValue + Serialize + Clone + Send + Sync, V: ToRedisArgs + FromRedisValue + Serialize + Clone + Send + Sync>(&self, key: K, value: V) -> RedisResult<u128> {
    //     let connection_manager = &mut self.client.get_multiplexed_tokio_connection().await?;
    //     connection_manager.set(Container::new(key), Container::new(value)).await
    // }

    pub async fn enqueue<K: ToRedisArgs + Serialize + PartialEq>(
        &self,
        key: K,
    ) -> Result<(), Error> {
        let mut con = self.get_multiplex_connection()
            .await?;
        let mut pipe = redis::pipe();
        pipe.atomic()
            .hset("queued", &key, &key)
            .ignore()
            .query_async(&mut con)
            .await
            .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call set"))
    }

    pub async fn make_active<K: ToRedisArgs + Serialize + PartialEq>(
        &self,
        key: K,
    ) -> Result<(), Error> {
        let mut con = self.get_multiplex_connection()
            .await?;
        let mut pipe = redis::pipe();
        pipe.atomic()
            .hdel("queued", &key)
            .hset("active", &key, &key)
            .ignore()
            .query_async(&mut con)
            .await
            .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call set"))
    }

    // pub async fn add_to_existing_action(&self, action_info: Arc<ActionInfo>) -> Result<(), Error> {
    //     let mut con = self.get_multiplex_connection()
    //         .await?;
    //     let mut pipe = redis::pipe();
    //     pipe.atomic()
    //         .hset(
    //             "id_action_name_map",
    //             &action_fingerprint.get_unique_id(),
    //             &action_fingerprint.get_encoded_action_name()
    //         )
    //         .hset(
    //             "action_name_id_map",
    //             &action_fingerprint.get_encoded_action_name(),
    //             &action_fingerprint.get_unique_id()
    //         )
    //         .ignore()
    //         .query_async(&mut con)
    //         .await
    //         .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call set"))
    // }


}

    // We need to be able to check the values of individual platform properties.
    // We also need to be able to find all the relevant platform properties for
    // a given job or worker.
    // The way to do this is to generate keys based on the following:
    // UniqueId: the WorkerId or hex encoded ActionInfoHashKey hash
    // the platform properties
    // a list containing the platform property keys
    // pub async fn store_platform_properties<id: UniqueId, platform_properties:
