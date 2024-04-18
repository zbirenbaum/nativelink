use nativelink_error::{Error};
use nativelink_util::action_messages::{ActionInfoHashKey, EncodedActionName};
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};

use crate::worker::WorkerId;

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize)]
pub struct ActionUniqueId (pub String);
impl std::fmt::Display for ActionUniqueId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize)]
pub enum ActionIdentifier {
    // Just a uuid string which can be used to find the unique qualifier
    ActionId(ActionUniqueId),
    // Hex encoded string which can be decoded to unique_qualifier
    ActionName(EncodedActionName)
}


#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize)]
pub enum UniqueId {
    // Just a u128 under the hood.
    Worker(WorkerId),
    // Just a uuid string which can be used to find the unique qualifier
    Action(ActionIdentifier),
}
