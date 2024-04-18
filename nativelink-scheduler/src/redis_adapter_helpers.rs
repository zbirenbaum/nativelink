use nativelink_error::{Error};
use nativelink_util::action_messages::{ActionInfoHashKey, EncodedActionName};
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};

use crate::worker::WorkerId;

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize)]

pub struct ActionUniqueId (String);

pub enum UniqueId {
    // Just a u128 under the hood.
    Worker(WorkerId),
    // Just a uuid string which can be used to find the unique qualifier
    Action(ActionUniqueId)
}

pub struct ActionFingerprint {
    unique_id: ActionUniqueId,
    action_name: EncodedActionName
}

impl ActionFingerprint {
    pub fn new(unique_qualifier: &ActionInfoHashKey) -> Self {
        Self {
            unique_id: ActionUniqueId(uuid::Uuid::new_v4().into()),
            action_name: EncodedActionName::from(unique_qualifier)
        }
    }

    pub fn get_unique_qualifier(&self) -> Result<ActionInfoHashKey, Error> {
        ActionInfoHashKey::try_from(&self.action_name)
    }

    pub fn get_encoded_action_name(&self) -> EncodedActionName {
        self.action_name.clone()
    }

    pub fn get_unique_id(&self) -> ActionUniqueId {
        self.unique_id.clone()
    }
}
