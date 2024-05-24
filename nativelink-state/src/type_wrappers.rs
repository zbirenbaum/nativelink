use std::collections::HashMap;
use std::sync::Arc;
use nativelink_config::stores::ConfigDigestHashFunction;
use nativelink_util::digest_hasher::{DigestHasherFunc, DigestHasherFuncImpl};
use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};
use redis_macros::{FromRedisValue, ToRedisArgs};
use tokio::sync::watch;
use bitflags::{bitflags, Flags};
use nativelink_error::{make_input_err, Error};
use tonic::async_trait;
use std::time::{Duration, SystemTime};
use nativelink_util::common::DigestInfo;
use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey, ActionResult, ActionStage, ActionState, Id};
use tokio_stream::Stream;
use serde::{Serialize, Deserialize};

#[derive(Eq, PartialEq, Hash, Clone, Ord, PartialOrd, Debug, Serialize, Deserialize)]
enum RedisPlatformPropertyType {
    Exact,
    Minimum,
    Priority,
    Unknown,
}

#[derive(Eq, PartialEq, Hash, Clone, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub struct RedisPlatformPropertyValue {
    property_type: RedisPlatformPropertyType,
    property_value: String
}

impl From<PlatformPropertyValue> for RedisPlatformPropertyValue {
    fn from(value: PlatformPropertyValue) -> Self {
        match value {
            PlatformPropertyValue::Exact(v) => Self {
                property_type: RedisPlatformPropertyType::Exact,
                property_value: v
            },
            PlatformPropertyValue::Minimum(v) => Self {
                property_type: RedisPlatformPropertyType::Minimum,
                property_value: v.to_string(),
            },
            PlatformPropertyValue::Priority(v) => Self {
                property_type: RedisPlatformPropertyType::Priority,
                property_value: v,
            },
            PlatformPropertyValue::Unknown(v) => Self {
                property_type: RedisPlatformPropertyType::Unknown,
                property_value: v,
            },
        }
    }
}

impl From<RedisPlatformPropertyValue> for PlatformPropertyValue {
    fn from(value: RedisPlatformPropertyValue) -> Self {
        match value.property_type {
            RedisPlatformPropertyType::Exact => PlatformPropertyValue::Exact(value.property_value),
            RedisPlatformPropertyType::Minimum => PlatformPropertyValue::Minimum(value.property_value.parse::<u64>().unwrap()),
            RedisPlatformPropertyType::Priority => PlatformPropertyValue::Priority(value.property_value),
            RedisPlatformPropertyType::Unknown => PlatformPropertyValue::Unknown(value.property_value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedisPlatformProperties {
    pub properties: HashMap<String, RedisPlatformPropertyValue>,
}

impl From<RedisPlatformProperties> for PlatformProperties {
    fn from(map: RedisPlatformProperties) -> Self {
        Self { properties: map
            .properties
            .iter()
            .map(|(k, v)| (k.to_owned(), PlatformPropertyValue::from(v.to_owned()))).collect()
        }
    }
}
impl From<PlatformProperties> for RedisPlatformProperties {
    fn from(map: PlatformProperties) -> Self {
        Self { properties: map
            .properties
            .iter()
            .map(|(k, v)| (k.to_owned(), RedisPlatformPropertyValue::from(v.to_owned()))).collect()
        }
    }
}


#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Copy)]
pub enum RedisDigestHasherFunc {
    Sha256,
    Blake3,
}

impl From<DigestHasherFunc> for RedisDigestHasherFunc {
    fn from(value: DigestHasherFunc) -> Self {
        match value {
            DigestHasherFunc::Blake3 => { Self::Blake3 },
            DigestHasherFunc::Sha256 => { Self::Sha256 },
        }
    }
}
impl From<RedisDigestHasherFunc> for DigestHasherFunc {
    fn from(value: RedisDigestHasherFunc) -> Self {
        match value {
            RedisDigestHasherFunc::Blake3 => { Self::Blake3 },
            RedisDigestHasherFunc::Sha256 => { Self::Sha256 },
        }
    }
}


#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct RedisActionInfo {
    pub command_digest: DigestInfo,
    pub input_root_digest: DigestInfo,
    pub timeout: Duration,
    pub platform_properties: RedisPlatformPropertyValue,
    pub priority: i32,
    pub load_timestamp: SystemTime,
    pub insert_timestamp: SystemTime,
    pub unique_qualifier: ActionInfoHashKey,
    pub skip_cache_lookup: bool,
    pub digest_function: RedisDigestHasherFunc,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToRedisArgs, FromRedisValue)]
pub struct OperationEntry {
    /// The stage(s) that the operation must be in.
    pub stages: OperationStageFlags,
    /// The operation id.
    pub operation_id: Id,
    /// The worker that the operation must be assigned to.
    pub worker_id: Option<Id>,
    // The action info for the operation
    /// The digest of the action that the operation must have.
    pub action_digest: DigestInfo,
    /// The operation must have it's worker timestamp before this time.
    pub worker_update_before: Option<SystemTime>,

    /// The operation must have been completed before this time.
    pub completed_before: Option<SystemTime>,

    /// The operation must have it's last client update before this time.
    pub last_client_update_before: Option<SystemTime>,
}

bitflags! {
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, FromRedisValue, ToRedisArgs)]
    pub struct OperationStageFlags: u32 {
        const CacheCheck = 1 << 1;
        const Queued     = 1 << 2;
        const Executing  = 1 << 3;
        const Completed  = 1 << 4;
        const None       = 0;
        const Any        = u32::MAX;
    }
}

impl OperationStageFlags {
    pub fn to_action_stage(&self, action_result: Option<ActionResult>) -> Result<ActionStage, Error> {
        match *self {
            OperationStageFlags::CacheCheck => Ok(ActionStage::CacheCheck),
            OperationStageFlags::Queued => Ok(ActionStage::Queued),
            OperationStageFlags::Executing => Ok(ActionStage::Executing),
            OperationStageFlags::Completed => {
                let Some(result) = action_result else {

                    return Err(make_input_err!("Action stage is completed but no result was provided"))
                };
                Ok(ActionStage::Completed(result))
            },
            _ => Ok(ActionStage::Unknown),
        }
    }
    pub const fn has_action_result(&self) -> bool {
        (OperationStageFlags::Completed.bits() & self.bits()) == 0
    }
}

impl From<ActionStage> for OperationStageFlags {
    fn from(state: ActionStage) -> Self {
        Self::from(&state)
    }
}

impl From<&ActionStage> for OperationStageFlags {
    fn from(state: &ActionStage) -> Self {
        match state {
            ActionStage::CompletedFromCache(_)
            | ActionStage::Unknown => Self::Any,
            ActionStage::Queued => Self::Queued,
            ActionStage::Completed(_) => Self::Completed,
            ActionStage::Executing => Self::Executing,
            ActionStage::CacheCheck => Self::CacheCheck
        }
    }
}


#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, ToRedisArgs, FromRedisValue)]
pub struct RedisOperation {
    /// The stage(s) that the operation must be in.
    stages: OperationStageFlags,
    /// The operation id.
    operation_id: Id,
    /// The worker that the operation must be assigned to.
    worker_id: Option<Id>,
    /// The ActionInfo for the operation.
    action_info: RedisActionInfo,
    /// The digest of the action that the operation must have.
    action_digest: Option<DigestInfo>,
    /// The operation must have it's worker timestamp before this time.
    worker_update_before: Option<SystemTime>,
    /// The operation must have been completed before this time.
    completed_before: Option<SystemTime>,
    /// The operation must have it's last client update before this time.
    last_client_update_before: Option<SystemTime>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OperationFilterKeys {
    Stages(OperationStageFlags),
    OperationId(Option<Id>),
    WorkerId(Option<Id>),
    ActionDigest(Option<DigestInfo>),
    WorkerUpdateBefore(Option<SystemTime>),
    CompletedBefore(Option<SystemTime>),
    LastClientUpdateBefore(Option<SystemTime>),
}
