use std::collections::HashMap;
use std::sync::Arc;
use nativelink_config::stores::ConfigDigestHashFunction;
use nativelink_util::digest_hasher::{DigestHasherFunc, DigestHasherFuncImpl};
use nativelink_util::evicting_map::InstantWrapper;
use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::de::DeserializeOwned;
use uuid::Uuid;
use tokio::sync::watch;
use bitflags::{bitflags, Flags};
use nativelink_error::{make_err, make_input_err, Error};
use tonic::async_trait;
use std::time::{Duration, SystemTime};
use nativelink_util::common::DigestInfo;
use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey, ActionResult, ActionStage, ActionState, OperationId, WorkerId};
use tokio_stream::Stream;
use serde::{Serialize, Deserialize};

macro_rules! field_names {
    (
        $(#[$outer:meta])*
        $vis:vis struct $name:ident { $($fname:ident : $ftype:ty),* }
    ) => {
        $(#[$outer])*
        $vis struct $name {
            $($fname : $ftype),*
        }

        impl $name {
            pub fn fields(&self) -> &'static [&'static str] {
                static NAMES: &'static [&'static str] = &[$(stringify!($fname)),*];
                NAMES
            }
        }
    }
}

#[derive(Eq, PartialEq, Hash, Clone, Ord, PartialOrd, Debug, Serialize, Deserialize)]
enum RedisPlatformPropertyType {
    Exact,
    Minimum,
    Priority,
    Unknown,
}

#[derive(Eq, PartialEq, Hash, Clone, Ord, PartialOrd, Debug, Deserialize)]
pub struct RedisPlatformPropertyValue {
    property_type: RedisPlatformPropertyType,
    property_value: String
}

impl Serialize for RedisPlatformPropertyValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer
    {
        match self.property_type {
            RedisPlatformPropertyType::Minimum => serializer.serialize_u64(
                self.property_value.as_str().parse::<u64>().unwrap()
            )
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
impl ToString for RedisDigestHasherFunc {
    fn to_string(&self) -> String {
        match self {
            Self::Blake3 => "blake3".to_string(),
            Self::Sha256 => "sha256".to_string(),
        }
    }
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToRedisArgs, FromRedisValue)]
pub struct OperationEntry {
    /// The stage(s) that the operation must be in.
    pub stages: OperationStageFlags,
    /// The operation id.
    pub operation_id: OperationId,
    /// The worker that the operation must be assigned to.
    pub worker_id: Option<WorkerId>,
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
pub struct RedisActionInfo {
    // Action Info Fields
    command_digest: DigestInfo,
    input_root_digest: DigestInfo,
    timeout: Duration,
    platform_properties: RedisPlatformProperties,
    priority: i32,
    load_timestamp: SystemTime,
    insert_timestamp: SystemTime,
    unique_qualifier: String,
    skip_cache_lookup: bool,
    digest_function: RedisDigestHasherFunc
}


impl From<ActionInfo> for RedisActionInfo {
    fn from(value: ActionInfo) -> Self {
        Self {
            command_digest: value.command_digest,
            input_root_digest: value.input_root_digest,
            timeout: value.timeout,
            platform_properties: value.platform_properties.into(),
            priority: value.priority,
            load_timestamp: value.load_timestamp,
            insert_timestamp: value.insert_timestamp,
            unique_qualifier: value.unique_qualifier.action_name(),
            skip_cache_lookup: value.skip_cache_lookup,
            digest_function: value.digest_function.into(),
        }
    }
}

impl From<RedisActionInfo> for ActionInfo {
    fn from(value: RedisActionInfo) -> Self {
        Self {
            command_digest: value.command_digest,
            input_root_digest: value.input_root_digest,
            timeout: value.timeout,
            platform_properties: PlatformProperties::from(value.platform_properties),
            priority: value.priority,
            load_timestamp: value.load_timestamp,
            insert_timestamp: value.insert_timestamp,
            unique_qualifier: ActionInfoHashKey::try_from(value.unique_qualifier.as_str()).unwrap(),
            skip_cache_lookup: value.skip_cache_lookup,
            digest_function: DigestHasherFunc::from(value.digest_function),
        }
    }
}

pub trait JsonConvertable<'a>: Serialize + Deserialize<'a> {
    fn as_json(&self) -> serde_json::Value;
    fn as_json_string(&self) -> String;
    fn from_str(s: &'a str) -> Self;
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct RedisOperation {
    // Other tracking fields
    pub stage: OperationStageFlags,
    pub operation_id: String,
    pub worker_id: Option<String>,
    pub last_worker_update: SystemTime,
    pub completed_at: Option<SystemTime>,
    pub last_client_update: Option<SystemTime>,
    pub action_info: RedisActionInfo
}

impl JsonConvertable<'_> for RedisOperation {
    fn as_json(&self) -> serde_json::Value {
        let json = RedisOperationJson::from(self);
        serde_json::to_value(json).unwrap()
    }
    fn as_json_string(&self) -> String {
        let json = RedisOperationJson::from(self);
        serde_json::to_string(&json).unwrap()
    }
    fn from_str(s: &'_ str) -> Self {
        serde_json::from_str(s).unwrap()
    }
}
impl From<RedisOperation> for ActionInfo {
    fn from(value: RedisOperation) -> Self {
        ActionInfo::from(value.action_info)
    }
}

field_names! {
    #[derive(Eq, PartialEq, Clone, Serialize, Deserialize)]
    pub struct RedisOperationJson {
        stage: OperationStageFlags,
        operation_id: String,
        worker_id: Option<String>,
        last_worker_update: SystemTime,
        completed_at: Option<SystemTime>,
        last_client_update: Option<SystemTime>,
        command_digest: DigestInfo,
        input_root_digest: DigestInfo,
        timeout: Duration,
        platform_properties: RedisPlatformProperties,
        priority: i32,
        load_timestamp: SystemTime,
        insert_timestamp: SystemTime,
        unique_qualifier: String,
        skip_cache_lookup: bool,
        digest_function: RedisDigestHasherFunc
    }
}

impl JsonConvertable<'_> for RedisOperationJson {
    fn as_json(&self) -> serde_json::Value {
        serde_json::to_value(self.clone()).unwrap()
    }
    fn as_json_string(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
    fn from_str(s: &'_ str) -> Self {
        serde_json::from_str(s).unwrap()
    }
}

impl From<RedisOperationJson> for RedisOperation {
    fn from(value: RedisOperationJson) -> Self {
        Self {
            stage: value.stage,
            operation_id: value.operation_id,
            worker_id: value.worker_id,
            last_worker_update: value.last_worker_update,
            completed_at: value.completed_at,
            last_client_update: value.last_client_update,
            action_info: RedisActionInfo {
                command_digest: value.command_digest,
                input_root_digest: value.input_root_digest,
                timeout: value.timeout,
                platform_properties: value.platform_properties,
                priority: value.priority,
                load_timestamp: value.load_timestamp,
                insert_timestamp: value.insert_timestamp,
                unique_qualifier: value.unique_qualifier,
                skip_cache_lookup: value.skip_cache_lookup,
                digest_function: value.digest_function,
            }
        }
    }
}

// Note: Redis index doesn't support nested objects so we can just unpack it to index everything instead
impl From<&RedisOperation> for RedisOperationJson {
    fn from(value: &RedisOperation) -> Self {
        Self {
            stage: value.stage.clone(),
            operation_id: value.operation_id.clone(),
            worker_id: value.worker_id.clone(),
            last_worker_update: value.last_worker_update,
            completed_at: value.completed_at,
            last_client_update: value.last_client_update,
            command_digest: value.action_info.command_digest,
            input_root_digest: value.action_info.input_root_digest,
            timeout: value.action_info.timeout,
            platform_properties: value.action_info.platform_properties.clone(),
            priority: value.action_info.priority,
            load_timestamp: value.action_info.load_timestamp,
            insert_timestamp: value.action_info.insert_timestamp,
            unique_qualifier: value.action_info.unique_qualifier.clone(),
            skip_cache_lookup: value.action_info.skip_cache_lookup,
            digest_function: value.action_info.digest_function,
        }
    }
}

// Note: Redis index doesn't support nested objects so we can just unpack it to index everything instead
impl From<RedisOperation> for RedisOperationJson {
    fn from(value: RedisOperation) -> Self {
        Self {
            stage: value.stage,
            operation_id: value.operation_id,
            worker_id: value.worker_id,
            last_worker_update: value.last_worker_update,
            completed_at: value.completed_at,
            last_client_update: value.last_client_update,
            command_digest: value.action_info.command_digest,
            input_root_digest: value.action_info.input_root_digest,
            timeout: value.action_info.timeout,
            platform_properties: value.action_info.platform_properties,
            priority: value.action_info.priority,
            load_timestamp: value.action_info.load_timestamp,
            insert_timestamp: value.action_info.insert_timestamp,
            unique_qualifier: value.action_info.unique_qualifier,
            skip_cache_lookup: value.action_info.skip_cache_lookup,
            digest_function: value.action_info.digest_function,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OperationFilterKeys {
    Stages(OperationStageFlags),
    OperationId(Option<OperationId>),
    WorkerId(Option<WorkerId>),
    ActionDigest(Option<DigestInfo>),
    WorkerUpdateBefore(Option<SystemTime>),
    CompletedBefore(Option<SystemTime>),
    LastClientUpdateBefore(Option<SystemTime>),
}



// #[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
// pub struct RedisActionInfo {
//     pub command_digest: DigestInfo,
//     pub input_root_digest: DigestInfo,
//     pub timeout: Duration,
//     pub platform_properties: RedisPlatformPropertyValue,
//     pub priority: i32,
//     pub load_timestamp: SystemTime,
//     pub insert_timestamp: SystemTime,
//     pub unique_qualifier: ActionInfoHashKey,
//     pub skip_cache_lookup: bool,
//     pub digest_function: RedisDigestHasherFunc,
// }
