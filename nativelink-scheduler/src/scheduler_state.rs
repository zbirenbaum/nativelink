#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]
use std::sync::Arc;
use futures::{Future, StreamExt};
use nativelink_error::{make_input_err, Code, ResultExt, Error};
use nativelink_proto::google::longrunning::Operation;
use nativelink_util::common::DigestInfo;
use nativelink_util::platform_properties::{self, PlatformProperties, PlatformPropertyValue};
use prost::Message;
use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey, ActionName, ActionResult, ActionStage, ActionState, OperationId, WorkerId };
use redis::aio::{MultiplexedConnection, PubSub};
use redis::{ AsyncCommands, AsyncIter, Client, FromRedisValue, Pipeline, RedisError, ToRedisArgs };
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize, Serializer};
use tokio::sync::watch;
use tonic::async_trait;
use uuid::Uuid;

trait Stringify {
    fn as_string(&self) -> String;
}

#[derive(Clone, ToRedisArgs, FromRedisValue, Deserialize, PartialEq)]
struct KeyWrapper<T: Stringify> {
    inner: T
}
impl<T: Stringify> Serialize for KeyWrapper<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.inner.as_string())
    }
}

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize, PartialEq)]
pub enum ActionFields {
    Id,
    Digest,
    Name,
    Stage,
    Attempts,
    LastError,
    Info,
}

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize, PartialEq)]
pub enum ActionMaps {
    Queued,
    Assigned,
    Running
}


#[async_trait]
pub trait ActionSchedulerStateStore: Sync + Send + Unpin {
    async fn subscribe<'a>(
        &'a self,
        key: &'a str,
        initial_state: Option<Arc<ActionState>>
    ) -> Result<watch::Receiver<Arc<ActionState>>, nativelink_error::Error>;
    fn dec_action_attempts(
        &self,
        operation_id: &OperationId
    ) -> Result<u64, Error>;

    fn inc_action_attempts(
        &self,
        operation_id: &OperationId
    ) -> Result<u64, Error>;

    async fn get_operation_id_for_action(
        &self,
        unique_qualifier: &ActionInfoHashKey
    ) -> Result<OperationId, Error>;

    async fn set_action_assignment(
        &self,
        operation_id: OperationId,
        assigned: bool,
    ) -> Result<(), Error>;

    async fn update_action_stages(
        &self,
        operations: &[(OperationId, ActionStage)],
    ) -> Result<(), Error>;

    async fn get_action_state(&self, id: OperationId) -> Result<ActionState, Error>;

    async fn publish_action_state(&self, id: OperationId) -> Result<(), Error>;

    async fn create_new_action(
        &self,
        action_info: &ActionInfo
    ) -> Result<OperationId, Error>;

    async fn find_action_by_hash_key(
        &self,
        unique_qualifier: &ActionInfoHashKey
    ) -> Result<Option<watch::Receiver<Arc<ActionState>>>, Error>;

    async fn get_action_info_for_actions(
        &self,
        actions: &[OperationId]
    ) -> Result<Vec<(OperationId, ActionInfo)>, Error>;

    async fn get_queued_actions(
        &self,
    ) -> Result<Vec<OperationId>, Error>;


    async fn remove_actions_from_queue(
        &self,
        actions: &[OperationId]
    ) -> Result<(), Error>;


   async fn add_actions_to_queue(
        &self,
        actions_with_priority: &[(OperationId, i32)]
    ) -> Result<(), Error>;
    // Return the action stage here.
    // If the stage is ActionStage::Completed the callee
    // should request the stored result directly.
    // Otherwise, the callee should call get_action_subscriber
    // and listen to it for updates to find out when its state changes
    async fn add_or_merge_action(
        &self,
        action_info: &ActionInfo
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error>;
}
