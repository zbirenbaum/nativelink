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
pub enum WorkerFields {
    Workers,
    // takes the key and value of the property and adds the worker to the list
    RunningOperations(WorkerId),
    IsPaused(WorkerId),
    IsDraining(WorkerId),
    PlatformProperties(WorkerId),
}

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize, PartialEq)]
pub enum ActionFields {
    Digest(OperationId),
    Name(OperationId),
    Stage(OperationId),
    Attempts(OperationId),
    LastError(OperationId),
    Info(OperationId),
}

#[derive(Clone, ToRedisArgs, FromRedisValue, Serialize, Deserialize, PartialEq)]
pub enum ActionMaps {
    // Sorted set of <OperationId, Priority>
    Queued,
    // <OperationId, WorkerId>
    Assigned(OperationId),
    // <OperationId, ActionResult>
    Completed(OperationId)
}

#[async_trait]
pub trait WorkerSchedulerStateStore: Sync + Send + Unpin {
    async fn get_actions_running_on_worker(
        &self,
        worker_id: &WorkerId
    ) -> Result<Vec<OperationId>, Error>;

    async fn assign_actions_to_worker(
        &self,
        worker_id: &WorkerId,
        operation_ids: Vec<OperationId>
    ) -> Result<Vec<OperationId>, Error>;

    async fn get_worker_running_action(
        &self,
        operation_id: &OperationId
    ) -> Result<Option<WorkerId>, Error>;

    async fn remove_actions_from_worker(
        &self,
        worker_id: &WorkerId,
        operation_id: Vec<OperationId>,
    ) -> Result<(), Error>;
}

#[async_trait]
pub trait ActionSchedulerStateStore: Sync + Send + Unpin {
    async fn subscribe<'a>(
        &'a self,
        key: &'a str,
        initial_state: Option<Arc<ActionState>>
    ) -> Result<watch::Receiver<Arc<ActionState>>, nativelink_error::Error>;

    fn inc_action_attempts(
        &self,
        operation_id: &OperationId
    ) -> Result<u64, Error>;

    async fn get_operation_id_for_action(
        &self,
        unique_qualifier: &ActionInfoHashKey
    ) -> Result<OperationId, Error>;


    async fn update_action_stage(
        &self,
        worker_id: Option<WorkerId>,
        id: OperationId,
        stage: ActionStage,
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

    async fn complete_action(
        &self,
        operation_id: &OperationId,
        result: ActionResult
    ) -> Result<(), Error>;

    async fn get_action_info_for_actions(
        &self,
        actions: Vec<OperationId>
    ) -> Result<Vec<ActionInfo>, Error>;

    // returns operation id and priority tuples
    async fn get_next_n_queued_actions(
        &self,
        num_actions: u64
    ) -> Result<Vec<(OperationId, i32)>, Error>;

    async fn remove_actions_from_queue(
        &self,
        actions: Vec<OperationId>
    ) -> Result<(), Error>;


   async fn add_actions_to_queue(
        &self,
        actions_with_priority: Vec<(OperationId, i32)>
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
