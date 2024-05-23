// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use redis_macros::{FromRedisValue, ToRedisArgs};
use tokio::sync::watch;
use bitflags::bitflags;
use nativelink_error::{make_input_err, Error};
use tonic::async_trait;
use std::time::SystemTime;
use nativelink_util::common::DigestInfo;
use nativelink_util::action_messages::{ActionInfo, ActionResult, ActionStage, ActionState, Id};
use tokio_stream::Stream;
use serde::{Serialize, Deserialize};


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

#[async_trait]
pub trait ActionStateResult {
    fn as_state(&self) -> Result<Arc<ActionState>, Error>;
    async fn as_receiver(&self) -> Result<watch::Receiver<Arc<ActionState>>, Error>;
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, ToRedisArgs, FromRedisValue)]
pub struct RedisOperation {
    /// The stage(s) that the operation must be in.
    stages: OperationStageFlags,
    /// The operation id.
    operation_id: Id,
    /// The worker that the operation must be assigned to.
    worker_id: Option<Id>,
    /// The digest of the action that the operation must have.
    action_digest: Option<DigestInfo>,
    /// The operation must have it's worker timestamp before this time.
    worker_update_before: Option<SystemTime>,
    /// The operation must have been completed before this time.
    completed_before: Option<SystemTime>,
    /// The operation must have it's last client update before this time.
    last_client_update_before: Option<SystemTime>,
}

/// The filters used to query operations from the state manager.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OperationFilter {
    /// The stage(s) that the operation must be in.
    stages: OperationStageFlags,

    /// The operation id.
    operation_id: Option<Id>,

    /// The worker that the operation must be assigned to.
    worker_id: Option<Id>,

    /// The digest of the action that the operation must have.
    action_digest: Option<DigestInfo>,

    /// The operation must have it's worker timestamp before this time.
    worker_update_before: Option<SystemTime>,

    /// The operation must have been completed before this time.
    completed_before: Option<SystemTime>,

    /// The operation must have it's last client update before this time.
    last_client_update_before: Option<SystemTime>,
}

pub struct OperationFilterKeys {
    /// The stage(s) that the operation must be in.
    stages: OperationStageFlags,

    /// The operation id.
    operation_id: Option<Id>,

    /// The worker that the operation must be assigned to.
    worker_id: Option<Id>,

    /// The digest of the action that the operation must have.
    action_digest: Option<DigestInfo>,

    /// The operation must have it's worker timestamp before this time.
    worker_update_before: Option<SystemTime>,

    /// The operation must have been completed before this time.
    completed_before: Option<SystemTime>,

    /// The operation must have it's last client update before this time.
    last_client_update_before: Option<SystemTime>,
}

impl OperationFilter {
    pub fn get_keys_for_id(&self, id: Id) {

    }
}

#[async_trait]
pub trait ClientStateManager {
    /// Add a new action to the queue or joins an existing action.
    async fn add_action(
        &self,
        action_info: ActionInfo,
    ) -> Result<Box<dyn ActionStateResult>, Error>;

    /// Returns a stream of operations that match the filter.
    fn filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Box<dyn Stream<Item = dyn ActionStateResult>>;
}

#[async_trait]
pub trait WorkerStateManager {
    /// Update that state of an operation.
    /// The worker must also send periodic updates even if the state
    /// did not change with a modified timestamp in order to prevent
    /// the operation from being considered stale and being rescheduled.
    async fn update_operation(
        &self,
        operation_id: Id,
        worker_id: Id,
        action_stage: Result<ActionStage, Error>,
    ) -> Result<(), Error>;
}

#[async_trait]
pub trait MatchingEngineStateManager {
    /// Returns a stream of operations that match the filter.
    async fn filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Box<dyn Stream<Item = dyn ActionStateResult>>;

    /// Update that state of an operation.
    async fn update_operation(
        &self,
        operation_id: Id,
        worker_id: Option<Id>,
        action_stage: ActionStage,
    ) -> Result<(), Error>;

    /// Remove an operation from the state manager.
    /// It is important to use this function to remove operations
    /// that are no longer needed to prevent memory leaks.
    async fn remove_operation(
        &self,
        operation_id: Id,
    ) -> Result<(), Error>;
}
