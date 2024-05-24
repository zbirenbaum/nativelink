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
use tokio::sync::watch;
use nativelink_error::Error;
use tonic::async_trait;
use std::time::SystemTime;
use nativelink_util::common::DigestInfo;
use nativelink_util::action_messages::{ActionInfo, ActionStage, ActionState, OperationId, WorkerId};
use tokio_stream::Stream;
use serde::{Serialize, Deserialize};
use crate::type_wrappers::OperationStageFlags;


#[async_trait]
pub trait ActionStateResult {
    fn as_state(&self) -> Result<Arc<ActionState>, Error>;
    async fn as_receiver(&self) -> Result<watch::Receiver<Arc<ActionState>>, Error>;
}

#[derive(Eq, PartialEq, Hash, Clone, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub enum RedisPlatformPropertyType {
    Exact,
    Minimum,
    Priority,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OperationFilter {
    /// The stage(s) that the operation must be in.
    pub stages: OperationStageFlags,

    /// The operation id.
    pub operation_id: Option<OperationId>,

    /// The worker that the operation must be assigned to.
    pub worker_id: Option<WorkerId>,

    /// The digest of the action that the operation must have.
    pub action_digest: Option<DigestInfo>,

    /// The operation must have it's worker timestamp before this time.
    pub worker_update_before: Option<SystemTime>,

    /// The operation must have been completed before this time.
    pub completed_before: Option<SystemTime>,

    /// The operation must have it's last client update before this time.
    pub last_client_update_before: Option<SystemTime>,
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
        operation_id: OperationId,
        worker_id: WorkerId,
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
        operation_id: OperationId,
        worker_id: Option<WorkerId>,
        action_stage: ActionStage,
    ) -> Result<(), Error>;

    /// Remove an operation from the state manager.
    /// It is important to use this function to remove operations
    /// that are no longer needed to prevent memory leaks.
    async fn remove_operation(
        &self,
        operation_id: OperationId,
    ) -> Result<(), Error>;
}
