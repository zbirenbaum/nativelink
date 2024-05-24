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

use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;
use async_trait::async_trait;

use bitflags::bitflags;
use futures::{Stream};
use tokio::sync::watch;

use crate::worker::WorkerId;
use nativelink_error::Error;
use nativelink_util::action_messages::{ActionInfoHashKey, ActionStage};
use nativelink_util::action_messages::{ActionInfo, ActionState};
use nativelink_util::common::DigestInfo;

bitflags! {
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct OperationStageFlags: u32 {
        const CacheCheck = 1 << 1;
        const Queued     = 1 << 2;
        const Executing  = 1 << 3;
        const Completed  = 1 << 4;
        const Any        = u32::MAX;
    }
}

pub struct MatchingEngineActionStateResult {
    pub action_info: ActionInfo,
}
impl MatchingEngineActionStateResult {
    fn new(action_info: ActionInfo) -> Self {
        Self { action_info }
    }
}

#[async_trait]
impl ActionStateResult for MatchingEngineActionStateResult {
    async fn as_state(&self) -> Result<Arc<ActionState>, Error> {
        unimplemented!()
    }

    async fn as_receiver(&self) -> Result<&'_ watch::Receiver<Arc<ActionState>>, Error> {
        unimplemented!()
    }

    async fn as_action_info(&self) -> Result<&ActionInfo, Error> {
        Ok(&self.action_info)
    }
}
#[async_trait]
pub trait ActionStateResult: Send + Sync + 'static {
    async fn as_state(&self) -> Result<Arc<ActionState>, Error>;
    async fn as_action_info(&self) -> Result<&ActionInfo, Error>;
    async fn as_receiver(&self) -> Result<&'_ watch::Receiver<Arc<ActionState>>, Error>;
}

/// Unique id of worker.
#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub struct Id {
    id: u128,
}

pub type OperationId = Id;

/// The filters used to query operations from the state manager.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OperationFilter {
    // TODO(adams): create rust builder pattern?

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

    pub unique_qualifier: Option<ActionInfoHashKey>,

    pub order_by: Option<OrderBy>
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OperationFields {
    Priority,
    Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderBy {
    pub fields: Vec<OperationFields>,
    pub desc: bool
}
#[async_trait]
pub trait ClientStateManager {
    /// Add a new action to the queue or joins an existing action.
    async fn add_action(& mut self, action_info: ActionInfo) -> Result<Arc<dyn ActionStateResult>, Error>;

    /// Returns a stream of operations that match the filter.
    async fn filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Vec<Box<dyn ActionStateResult>>;
}


#[async_trait]
pub trait WorkerStateManager {
    /// Update that state of an operation.
    /// The worker must also send periodic updates even if the state
    /// did not change with a modified timestamp in order to prevent
    /// the operation from being considered stale and being rescheduled.
    async fn update_operation(
        &mut self,
        action_info_hash_key: ActionInfoHashKey, // TODO(adams): Make OperationId
        worker_id: WorkerId,
        action_stage: Result<ActionStage, Error>,
    ) -> Result<(), Error>;
}

#[async_trait]
pub trait MatchingEngineStateManager {
    /// Returns a stream of operations that match the filter.
    fn filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Vec<Box<dyn ActionStateResult>>;

    /// Update that state of an operation.
    async fn update_operation(
        &self,
        action_info_hash_key: ActionInfoHashKey, // TODO(adams): Make OperationId
        worker_id: Option<WorkerId>,
        action_stage: Result<ActionStage, Error>,
    ) -> Result<(), Error>;

    /// Remove an operation from the state manager.
    /// It is important to use this function to remove operations
    /// that are no longer needed to prevent memory leaks.
    async fn remove_operation(&self, action_info_hash_key: ActionInfoHashKey /* TODO(adams): Make OperationId */) -> Result<(), Error>;
}
