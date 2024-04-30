// Copyright 2023 The NativeLink Authors. All rights reserved.
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

use async_trait::async_trait;
use nativelink_error::Error;
use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey, ActionStage, ActionState, OperationId};
use nativelink_util::metrics_utils::Registry;
use tokio::sync::watch;

use crate::action_scheduler::ActionScheduler;
use crate::platform_property_manager::PlatformPropertyManager;
use crate::state_manager::StateManager;
// use crate::state_manager::StateManager;
use crate::worker::{Worker, WorkerId, WorkerTimestamp};
use crate::worker_scheduler::WorkerScheduler;

// pub struct ActionSchedulerInstance {
//     supported_platform_properties: scheduler_cfg.supported_platform_properties
//     state_manager: Arc<StateManager>,
// }
//
//
// impl ActionSchedulerInstance {
//     pub fn new(
//         scheduler_cfg: &nativelink_config::schedulers::ActionSchedulerInstance,
//     ) -> Self {
//         Self {
//             supported_platform_properties: scheduler_cfg.supported_platform_properties
//                 .clone()
//                 .unwrap_or_default(),
//             state_manager: Arc::new(StateManager::new(
//                 scheduler_cfg.db_url.clone()
//             ))
//         }
//     }
// }
//
#[async_trait]
impl ActionScheduler for ActionSchedulerInstance {
    async fn add_action(
        &self,
        action_info: ActionInfo,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        self.state_manager.add_action(action_info).await
    }

    /// Returns the platform property manager.
    async fn get_platform_property_manager(
        &self,
        _instance_name: &str,
    ) -> Result<Arc<PlatformPropertyManager>, Error> {
        todo!()
    }

    /// Find an existing action by its name.
    async fn find_existing_action(
        &self,
        _action_id: &OperationId,
    ) -> Option<watch::Receiver<Arc<ActionState>>> {
        todo!()
    }

    /// Cleans up the cache of recently completed actions.
    async fn clean_recently_completed_actions(&self) {
        todo!()
    }

    /// Register the metrics for the action scheduler.
    fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {

        todo!()
    }
}

pub struct WorkerSchedulerInstance {
    state_manager: Arc<StateManager>,
}

impl WorkerSchedulerInstance {
    pub fn new(
        scheduler_cfg: &nativelink_config::schedulers::WorkerSchedulerInstance) -> Self {
        Self {
            state_manager: Arc::new(StateManager::new(
                scheduler_cfg.db_url.clone(),
                scheduler_cfg.supported_platform_properties
                    .clone()
                    .unwrap_or_default()
            )),
        }
    }
}

#[async_trait]
impl WorkerScheduler for WorkerSchedulerInstance {
    /// Returns the platform property manager.
    fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
        todo!()
    }

    /// Adds a worker to the scheduler and begin using it to execute actions (when able).
    async fn add_worker(&self, _worker: Worker) -> Result<(), Error> {
        todo!()
    }

    /// Similar to `update_action()`, but called when there was an error that is not
    /// related to the task, but rather the worker itself.
    async fn update_action_with_internal_error(
        &self,
        _worker_id: &WorkerId,
        _action_info_hash_key: &ActionInfoHashKey,
        _err: Error,
    ) {
        todo!()
    }

    /// Updates the status of an action to the scheduler from the worker.
    async fn update_action(
        &self,
        _worker_id: &WorkerId,
        _action_info_hash_key: &ActionInfoHashKey,
        _action_stage: ActionStage,
    ) -> Result<(), Error> {
        todo!()
    }

    /// Event for when the keep alive message was received from the worker.
    async fn worker_keep_alive_received(
        &self,
        _worker_id: &WorkerId,
        _timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        todo!()
    }

    /// Removes worker from pool and reschedule any tasks that might be running on it.
    async fn remove_worker(&self, _worker_id: WorkerId) {
        todo!()
    }

    /// Removes timed out workers from the pool. This is called periodically by an
    /// external source.
    async fn remove_timedout_workers(&self, _now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        todo!()
    }

    /// Sets if the worker is draining or not.
    async fn set_drain_worker(&self, _worker_id: WorkerId, _is_draining: bool) -> Result<(), Error> {
        todo!()
    }

    /// Register the metrics for the worker scheduler.
    fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {}
}
