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
#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]
use async_trait::async_trait;
use futures::Future;
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionStage, ActionState, Id, OperationId, WorkerId,
    WorkerTimestamp,
};
use nativelink_util::metrics_utils::Registry;
use nativelink_util::platform_properties::PlatformProperties;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

use crate::action_scheduler::ActionScheduler;
use crate::platform_property_manager::PlatformPropertyManager;
use crate::state_manager::StateManager;
// use crate::state_manager::StateManager;
use crate::worker::{Worker, WorkerUpdate, Workers};
use crate::worker_scheduler::WorkerScheduler;
use lru::LruCache;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_error::{error_if, make_err, make_input_err, Code, Error, ResultExt};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::error;
use tracing::warn;

/// Default timeout for workers in seconds.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_WORKER_TIMEOUT_S: u64 = 5;

/// Default timeout for recently completed actions in seconds.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_RETAIN_COMPLETED_FOR_S: u64 = 60;

/// Default times a job can retry before failing.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_MAX_JOB_RETRIES: usize = 3;

pub struct SchedulerInstanceState {
    state_manager: StateManager,
    workers: Mutex<Workers>,
    tasks_or_workers_change_notify: Arc<Notify>,
    platform_property_manager: Arc<PlatformPropertyManager>,
    worker_timeout_s: u64,
    retain_completed_for_s: u64,
    max_job_retries: usize,
}

impl SchedulerInstanceState {
    pub async fn get_workers_mut(&self) -> MutexGuard<'_, Workers> {
        let lock = self.workers.lock();
        lock
    }
    pub async fn do_try_match(&self) {}

    fn immediate_evict_worker(&self, worker_id: &WorkerId, err: Error) {
        let mut workers = self.workers.lock();
        workers.immediate_evict_worker(worker_id, err);
        // Note: Calling this many time is very cheap, it'll only trigger `do_try_match` once.
        self.tasks_or_workers_change_notify.notify_one();
    }
}

#[async_trait]
impl ActionScheduler for SchedulerInstanceState {
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
        Ok(self.platform_property_manager.clone())
    }

    async fn find_existing_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Option<watch::Receiver<Arc<ActionState>>> {
        self.state_manager
            .find_existing_action(unique_qualifier)
            .await
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

// #[async_trait]
// impl WorkerScheduler for SchedulerInstanceState {
//     /// Returns the platform property manager.
//     fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
//         &self.platform_property_manager
//     }
//
//     /// Adds a worker to the scheduler and begin using it to execute actions (when able).
//     async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
//         let mut workers = self.workers.lock();
//         let worker_id = worker.id.clone();
//         let res = workers
//             .add_worker(worker)
//             .err_tip(|| "Error while adding worker, removing from pool");
//         if let Err(err) = &res {
//             self.immediate_evict_worker(&worker_id, err.clone());
//         }
//         self.tasks_or_workers_change_notify.notify_one();
//         res
//     }
//
//     /// Similar to `update_action()`, but called when there was an error that is not
//     /// related to the task, but rather the worker itself.
//     async fn update_action_with_internal_error(
//         &self,
//         _worker_id: &WorkerId,
//         _unique_qualifier: &ActionInfoHashKey,
//         _err: Error,
//     ) -> Result<(), Error> {
//         todo!()
//     }
//
//     /// Updates the status of an action to the scheduler from the worker.
//     async fn update_action(
//         &self,
//         worker_id: &WorkerId,
//         unique_qualifier: &ActionInfoHashKey,
//         action_stage: ActionStage,
//     ) -> Result<(), Error> {
//         self.state_manager.update_action(worker_id, unique_qualifier, action_stage).await
//     }
//
//     /// Event for when the keep alive message was received from the worker.
//     async fn worker_keep_alive_received(
//         &self,
//         worker_id: &WorkerId,
//         timestamp: WorkerTimestamp,
//     ) -> Result<(), Error> {
//         let mut workers = self.workers.lock();
//         workers
//             .refresh_lifetime(worker_id, timestamp)
//             .err_tip(|| "Error refreshing lifetime in worker_keep_alive_received()");
//         Ok(())
//     }
//
//     /// Removes worker from pool and reschedule any tasks that might be running on it.
//     async fn remove_worker(&self, worker_id: &WorkerId) {
//         let mut workers = self.workers.lock();
//         workers.remove_worker(worker_id);
//         self.tasks_or_workers_change_notify.notify_one();
//     }
//
//     /// Removes timed out workers from the pool. This is called periodically by an
//     /// external source.
//     async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
//         let mut workers = self.workers.lock();
//         workers.remove_timedout_workers(now_timestamp, self.worker_timeout_s);
//         Ok(())
//     }
//
//     /// Sets if the worker is draining or not.
//     async fn set_drain_worker(&self, worker_id: WorkerId, is_draining: bool) -> Result<(), Error> {
//         let mut workers = self.workers.lock();
//         workers.set_drain_worker(worker_id, is_draining);
//         self.tasks_or_workers_change_notify.notify_one();
//         Ok(())
//     }
//
//     /// Register the metrics for the worker scheduler.
//     fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {}
// }
