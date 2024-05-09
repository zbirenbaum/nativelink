
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
use std::sync::Arc;
use std::time::Duration;
use futures::Future;
use parking_lot::Mutex;
use async_trait::async_trait;
use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey, ActionStage, ActionState, Id, OperationId, WorkerId, WorkerTimestamp};
use nativelink_util::metrics_utils::Registry;
use nativelink_util::platform_properties::PlatformProperties;
use tokio::sync::watch;

use crate::action_scheduler::ActionScheduler;
use crate::platform_property_manager::PlatformPropertyManager;
use crate::state_manager::StateManager;
// use crate::state_manager::StateManager;
use crate::worker::{Worker, Workers, WorkerUpdate};
use crate::worker_scheduler::WorkerScheduler;
use tracing::warn;
use nativelink_error::{error_if, make_err, make_input_err, Error, ResultExt, Code};
use nativelink_config::schedulers::WorkerAllocationStrategy;
use lru::LruCache;
use tracing::error;
use tokio::sync::Notify;
use tokio::task::JoinHandle;


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
    pub async fn do_try_match(&self) {


    }

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
        self.state_manager.find_existing_action(unique_qualifier).await
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

#[async_trait]
impl WorkerScheduler for SchedulerInstanceState {
    /// Returns the platform property manager.
    fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
        &self.platform_property_manager
    }

    /// Adds a worker to the scheduler and begin using it to execute actions (when able).
    async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
        let mut workers = self.workers.lock();
        let worker_id = worker.id.clone();
        let res = workers
            .add_worker(worker)
            .err_tip(|| "Error while adding worker, removing from pool");
        if let Err(err) = &res {
            self.immediate_evict_worker(&worker_id, err.clone());
        }
        self.tasks_or_workers_change_notify.notify_one();
        res
    }

    /// Similar to `update_action()`, but called when there was an error that is not
    /// related to the task, but rather the worker itself.
    async fn update_action_with_internal_error(
        &self,
        worker_id: &WorkerId,
        unique_qualifier: &ActionInfoHashKey,
        err: Error,
    ) -> Result<(), Error> {
        self.state_manager.update_action_with_internal_error(unique_qualifier).await?;
        let Some((action_info, mut running_action)) =
            self.active_actions.remove_entry(unique_qualifier)
        else {
            self.metrics
                .update_action_with_internal_error_no_action
                .inc();
            error!("Could not find action info in active actions : {unique_qualifier:?}");
            return;
        };

        let due_to_backpressure = err.code == Code::ResourceExhausted;
        // Don't count a backpressure failure as an attempt for an action.
        if due_to_backpressure {
            self.metrics
                .update_action_with_internal_error_backpressure
                .inc();
            running_action.attempts -= 1;
        }
        let Some(running_action_worker_id) = running_action.worker_id else {
            return error!(
            "Got a result from a worker that should not be running the action, Removing worker. Expected action to be unassigned got worker {worker_id}"
          );
        };

        let mut workers = self.workers.lock();
        // Clear this action from the current worker.
        if let Some(worker) = workers.workers.get_mut(worker_id) {
            let was_paused = !worker.can_accept_work();
            // This unpauses, but since we're completing with an error, don't
            // unpause unless all actions have completed.
            worker.complete_action(&action_info);
            // Only pause if there's an action still waiting that will unpause.
            if (was_paused || due_to_backpressure) && worker.has_actions() {
                worker.is_paused = true;
            }
        }

        // Re-queue the action or fail on max attempts.
        self.retry_action(&action_info, worker_id, err);
        self.tasks_or_workers_change_notify.notify_one();
        let mut workers = self.workers.lock();

        workers
            .update_action_with_internal_error(worker_id, unique_qualifier, err)
            .err_tip(|| "Error refreshing lifetime in worker_keep_alive_received()");
    }

    /// Updates the status of an action to the scheduler from the worker.
    async fn update_action(
        &self,
        worker_id: &WorkerId,
        unique_qualifier: &ActionInfoHashKey,
        action_stage: ActionStage,
    ) -> Result<(), Error> {
        self.state_manager.update_action(worker_id, unique_qualifier, action_stage).await
    }

    /// Event for when the keep alive message was received from the worker.
    async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        let mut workers = self.workers.lock();
        workers
            .refresh_lifetime(worker_id, timestamp)
            .err_tip(|| "Error refreshing lifetime in worker_keep_alive_received()");
        Ok(())
    }

    /// Removes worker from pool and reschedule any tasks that might be running on it.
    async fn remove_worker(&self, worker_id: &WorkerId) {
        let mut workers = self.workers.lock();
        workers.remove_worker(worker_id);
        self.tasks_or_workers_change_notify.notify_one();
    }

    /// Removes timed out workers from the pool. This is called periodically by an
    /// external source.
    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        let mut workers = self.workers.lock();
        workers.remove_timedout_workers(now_timestamp, self.worker_timeout_s);
        Ok(())
    }

    /// Sets if the worker is draining or not.
    async fn set_drain_worker(&self, worker_id: WorkerId, is_draining: bool) -> Result<(), Error> {
        let mut workers = self.workers.lock();
        workers.set_drain_worker(worker_id, is_draining);
        self.tasks_or_workers_change_notify.notify_one();
        Ok(())
    }

    /// Register the metrics for the worker scheduler.
    fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {}
}
