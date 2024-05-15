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
use async_lock::MutexGuard;
use futures::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionStage, ActionState, Id, OperationId, WorkerId,
    WorkerTimestamp,
};
use nativelink_util::metrics_utils::Registry;
use nativelink_util::platform_properties::PlatformProperties;
use parking_lot::Mutex;
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
use tracing::{event, warn, Level};

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
    workers: async_lock::Mutex<Workers>,
    tasks_or_workers_change_notify: Arc<Notify>,
    platform_property_manager: Arc<PlatformPropertyManager>,
    worker_timeout_s: u64,
    retain_completed_for_s: u64,
    max_job_retries: usize,
}

impl SchedulerInstanceState {
    pub async fn do_try_match(&self) {
        // TODO(blaise.bruer) This is a bit difficult because of how rust's borrow checker gets in
        // the way. We need to conditionally remove items from the `queued_action`. Rust is working
        // to add `drain_filter`, which would in theory solve this problem, but because we need
        // to iterate the items in reverse it becomes more difficult (and it is currently an
        // unstable feature [see: https://github.com/rust-lang/rust/issues/70530]).
        println!("Matching");
        let queued_actions_res: Result<Vec<OperationId>, Error> =
            self.state_manager.get_queued_actions().await;
        let mut worker_lock = self.workers.lock().await;
        let Ok(queued_actions) = queued_actions_res else {
            return;
        };
        let queued_actions_with_infos: Vec<(OperationId, ActionInfo)> = self
            .state_manager
            .get_action_infos(&queued_actions)
            .await
            .unwrap();

        for (operation_id, action_info) in queued_actions_with_infos {
            let Some(worker) =
                ({ worker_lock.find_worker_with_properties_mut(&action_info.platform_properties) })
            else {
                continue;
            };
            let worker_id = worker.id;

            // Try to notify our worker of the new action to run, if it fails remove the worker from the
            // pool and try to find another worker.
            let notify_worker_result =
                worker.notify_update(WorkerUpdate::RunAction(action_info.clone().into()));
            if notify_worker_result.is_err() {
                // Remove worker, as it is no longer receiving messages and let it try to find another worker.
                event!(
                    Level::WARN,
                    ?worker_id,
                    ?action_info,
                    "Worker command failed, removing worker",
                );
                {
                    let mut workers = self.workers.lock().await;
                    workers.immediate_evict_worker(
                        &worker_id,
                        make_err!(
                            Code::Internal,
                            "Worker command failed, removing worker {}",
                            worker_id
                        ),
                    );
                }
                return;
            }

            // At this point everything looks good, so remove it from the queue and add it to active actions.
            self.state_manager
                .remove_actions_from_queue(&[operation_id])
                .await;
            self.state_manager
                .update_action(
                    &worker_id,
                    &action_info.unique_qualifier,
                    ActionStage::Executing,
                )
                .await;

            // awaited_action.attempts += 1;
        }
    }
}

#[async_trait]
impl ActionScheduler for SchedulerInstanceState {
    async fn add_action(
        &self,
        action_info: ActionInfo,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        let res = self.state_manager.add_action(action_info).await;
        self.tasks_or_workers_change_notify.notify_one();
        res
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
        self.state_manager
            .clean_recently_completed_actions(self.retain_completed_for_s);
    }

    /// Register the metrics for the action scheduler.
    fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {
        todo!()
    }
}
#[async_trait]
impl WorkerScheduler for SchedulerInstanceState {
    async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
        let mut workers = self.workers.lock().await;
        workers.add_worker(worker)
    }
    /// Returns the platform property manager.
    fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
        &self.platform_property_manager
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
        worker_id: &WorkerId,
        action_info_hash_key: &ActionInfoHashKey,
        action_stage: ActionStage,
    ) -> Result<(), Error> {
        self.state_manager
            .update_action(worker_id, action_info_hash_key, action_stage)
            .await
    }

    /// Event for when the keep alive message was received from the worker.
    async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        let mut workers = self.workers.lock().await;
        workers
            .refresh_lifetime(worker_id, timestamp)
            .err_tip(|| "Error refreshing lifetime in worker_keep_alive_received()")
    }

    async fn remove_worker(&self, worker_id: &WorkerId) {
        let mut workers = self.workers.lock().await;

        let err = make_err!(
            Code::Internal,
            "Worker command failed, removing worker {}",
            worker_id
        );
        warn!("{:?}", err);
        workers.immediate_evict_worker(worker_id, err);
        self.tasks_or_workers_change_notify.notify_one();
    }

    /// Removes timed out workers from the pool. This is called periodically by an
    /// external source.
    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        let mut workers = self.workers.lock().await;
        let res = workers.remove_timedout_workers(now_timestamp, self.worker_timeout_s);
        self.tasks_or_workers_change_notify.notify_one();
        res
    }

    /// Sets if the worker is draining or not.
    async fn set_drain_worker(&self, worker_id: WorkerId, is_draining: bool) -> Result<(), Error> {
        let mut workers = self.workers.lock().await;
        let res = workers.set_drain_worker(worker_id, is_draining);
        self.tasks_or_workers_change_notify.notify_one();
        res
    }
}

pub struct SchedulerInstance {
    inner: Arc<SchedulerInstanceState>,
    task_worker_matching_future: JoinHandle<()>,
}

impl SchedulerInstance {
    #[inline]
    #[must_use]
    pub fn new(scheduler_cfg: &nativelink_config::schedulers::SchedulerInstance) -> Self {
        Self::new_with_callback(scheduler_cfg, || {
            // The cost of running `do_try_match()` is very high, but constant
            // in relation to the number of changes that have happened. This means
            // that grabbing this lock to process `do_try_match()` should always
            // yield to any other tasks that might want the lock. The easiest and
            // most fair way to do this is to sleep for a small amount of time.
            // Using something like tokio::task::yield_now() does not yield as
            // aggresively as we'd like if new futures are scheduled within a future.
            tokio::time::sleep(Duration::from_millis(1))
        })
    }

    pub fn new_with_callback<
        Fut: Future<Output = ()> + Send,
        F: Fn() -> Fut + Send + Sync + 'static,
    >(
        scheduler_cfg: &nativelink_config::schedulers::SchedulerInstance,
        on_matching_engine_run: F,
    ) -> Self {
        let platform_property_manager = Arc::new(PlatformPropertyManager::new(
            scheduler_cfg
                .supported_platform_properties
                .clone()
                .unwrap_or_default(),
        ));
        let mut worker_timeout_s = scheduler_cfg.worker_timeout_s;
        if worker_timeout_s == 0 {
            worker_timeout_s = DEFAULT_WORKER_TIMEOUT_S;
        }

        let mut retain_completed_for_s = scheduler_cfg.retain_completed_for_s;
        if retain_completed_for_s == 0 {
            retain_completed_for_s = DEFAULT_RETAIN_COMPLETED_FOR_S;
        }

        let mut max_job_retries = scheduler_cfg.max_job_retries;
        if max_job_retries == 0 {
            max_job_retries = DEFAULT_MAX_JOB_RETRIES;
        }

        let tasks_or_workers_change_notify = Arc::new(Notify::new());
        let inner = Arc::new(SchedulerInstanceState {
            workers: async_lock::Mutex::new(Workers::new(scheduler_cfg.allocation_strategy)),
            state_manager: StateManager::new(scheduler_cfg.db_url.clone()),
            platform_property_manager,
            tasks_or_workers_change_notify: tasks_or_workers_change_notify.clone(),
            worker_timeout_s,
            retain_completed_for_s,
            max_job_retries,
        });

        let weak_inner = Arc::downgrade(&inner);
        Self {
            inner,
            task_worker_matching_future: tokio::spawn(async move {
                // Break out of the loop only when the inner is dropped.
                loop {
                    tasks_or_workers_change_notify.notified().await;
                    match weak_inner.upgrade() {
                        // Note: According to `parking_lot` documentation, the default
                        // `Mutex` implementation is eventual fairness, so we don't
                        // really need to worry about this thread taking the lock
                        // starving other threads too much.
                        Some(inner) => {
                            inner.do_try_match().await;
                        }
                        // If the inner went away it means the scheduler is shutting
                        // down, so we need to resolve our future.
                        None => return,
                    };
                    on_matching_engine_run().await;
                }
                // Unreachable.
            }),
        }
    }

    pub async fn contains_worker_for_test(&self, worker_id: &WorkerId) -> bool {
        let inner = self.inner.workers.lock().await;
        inner.workers.contains(worker_id)
    }

    /// Checks to see if the worker can accept work. Should only be used in unit tests.
    pub async fn can_worker_accept_work_for_test(
        &self,
        worker_id: &WorkerId,
    ) -> Result<bool, Error> {
        let mut inner = self.inner.workers.lock().await;
        let worker = inner.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!("WorkerId '{}' does not exist in workers map", worker_id)
        })?;
        Ok(worker.can_accept_work())
    }

    /// A unit test function used to send the keep alive message to the worker from the server.
    pub async fn send_keep_alive_to_worker_for_test(
        &self,
        worker_id: &WorkerId,
    ) -> Result<(), Error> {
        let mut inner = self.inner.workers.lock().await;
        let worker = inner.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!("WorkerId '{}' does not exist in workers map", worker_id)
        })?;
        worker.keep_alive()
    }
}

impl Drop for SchedulerInstance {
    fn drop(&mut self) {
        self.task_worker_matching_future.abort();
    }
}

#[async_trait]
impl ActionScheduler for SchedulerInstance {
    async fn add_action(
        &self,
        action_info: ActionInfo,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        self.inner.add_action(action_info).await
    }

    /// Returns the platform property manager.
    async fn get_platform_property_manager(
        &self,
        _instance_name: &str,
    ) -> Result<Arc<PlatformPropertyManager>, Error> {
        Ok(self.inner.platform_property_manager.clone())
    }

    async fn find_existing_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Option<watch::Receiver<Arc<ActionState>>> {
        self.inner
            .state_manager
            .find_existing_action(unique_qualifier)
            .await
    }

    /// Cleans up the cache of recently completed actions.
    async fn clean_recently_completed_actions(&self) {
        self.inner.clean_recently_completed_actions();
    }

    /// Register the metrics for the action scheduler.
    fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {}
}

#[async_trait]
impl WorkerScheduler for SchedulerInstance {
    /// Returns the platform property manager.
    fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
        &self.inner.platform_property_manager
    }

    /// Adds a worker to the scheduler and begin using it to execute actions (when able).
    async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
        self.inner.add_worker(worker).await
    }

    /// Updates the status of an action to the scheduler from the worker.
    async fn update_action(
        &self,
        worker_id: &WorkerId,
        action_info_hash_key: &ActionInfoHashKey,
        action_stage: ActionStage,
    ) -> Result<(), Error> {
        self.inner
            .update_action(worker_id, action_info_hash_key, action_stage)
            .await
    }

    /// Event for when the keep alive message was received from the worker.
    async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        self.inner
            .worker_keep_alive_received(worker_id, timestamp)
            .await
    }

    async fn remove_worker(&self, worker_id: &WorkerId) {
        self.inner.remove_worker(worker_id).await
    }

    /// Removes timed out workers from the pool. This is called periodically by an
    /// external source.
    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        self.inner.remove_timedout_workers(now_timestamp).await
    }

    /// Sets if the worker is draining or not.
    async fn set_drain_worker(&self, worker_id: WorkerId, is_draining: bool) -> Result<(), Error> {
        self.inner.set_drain_worker(worker_id, is_draining).await
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
    /// Register the metrics for the worker scheduler.
    fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {}
}
