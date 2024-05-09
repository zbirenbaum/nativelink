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
use crate::worker::{Worker, WorkerUpdate};
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
            workers: Mutex::new(Workers::new(scheduler_cfg.allocation_strategy)),
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
                            inner.do_try_match();
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

    pub fn contains_worker_for_test(&self, worker_id: &WorkerId) -> bool {
        let inner = self.inner.workers.lock();
        inner.workers.contains(worker_id)
    }

    /// Checks to see if the worker can accept work. Should only be used in unit tests.
    pub fn can_worker_accept_work_for_test(&self, worker_id: &WorkerId) -> Result<bool, Error> {
        let mut inner = self.inner.workers.lock();
        let worker = inner.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!("WorkerId '{}' does not exist in workers map", worker_id)
        })?;
        Ok(worker.can_accept_work())
    }

    /// A unit test function used to send the keep alive message to the worker from the server.
    pub fn send_keep_alive_to_worker_for_test(&self, worker_id: &WorkerId) -> Result<(), Error> {
        let mut inner = self.inner.workers.lock();
        let worker = inner.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!("WorkerId '{}' does not exist in workers map", worker_id)
        })?;
        worker.keep_alive()
    }

}
impl SchedulerInstanceState {
    pub async fn do_try_match(&self) {

    }

    pub async fn retry_action(&self, action_info: &Arc<ActionInfo>, worker_id: &WorkerId, err: Error) {
        // Try to remove action from running actions otherwise error
        // If action atttemps > max retries:
    // match self.active_actions.remove(action_info) {
    //     Some(running_action) => {
    //         let mut awaited_action = running_action;
    //         let send_result = if awaited_action.attempts >= self.max_job_retries {
    //             self.metrics.retry_action_max_attempts_reached.inc();
    //             Arc::make_mut(&mut awaited_action.current_state).stage = ActionStage::Completed(ActionResult {
    //                 execution_metadata: ExecutionMetadata {
    //                     worker: format!("{worker_id}"),
    //                     ..ExecutionMetadata::default()
    //                 },
    //                 error: Some(err.merge(make_err!(
    //                     Code::Internal,
    //                     "Job cancelled because it attempted to execute too many times and failed"
    //                 ))),
    //                 ..ActionResult::default()
    //             });
    //             awaited_action
    //                 .notify_channel
    //                 .send(awaited_action.current_state.clone())
    //             // Do not put the action back in the queue here, as this action attempted to run too many
    //             // times.
    //         } else {
    //             self.metrics.retry_action.inc();
    //             Arc::make_mut(&mut awaited_action.current_state).stage = ActionStage::Queued;
    //             let send_result = awaited_action
    //                 .notify_channel
    //                 .send(awaited_action.current_state.clone());
    //             self.queued_actions_set.insert(action_info.clone());
    //             self.queued_actions
    //                 .insert(action_info.clone(), awaited_action);
    //             send_result
    //         };
    //
    //         if send_result.is_err() {
    //             self.metrics.retry_action_no_more_listeners.inc();
    //             // Don't remove this task, instead we keep them around for a bit just in case
    //             // the client disconnected and will reconnect and ask for same job to be executed
    //             // again.
    //             warn!(
    //                 "Action {} has no more listeners during evict_worker()",
    //                 action_info.digest().hash_str()
    //             );
    //         }
    //     }
    //     None => {
    //         error!("Worker stated it was running an action, but it was not in the active_actions : Worker: {:?}, ActionInfo: {:?}", worker_id, action_info);
    //     }
    }

    fn immediate_evict_worker(&mut self, worker_id: &WorkerId, err: Error) {
        if let Some(mut worker) = self.remove_worker(worker_id) {
            // We don't care if we fail to send message to worker, this is only a best attempt.
            let _ = worker.notify_update(WorkerUpdate::Disconnect);
            // We create a temporary Vec to avoid doubt about a possible code
            // path touching the worker.running_action_infos elsewhere.
            for action_info in worker.running_action_infos.drain() {
                self.retry_action(&action_info, worker_id, err.clone());
            }
        }
        // Note: Calling this many time is very cheap, it'll only trigger `do_try_match` once.
        self.tasks_or_workers_change_notify.notify_one();
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
        self.inner.state_manager.add_action(action_info).await
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


struct Workers {
    workers: LruCache<WorkerId, Worker>,
    /// The allocation strategy for workers.
    allocation_strategy: WorkerAllocationStrategy,
}

impl Workers {
    fn new(allocation_strategy: WorkerAllocationStrategy) -> Self {
        Self {
            workers: LruCache::unbounded(),
            allocation_strategy,
        }
    }

    /// Refreshes the lifetime of the worker with the given timestamp.
    fn refresh_lifetime(
        &mut self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        let worker = self.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!(
                "Worker not found in worker map in refresh_lifetime() {}",
                worker_id
            )
        })?;
        error_if!(
            worker.last_update_timestamp > timestamp,
            "Worker already had a timestamp of {}, but tried to update it with {}",
            worker.last_update_timestamp,
            timestamp
        );
        worker.last_update_timestamp = timestamp;
        Ok(())
    }

    /// Adds a worker to the pool.
    /// Note: This function will not do any task matching.
    fn add_worker(&mut self, worker: Worker) -> Result<(), Error> {
        let worker_id = worker.id;
        self.workers.put(worker_id, worker);

        // Worker is not cloneable, and we do not want to send the initial connection results until
        // we have added it to the map, or we might get some strange race conditions due to the way
        // the multi-threaded runtime works.
        let worker = self.workers.peek_mut(&worker_id).unwrap();
        let res = worker
            .send_initial_connection_result()
            .err_tip(|| "Failed to send initial connection result to worker");
        if let Err(e) = &res {
            error!(
                "Worker connection appears to have been closed while adding to pool : {:?}",
                e
            );
        }
        res
    }

    /// Removes worker from pool.
    /// Note: The caller is responsible for any rescheduling of any tasks that might be
    /// running.
    fn remove_worker(&mut self, worker_id: &WorkerId) -> Option<Worker> {
        self.workers.pop(worker_id)
    }

    /// Attempts to find a worker that is capable of running this action.
    // TODO(blaise.bruer) This algorithm is not very efficient. Simple testing using a tree-like
    // structure showed worse performance on a 10_000 worker * 7 properties * 1000 queued tasks
    // simulation of worst cases in a single threaded environment.
    fn find_worker_with_properties_mut<'a>(
        &'a mut self,
        action_platform_properties: &PlatformProperties,
    ) -> Option<&'a mut Worker> {
        let action_properties = action_platform_properties;
        let mut workers_iter = self.workers.iter_mut();
        let workers_iter = match self.allocation_strategy {
            // Use rfind to get the least recently used that satisfies the properties.
            WorkerAllocationStrategy::least_recently_used => workers_iter.rfind(|(_, w)| {
                w.can_accept_work() && action_properties.is_satisfied_by(&w.platform_properties)
            }),
            // Use find to get the most recently used that satisfies the properties.
            WorkerAllocationStrategy::most_recently_used => workers_iter.find(|(_, w)| {
                w.can_accept_work() && action_properties.is_satisfied_by(&w.platform_properties)
            }),
        };
        let worker_id = workers_iter.map(|(_, w)| &w.id);
        // We need to "touch" the worker to ensure it gets re-ordered in the LRUCache, since it was selected.
        if let Some(&worker_id) = worker_id {
            self.workers.get_mut(&worker_id)
        } else {
            None
        }
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
        let res = workers.add_worker(worker);
        if let Err(err) = &res {
            workers.immediate_evict_worker(&worker_id, err.clone());
        }
        self.tasks_or_workers_change_notify.notify_one();
        res
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
        self.state_manager.update_action(worker_id, action_info_hash_key, action_stage).await
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
    async fn remove_worker(&self, worker_id: &WorkerId) {
        let err = nativelink_error::make_err!(nativelink_error::Code::Internal, "Received request to remove worker");
        let mut inner = self.workers.lock();
        if let Some(mut worker) = inner.remove_worker(worker_id) {
            let _ = worker.notify_update(WorkerUpdate::Disconnect);
            // We create a temporary Vec to avoid doubt about a possible code
            // path touching the worker.running_action_infos elsewhere.
            for action_info in worker.running_action_infos.drain() {
                self.retry_action(&action_info, worker_id, err.clone());
            }
        }
        self.tasks_or_workers_change_notify.notify_one();
    }

    /// Removes timed out workers from the pool. This is called periodically by an
    /// external source.
    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        let err = nativelink_error::make_err!(nativelink_error::Code::Internal, "Received request to remove worker");
        let mut workers = self.workers.lock();
            let worker_ids_to_remove: Vec<WorkerId> = workers
            .workers
            .iter()
            .rev()
            .map_while(|(worker_id, worker)| {
                if worker.last_update_timestamp <= now_timestamp - self.worker_timeout_s {
                    Some(*worker_id)
                } else {
                    None
                }
            })
            .collect();
        for worker_id in &worker_ids_to_remove {
            let err = make_err!(
                Code::Internal,
                "Worker {worker_id} timed out, removing from pool"
            );
            warn!("{:?}", err);
            inner.immediate_evict_worker(worker_id, err);
        }

        Ok(())
    }

    /// Sets if the worker is draining or not.
    async fn set_drain_worker(&self, _worker_id: WorkerId, _is_draining: bool) -> Result<(), Error> {
        todo!()
    }

    /// Register the metrics for the worker scheduler.
    fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {}
}
