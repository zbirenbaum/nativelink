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
use std::time:: SystemTime;

use async_trait::async_trait;
use futures::Future;
use nativelink_error::Error;
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionStage, ActionState,
};
use nativelink_util::metrics_utils:: Registry;
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;
use tokio::time::Duration;

use crate::action_scheduler::ActionScheduler;
use crate::platform_property_manager::PlatformPropertyManager;
use crate::worker::{Worker, WorkerId, WorkerTimestamp};
use crate::worker_scheduler::WorkerScheduler;
use crate::scheduler_state::StateManager;

/// Default timeout for workers in seconds.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_WORKER_TIMEOUT_S: u64 = 5;

/// Default timeout for recently completed actions in seconds.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_RETAIN_COMPLETED_FOR_S: u64 = 60;

/// Default times a job can retry before failing.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_MAX_JOB_RETRIES: usize = 3;

struct DistributedScheduler {
    inner: Arc<StateManager>,
    platform_property_manager: Arc<PlatformPropertyManager>,
    task_worker_matching_future: JoinHandle<()>,
    retain_completed_for: Duration,
    worker_timeout_s: u64,
    max_job_retries: usize
}



impl DistributedScheduler {
    #[inline]
    #[must_use]
    pub fn new(
        scheduler_cfg: &nativelink_config::schedulers::SimpleScheduler,
        state_manager: Arc<StateManager>,
        tasks_or_workers_change_notify: Arc<Notify>
    ) -> Self {
        Self::new_with_callback(scheduler_cfg, state_manager, || {
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
        scheduler_cfg: &nativelink_config::schedulers::SimpleScheduler,
        state_manager: Arc<StateManager>,
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

        let tasks_or_workers_change_notify = state_manager.get_task_worker_change_notify_handle();

        let weak_inner = Arc::downgrade(&state_manager).clone();
        let allocation_strategy = scheduler_cfg.allocation_strategy;
        Self {
            inner: state_manager,
            platform_property_manager,
            retain_completed_for: Duration::new(retain_completed_for_s, 0),
            worker_timeout_s,
            max_job_retries,
            task_worker_matching_future: tokio::spawn(async move {
                // Break out of the loop only when the inner is dropped.
                loop {
                    tasks_or_workers_change_notify.notified().await;
                    match weak_inner.upgrade() {
                        Some(state_manager) => {
                            state_manager.do_try_match(allocation_strategy, max_job_retries);
                        }
                        None => { return }
                    }
                    on_matching_engine_run().await;
                }
                // Unreachable.
            }),
        }
    }

    /// Checks to see if the worker exists in the worker pool. Should only be used in unit tests.
    #[must_use]
    pub fn contains_worker_for_test(&self, worker_id: &WorkerId) -> bool {
        self.inner.contains_worker_for_test(worker_id)
    }

    /// Checks to see if the worker can accept work. Should only be used in unit tests.
    pub fn can_worker_accept_work_for_test(&self, worker_id: &WorkerId) -> Result<bool, Error> {
        self.inner.can_worker_accept_work_for_test(worker_id)
    }

    /// A unit test function used to send the keep alive message to the worker from the server.
    pub fn send_keep_alive_to_worker_for_test(&self, worker_id: &WorkerId) -> Result<(), Error> {
        self.inner.send_keep_alive_to_worker_for_test(worker_id)
    }
}

#[async_trait]
impl ActionScheduler for DistributedScheduler {
    async fn get_platform_property_manager(
        &self,
        _instance_name: &str,
    ) -> Result<Arc<PlatformPropertyManager>, Error> {
        Ok(self.platform_property_manager.clone())
    }

    async fn add_action(
        &self,
        action_info: ActionInfo,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        self.inner.add_action(action_info).await
    }

    async fn find_existing_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Option<watch::Receiver<Arc<ActionState>>> {
        self.inner.find_existing_action(unique_qualifier).await
    }

    async fn clean_recently_completed_actions(&self) {
        let expiry_time = SystemTime::now()
            .checked_sub(self.retain_completed_for)
            .unwrap();
        self.inner.clean_recently_completed_actions(expiry_time);
    }

    fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {}
}

#[async_trait]
impl WorkerScheduler for DistributedScheduler {
    fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
        self.platform_property_manager.as_ref()
    }

    async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
        self.inner.add_worker(worker, self.max_job_retries).await
    }

    async fn update_action_with_internal_error(
        &self,
        worker_id: &WorkerId,
        action_info_hash_key: &ActionInfoHashKey,
        err: Error,
    ) {
        self.inner.update_action_with_internal_error(worker_id, action_info_hash_key, self.max_job_retries, err).await
    }

    async fn update_action(
        &self,
        worker_id: &WorkerId,
        action_info_hash_key: &ActionInfoHashKey,
        action_stage: ActionStage,
    ) -> Result<(), Error> {
        self.inner.update_action(worker_id, action_info_hash_key, action_stage, self.max_job_retries).await
    }

    async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        self.inner.worker_keep_alive_received(worker_id, timestamp).await
    }

    async fn remove_worker(&self, worker_id: WorkerId) {
        self.inner.remove_worker(worker_id, self.max_job_retries).await
    }

    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        self.inner.remove_timedout_workers(now_timestamp, self.worker_timeout_s, self.max_job_retries).await
    }

    async fn set_drain_worker(&self, worker_id: WorkerId, is_draining: bool) -> Result<(), Error> {
        self.inner.set_drain_worker(worker_id, is_draining).await
    }

    fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {
        // We do not register anything here because we only want to register metrics
        // once and we rely on the `ActionScheduler::register_metrics()` to do that.
    }
}

impl Drop for DistributedScheduler {
    fn drop(&mut self) {
        self.task_worker_matching_future.abort();
    }
}
