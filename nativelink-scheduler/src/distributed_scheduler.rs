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
use parking_lot::Mutex;
use async_trait::async_trait;
use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey, ActionStage, ActionState,  WorkerId, WorkerTimestamp};
use nativelink_util::metrics_utils::Registry;
use nativelink_util::platform_properties::PlatformProperties;
use tokio::sync::watch;

use crate::action_scheduler::ActionScheduler;
use crate::platform_property_manager::PlatformPropertyManager;
use crate::state_manager::StateManager;
// use crate::state_manager::StateManager;
use crate::worker::Worker;
use crate::worker_scheduler::WorkerScheduler;
use nativelink_error::{error_if,  make_input_err, Error, ResultExt};
use nativelink_config::schedulers::WorkerAllocationStrategy;
use lru::LruCache;
use tracing::error;


pub struct SchedulerInstance {
    platform_property_manager: Arc<PlatformPropertyManager>,
    state_manager: Arc<StateManager>,
    workers: Mutex<Workers>
}

impl SchedulerInstance {
    pub fn new(
        scheduler_cfg: &nativelink_config::schedulers::SchedulerInstance,
    ) -> Self {
        let platform_property_manager = Arc::new(PlatformPropertyManager::new(
            scheduler_cfg
                .supported_platform_properties
                .clone()
                .unwrap_or_default(),
        ));
        Self {
            platform_property_manager,
            state_manager: Arc::new(StateManager::new(
                scheduler_cfg.db_url.clone()
            )),
            workers: Mutex::new(Workers::new(scheduler_cfg.allocation_strategy))
        }
    }
}

#[async_trait]
impl ActionScheduler for SchedulerInstance {
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
    fn find_worker_for_action_mut<'a>(
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
impl WorkerScheduler for SchedulerInstance {
    /// Returns the platform property manager.
    fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
        &self.platform_property_manager
    }

    /// Adds a worker to the scheduler and begin using it to execute actions (when able).
    async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
        let mut workers = self.workers.lock();
        workers.add_worker(worker)
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
