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
use hashbrown::HashMap;

use nativelink_config::schedulers::PropertyType;
use nativelink_error::{Error, Code};
use nativelink_util::{action_messages::{ActionInfo, ActionState, OperationId}, platform_properties::PlatformPropertyValue};
use tokio::sync::watch;

use crate::{platform_property_manager::{self, PlatformPropertyManager}, redis_adapter::RedisAdapter};


pub struct LocalSchedulerStateStorage {
    workers: Workers,
    actions: HashMap<Arc<ActionInfo>, OperationId>,
    queued_actions_set: HashSet<OperationId>,
    queued_actions: BTreeMap<OperationId, AwaitedAction>,
    active_actions: HashMap<OperationId, AwaitedAction>,
    recently_completed_actions: HashSet<CompletedAction>,
}

pub struct LocalSchedulerState {
    inner: Arc<Mutex<LocalSchedulerStateImpl>>
}


enum BackPlane {
    Local(LocalSchedulerState),
    Redis(RedisAdapter),
}


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
        awaited_action: &AwaitedAction,
    ) -> Option<&'a mut Worker> {
        assert!(matches!(
            awaited_action.current_state.stage,
            ActionStage::Queued
        ));
        let action_properties = &awaited_action.action_info.platform_properties;
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
