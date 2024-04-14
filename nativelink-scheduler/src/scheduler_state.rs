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

use std::borrow::Borrow;
use std::cmp;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::SystemTime;

use hashbrown::{HashMap, HashSet};
use lru::LruCache;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_error::{error_if, make_err, make_input_err, Code, Error, ResultExt};
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionResult, ActionStage, ActionState, ExecutionMetadata,
};
use nativelink_util::metrics_utils::Registry;
use parking_lot::{Mutex, MutexGuard};
use tokio::sync::{watch, Notify};
use tracing::{error, warn};

use crate::worker::{Worker, WorkerId, WorkerTimestamp, WorkerUpdate};

/// An action that is being awaited on and last known state.
struct AwaitedAction {
    action_info: Arc<ActionInfo>,
    current_state: Arc<ActionState>,
    notify_channel: watch::Sender<Arc<ActionState>>,

    /// Number of attempts the job has been tried.
    attempts: usize,
    /// Possible last error set by the worker. If empty and attempts is set, it may be due to
    /// something like a worker timeout.
    last_error: Option<Error>,

    /// Worker that is currently running this action, None if unassigned.
    worker_id: Option<WorkerId>,
}

struct Workers {
    workers: LruCache<WorkerId, Worker>,
}

impl Workers {
    fn new() -> Self {
        Self {
            workers: LruCache::unbounded(),
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
        allocation_strategy: WorkerAllocationStrategy
    ) -> Option<&'a mut Worker> {
        assert!(matches!(
            awaited_action.current_state.stage,
            ActionStage::Queued
        ));
        let action_properties = &awaited_action.action_info.platform_properties;
        let mut workers_iter = self.workers.iter_mut();
        let workers_iter = match allocation_strategy {
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

struct CompletedAction {
    completed_time: SystemTime,
    state: Arc<ActionState>,
}

impl Hash for CompletedAction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        ActionInfoHashKey::hash(&self.state.unique_qualifier, state);
    }
}

impl PartialEq for CompletedAction {
    fn eq(&self, other: &Self) -> bool {
        ActionInfoHashKey::eq(&self.state.unique_qualifier, &other.state.unique_qualifier)
    }
}

impl Eq for CompletedAction {}

impl Borrow<ActionInfoHashKey> for CompletedAction {
    #[inline]
    fn borrow(&self) -> &ActionInfoHashKey {
        &self.state.unique_qualifier
    }
}

pub struct SchedulerState {
    // BTreeMap uses `cmp` to do it's comparisons, this is a problem because we want to sort our
    // actions based on priority and insert timestamp but also want to find and join new actions
    // onto already executing (or queued) actions. We don't know the insert timestamp of queued
    // actions, so we won't be able to find it in a BTreeMap without iterating the entire map. To
    // get around this issue, we use two containers, one that will search using `Eq` which will
    // only match on the `unique_qualifier` field, which ignores fields that would prevent
    // multiplexing, and another which uses `Ord` for sorting.
    //
    // Important: These two fields must be kept in-sync, so if you modify one, you likely need to
    // modify the other.
    queued_actions_set: HashSet<Arc<ActionInfo>>,
    queued_actions: BTreeMap<Arc<ActionInfo>, AwaitedAction>,
    workers: Workers,
    active_actions: HashMap<Arc<ActionInfo>, AwaitedAction>,
    // These actions completed recently but had no listener, they might have
    // completed while the caller was thinking about calling wait_execution, so
    // keep their completion state around for a while to send back.
    // TODO(#192) Revisit if this is the best way to handle recently completed actions.
    recently_completed_actions: HashSet<CompletedAction>,
}

impl SchedulerState {
    fn subscribe_to_channel(awaited_action: &AwaitedAction) -> watch::Receiver<Arc<ActionState>> {
        let rx = awaited_action.notify_channel.subscribe();
        // TODO: Fix this when fixed upstream tokio-rs/tokio#5871
        awaited_action
            .notify_channel
            .send(awaited_action.current_state.clone())
            .unwrap();
        rx
    }

    /// Attempts to find a worker to execute an action and begins executing it.
    /// If an action is already running that is cacheable it may merge this action
    /// with the results and state changes of the already running action.
    /// If the task cannot be executed immediately it will be queued for execution
    /// based on priority and other metrics.
    /// All further updates to the action will be provided through `listener`.
    fn add_action(
        &mut self,
        action_info: ActionInfo,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        // Check to see if the action is running, if it is and cacheable, merge the actions.
        if let Some(running_action) = self.active_actions.get_mut(&action_info) {
            return Ok(Self::subscribe_to_channel(running_action));
        }

        // Check to see if the action is queued, if it is and cacheable, merge the actions.
        if let Some(mut arc_action_info) = self.queued_actions_set.take(&action_info) {
            let (original_action_info, queued_action) = self
                .queued_actions
                .remove_entry(&arc_action_info)
                .err_tip(|| "Internal error queued_actions and queued_actions_set should match")?;

            let new_priority = cmp::max(original_action_info.priority, action_info.priority);
            drop(original_action_info); // This increases the chance Arc::make_mut won't copy.

            // In the event our task is higher priority than the one already scheduled, increase
            // the priority of the scheduled one.
            Arc::make_mut(&mut arc_action_info).priority = new_priority;

            let rx = queued_action.notify_channel.subscribe();
            // TODO: Fix this when fixed upstream tokio-rs/tokio#5871
            let _ = queued_action
                .notify_channel
                .send(queued_action.current_state.clone());

            // Even if we fail to send our action to the client, we need to add this action back to the
            // queue because it was remove earlier.
            self.queued_actions
                .insert(arc_action_info.clone(), queued_action);
            self.queued_actions_set.insert(arc_action_info);
            return Ok(rx);
        }

        // Action needs to be added to queue or is not cacheable.
        let action_info = Arc::new(action_info);

        let current_state = Arc::new(ActionState {
            unique_qualifier: action_info.unique_qualifier.clone(),
            stage: ActionStage::Queued,
        });

        let (tx, rx) = watch::channel(current_state.clone());
        self.queued_actions_set.insert(action_info.clone());
        self.queued_actions.insert(
            action_info.clone(),
            AwaitedAction {
                action_info,
                current_state,
                notify_channel: tx,
                attempts: 0,
                last_error: None,
                worker_id: None,
            },
        );
        Ok(rx)
    }

    fn clean_expired_completed_actions(&mut self, expiry_time: SystemTime) {
        self.recently_completed_actions
            .retain(|action| action.completed_time > expiry_time);
    }

    fn find_recently_completed_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Option<watch::Receiver<Arc<ActionState>>> {
        self.recently_completed_actions
            .get(unique_qualifier)
            .map(|action| watch::channel(action.state.clone()).1)
    }

    fn find_existing_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Option<watch::Receiver<Arc<ActionState>>> {
        self.queued_actions_set
            .get(unique_qualifier)
            .and_then(|action_info| self.queued_actions.get(action_info))
            .or_else(|| self.active_actions.get(unique_qualifier))
            .map(Self::subscribe_to_channel)
    }

    fn retry_action(&mut self, action_info: &Arc<ActionInfo>, worker_id: &WorkerId, max_job_retries: usize, err: Error) {
        match self.active_actions.remove(action_info) {
            Some(running_action) => {
                let mut awaited_action = running_action;
                let send_result = if awaited_action.attempts >= max_job_retries {
                    Arc::make_mut(&mut awaited_action.current_state).stage = ActionStage::Completed(ActionResult {
                        execution_metadata: ExecutionMetadata {
                            worker: format!("{worker_id}"),
                            ..ExecutionMetadata::default()
                        },
                        error: Some(err.merge(make_err!(
                            Code::Internal,
                            "Job cancelled because it attempted to execute too many times and failed"
                        ))),
                        ..ActionResult::default()
                    });
                    awaited_action
                        .notify_channel
                        .send(awaited_action.current_state.clone())
                    // Do not put the action back in the queue here, as this action attempted to run too many
                    // times.
                } else {
                    Arc::make_mut(&mut awaited_action.current_state).stage = ActionStage::Queued;
                    let send_result = awaited_action
                        .notify_channel
                        .send(awaited_action.current_state.clone());
                    self.queued_actions_set.insert(action_info.clone());
                    self.queued_actions
                        .insert(action_info.clone(), awaited_action);
                    send_result
                };

                if send_result.is_err() {
                    // Don't remove this task, instead we keep them around for a bit just in case
                    // the client disconnected and will reconnect and ask for same job to be executed
                    // again.
                    warn!(
                        "Action {} has no more listeners during evict_worker()",
                        action_info.digest().hash_str()
                    );
                }
            }
            None => {
                error!("Worker stated it was running an action, but it was not in the active_actions : Worker: {:?}, ActionInfo: {:?}", worker_id, action_info);
            }
        }
    }

    /// Evicts the worker from the pool and puts items back into the queue if anything was being executed on it.
    fn immediate_evict_worker(&mut self, worker_id: &WorkerId, max_job_retries: usize, err: Error) {
        if let Some(mut worker) = self.workers.remove_worker(worker_id) {
            // We don't care if we fail to send message to worker, this is only a best attempt.
            let _ = worker.notify_update(WorkerUpdate::Disconnect);
            // We create a temporary Vec to avoid doubt about a possible code
            // path touching the worker.running_action_infos elsewhere.
            for action_info in worker.running_action_infos.drain() {
                self.retry_action(&action_info, worker_id, max_job_retries, err.clone());
            }
        }
    }

    /// Sets if the worker is draining or not.
    fn set_drain_worker(&mut self, worker_id: WorkerId, is_draining: bool) -> Result<(), Error> {
        let worker = self
            .workers
            .workers
            .get_mut(&worker_id)
            .err_tip(|| format!("Worker {worker_id} doesn't exist in the pool"))?;
        worker.is_draining = is_draining;
        Ok(())
    }

    // TODO(blaise.bruer) This is an O(n*m) (aka n^2) algorithm. In theory we can create a map
    // of capabilities of each worker and then try and match the actions to the worker using
    // the map lookup (ie. map reduce).
    fn do_try_match(&mut self, allocation_strategy: WorkerAllocationStrategy, max_job_retries: usize) {
        // TODO(blaise.bruer) This is a bit difficult because of how rust's borrow checker gets in
        // the way. We need to conditionally remove items from the `queued_action`. Rust is working
        // to add `drain_filter`, which would in theory solve this problem, but because we need
        // to iterate the items in reverse it becomes more difficult (and it is currently an
        // unstable feature [see: https://github.com/rust-lang/rust/issues/70530]).
        let action_infos: Vec<Arc<ActionInfo>> =
            self.queued_actions.keys().rev().cloned().collect();
        for action_info in action_infos {
            let Some(awaited_action) = self.queued_actions.get(action_info.as_ref()) else {
                error!(
                    "queued_actions out of sync with itself for action {}",
                    action_info.digest().hash_str()
                );
                continue;
            };
            let Some(worker) = self.workers.find_worker_for_action_mut(
                awaited_action,
                allocation_strategy
            ) else {
                // No worker found, check the next action to see if there's a
                // matching one for that.
                continue;
            };
            let worker_id = worker.id;

            // Try to notify our worker of the new action to run, if it fails remove the worker from the
            // pool and try to find another worker.
            let notify_worker_result =
                worker.notify_update(WorkerUpdate::RunAction(action_info.clone()));
            if notify_worker_result.is_err() {
                // Remove worker, as it is no longer receiving messages and let it try to find another worker.
                let err = make_err!(
                    Code::Internal,
                    "Worker command failed, removing worker {}",
                    worker_id
                );
                warn!("{:?}", err);
                self.immediate_evict_worker(&worker_id, max_job_retries, err);
                return;
            }

            // At this point everything looks good, so remove it from the queue and add it to active actions.
            let (action_info, mut awaited_action) = self
                .queued_actions
                .remove_entry(action_info.as_ref())
                .unwrap();
            assert!(
                self.queued_actions_set.remove(&action_info),
                "queued_actions_set should always have same keys as queued_actions"
            );
            Arc::make_mut(&mut awaited_action.current_state).stage = ActionStage::Executing;
            awaited_action.worker_id = Some(worker_id);
            let send_result = awaited_action
                .notify_channel
                .send(awaited_action.current_state.clone());
            if send_result.is_err() {
                // Don't remove this task, instead we keep them around for a bit just in case
                // the client disconnected and will reconnect and ask for same job to be executed
                // again.
                warn!(
                    "Action {} has no more listeners",
                    awaited_action.action_info.digest().hash_str()
                );
            }
            awaited_action.attempts += 1;
            self.active_actions.insert(action_info, awaited_action);
        }
    }

    fn update_action_with_internal_error(
        &mut self,
        worker_id: &WorkerId,
        action_info_hash_key: &ActionInfoHashKey,
        max_job_retries: usize,
        err: Error,
    ) {
        let Some((action_info, mut running_action)) =
            self.active_actions.remove_entry(action_info_hash_key)
        else {
            error!("Could not find action info in active actions : {action_info_hash_key:?}");
            return;
        };

        let due_to_backpressure = err.code == Code::ResourceExhausted;
        // Don't count a backpressure failure as an attempt for an action.
        if due_to_backpressure {
            running_action.attempts -= 1;
        }
        let Some(running_action_worker_id) = running_action.worker_id else {
            return error!(
            "Got a result from a worker that should not be running the action, Removing worker. Expected action to be unassigned got worker {worker_id}"
          );
        };
        if running_action_worker_id == *worker_id {
            // Don't set the error on an action that's running somewhere else.
            warn!("Internal error for worker {}: {}", worker_id, err);
            running_action.last_error = Some(err.clone());
        }

        // Now put it back. retry_action() needs it to be there to send errors properly.
        self.active_actions
            .insert(action_info.clone(), running_action);

        // Clear this action from the current worker.
        if let Some(worker) = self.workers.workers.get_mut(worker_id) {
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
        self.retry_action(&action_info, worker_id, max_job_retries, err);
    }

    fn update_action(
        &mut self,
        worker_id: &WorkerId,
        action_info_hash_key: &ActionInfoHashKey,
        action_stage: ActionStage,
        max_job_retries: usize
    ) -> Result<(), Error> {
        if !action_stage.has_action_result() {
            let err = make_err!(
                Code::Internal,
                "Worker '{worker_id}' set the action_stage of running action {action_info_hash_key:?} to {action_stage:?}. Removing worker.",
            );
            error!("{:?}", err);
            self.immediate_evict_worker(worker_id, max_job_retries, err.clone());
            return Err(err);
        }

        let (action_info, mut running_action) = self
            .active_actions
            .remove_entry(action_info_hash_key)
            .err_tip(|| {
                format!("Could not find action info in active actions : {action_info_hash_key:?}")
            })?;

        if running_action.worker_id != Some(*worker_id) {
            let err = match running_action.worker_id {

                Some(running_action_worker_id) => make_err!(
                    Code::Internal,
                    "Got a result from a worker that should not be running the action, Removing worker. Expected worker {running_action_worker_id} got worker {worker_id}",
                ),
                None => make_err!(
                    Code::Internal,
                    "Got a result from a worker that should not be running the action, Removing worker. Expected action to be unassigned got worker {worker_id}",
                ),
            };
            error!("{:?}", err);
            // First put it back in our active_actions or we will drop the task.
            self.active_actions.insert(action_info, running_action);
            self.immediate_evict_worker(worker_id, max_job_retries, err.clone());
            return Err(err);
        }

        Arc::make_mut(&mut running_action.current_state).stage = action_stage;

        let send_result = running_action
            .notify_channel
            .send(running_action.current_state.clone());

        if !running_action.current_state.stage.is_finished() {
            if send_result.is_err() {
                warn!(
                    "Action {} has no more listeners during update_action()",
                    action_info.digest().hash_str()
                );
            }
            // If the operation is not finished it means the worker is still working on it, so put it
            // back or else we will loose track of the task.
            self.active_actions.insert(action_info, running_action);
            return Ok(());
        }

        // Keep in case this is asked for soon.
        self.recently_completed_actions.insert(CompletedAction {
            completed_time: SystemTime::now(),
            state: running_action.current_state,
        });

        let worker = self.workers.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!("WorkerId '{}' does not exist in workers map", worker_id)
        })?;
        worker.complete_action(&action_info);
        Ok(())
    }
}

/// Engine used to manage the queued/running tasks and relationship with
/// the worker nodes. All state on how the workers and actions are interacting
/// should be held in this struct.
pub struct StateManager {
    inner: Arc<Mutex<SchedulerState>>,
    tasks_or_workers_change_notify: Arc<Notify>
}

impl StateManager {
    #[inline]
    #[must_use]
    pub fn new(tasks_or_workers_change_notify: Arc<Notify>) -> Self {
        let state = Arc::new(Mutex::new(SchedulerState {
            queued_actions_set: HashSet::new(),
            queued_actions: BTreeMap::new(),
            workers: Workers::new(),
            active_actions: HashMap::new(),
            recently_completed_actions: HashSet::new(),
        }));
        Self {
            inner: state,
            tasks_or_workers_change_notify
        }
    }

    pub fn create_new_state(&self) {

    }

    pub fn do_try_match(&self, allocation_strategy: WorkerAllocationStrategy, max_job_retries: usize) {
        let mut inner = self.get_inner_lock();
        inner.do_try_match(allocation_strategy, max_job_retries)
    }


    /// Checks to see if the worker exists in the worker pool. Should only be used in unit tests.
    #[must_use]
    pub fn contains_worker_for_test(&self, worker_id: &WorkerId) -> bool {
        let inner = self.get_inner_lock();
        inner.workers.workers.contains(worker_id)
    }

    /// Checks to see if the worker can accept work. Should only be used in unit tests.
    pub fn can_worker_accept_work_for_test(&self, worker_id: &WorkerId) -> Result<bool, Error> {
        let mut inner = self.get_inner_lock();
        let worker = inner.workers.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!("WorkerId '{}' does not exist in workers map", worker_id)
        })?;
        Ok(worker.can_accept_work())
    }

    /// A unit test function used to send the keep alive message to the worker from the server.
    pub fn send_keep_alive_to_worker_for_test(&self, worker_id: &WorkerId) -> Result<(), Error> {
        let mut inner = self.get_inner_lock();
        let worker = inner.workers.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!("WorkerId '{}' does not exist in workers map", worker_id)
        })?;
        worker.keep_alive()
    }

    fn get_inner_lock(&self) -> MutexGuard<'_, SchedulerState> {
        let lock = self.inner.lock();
        lock
    }

    pub async fn add_action(
        &self,
        action_info: ActionInfo,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        let mut inner = self.get_inner_lock();
        let res = inner.add_action(action_info);
        self.tasks_or_workers_change_notify.notify_one();
        res
    }

    pub async fn find_existing_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Option<watch::Receiver<Arc<ActionState>>> {
        let inner = self.get_inner_lock();
        inner
            .find_existing_action(unique_qualifier)
            .or_else(|| inner.find_recently_completed_action(unique_qualifier))
    }

    pub async fn clean_recently_completed_actions(&self, expiry_time: SystemTime) {
        self.get_inner_lock().clean_expired_completed_actions(expiry_time);
    }

    pub async fn add_worker(&self, worker: Worker, max_job_retries: usize) -> Result<(), Error> {
        let worker_id = worker.id;
        let mut inner = self.get_inner_lock();
        let res = inner
            .workers
            .add_worker(worker)
            .err_tip(|| "Error while adding worker, removing from pool");
        if let Err(err) = &res {
            inner.immediate_evict_worker(&worker_id, max_job_retries, err.clone());
        }
        self.tasks_or_workers_change_notify.notify_one();
        res
    }

    pub async fn update_action_with_internal_error(
        &self,
        worker_id: &WorkerId,
        action_info_hash_key: &ActionInfoHashKey,
        max_job_retries: usize,
        err: Error,
    ) {
        let mut inner = self.get_inner_lock();
        inner.update_action_with_internal_error(worker_id, action_info_hash_key, max_job_retries, err);
        self.tasks_or_workers_change_notify.notify_one();
    }

    pub fn get_task_worker_change_notify_handle(&self) -> Arc<Notify> {
        self.tasks_or_workers_change_notify.clone()
    }

    pub async fn update_action(
        &self,
        worker_id: &WorkerId,
        action_info_hash_key: &ActionInfoHashKey,
        action_stage: ActionStage,
        max_job_retries: usize
    ) -> Result<(), Error> {
        let mut inner = self.get_inner_lock();
        let res = inner.update_action(worker_id, action_info_hash_key, action_stage, max_job_retries);
        self.tasks_or_workers_change_notify.notify_one();
        res

    }

    pub async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        let mut inner = self.get_inner_lock();
        inner
            .workers
            .refresh_lifetime(worker_id, timestamp)
            .err_tip(|| "Error refreshing lifetime in worker_keep_alive_received()")
    }

    pub async fn remove_worker(&self, worker_id: WorkerId, max_job_retries: usize) {
        let mut inner = self.get_inner_lock();
        inner.immediate_evict_worker(
            &worker_id,
            max_job_retries,
            make_err!(Code::Internal, "Received request to remove worker"),
        );

        // Note: Calling this many time is very cheap, it'll only trigger `do_try_match` once.
        self.tasks_or_workers_change_notify.notify_one();
    }

    pub async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp, worker_timeout_s: u64, max_job_retries: usize) -> Result<(), Error> {
        let mut inner = self.get_inner_lock();
        // Items should be sorted based on last_update_timestamp, so we don't need to iterate the entire
        // map most of the time.
        let worker_ids_to_remove: Vec<WorkerId> = inner
            .workers
            .workers
            .iter()
            .rev()
            .map_while(|(worker_id, worker)| {
                if worker.last_update_timestamp <= now_timestamp - worker_timeout_s {
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
            inner.immediate_evict_worker(worker_id, max_job_retries, err);
        }
        self.tasks_or_workers_change_notify.notify_one();

        Ok(())
    }

    pub async fn set_drain_worker(&self, worker_id: WorkerId, is_draining: bool) -> Result<(), Error> {
        let mut inner = self.get_inner_lock();
        let res = inner.set_drain_worker(worker_id, is_draining);
        self.tasks_or_workers_change_notify.notify_one();
        res
    }

    pub fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {
        // We do not register anything here because we only want to register metrics
        // once and we rely on the `ActionScheduler::register_metrics()` to do that.
    }
}
