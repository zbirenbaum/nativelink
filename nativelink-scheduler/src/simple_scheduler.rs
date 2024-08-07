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
use futures::Future;
use nativelink_config::stores::EvictionPolicy;
use nativelink_error::{Error, ResultExt};
use nativelink_metric::{MetricsComponent, RootMetricsComponent};
use nativelink_util::action_messages::{
    ActionInfo, ActionStage, ActionState, OperationId, WorkerId,
};
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, MatchingEngineStateManager,
    OperationFilter, OperationStageFlags, OrderDirection,
};
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use tokio::sync::Notify;
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tracing::{event, Level};

use crate::action_scheduler::{ActionListener, ActionScheduler};
use crate::api_worker_scheduler::ApiWorkerScheduler;
use crate::awaited_action_db::{AwaitedActionDb, AwaitedActionSubscriber};
use crate::memory_awaited_action_db::MemoryAwaitedActionDb;
use crate::platform_property_manager::PlatformPropertyManager;
use crate::simple_scheduler_state_manager::SimpleSchedulerStateManager;
use crate::worker::{Worker, WorkerTimestamp};
use crate::worker_scheduler::WorkerScheduler;

/// Default timeout for workers in seconds.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_WORKER_TIMEOUT_S: u64 = 5;

/// Default timeout for recently completed actions in seconds.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_RETAIN_COMPLETED_FOR_S: u32 = 60;

/// Default times a job can retry before failing.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_MAX_JOB_RETRIES: usize = 3;

struct SimpleSchedulerActionListener {
    client_operation_id: OperationId,
    action_state_result: Box<dyn ActionStateResult>,
}

impl SimpleSchedulerActionListener {
    fn new(
        client_operation_id: OperationId,
        action_state_result: Box<dyn ActionStateResult>,
    ) -> Self {
        Self {
            client_operation_id,
            action_state_result,
        }
    }
}

impl ActionListener for SimpleSchedulerActionListener {
    fn client_operation_id(&self) -> &OperationId {
        &self.client_operation_id
    }

    fn changed(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<ActionState>, Error>> + Send + '_>> {
        Box::pin(async move {
            let action_state = self
                .action_state_result
                .changed()
                .await
                .err_tip(|| "In SimpleSchedulerActionListener::changed getting receiver")?;
            Ok(action_state)
        })
    }
}

/// Engine used to manage the queued/running tasks and relationship with
/// the worker nodes. All state on how the workers and actions are interacting
/// should be held in this struct.
#[derive(MetricsComponent)]
pub struct SimpleScheduler {
    /// Manager for matching engine side of the state manager.
    matching_engine_state_manager: Arc<dyn MatchingEngineStateManager>,

    /// Manager for client state of this scheduler.
    #[metric(group = "client_state_manager")]
    client_state_manager: Arc<dyn ClientStateManager>,

    /// Manager for platform of this scheduler.
    #[metric(group = "platform_properties")]
    platform_property_manager: Arc<PlatformPropertyManager>,

    /// A `Workers` pool that contains all workers that are available to execute actions in a priority
    /// order based on the allocation strategy.
    worker_scheduler: Arc<ApiWorkerScheduler>,

    /// Background task that tries to match actions to workers. If this struct
    /// is dropped the spawn will be cancelled as well.
    _task_worker_matching_spawn: JoinHandleDropGuard<()>,
}

impl SimpleScheduler {
    /// Attempts to find a worker to execute an action and begins executing it.
    /// If an action is already running that is cacheable it may merge this
    /// action with the results and state changes of the already running
    /// action. If the task cannot be executed immediately it will be queued
    /// for execution based on priority and other metrics.
    /// All further updates to the action will be provided through the returned
    /// value.
    async fn add_action(
        &self,
        client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
    ) -> Result<Pin<Box<dyn ActionListener>>, Error> {
        let add_action_result = self
            .client_state_manager
            .add_action(client_operation_id.clone(), action_info)
            .await?;

        Ok(Box::pin(SimpleSchedulerActionListener::new(
            client_operation_id,
            add_action_result,
        )))
    }

    async fn find_by_client_operation_id(
        &self,
        client_operation_id: &OperationId,
    ) -> Result<Option<Pin<Box<dyn ActionListener>>>, Error> {
        let filter = OperationFilter {
            client_operation_id: Some(client_operation_id.clone()),
            ..Default::default()
        };
        let filter_result = self.client_state_manager.filter_operations(filter).await;

        let mut stream = filter_result
            .err_tip(|| "In SimpleScheduler::find_by_client_operation_id getting filter result")?;
        let Some(action_state_result) = stream.next().await else {
            return Ok(None);
        };
        Ok(Some(Box::pin(SimpleSchedulerActionListener::new(
            client_operation_id.clone(),
            action_state_result,
        ))))
    }

    async fn get_queued_operations(&self) -> Result<ActionStateResultStream, Error> {
        let filter = OperationFilter {
            stages: OperationStageFlags::Queued,
            order_by_priority_direction: Some(OrderDirection::Desc),
            ..Default::default()
        };
        self.matching_engine_state_manager
            .filter_operations(filter)
            .await
            .err_tip(|| "In SimpleScheduler::get_queued_operations getting filter result")
    }

    // TODO(blaise.bruer) This is an O(n*m) (aka n^2) algorithm. In theory we
    // can create a map of capabilities of each worker and then try and match
    // the actions to the worker using the map lookup (ie. map reduce).
    async fn do_try_match(&self) -> Result<(), Error> {
        async fn match_action_to_worker(
            action_state_result: &dyn ActionStateResult,
            workers: &ApiWorkerScheduler,
            matching_engine_state_manager: &dyn MatchingEngineStateManager,
        ) -> Result<(), Error> {
            let action_info = action_state_result
                .as_action_info()
                .await
                .err_tip(|| "Failed to get action_info from as_action_info_result stream")?;

            // Try to find a worker for the action.
            let worker_id = {
                let platform_properties = &action_info.platform_properties;
                match workers.find_worker_for_action(platform_properties).await {
                    Some(worker_id) => worker_id,
                    // If we could not find a worker for the action,
                    // we have nothing to do.
                    None => return Ok(()),
                }
            };

            // Extract the operation_id from the action_state.
            let operation_id = {
                let action_state = action_state_result
                    .as_state()
                    .await
                    .err_tip(|| "Failed to get action_info from as_state_result stream")?;
                action_state.operation_id.clone()
            };

            // Tell the matching engine that the operation is being assigned to a worker.
            matching_engine_state_manager
                .assign_operation(&operation_id, Ok(&worker_id))
                .await
                .err_tip(|| "Failed to assign operation in do_try_match")?;

            // Notify the worker to run the action.
            {
                workers
                    .worker_notify_run_action(worker_id, operation_id, action_info)
                    .await
                    .err_tip(|| {
                        "Failed to run worker_notify_run_action in SimpleScheduler::do_try_match"
                    })
            }
        }

        let mut result = Ok(());

        let mut stream = self
            .get_queued_operations()
            .await
            .err_tip(|| "Failed to get queued operations in do_try_match")?;

        while let Some(action_state_result) = stream.next().await {
            result = result.merge(
                match_action_to_worker(
                    action_state_result.as_ref(),
                    self.worker_scheduler.as_ref(),
                    self.matching_engine_state_manager.as_ref(),
                )
                .await,
            );
        }
        result
    }
}

enum BackendDynTypeTest {
    Memory,
    Other
}


#[derive(MetricsComponent)]
struct DynTypeTest {}
struct DynTypeTestSubscriber {}

impl AwaitedActionSubscriber for DynTypeTestSubscriber {
    fn borrow(&self) -> crate::awaited_action_db::AwaitedAction {
        todo!()
    }
    fn changed(&mut self) -> impl Future<Output = Result<crate::awaited_action_db::AwaitedAction, Error>> + Send {
        async { todo!() }
    }
}
impl AwaitedActionDb for DynTypeTest {
    type Subscriber = DynTypeTestSubscriber;
    fn add_action(
        &self,
        _client_operation_id: OperationId,
        _action_info: Arc<ActionInfo>,
    ) -> impl Future<Output = Result<Self::Subscriber, Error>> + Send {
        async { todo!() }
    }
    fn get_by_operation_id(
        &self,
        _operation_id: &OperationId,
    ) -> impl Future<Output = Result<Option<Self::Subscriber>, Error>> + Send {
        async { todo!() }
    }
    fn get_range_of_actions(
        &self,
        _state: crate::awaited_action_db::SortedAwaitedActionState,
        _start: std::ops::Bound<crate::awaited_action_db::SortedAwaitedAction>,
        _end: std::ops::Bound<crate::awaited_action_db::SortedAwaitedAction>,
        _desc: bool,
    ) -> impl Future<Output = impl futures::Stream<Item = Result<Self::Subscriber, Error>> + Send> + Send {
        async { tokio_stream::iter(vec![]) }
    }
    fn update_awaited_action(
        &self,
        _new_awaited_action: crate::awaited_action_db::AwaitedAction,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        async { todo!() }
    }
    fn get_all_awaited_actions(
        &self,
    ) -> impl Future<Output = impl futures::Stream<Item = Result<Self::Subscriber, Error>> + Send> + Send {
        async { tokio_stream::iter(vec![]) }
    }
    fn get_awaited_action_by_id(
        &self,
        _client_operation_id: &OperationId,
    ) -> impl Future<Output = Result<Option<Self::Subscriber>, Error>> + Send {
        async { todo!() }
    }
}
impl SimpleScheduler {
    pub fn new(
        scheduler_cfg: &nativelink_config::schedulers::SimpleScheduler,
    ) -> (Arc<Self>, Arc<dyn WorkerScheduler>) {
        Self::new_with_callback(
            scheduler_cfg,
            || {
                // The cost of running `do_try_match()` is very high, but constant
                // in relation to the number of changes that have happened. This
                // means that grabbing this lock to process `do_try_match()` should
                // always yield to any other tasks that might want the lock. The
                // easiest and most fair way to do this is to sleep for a small
                // amount of time. Using something like tokio::task::yield_now()
                // does not yield as aggresively as we'd like if new futures are
                // scheduled within a future.
                tokio::time::sleep(Duration::from_millis(1))
            },
            SystemTime::now,
        )
    }

    pub fn new_with_callback<
        Fut: Future<Output = ()> + Send,
        F: Fn() -> Fut + Send + Sync + 'static,
        I: InstantWrapper,
        NowFn: Fn() -> I + Clone + Send + Sync + 'static,
    >(
        scheduler_cfg: &nativelink_config::schedulers::SimpleScheduler,
        on_matching_engine_run: F,
        now_fn: NowFn,
    ) -> (Arc<Self>, Arc<dyn WorkerScheduler>) {
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

        let backend_test = BackendDynTypeTest::Memory;
        let tasks_or_worker_change_notify = Arc::new(Notify::new());
        let state_manager = SimpleSchedulerStateManager::new(
            tasks_or_worker_change_notify.clone(),
            max_job_retries,
            match backend_test {
                BackendDynTypeTest::Other => {
                    DynTypeTest {} as dyn AwaitedActionDb
                }
                BackendDynTypeTest::Memory => {
                    MemoryAwaitedActionDb::new(
                        &EvictionPolicy {
                            max_seconds: retain_completed_for_s,
                            ..Default::default()
                        },
                        now_fn,
                    ) as dyn AwaitedActionDb
                }
            }
        );

        let worker_scheduler = ApiWorkerScheduler::new(
            state_manager.clone(),
            platform_property_manager.clone(),
            scheduler_cfg.allocation_strategy,
            tasks_or_worker_change_notify.clone(),
            worker_timeout_s,
        );

        let worker_scheduler_clone = worker_scheduler.clone();

        let action_scheduler = Arc::new_cyclic(move |weak_self| -> Self {
            let weak_inner = weak_self.clone();
            let task_worker_matching_spawn =
                spawn!("simple_scheduler_task_worker_matching", async move {
                    // Break out of the loop only when the inner is dropped.
                    loop {
                        tasks_or_worker_change_notify.notified().await;
                        let result = match weak_inner.upgrade() {
                            Some(scheduler) => scheduler.do_try_match().await,
                            // If the inner went away it means the scheduler is shutting
                            // down, so we need to resolve our future.
                            None => return,
                        };
                        if let Err(err) = result {
                            event!(Level::ERROR, ?err, "Error while running do_try_match");
                        }

                        on_matching_engine_run().await;
                    }
                    // Unreachable.
                });
            SimpleScheduler {
                matching_engine_state_manager: state_manager.clone(),
                client_state_manager: state_manager.clone(),
                worker_scheduler,
                platform_property_manager,
                _task_worker_matching_spawn: task_worker_matching_spawn,
            }
        });
        (action_scheduler, worker_scheduler_clone)
    }
}

#[async_trait]
impl ActionScheduler for SimpleScheduler {
    async fn get_platform_property_manager(
        &self,
        _instance_name: &str,
    ) -> Result<Arc<PlatformPropertyManager>, Error> {
        Ok(self.platform_property_manager.clone())
    }

    async fn add_action(
        &self,
        client_operation_id: OperationId,
        action_info: ActionInfo,
    ) -> Result<Pin<Box<dyn ActionListener>>, Error> {
        self.add_action(client_operation_id, Arc::new(action_info))
            .await
    }

    async fn find_by_client_operation_id(
        &self,
        client_operation_id: &OperationId,
    ) -> Result<Option<Pin<Box<dyn ActionListener>>>, Error> {
        let maybe_receiver = self
            .find_by_client_operation_id(client_operation_id)
            .await
            .err_tip(|| {
                format!("Error while finding action with client id: {client_operation_id:?}")
            })?;
        Ok(maybe_receiver)
    }
}

#[async_trait]
impl WorkerScheduler for SimpleScheduler {
    fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
        self.worker_scheduler.get_platform_property_manager()
    }

    async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
        self.worker_scheduler.add_worker(worker).await
    }

    async fn update_action(
        &self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
        action_stage: Result<ActionStage, Error>,
    ) -> Result<(), Error> {
        self.worker_scheduler
            .update_action(worker_id, operation_id, action_stage)
            .await
    }

    async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        self.worker_scheduler
            .worker_keep_alive_received(worker_id, timestamp)
            .await
    }

    async fn remove_worker(&self, worker_id: &WorkerId) -> Result<(), Error> {
        self.worker_scheduler.remove_worker(worker_id).await
    }

    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        self.worker_scheduler
            .remove_timedout_workers(now_timestamp)
            .await
    }

    async fn set_drain_worker(&self, worker_id: &WorkerId, is_draining: bool) -> Result<(), Error> {
        self.worker_scheduler
            .set_drain_worker(worker_id, is_draining)
            .await
    }
}

impl RootMetricsComponent for SimpleScheduler {}
