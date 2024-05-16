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
use hashbrown::HashMap;
use parking_lot::{Mutex, MutexGuard};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::platform_property_manager::{self, PlatformPropertyManager};
use crate::redis_adapter::RedisAdapter;
use crate::scheduler_state::ActionSchedulerStateStore;
use lru::LruCache;
use nativelink_config::schedulers::PropertyType;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_error::{error_if, make_err, make_input_err, Code, Error, ResultExt};
use nativelink_util::{
    action_messages::{
        ActionInfo, ActionInfoHashKey, ActionStage, ActionState, OperationId, WorkerId,
    },
    platform_properties::PlatformPropertyValue,
};
use tokio::sync::watch;
use tracing::error;
/// Engine used to manage the queued/running tasks and relationship with
/// the worker nodes. All state on how the workers and actions are interacting
/// should be held in this struct.
pub struct StateManager {
    inner: RedisAdapter,
}

impl StateManager {
    #[inline]
    #[must_use]
    pub fn new(db_url: String) -> Self {
        let adapter = RedisAdapter::new(db_url.clone());
        // TODO: Once this works, abstract the RedisAdapter to a DatabaseAdapter enum
        // and dynamically create the correct adapter type
        Self { inner: adapter }
    }

    pub async fn add_action(
        &self,
        action_info: ActionInfo,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        self.inner
            .add_or_merge_action(&action_info)
            .await
            .map_err(|e| Error {
                code: Code::Internal,
                messages: vec![e.to_string()],
            })
    }

    pub async fn clean_recently_completed_actions(&self, _retain_completed_for_s: u64) {
        println!("Should be cleaning");
    }

    /// Updates the status of an action to the scheduler from the worker.
    pub async fn update_action(
        &self,
        _worker_id: &WorkerId,
        unique_qualifier: &ActionInfoHashKey,
        action_stage: ActionStage,
    ) -> Result<(), Error> {
        let operation_id = self
            .inner
            .get_operation_id_for_action(unique_qualifier)
            .await?;
        self.inner
            .update_action_stages(&[(operation_id, action_stage)])
            .await
    }

    pub async fn find_existing_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Option<watch::Receiver<Arc<ActionState>>> {
        let res = self
            .inner
            .find_action_by_hash_key(unique_qualifier)
            .await
            .map_err(|e| Error {
                code: Code::Internal,
                messages: vec![e.to_string()],
            });
        match res {
            Ok(v) => v,
            Err(_) => None,
        }
    }

    pub async fn get_operation_id(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Result<OperationId, Error> {
        self.inner
            .get_operation_id_for_action(unique_qualifier)
            .await
            .map_err(|e| Error {
                code: Code::Internal,
                messages: vec![e.to_string()],
            })
    }

    pub async fn update_action_with_internal_error(
        &self,
        _unique_qualifier: &ActionInfoHashKey,
        _err: Error,
    ) -> Result<(), Error> {
        todo!()
    }

    pub async fn get_queued_actions(&self) -> Result<Vec<OperationId>, Error> {
        self.inner.get_queued_actions().await
    }

    pub async fn get_action_infos(
        &self,
        ids: &[OperationId],
    ) -> Result<Vec<(OperationId, ActionInfo)>, Error> {
        self.inner.get_action_info_for_actions(ids).await
    }
    pub async fn assign_actions(&self, ids: &[OperationId]) -> Result<(), Error> {
        self.inner.assign_actions(ids).await
    }
    pub async fn requeue_expired_actions(&self, timeout_s: u64) -> Result<usize, Error> {
        self.inner.requeue_expired_actions(timeout_s).await
    }

    pub async fn update_action_stages(
        &self,
        operations: &[(OperationId, ActionStage)],
    ) -> Result<(), Error> {
        self.inner.update_action_stages(operations).await
    }
}
//
// pub async fn find_existing_action(
//     &self,
//     unique_qualifier: &ActionInfoHashKey,
// ) -> Option<watch::Receiver<Option<Arc<ActionState>>>> {
//     self.inner.find_existing_action(unique_qualifier).await
// }
//
// pub async fn clean_recently_completed_actions(&self, expiry_time: SystemTime) {
//     self.inner
//         .clean_recently_completed_actions(expiry_time)
//         .await;
// }
//
// pub async fn add_worker(&self, worker: Worker, max_job_retries: usize) -> Result<(), Error> {
//     let res = self.inner.add_worker(worker, max_job_retries).await;
//     self.tasks_or_workers_change_notify.notify_one();
//     res
// }
//
// pub async fn update_action_with_internal_error(
//     &self,
//     worker_id: &WorkerId,
//     action_info_hash_key: &ActionInfoHashKey,
//     max_job_retries: usize,
//     err: Error,
// ) {
//     self.inner
//         .update_action_with_internal_error(
//             worker_id,
//             action_info_hash_key,
//             max_job_retries,
//             err,
//         )
//         .await;
//     self.tasks_or_workers_change_notify.notify_one();
// }
//
// pub fn get_task_worker_change_notify_handle(&self) -> Arc<Notify> {
//     self.tasks_or_workers_change_notify.clone()
// }
//
// pub async fn update_action(
//     &self,
//     worker_id: &WorkerId,
//     action_info_hash_key: &ActionInfoHashKey,
//     action_stage: ActionStage,
//     max_job_retries: usize,
// ) -> Result<(), Error> {
//     let res = self
//         .inner
//         .update_action(
//             worker_id,
//             action_info_hash_key,
//             action_stage,
//             max_job_retries,
//         )
//         .await;
//     self.tasks_or_workers_change_notify.notify_one();
//     res
// }
//
// pub async fn worker_keep_alive_received(
//     &self,
//     worker_id: &WorkerId,
//     timestamp: WorkerTimestamp,
// ) -> Result<(), Error> {
//     self.inner
//         .worker_keep_alive_received(worker_id, timestamp)
//         .await
// }
//
// pub async fn remove_worker(&self, worker_id: WorkerId, max_job_retries: usize) {
//     let res = self.inner.remove_worker(worker_id, max_job_retries).await;
//
//     // Note: Calling this many time is very cheap, it'll only trigger `do_try_match` once.
//     self.tasks_or_workers_change_notify.notify_one();
//     res
// }
//
// pub async fn remove_timedout_workers(
//     &self,
//     now_timestamp: WorkerTimestamp,
//     worker_timeout_s: u64,
//     max_job_retries: usize,
// ) -> Result<(), Error> {
//     let res = self
//         .inner
//         .remove_timedout_workers(now_timestamp, worker_timeout_s, max_job_retries)
//         .await;
//     self.tasks_or_workers_change_notify.notify_one();
//     res
// }
//
// pub async fn set_drain_worker(
//     &self,
//     worker_id: WorkerId,
//     is_draining: bool,
// ) -> Result<(), Error> {
//     let res = self.inner.set_drain_worker(worker_id, is_draining).await;
//     self.tasks_or_workers_change_notify.notify_one();
//     res
// }
