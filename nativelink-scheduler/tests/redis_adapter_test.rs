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
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::{Future, FutureExt, StreamExt};
use redis::AsyncCommands;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_scheduler::action_scheduler::ActionScheduler;
use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey, ActionState};
use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};
mod utils {
    pub(crate) mod scheduler_utils;
}
use nativelink_proto::build::bazel::remote::execution::v2::{digest_function, ExecuteRequest};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    update_for_worker, ConnectionResult, StartExecute, UpdateForWorker,
};
use nativelink_scheduler::distributed_scheduler::SchedulerInstance;
use nativelink_scheduler::redis_adapter::RedisAdapter;
use nativelink_scheduler::worker::{Worker, WorkerId, WorkerTimestamp};
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_scheduler::scheduler_state::{WorkerSchedulerStateStore, ActionSchedulerStateStore};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use tokio::sync::{mpsc, watch};
use utils::scheduler_utils::{make_base_action_info, INSTANCE_NAME};

async fn verify_initial_connection_message(
    worker_id: WorkerId,
    rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>,
) {
    use pretty_assertions::assert_eq;
    // Worker should have been sent an execute command.
    let expected_msg_for_worker = UpdateForWorker {
        update: Some(update_for_worker::Update::ConnectionResult(
            ConnectionResult {
                worker_id: worker_id.to_string(),
            },
        )),
    };
    let msg_for_worker = rx.recv().await.unwrap();
    assert_eq!(msg_for_worker, expected_msg_for_worker);
}

const NOW_TIME: u64 = 10000;

fn make_system_time(add_time: u64) -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_secs(NOW_TIME + add_time))
        .unwrap()
}

async fn setup_new_worker(
    scheduler: &SchedulerInstance,
    worker_id: WorkerId,
    props: PlatformProperties,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let worker = Worker::new(worker_id, props, tx, NOW_TIME);
    scheduler
        .add_worker(worker)
        .await
        .err_tip(|| "Failed to add worker")?;
    tokio::task::yield_now().await; // Allow task<->worker matcher to run.
    verify_initial_connection_message(worker_id, &mut rx).await;
    Ok(rx)
}

async fn setup_action(
    scheduler: &SchedulerInstance,
    action_digest: DigestInfo,
    platform_properties: PlatformProperties,
    insert_timestamp: SystemTime,
) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
    let mut action_info = make_base_action_info(insert_timestamp);
    action_info.platform_properties = platform_properties;
    action_info.unique_qualifier.digest = action_digest;
    let result = scheduler.add_action(action_info).await;
    tokio::task::yield_now().await; // Allow task<->worker matcher to run.
    result
}

#[cfg(test)]
mod scheduler_tests {
    use std::thread::sleep;

    use nativelink_util::action_messages::ActionStage;
    use pretty_assertions::assert_eq;
    use redis::cmd;

    use super::*; // Must be declared in every module.

    const WORKER_TIMEOUT_S: u64 = 100;

    #[tokio::test]
    async fn basic_add_action_with_one_worker_test() -> Result<(), Error> {
        let high_priority_action = Arc::new(ActionInfo {
            command_digest: DigestInfo::new([0u8; 32], 0),
            input_root_digest: DigestInfo::new([0u8; 32], 0),
            timeout: Duration::from_secs(10),
            platform_properties: PlatformProperties {
                properties: HashMap::new(),
            },
            priority: 1000,
            load_timestamp: SystemTime::UNIX_EPOCH,
            insert_timestamp: SystemTime::UNIX_EPOCH,
            unique_qualifier: ActionInfoHashKey {
                instance_name: INSTANCE_NAME.to_string(),
                digest: DigestInfo::new([0u8; 32], 0),
                salt: 0,
            },
            skip_cache_lookup: true,
            digest_function: DigestHasherFunc::Sha256,
        });
        let _lowest_priority_action = Arc::new(ActionInfo {
            command_digest: DigestInfo::new([0u8; 32], 0),
            input_root_digest: DigestInfo::new([0u8; 32], 0),
            timeout: Duration::from_secs(10),
            platform_properties: PlatformProperties {
                properties: HashMap::new(),
            },
            priority: 0,
            load_timestamp: SystemTime::UNIX_EPOCH,
            insert_timestamp: SystemTime::UNIX_EPOCH,
            unique_qualifier: ActionInfoHashKey {
                instance_name: INSTANCE_NAME.to_string(),
                digest: DigestInfo::new([1u8; 32], 0),
                salt: 0,
            },
            skip_cache_lookup: true,
            digest_function: DigestHasherFunc::Sha256,
        });
        let redis_adapter = RedisAdapter::new("redis://127.0.0.1/".to_string());
        let mut con = redis_adapter.client.get_connection().unwrap();
        let _: redis::Value = redis::cmd("FLUSHALL").arg("SYNC").query(&mut con).unwrap();
        let mut sub_1 = redis_adapter
            .add_or_merge_action(&high_priority_action)
            .await?;

        let id = redis_adapter
            .get_operation_id_for_action(&high_priority_action.unique_qualifier)
            .await?;
        redis_adapter
            .update_action_stage(
                None,
                id,
                nativelink_util::action_messages::ActionStage::Queued,
            )
            .await?;

        println!("Stored state: {:?}", redis_adapter.get_action_state(id).await?);
        redis_adapter
            .update_action_stage(
                Some(WorkerId::new()),
                id,
                nativelink_util::action_messages::ActionStage::Executing,
            )
            .await?;
        println!("Stored state: {:?}", redis_adapter.get_action_state(id).await?);
        let _ = sub_1.changed().await;

        let stage = sub_1.borrow_and_update().stage.clone();
        println!("Received stage: {:?}", stage);



        // assert_eq!(stage, ActionStage::Queued);

        // redis_adapter
        //     .update_action_stage(
        //         None,
        //         id,
        //         nativelink_util::action_messages::ActionStage::Executing,
        //     )
        //     .await?;
        // tokio::task::yield_now().await;
        // let stage = sub_1.borrow_and_update().stage.clone();
        // assert_eq!(stage, ActionStage::Executing);
        // println!("{:?}", sub_1.borrow_and_update());
        // println!("{:?}", sub_1.borrow_and_update());
        // let actions = redis_adapter.get_next_n_queued_actions(2).await?;
        // println!("{:?}", actions);
        Ok(())

    }
}
