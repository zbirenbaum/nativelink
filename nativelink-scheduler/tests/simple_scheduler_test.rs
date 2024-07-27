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

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::poll;
use futures::task::Poll;
use mock_instant::MockClock;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{digest_function, ExecuteRequest};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    update_for_worker, ConnectionResult, StartExecute, UpdateForWorker,
};
use nativelink_scheduler::action_scheduler::{ActionListener, ActionScheduler};
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker::Worker;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::action_messages::{
    ActionResult, ActionStage, ActionState, ActionUniqueKey, ActionUniqueQualifier,
    ClientOperationId, DirectoryInfo, ExecutionMetadata, FileInfo, NameOrPath, OperationId,
    SymlinkInfo, WorkerId, INTERNAL_ERROR_EXIT_CODE,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};
use pretty_assertions::assert_eq;
use tokio::sync::mpsc;
use utils::scheduler_utils::{make_base_action_info, INSTANCE_NAME};
use uuid::Uuid;

mod utils {
    pub(crate) mod scheduler_utils;
}

fn update_eq(expected: UpdateForWorker, actual: UpdateForWorker, ignore_id: bool) -> bool {
    let Some(expected_update) = expected.update else {
        return actual.update.is_none();
    };
    let Some(actual_update) = actual.update else {
        return false;
    };
    match actual_update {
        update_for_worker::Update::Disconnect(()) => {
            matches!(expected_update, update_for_worker::Update::Disconnect(()))
        }
        update_for_worker::Update::KeepAlive(()) => {
            matches!(expected_update, update_for_worker::Update::KeepAlive(()))
        }
        update_for_worker::Update::StartAction(actual_update) => match expected_update {
            update_for_worker::Update::StartAction(mut expected_update) => {
                if ignore_id {
                    expected_update
                        .operation_id
                        .clone_from(&actual_update.operation_id);
                }
                expected_update == actual_update
            }
            _ => false,
        },
        update_for_worker::Update::KillOperationRequest(actual_update) => match expected_update {
            update_for_worker::Update::KillOperationRequest(expected_update) => {
                expected_update == actual_update
            }
            _ => false,
        },
        update_for_worker::Update::ConnectionResult(actual_update) => match expected_update {
            update_for_worker::Update::ConnectionResult(expected_update) => {
                expected_update == actual_update
            }
            _ => false,
        },
    }
}
async fn verify_initial_connection_message(
    worker_id: WorkerId,
    rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>,
) {
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
    scheduler: &SimpleScheduler,
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
    scheduler: &SimpleScheduler,
    action_digest: DigestInfo,
    platform_properties: PlatformProperties,
    insert_timestamp: SystemTime,
) -> Result<Pin<Box<dyn ActionListener>>, Error> {
    let mut action_info = make_base_action_info(insert_timestamp, action_digest);
    action_info.platform_properties = platform_properties;
    let client_id = ClientOperationId::new(action_info.unique_qualifier.clone());
    let result = scheduler.add_action(client_id, action_info).await;
    tokio::task::yield_now().await; // Allow task<->worker matcher to run.
    result
}

const WORKER_TIMEOUT_S: u64 = 100;

#[nativelink_test]
async fn basic_add_action_with_one_worker_test() -> Result<(), Error> {
    let worker_id: WorkerId = WorkerId(Uuid::new_v4());

    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id, PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        insert_timestamp,
    )
    .await
    .unwrap();

    {
        // Worker should have been sent an execute command.
        let expected_msg_for_worker = UpdateForWorker {
            update: Some(update_for_worker::Update::StartAction(StartExecute {
                execute_request: Some(ExecuteRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    action_digest: Some(action_digest.into()),
                    digest_function: digest_function::Value::Sha256.into(),
                    ..Default::default()
                }),
                operation_id: "Unknown Generated internally".to_string(),
                queued_timestamp: Some(insert_timestamp.into()),
            })),
        };
        let msg_for_worker = rx_from_worker.recv().await.unwrap();
        // Operation ID is random so we ignore it.
        assert!(update_eq(expected_msg_for_worker, msg_for_worker, true));
    }
    {
        // Client should get notification saying it's being executed.
        let action_state = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            id: action_state.id.clone(),
            stage: ActionStage::Executing,
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

#[nativelink_test]
async fn find_executing_action() -> Result<(), Error> {
    let worker_id: WorkerId = WorkerId(Uuid::new_v4());

    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id, PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        insert_timestamp,
    )
    .await
    .unwrap();

    let client_operation_id = action_listener.client_operation_id().clone();
    // Drop our receiver and look up a new one.
    drop(action_listener);
    let mut action_listener = scheduler
        .find_by_client_operation_id(&client_operation_id)
        .await
        .expect("Action not found")
        .unwrap();

    {
        // Worker should have been sent an execute command.
        let expected_msg_for_worker = UpdateForWorker {
            update: Some(update_for_worker::Update::StartAction(StartExecute {
                execute_request: Some(ExecuteRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    action_digest: Some(action_digest.into()),
                    digest_function: digest_function::Value::Sha256.into(),
                    ..Default::default()
                }),
                operation_id: "Unknown Generated internally".to_string(),
                queued_timestamp: Some(insert_timestamp.into()),
            })),
        };
        let msg_for_worker = rx_from_worker.recv().await.unwrap();
        // Operation ID is random so we ignore it.
        assert!(update_eq(expected_msg_for_worker, msg_for_worker, true));
    }
    {
        // Client should get notification saying it's being executed.
        let action_state = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            id: action_state.id.clone(),
            stage: ActionStage::Executing,
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

#[nativelink_test]
async fn remove_worker_reschedules_multiple_running_job_test() -> Result<(), Error> {
    let worker_id1: WorkerId = WorkerId(Uuid::new_v4());
    let worker_id2: WorkerId = WorkerId(Uuid::new_v4());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler {
            worker_timeout_s: WORKER_TIMEOUT_S,
            ..Default::default()
        },
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest1 = DigestInfo::new([99u8; 32], 512);
    let action_digest2 = DigestInfo::new([88u8; 32], 512);

    let mut rx_from_worker1 =
        setup_new_worker(&scheduler, worker_id1, PlatformProperties::default()).await?;
    let insert_timestamp1 = make_system_time(1);
    let mut client1_action_listener = setup_action(
        &scheduler,
        action_digest1,
        PlatformProperties::default(),
        insert_timestamp1,
    )
    .await?;
    let insert_timestamp2 = make_system_time(2);
    let mut client2_action_listener = setup_action(
        &scheduler,
        action_digest2,
        PlatformProperties::default(),
        insert_timestamp2,
    )
    .await?;

    let mut expected_start_execute_for_worker1 = StartExecute {
        execute_request: Some(ExecuteRequest {
            instance_name: INSTANCE_NAME.to_string(),
            action_digest: Some(action_digest1.into()),
            digest_function: digest_function::Value::Sha256.into(),
            ..Default::default()
        }),
        operation_id: "WILL BE SET BELOW".to_string(),
        queued_timestamp: Some(insert_timestamp1.into()),
    };

    let mut expected_start_execute_for_worker2 = StartExecute {
        execute_request: Some(ExecuteRequest {
            instance_name: INSTANCE_NAME.to_string(),
            action_digest: Some(action_digest2.into()),
            digest_function: digest_function::Value::Sha256.into(),
            ..Default::default()
        }),
        operation_id: "WILL BE SET BELOW".to_string(),
        queued_timestamp: Some(insert_timestamp2.into()),
    };
    let operation_id1 = {
        // Worker1 should now see first execution request.
        let update_for_worker = rx_from_worker1
            .recv()
            .await
            .expect("Worker terminated stream")
            .update
            .expect("`update` should be set on UpdateForWorker");
        let (operation_id, rx_start_execute) = match update_for_worker {
            update_for_worker::Update::StartAction(start_execute) => (
                OperationId::try_from(start_execute.operation_id.as_str()).unwrap(),
                start_execute,
            ),
            v => panic!("Expected StartAction, got : {v:?}"),
        };
        expected_start_execute_for_worker1.operation_id = operation_id.to_string();
        assert_eq!(expected_start_execute_for_worker1, rx_start_execute);
        operation_id
    };
    let operation_id2 = {
        // Worker1 should now see second execution request.
        let update_for_worker = rx_from_worker1
            .recv()
            .await
            .expect("Worker terminated stream")
            .update
            .expect("`update` should be set on UpdateForWorker");
        let (operation_id, rx_start_execute) = match update_for_worker {
            update_for_worker::Update::StartAction(start_execute) => (
                OperationId::try_from(start_execute.operation_id.as_str()).unwrap(),
                start_execute,
            ),
            v => panic!("Expected StartAction, got : {v:?}"),
        };
        expected_start_execute_for_worker2.operation_id = operation_id.to_string();
        assert_eq!(expected_start_execute_for_worker2, rx_start_execute);
        operation_id
    };

    // Add a second worker that can take jobs if the first dies.
    let mut rx_from_worker2 =
        setup_new_worker(&scheduler, worker_id2, PlatformProperties::default()).await?;

    {
        let expected_action_stage = ActionStage::Executing;
        // Client should get notification saying it's being executed.
        let action_state = client1_action_listener.changed().await.unwrap();
        // We now know the name of the action so populate it.
        assert_eq!(&action_state.stage, &expected_action_stage);
    }
    {
        let expected_action_stage = ActionStage::Executing;
        // Client should get notification saying it's being executed.
        let action_state = client2_action_listener.changed().await.unwrap();
        // We now know the name of the action so populate it.
        assert_eq!(&action_state.stage, &expected_action_stage);
    }

    // Now remove worker.
    let _ = scheduler.remove_worker(&worker_id1).await;
    tokio::task::yield_now().await; // Allow task<->worker matcher to run.

    {
        // Worker1 should have received a disconnect message.
        let msg_for_worker = rx_from_worker1.recv().await.unwrap();
        assert_eq!(
            msg_for_worker,
            UpdateForWorker {
                update: Some(update_for_worker::Update::Disconnect(()))
            }
        );
    }
    {
        let expected_action_stage = ActionStage::Executing;
        // Client should get notification saying it's being executed.
        let action_state = client1_action_listener.changed().await.unwrap();
        // We now know the name of the action so populate it.
        assert_eq!(&action_state.stage, &expected_action_stage);
    }
    {
        let expected_action_stage = ActionStage::Executing;
        // Client should get notification saying it's being executed.
        let action_state = client2_action_listener.changed().await.unwrap();
        // We now know the name of the action so populate it.
        assert_eq!(&action_state.stage, &expected_action_stage);
    }
    {
        // Worker2 should now see execution request.
        let msg_for_worker = rx_from_worker2.recv().await.unwrap();
        expected_start_execute_for_worker1.operation_id = operation_id1.to_string();
        assert_eq!(
            msg_for_worker,
            UpdateForWorker {
                update: Some(update_for_worker::Update::StartAction(
                    expected_start_execute_for_worker1
                )),
            }
        );
    }
    {
        // Worker2 should now see execution request.
        let msg_for_worker = rx_from_worker2.recv().await.unwrap();
        expected_start_execute_for_worker2.operation_id = operation_id2.to_string();
        assert_eq!(
            msg_for_worker,
            UpdateForWorker {
                update: Some(update_for_worker::Update::StartAction(
                    expected_start_execute_for_worker2
                )),
            }
        );
    }

    Ok(())
}

#[nativelink_test]
async fn set_drain_worker_pauses_and_resumes_worker_test() -> Result<(), Error> {
    let worker_id: WorkerId = WorkerId(Uuid::new_v4());

    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id, PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        insert_timestamp,
    )
    .await?;

    let _operation_id = {
        // Other tests check full data. We only care if we got StartAction.
        let operation_id = match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(start_execute)) => {
                OperationId::try_from(start_execute.operation_id.as_str()).unwrap()
            }
            v => panic!("Expected StartAction, got : {v:?}"),
        };
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            action_listener.changed().await.unwrap().stage,
            ActionStage::Executing
        );
        operation_id
    };

    // Set the worker draining.
    scheduler.set_drain_worker(&worker_id, true).await?;
    tokio::task::yield_now().await;

    let action_digest = DigestInfo::new([88u8; 32], 512);
    let insert_timestamp = make_system_time(14);
    let mut action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        insert_timestamp,
    )
    .await?;

    {
        // Client should get notification saying it's been queued.
        let action_state = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            id: action_state.id.clone(),
            stage: ActionStage::Queued,
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    // Set the worker not draining.
    scheduler.set_drain_worker(&worker_id, false).await?;
    tokio::task::yield_now().await;

    {
        // Client should get notification saying it's being executed.
        let action_state = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            id: action_state.id.clone(),
            stage: ActionStage::Executing,
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

#[nativelink_test]
async fn worker_should_not_queue_if_properties_dont_match_test() -> Result<(), Error> {
    let worker_id1: WorkerId = WorkerId(Uuid::new_v4());
    let worker_id2: WorkerId = WorkerId(Uuid::new_v4());

    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);
    let mut platform_properties = PlatformProperties::default();
    platform_properties.properties.insert(
        "prop".to_string(),
        PlatformPropertyValue::Exact("1".to_string()),
    );
    let mut worker_properties = platform_properties.clone();
    worker_properties.properties.insert(
        "prop".to_string(),
        PlatformPropertyValue::Exact("2".to_string()),
    );

    let mut rx_from_worker1 =
        setup_new_worker(&scheduler, worker_id1, platform_properties.clone()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener = setup_action(
        &scheduler,
        action_digest,
        worker_properties.clone(),
        insert_timestamp,
    )
    .await?;

    {
        // Client should get notification saying it's been queued.
        let action_state = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            id: action_state.id.clone(),
            stage: ActionStage::Queued,
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    let mut rx_from_worker2 = setup_new_worker(&scheduler, worker_id2, worker_properties).await?;
    {
        // Worker should have been sent an execute command.
        let expected_msg_for_worker = UpdateForWorker {
            update: Some(update_for_worker::Update::StartAction(StartExecute {
                execute_request: Some(ExecuteRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    action_digest: Some(action_digest.into()),
                    digest_function: digest_function::Value::Sha256.into(),
                    ..Default::default()
                }),
                operation_id: "Unknown Generated internally".to_string(),
                queued_timestamp: Some(insert_timestamp.into()),
            })),
        };
        let msg_for_worker = rx_from_worker2.recv().await.unwrap();
        assert!(update_eq(expected_msg_for_worker, msg_for_worker, true));
    }
    {
        // Client should get notification saying it's being executed.
        let action_state = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            id: action_state.id.clone(),
            stage: ActionStage::Executing,
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    // Our first worker should have no updates over this test.
    assert_eq!(
        rx_from_worker1.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    );

    Ok(())
}

#[nativelink_test]
async fn cacheable_items_join_same_action_queued_test() -> Result<(), Error> {
    let worker_id: WorkerId = WorkerId(Uuid::new_v4());

    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let unique_qualifier = ActionUniqueQualifier::Cachable(ActionUniqueKey {
        instance_name: String::new(),
        digest: DigestInfo::zero_digest(),
        digest_function: DigestHasherFunc::Sha256,
    });
    let id = OperationId::new(unique_qualifier);
    let mut expected_action_state = ActionState {
        id,
        stage: ActionStage::Queued,
    };

    let insert_timestamp1 = make_system_time(1);
    let insert_timestamp2 = make_system_time(2);
    let mut client1_action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        insert_timestamp1,
    )
    .await?;
    let mut client2_action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        insert_timestamp2,
    )
    .await?;

    {
        // Clients should get notification saying it's been queued.
        let action_state1 = client1_action_listener.changed().await.unwrap();
        let action_state2 = client2_action_listener.changed().await.unwrap();
        // Name is random so we set force it to be the same.
        expected_action_state.id = action_state1.id.clone();
        assert_eq!(action_state1.as_ref(), &expected_action_state);
        assert_eq!(action_state2.as_ref(), &expected_action_state);
    }

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id, PlatformProperties::default()).await?;

    {
        // Worker should have been sent an execute command.
        let expected_msg_for_worker = UpdateForWorker {
            update: Some(update_for_worker::Update::StartAction(StartExecute {
                execute_request: Some(ExecuteRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    action_digest: Some(action_digest.into()),
                    digest_function: digest_function::Value::Sha256.into(),
                    ..Default::default()
                }),
                operation_id: "Unknown Generated internally".to_string(),
                queued_timestamp: Some(insert_timestamp1.into()),
            })),
        };
        let msg_for_worker = rx_from_worker.recv().await.unwrap();
        // Operation ID is random so we ignore it.
        assert!(update_eq(expected_msg_for_worker, msg_for_worker, true));
    }

    // Action should now be executing.
    expected_action_state.stage = ActionStage::Executing;
    {
        // Both client1 and client2 should be receiving the same updates.
        // Most importantly the `name` (which is random) will be the same.
        assert_eq!(
            client1_action_listener.changed().await.unwrap().as_ref(),
            &expected_action_state
        );
        assert_eq!(
            client2_action_listener.changed().await.unwrap().as_ref(),
            &expected_action_state
        );
    }

    {
        // Now if another action is requested it should also join with executing action.
        let insert_timestamp3 = make_system_time(2);
        let mut client3_action_listener = setup_action(
            &scheduler,
            action_digest,
            PlatformProperties::default(),
            insert_timestamp3,
        )
        .await?;
        assert_eq!(
            client3_action_listener.changed().await.unwrap().as_ref(),
            &expected_action_state
        );
    }

    Ok(())
}

#[nativelink_test]
async fn worker_disconnects_does_not_schedule_for_execution_test() -> Result<(), Error> {
    let worker_id: WorkerId = WorkerId(Uuid::new_v4());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let rx_from_worker =
        setup_new_worker(&scheduler, worker_id, PlatformProperties::default()).await?;

    // Now act like the worker disconnected.
    drop(rx_from_worker);

    let insert_timestamp = make_system_time(1);
    let mut action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        insert_timestamp,
    )
    .await?;
    {
        // Client should get notification saying it's being queued not executed.
        let action_state = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            id: action_state.id.clone(),
            stage: ActionStage::Queued,
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

#[nativelink_test]
async fn worker_timesout_reschedules_running_job_test() -> Result<(), Error> {
    let worker_id1: WorkerId = WorkerId(Uuid::new_v4());
    let worker_id2: WorkerId = WorkerId(Uuid::new_v4());
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler {
            worker_timeout_s: WORKER_TIMEOUT_S,
            ..Default::default()
        },
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    // Note: This needs to stay in scope or a disconnect will trigger.
    let mut rx_from_worker1 =
        setup_new_worker(&scheduler, worker_id1, PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        insert_timestamp,
    )
    .await?;

    // Note: This needs to stay in scope or a disconnect will trigger.
    let mut rx_from_worker2 =
        setup_new_worker(&scheduler, worker_id2, PlatformProperties::default()).await?;

    let mut start_execute = StartExecute {
        execute_request: Some(ExecuteRequest {
            instance_name: INSTANCE_NAME.to_string(),
            action_digest: Some(action_digest.into()),
            digest_function: digest_function::Value::Sha256.into(),
            ..Default::default()
        }),
        operation_id: "UNKNOWN HERE, WE WILL SET IT LATER".to_string(),
        queued_timestamp: Some(insert_timestamp.into()),
    };

    let operation_id = {
        // Worker1 should now see execution request.
        let msg_for_worker = rx_from_worker1.recv().await.unwrap();
        let operation_id = if let update_for_worker::Update::StartAction(start_execute) =
            msg_for_worker.update.as_ref().unwrap()
        {
            start_execute.operation_id.clone()
        } else {
            panic!("Expected StartAction, got : {msg_for_worker:?}");
        };
        start_execute.operation_id.clone_from(&operation_id);
        assert_eq!(
            msg_for_worker,
            UpdateForWorker {
                update: Some(update_for_worker::Update::StartAction(
                    start_execute.clone()
                )),
            }
        );
        OperationId::try_from(operation_id.as_str()).unwrap()
    };

    {
        // Client should get notification saying it's being executed.
        let action_state = action_listener.changed().await.unwrap();
        assert_eq!(
            action_state.as_ref(),
            &ActionState {
                id: operation_id.clone(),
                stage: ActionStage::Executing,
            }
        );
    }

    // Keep worker 2 alive.
    scheduler
        .worker_keep_alive_received(&worker_id2, NOW_TIME + WORKER_TIMEOUT_S)
        .await?;
    // This should remove worker 1 (the one executing our job).
    scheduler
        .remove_timedout_workers(NOW_TIME + WORKER_TIMEOUT_S)
        .await?;
    tokio::task::yield_now().await; // Allow task<->worker matcher to run.

    {
        // Worker1 should have received a disconnect message.
        let msg_for_worker = rx_from_worker1.recv().await.unwrap();
        assert_eq!(
            msg_for_worker,
            UpdateForWorker {
                update: Some(update_for_worker::Update::Disconnect(()))
            }
        );
    }
    {
        // Client should get notification saying it's being executed.
        let action_state = action_listener.changed().await.unwrap();
        assert_eq!(
            action_state.as_ref(),
            &ActionState {
                id: operation_id.clone(),
                stage: ActionStage::Executing,
            }
        );
    }
    {
        // Worker2 should now see execution request.
        let msg_for_worker = rx_from_worker2.recv().await.unwrap();
        assert_eq!(
            msg_for_worker,
            UpdateForWorker {
                update: Some(update_for_worker::Update::StartAction(
                    start_execute.clone()
                )),
            }
        );
    }

    Ok(())
}

#[nativelink_test]
async fn update_action_sends_completed_result_to_client_test() -> Result<(), Error> {
    let worker_id: WorkerId = WorkerId(Uuid::new_v4());

    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id, PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        insert_timestamp,
    )
    .await?;

    let operation_id = {
        // Other tests check full data. We only care if we got StartAction.
        match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(start_execute)) => {
                // Other tests check full data. We only care if client thinks we are Executing.
                assert_eq!(
                    action_listener.changed().await.unwrap().stage,
                    ActionStage::Executing
                );
                start_execute.operation_id
            }
            v => panic!("Expected StartAction, got : {v:?}"),
        }
    };

    let action_result = ActionResult {
        output_files: vec![FileInfo {
            name_or_path: NameOrPath::Name("hello".to_string()),
            digest: DigestInfo::new([5u8; 32], 18),
            is_executable: true,
        }],
        output_folders: vec![DirectoryInfo {
            path: "123".to_string(),
            tree_digest: DigestInfo::new([9u8; 32], 100),
        }],
        output_file_symlinks: vec![SymlinkInfo {
            name_or_path: NameOrPath::Name("foo".to_string()),
            target: "bar".to_string(),
        }],
        output_directory_symlinks: vec![SymlinkInfo {
            name_or_path: NameOrPath::Name("foo2".to_string()),
            target: "bar2".to_string(),
        }],
        exit_code: 0,
        stdout_digest: DigestInfo::new([6u8; 32], 19),
        stderr_digest: DigestInfo::new([7u8; 32], 20),
        execution_metadata: ExecutionMetadata {
            worker: worker_id.to_string(),
            queued_timestamp: make_system_time(5),
            worker_start_timestamp: make_system_time(6),
            worker_completed_timestamp: make_system_time(7),
            input_fetch_start_timestamp: make_system_time(8),
            input_fetch_completed_timestamp: make_system_time(9),
            execution_start_timestamp: make_system_time(10),
            execution_completed_timestamp: make_system_time(11),
            output_upload_start_timestamp: make_system_time(12),
            output_upload_completed_timestamp: make_system_time(13),
        },
        server_logs: HashMap::default(),
        error: None,
        message: String::new(),
    };
    scheduler
        .update_action(
            &worker_id,
            &OperationId::try_from(operation_id.as_str())?,
            Ok(ActionStage::Completed(action_result.clone())),
        )
        .await?;

    {
        // Client should get notification saying it has been completed.
        let action_state = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            id: action_state.id.clone(),
            stage: ActionStage::Completed(action_result),
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

#[nativelink_test]
async fn update_action_sends_completed_result_after_disconnect() -> Result<(), Error> {
    let worker_id: WorkerId = WorkerId(Uuid::new_v4());

    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id, PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        insert_timestamp,
    )
    .await?;

    let client_id = action_listener.client_operation_id().clone();

    // Drop our receiver and don't reconnect until completed.
    drop(action_listener);

    let operation_id = {
        // Other tests check full data. We only care if we got StartAction.
        let operation_id = match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(exec)) => exec.operation_id,
            v => panic!("Expected StartAction, got : {v:?}"),
        };
        // Other tests check full data. We only care if client thinks we are Executing.
        OperationId::try_from(operation_id.as_str())?
    };

    let action_result = ActionResult {
        output_files: vec![FileInfo {
            name_or_path: NameOrPath::Name("hello".to_string()),
            digest: DigestInfo::new([5u8; 32], 18),
            is_executable: true,
        }],
        output_folders: vec![DirectoryInfo {
            path: "123".to_string(),
            tree_digest: DigestInfo::new([9u8; 32], 100),
        }],
        output_file_symlinks: vec![SymlinkInfo {
            name_or_path: NameOrPath::Name("foo".to_string()),
            target: "bar".to_string(),
        }],
        output_directory_symlinks: vec![SymlinkInfo {
            name_or_path: NameOrPath::Name("foo2".to_string()),
            target: "bar2".to_string(),
        }],
        exit_code: 0,
        stdout_digest: DigestInfo::new([6u8; 32], 19),
        stderr_digest: DigestInfo::new([7u8; 32], 20),
        execution_metadata: ExecutionMetadata {
            worker: worker_id.to_string(),
            queued_timestamp: make_system_time(5),
            worker_start_timestamp: make_system_time(6),
            worker_completed_timestamp: make_system_time(7),
            input_fetch_start_timestamp: make_system_time(8),
            input_fetch_completed_timestamp: make_system_time(9),
            execution_start_timestamp: make_system_time(10),
            execution_completed_timestamp: make_system_time(11),
            output_upload_start_timestamp: make_system_time(12),
            output_upload_completed_timestamp: make_system_time(13),
        },
        server_logs: HashMap::default(),
        error: None,
        message: String::new(),
    };
    scheduler
        .update_action(
            &worker_id,
            &operation_id,
            Ok(ActionStage::Completed(action_result.clone())),
        )
        .await?;

    // Now look up a channel after the action has completed.
    let mut action_listener = scheduler
        .find_by_client_operation_id(&client_id)
        .await
        .unwrap()
        .expect("Action not found");
    {
        // Client should get notification saying it has been completed.
        let action_state = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            id: action_state.id.clone(),
            stage: ActionStage::Completed(action_result),
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

#[nativelink_test]
async fn update_action_with_wrong_worker_id_errors_test() -> Result<(), Error> {
    let good_worker_id: WorkerId = WorkerId(Uuid::new_v4());
    let rogue_worker_id: WorkerId = WorkerId(Uuid::new_v4());

    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, good_worker_id, PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        insert_timestamp,
    )
    .await?;

    {
        // Other tests check full data. We only care if we got StartAction.
        match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(_)) => { /* Success */ }
            v => panic!("Expected StartAction, got : {v:?}"),
        }
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            action_listener.changed().await.unwrap().stage,
            ActionStage::Executing
        );
    }
    let _ = setup_new_worker(&scheduler, rogue_worker_id, PlatformProperties::default()).await?;

    let action_info_hash_key = ActionUniqueQualifier::Cachable(ActionUniqueKey {
        instance_name: INSTANCE_NAME.to_string(),
        digest_function: DigestHasherFunc::Sha256,
        digest: action_digest,
    });
    let action_result = ActionResult {
        output_files: Vec::default(),
        output_folders: Vec::default(),
        output_file_symlinks: Vec::default(),
        output_directory_symlinks: Vec::default(),
        exit_code: 0,
        stdout_digest: DigestInfo::new([6u8; 32], 19),
        stderr_digest: DigestInfo::new([7u8; 32], 20),
        execution_metadata: ExecutionMetadata {
            worker: good_worker_id.to_string(),
            queued_timestamp: make_system_time(5),
            worker_start_timestamp: make_system_time(6),
            worker_completed_timestamp: make_system_time(7),
            input_fetch_start_timestamp: make_system_time(8),
            input_fetch_completed_timestamp: make_system_time(9),
            execution_start_timestamp: make_system_time(10),
            execution_completed_timestamp: make_system_time(11),
            output_upload_start_timestamp: make_system_time(12),
            output_upload_completed_timestamp: make_system_time(13),
        },
        server_logs: HashMap::default(),
        error: None,
        message: String::new(),
    };
    let update_action_result = scheduler
        .update_action(
            &rogue_worker_id,
            &OperationId::new(action_info_hash_key),
            Ok(ActionStage::Completed(action_result.clone())),
        )
        .await;

    {
        const EXPECTED_ERR: &str = "should not be running on worker";
        // Our request should have sent an error back.
        assert!(
            update_action_result.is_err(),
            "Expected error, got: {:?}",
            &update_action_result
        );
        let err = update_action_result.unwrap_err();
        assert!(
            err.to_string().contains(EXPECTED_ERR),
            "Error should contain '{EXPECTED_ERR}', got: {err:?}",
        );
    }
    {
        // Ensure client did not get notified.
        assert_eq!(
            poll!(action_listener.changed()),
            Poll::Pending,
            "Client should not have been notified of event"
        );
    }

    Ok(())
}

#[nativelink_test]
async fn does_not_crash_if_operation_joined_then_relaunched() -> Result<(), Error> {
    let worker_id: WorkerId = WorkerId(Uuid::new_v4());

    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let unique_qualifier = ActionUniqueQualifier::Cachable(ActionUniqueKey {
        instance_name: String::new(),
        digest: DigestInfo::zero_digest(),
        digest_function: DigestHasherFunc::Sha256,
    });
    let id = OperationId::new(unique_qualifier);
    let mut expected_action_state = ActionState {
        id,
        stage: ActionStage::Executing,
    };

    let insert_timestamp = make_system_time(1);
    let mut action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        insert_timestamp,
    )
    .await?;
    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id, PlatformProperties::default()).await?;

    {
        // Worker should have been sent an execute command.
        let expected_msg_for_worker = UpdateForWorker {
            update: Some(update_for_worker::Update::StartAction(StartExecute {
                execute_request: Some(ExecuteRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    action_digest: Some(action_digest.into()),
                    digest_function: digest_function::Value::Sha256.into(),
                    ..Default::default()
                }),
                operation_id: "Unknown Generated internally".to_string(),
                queued_timestamp: Some(insert_timestamp.into()),
            })),
        };
        let msg_for_worker = rx_from_worker.recv().await.unwrap();
        // Operation ID is random so we ignore it.
        assert!(update_eq(expected_msg_for_worker, msg_for_worker, true));
    }

    let operation_id = {
        // Client should get notification saying it's being executed.
        let action_state = action_listener.changed().await.unwrap();
        // We now know the name of the action so populate it.
        expected_action_state.id = action_state.id.clone();
        assert_eq!(action_state.as_ref(), &expected_action_state);
        action_state.id.clone()
    };

    let action_result = ActionResult {
        output_files: Vec::default(),
        output_folders: Vec::default(),
        output_directory_symlinks: Vec::default(),
        output_file_symlinks: Vec::default(),
        exit_code: Default::default(),
        stdout_digest: DigestInfo::new([1u8; 32], 512),
        stderr_digest: DigestInfo::new([2u8; 32], 512),
        execution_metadata: ExecutionMetadata {
            worker: String::new(),
            queued_timestamp: SystemTime::UNIX_EPOCH,
            worker_start_timestamp: SystemTime::UNIX_EPOCH,
            worker_completed_timestamp: SystemTime::UNIX_EPOCH,
            input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
            input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
            execution_start_timestamp: SystemTime::UNIX_EPOCH,
            execution_completed_timestamp: SystemTime::UNIX_EPOCH,
            output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
            output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
        },
        server_logs: HashMap::default(),
        error: None,
        message: String::new(),
    };

    scheduler
        .update_action(
            &worker_id,
            &operation_id,
            Ok(ActionStage::Completed(action_result.clone())),
        )
        .await?;

    {
        // Action should now be executing.
        expected_action_state.stage = ActionStage::Completed(action_result.clone());
        assert_eq!(
            action_listener.changed().await.unwrap().as_ref(),
            &expected_action_state
        );
    }

    // Now we need to ensure that if we schedule another execution of the same job it doesn't
    // fail.

    {
        let insert_timestamp = make_system_time(1);
        let mut action_listener = setup_action(
            &scheduler,
            action_digest,
            PlatformProperties::default(),
            insert_timestamp,
        )
        .await?;
        // We didn't disconnect our worker, so it will have scheduled it to the worker.
        expected_action_state.stage = ActionStage::Executing;
        let action_state = action_listener.changed().await.unwrap();
        // The name of the action changed (since it's a new action), so update it.
        expected_action_state.id = action_state.id.clone();
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

/// This tests to ensure that platform property restrictions allow jobs to continue to run after
/// a job finished on a specific worker (eg: restore platform properties).
#[nativelink_test]
async fn run_two_jobs_on_same_worker_with_platform_properties_restrictions() -> Result<(), Error> {
    let worker_id: WorkerId = WorkerId(Uuid::new_v4());

    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest1 = DigestInfo::new([11u8; 32], 512);
    let action_digest2 = DigestInfo::new([99u8; 32], 512);

    let mut properties = HashMap::new();
    properties.insert("prop1".to_string(), PlatformPropertyValue::Minimum(1));
    let platform_properties = PlatformProperties { properties };
    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id, platform_properties.clone()).await?;
    let insert_timestamp1 = make_system_time(1);
    let mut client1_action_listener = setup_action(
        &scheduler,
        action_digest1,
        platform_properties.clone(),
        insert_timestamp1,
    )
    .await?;
    let insert_timestamp2 = make_system_time(1);
    let mut client2_action_listener = setup_action(
        &scheduler,
        action_digest2,
        platform_properties,
        insert_timestamp2,
    )
    .await?;

    match rx_from_worker.recv().await.unwrap().update {
        Some(update_for_worker::Update::StartAction(_)) => { /* Success */ }
        v => panic!("Expected StartAction, got : {v:?}"),
    }
    let (operation_id1, operation_id2) = {
        let state_1 = client1_action_listener.changed().await.unwrap();
        let state_2 = client2_action_listener.changed().await.unwrap();
        // First client should be in an Executing state.
        assert_eq!(state_1.stage, ActionStage::Executing);
        // Second client should be in a queued state.
        assert_eq!(state_2.stage, ActionStage::Queued);
        (state_1.id.clone(), state_2.id.clone())
    };

    let action_result = ActionResult {
        output_files: Vec::default(),
        output_folders: Vec::default(),
        output_file_symlinks: Vec::default(),
        output_directory_symlinks: Vec::default(),
        exit_code: 0,
        stdout_digest: DigestInfo::new([6u8; 32], 19),
        stderr_digest: DigestInfo::new([7u8; 32], 20),
        execution_metadata: ExecutionMetadata {
            worker: worker_id.to_string(),
            queued_timestamp: make_system_time(5),
            worker_start_timestamp: make_system_time(6),
            worker_completed_timestamp: make_system_time(7),
            input_fetch_start_timestamp: make_system_time(8),
            input_fetch_completed_timestamp: make_system_time(9),
            execution_start_timestamp: make_system_time(10),
            execution_completed_timestamp: make_system_time(11),
            output_upload_start_timestamp: make_system_time(12),
            output_upload_completed_timestamp: make_system_time(13),
        },
        server_logs: HashMap::default(),
        error: None,
        message: String::new(),
    };

    // Tell scheduler our first task is completed.
    scheduler
        .update_action(
            &worker_id,
            &operation_id1,
            Ok(ActionStage::Completed(action_result.clone())),
        )
        .await?;

    {
        // First action should now be completed.
        let action_state = client1_action_listener.changed().await.unwrap();
        let mut expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            id: action_state.id.clone(),
            stage: ActionStage::Completed(action_result.clone()),
        };
        // We now know the name of the action so populate it.
        expected_action_state.id.unique_qualifier = action_state.id.unique_qualifier.clone();
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    // At this stage it should have added back any platform_properties and the next
    // task should be executing on the same worker.

    {
        // Our second client should now executing.
        match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(_)) => { /* Success */ }
            v => panic!("Expected StartAction, got : {v:?}"),
        }
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            client2_action_listener.changed().await.unwrap().stage,
            ActionStage::Executing
        );
    }

    // Tell scheduler our second task is completed.
    scheduler
        .update_action(
            &worker_id,
            &operation_id2,
            Ok(ActionStage::Completed(action_result.clone())),
        )
        .await?;

    {
        // Our second client should be notified it completed.
        let action_state = client2_action_listener.changed().await.unwrap();
        let mut expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            id: action_state.id.clone(),
            stage: ActionStage::Completed(action_result.clone()),
        };
        // We now know the name of the action so populate it.
        expected_action_state.id.unique_qualifier = action_state.id.unique_qualifier.clone();
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    Ok(())
}

/// This tests that actions are performed in the order they were queued.
#[nativelink_test]
async fn run_jobs_in_the_order_they_were_queued() -> Result<(), Error> {
    let worker_id: WorkerId = WorkerId(Uuid::new_v4());

    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest1 = DigestInfo::new([11u8; 32], 512);
    let action_digest2 = DigestInfo::new([99u8; 32], 512);

    // Use property to restrict the worker to a single action at a time.
    let mut properties = HashMap::new();
    properties.insert("prop1".to_string(), PlatformPropertyValue::Minimum(1));
    let platform_properties = PlatformProperties { properties };
    // This is queued after the next one (even though it's placed in the map
    // first), so it should execute second.
    let insert_timestamp2 = make_system_time(2);
    let mut client2_action_listener = setup_action(
        &scheduler,
        action_digest2,
        platform_properties.clone(),
        insert_timestamp2,
    )
    .await?;
    let insert_timestamp1 = make_system_time(1);
    let mut client1_action_listener = setup_action(
        &scheduler,
        action_digest1,
        platform_properties.clone(),
        insert_timestamp1,
    )
    .await?;

    // Add the worker after the queue has been set up.
    let mut rx_from_worker = setup_new_worker(&scheduler, worker_id, platform_properties).await?;

    match rx_from_worker.recv().await.unwrap().update {
        Some(update_for_worker::Update::StartAction(_)) => { /* Success */ }
        v => panic!("Expected StartAction, got : {v:?}"),
    }
    {
        // First client should be in an Executing state.
        assert_eq!(
            client1_action_listener.changed().await.unwrap().stage,
            ActionStage::Executing
        );
        // Second client should be in a queued state.
        assert_eq!(
            client2_action_listener.changed().await.unwrap().stage,
            ActionStage::Queued
        );
    }

    Ok(())
}

#[nativelink_test]
async fn worker_retries_on_internal_error_and_fails_test() -> Result<(), Error> {
    let worker_id: WorkerId = WorkerId(Uuid::new_v4());

    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler {
            max_job_retries: 1,
            ..Default::default()
        },
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id, PlatformProperties::default()).await?;
    let insert_timestamp = make_system_time(1);
    let mut action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        insert_timestamp,
    )
    .await?;

    let operation_id = {
        // Other tests check full data. We only care if we got StartAction.
        let operation_id = match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(exec)) => exec.operation_id,
            v => panic!("Expected StartAction, got : {v:?}"),
        };
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            action_listener.changed().await.unwrap().stage,
            ActionStage::Executing
        );
        OperationId::try_from(operation_id.as_str())?
    };

    let _ = scheduler
        .update_action(
            &worker_id,
            &operation_id,
            Err(make_err!(Code::Internal, "Some error")),
        )
        .await;

    {
        // Client should get notification saying it has been queued again.
        let action_state = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            id: action_state.id.clone(),
            stage: ActionStage::Queued,
        };
        assert_eq!(action_state.as_ref(), &expected_action_state);
    }

    // Now connect a new worker and it should pickup the action.
    let mut rx_from_worker =
        setup_new_worker(&scheduler, worker_id, PlatformProperties::default()).await?;
    {
        // Other tests check full data. We only care if we got StartAction.
        match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(_)) => { /* Success */ }
            v => panic!("Expected StartAction, got : {v:?}"),
        }
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            action_listener.changed().await.unwrap().stage,
            ActionStage::Executing
        );
    }

    let err = make_err!(Code::Internal, "Some error");
    // Send internal error from worker again.
    let _ = scheduler
        .update_action(&worker_id, &operation_id, Err(err.clone()))
        .await;

    {
        // Client should get notification saying it has been queued again.
        let action_state = action_listener.changed().await.unwrap();
        let expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            id: action_state.id.clone(),
            stage: ActionStage::Completed(ActionResult {
                output_files: Vec::default(),
                output_folders: Vec::default(),
                output_file_symlinks: Vec::default(),
                output_directory_symlinks: Vec::default(),
                exit_code: INTERNAL_ERROR_EXIT_CODE,
                stdout_digest: DigestInfo::zero_digest(),
                stderr_digest: DigestInfo::zero_digest(),
                execution_metadata: ExecutionMetadata {
                    worker: worker_id.to_string(),
                    queued_timestamp: SystemTime::UNIX_EPOCH,
                    worker_start_timestamp: SystemTime::UNIX_EPOCH,
                    worker_completed_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
                    execution_start_timestamp: SystemTime::UNIX_EPOCH,
                    execution_completed_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
                },
                server_logs: HashMap::default(),
                error: Some(err.clone()),
                message: String::new(),
            }),
        };
        let mut received_state = action_state.as_ref().clone();
        if let ActionStage::Completed(stage) = &mut received_state.stage {
            if let Some(real_err) = &mut stage.error {
                assert!(
                    real_err.to_string().contains("Job cancelled because it attempted to execute too many times and failed"),
                    "{real_err} did not contain 'Job cancelled because it attempted to execute too many times and failed'",
                );
                *real_err = err;
            }
        } else {
            panic!("Expected Completed, got : {:?}", action_state.stage);
        };
        assert_eq!(received_state, expected_action_state);
    }

    Ok(())
}

#[nativelink_test]
async fn ensure_scheduler_drops_inner_spawn() -> Result<(), Error> {
    struct DropChecker {
        dropped: Arc<AtomicBool>,
    }
    impl Drop for DropChecker {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let drop_checker = Arc::new(DropChecker {
        dropped: dropped.clone(),
    });

    // Since the inner spawn owns this callback, we can use the callback to know if the
    // inner spawn was dropped because our callback would be dropped, which dropps our
    // DropChecker.
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        move || {
            // This will ensure dropping happens if this function is ever dropped.
            let _drop_checker = drop_checker.clone();
            async move {}
        },
        MockInstantWrapped::default,
    );
    assert_eq!(dropped.load(Ordering::Relaxed), false);

    drop(scheduler);
    tokio::task::yield_now().await; // The drop may happen in a different task.

    // Ensure our callback was dropped.
    assert_eq!(dropped.load(Ordering::Relaxed), true);

    Ok(())
}

/// Regression test for: https://github.com/TraceMachina/nativelink/issues/257.
#[nativelink_test]
async fn ensure_task_or_worker_change_notification_received_test() -> Result<(), Error> {
    let worker_id1: WorkerId = WorkerId(Uuid::new_v4());
    let worker_id2: WorkerId = WorkerId(Uuid::new_v4());

    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let mut rx_from_worker1 =
        setup_new_worker(&scheduler, worker_id1, PlatformProperties::default()).await?;
    let mut action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        make_system_time(1),
    )
    .await?;

    let mut rx_from_worker2 =
        setup_new_worker(&scheduler, worker_id2, PlatformProperties::default()).await?;

    let operation_id = {
        // Other tests check full data. We only care if we got StartAction.
        let operation_id = match rx_from_worker1.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(exec)) => exec.operation_id,
            v => panic!("Expected StartAction, got : {v:?}"),
        };
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            action_listener.changed().await.unwrap().stage,
            ActionStage::Executing
        );
        OperationId::try_from(operation_id.as_str())?
    };

    let _ = scheduler
        .update_action(
            &worker_id1,
            &operation_id,
            Err(make_err!(Code::NotFound, "Some error")),
        )
        .await;

    tokio::task::yield_now().await; // Allow task<->worker matcher to run.

    // Now connect a new worker and it should pickup the action.
    {
        // Other tests check full data. We only care if we got StartAction.
        rx_from_worker2
            .recv()
            .await
            .err_tip(|| "worker went away")?;
        // Other tests check full data. We only care if client thinks we are Executing.
        assert_eq!(
            action_listener.changed().await.unwrap().stage,
            ActionStage::Executing
        );
    }

    Ok(())
}

// Note: This is a regression test for:
// https://github.com/TraceMachina/nativelink/issues/1197
#[nativelink_test]
async fn client_reconnect_keeps_action_alive() -> Result<(), Error> {
    let (scheduler, _worker_scheduler) = SimpleScheduler::new_with_callback(
        &nativelink_config::schedulers::SimpleScheduler::default(),
        || async move {},
        MockInstantWrapped::default,
    );
    let action_digest = DigestInfo::new([99u8; 32], 512);

    let insert_timestamp = make_system_time(1);
    let action_listener = setup_action(
        &scheduler,
        action_digest,
        PlatformProperties::default(),
        insert_timestamp,
    )
    .await
    .unwrap();

    let client_id = action_listener.client_operation_id().clone();

    // Simulate client disconnecting.
    drop(action_listener);

    let mut new_action_listener = scheduler
        .find_by_client_operation_id(&client_id)
        .await
        .unwrap()
        .expect("Action not found");

    // We should get one notification saying it's queued.
    assert_eq!(
        new_action_listener.changed().await.unwrap().stage,
        ActionStage::Queued
    );

    let changed_fut = new_action_listener.changed();
    tokio::pin!(changed_fut);

    // Now increment time and ensure the action does not get evicted.
    for _ in 0..500 {
        MockClock::advance(Duration::from_secs(2));
        // All others should be pending.
        assert_eq!(poll!(&mut changed_fut), Poll::Pending);
        tokio::task::yield_now().await;
        // Eviction happens when someone touches the internal
        // evicting map. So we constantly ask for some other client
        // to trigger eviction logic.
        scheduler
            .find_by_client_operation_id(&ClientOperationId::from_raw_string(
                "dummy_client_id".to_string(),
            ))
            .await
            .unwrap();
    }

    Ok(())
}
