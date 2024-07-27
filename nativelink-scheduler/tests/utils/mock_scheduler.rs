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

use async_trait::async_trait;
use nativelink_error::{make_input_err, Error};
use nativelink_metric::{MetricsComponent, RootMetricsComponent};
use nativelink_scheduler::action_scheduler::{ActionListener, ActionScheduler};
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_util::action_messages::{ActionInfo, ClientOperationId};
use tokio::sync::{mpsc, Mutex};

#[allow(clippy::large_enum_variant)]
enum ActionSchedulerCalls {
    GetPlatformPropertyManager(String),
    AddAction((ClientOperationId, ActionInfo)),
    FindExistingAction(ClientOperationId),
}

enum ActionSchedulerReturns {
    GetPlatformPropertyManager(Result<Arc<PlatformPropertyManager>, Error>),
    AddAction(Result<Pin<Box<dyn ActionListener>>, Error>),
    FindExistingAction(Result<Option<Pin<Box<dyn ActionListener>>>, Error>),
}

#[derive(MetricsComponent)]
pub struct MockActionScheduler {
    rx_call: Mutex<mpsc::UnboundedReceiver<ActionSchedulerCalls>>,
    tx_call: mpsc::UnboundedSender<ActionSchedulerCalls>,

    rx_resp: Mutex<mpsc::UnboundedReceiver<ActionSchedulerReturns>>,
    tx_resp: mpsc::UnboundedSender<ActionSchedulerReturns>,
}

impl Default for MockActionScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl MockActionScheduler {
    pub fn new() -> Self {
        let (tx_call, rx_call) = mpsc::unbounded_channel();
        let (tx_resp, rx_resp) = mpsc::unbounded_channel();
        Self {
            rx_call: Mutex::new(rx_call),
            tx_call,
            rx_resp: Mutex::new(rx_resp),
            tx_resp,
        }
    }

    pub async fn expect_get_platform_property_manager(
        &self,
        result: Result<Arc<PlatformPropertyManager>, Error>,
    ) -> String {
        let mut rx_call_lock = self.rx_call.lock().await;
        let ActionSchedulerCalls::GetPlatformPropertyManager(req) = rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        else {
            panic!("Got incorrect call waiting for get_platform_property_manager")
        };
        self.tx_resp
            .send(ActionSchedulerReturns::GetPlatformPropertyManager(result))
            .map_err(|_| make_input_err!("Could not send request to mpsc"))
            .unwrap();
        req
    }

    pub async fn expect_add_action(
        &self,
        result: Result<Pin<Box<dyn ActionListener>>, Error>,
    ) -> (ClientOperationId, ActionInfo) {
        let mut rx_call_lock = self.rx_call.lock().await;
        let ActionSchedulerCalls::AddAction(req) = rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        else {
            panic!("Got incorrect call waiting for get_platform_property_manager")
        };
        self.tx_resp
            .send(ActionSchedulerReturns::AddAction(result))
            .map_err(|_| make_input_err!("Could not send request to mpsc"))
            .unwrap();
        req
    }

    pub async fn expect_find_by_client_operation_id(
        &self,
        result: Result<Option<Pin<Box<dyn ActionListener>>>, Error>,
    ) -> ClientOperationId {
        let mut rx_call_lock = self.rx_call.lock().await;
        let ActionSchedulerCalls::FindExistingAction(req) = rx_call_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        else {
            panic!("Got incorrect call waiting for find_by_client_operation_id")
        };
        self.tx_resp
            .send(ActionSchedulerReturns::FindExistingAction(result))
            .map_err(|_| make_input_err!("Could not send request to mpsc"))
            .unwrap();
        req
    }
}

#[async_trait]
impl ActionScheduler for MockActionScheduler {
    async fn get_platform_property_manager(
        &self,
        instance_name: &str,
    ) -> Result<Arc<PlatformPropertyManager>, Error> {
        self.tx_call
            .send(ActionSchedulerCalls::GetPlatformPropertyManager(
                instance_name.to_string(),
            ))
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            ActionSchedulerReturns::GetPlatformPropertyManager(result) => result,
            _ => panic!("Expected get_platform_property_manager return value"),
        }
    }

    async fn add_action(
        &self,
        client_operation_id: ClientOperationId,
        action_info: ActionInfo,
    ) -> Result<Pin<Box<dyn ActionListener>>, Error> {
        self.tx_call
            .send(ActionSchedulerCalls::AddAction((
                client_operation_id,
                action_info,
            )))
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            ActionSchedulerReturns::AddAction(result) => result,
            _ => panic!("Expected add_action return value"),
        }
    }

    async fn find_by_client_operation_id(
        &self,
        client_operation_id: &ClientOperationId,
    ) -> Result<Option<Pin<Box<dyn ActionListener>>>, Error> {
        self.tx_call
            .send(ActionSchedulerCalls::FindExistingAction(
                client_operation_id.clone(),
            ))
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock
            .recv()
            .await
            .expect("Could not receive msg in mpsc")
        {
            ActionSchedulerReturns::FindExistingAction(result) => result,
            _ => panic!("Expected find_by_client_operation_id return value"),
        }
    }
}

impl RootMetricsComponent for MockActionScheduler {}
