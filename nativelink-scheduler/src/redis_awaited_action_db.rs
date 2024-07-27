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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::future::IntoFuture;
use std::ops::Bound;
use std::time::{Duration, Instant};
use std::sync::Arc;

use async_lock::{Mutex, MutexGuard};
use nativelink_error::{Code, make_err, make_input_err, Error, ResultExt};
use nativelink_store::redis_store::RedisStore;
use nativelink_util::action_messages::{
    ActionInfo, ActionUniqueKey, ActionUniqueQualifier, ClientOperationId, OperationId
};

use bincode::{serialize, deserialize};
use futures::{FutureExt, Stream, StreamExt};
use nativelink_util::task::JoinHandleDropGuard;
use nativelink_util::{background_spawn, spawn};
use nativelink_util::chunked_stream::ChunkedStream;
use nativelink_util::connection_manager::ConnectionManager;
use nativelink_util::store_trait::StoreLike;
use fred::clients::{RedisClient, SubscriberClient};
use serde::de::value;
use tokio::sync::{mpsc, watch};
use tonic::client;
use tracing::{event, Level};

use crate::awaited_action_db::{
    AwaitedAction, AwaitedActionDb, AwaitedActionSubscriber, SortedAwaitedAction,
    SortedAwaitedActionState,
};

/// Number of events to process per cycle.
const MAX_ACTION_EVENTS_RX_PER_CYCLE: usize = 1024;

/// Duration to wait before sending client keep alive messages.
const CLIENT_KEEPALIVE_DURATION: Duration = Duration::from_secs(10);

/// Information required to track an individual client
/// keep alive config and state.
// struct ClientKeepAlive {
//     /// The client operation id.
//     client_operation_id: ClientOperationId,
//     /// The last time a keep alive was sent.
//     last_keep_alive: Instant,
//     /// The sender to notify of this struct being dropped.
//     drop_tx: mpsc::UnboundedSender<RedisActionEvent>,
// }

/// Actions the AwaitedActionsDb needs to process.
enum RedisActionEvent {
    /// A client has sent a keep alive message.
    // ClientKeepAlive(ClientOperationId),
    /// A client has dropped and pointed to OperationId.
    SubscriberDroppedOperation(OperationId),
}

/// Subscriber that can be used to monitor when AwaitedActions change.
pub struct RedisOperationSubscriber {
    /// The receiver to listen for changes.
    awaited_action_rx: watch::Receiver<AwaitedAction>,
    /// The client operation id and keep alive information.
    operation_id: OperationId,
    /// Drop tx
    drop_tx: mpsc::UnboundedSender<RedisActionEvent>
}


impl RedisOperationSubscriber {}

impl AwaitedActionSubscriber for RedisOperationSubscriber {
    async fn changed(&mut self) -> Result<AwaitedAction, Error> {
        self.awaited_action_rx.changed().await;
        Ok(self.awaited_action_rx.borrow().clone())
    }

    fn borrow(&self) -> AwaitedAction {
        self.awaited_action_rx.borrow().clone()
    }
}

impl Drop for RedisOperationSubscriber {
    fn drop(&mut self) {
        self.drop_tx.send(RedisActionEvent::SubscriberDroppedOperation(self.operation_id.clone()));
    }
}


struct RedisAwaitedActionDbImpl {
    sub_count: HashMap<OperationId, usize>,
    tx_map: HashMap<OperationId, watch::Sender<AwaitedAction>>,
}

impl RedisAwaitedActionDbImpl {
    pub async fn get_operation_subscriber_count(&mut self, operation_id: &OperationId) -> usize {
        match self.sub_count.get(operation_id) {
            Some(count) => *count,
            None => 0
        }
    }

    pub async fn set_operation_subscriber_count(&mut self, operation_id: &OperationId, count: usize) {
        if count == 0 {
            // need to clean up the sender in other map when this happens
            self.sub_count.remove(operation_id);
        }
        else { self.sub_count.insert(operation_id.clone(), count); }
    }

    pub async fn get_operation_sender(&mut self, operation_id: &OperationId) -> Option<watch::Sender<AwaitedAction>> {
        self.tx_map.get(operation_id).cloned()
    }

    pub async fn set_operation_sender(&mut self, operation_id: &OperationId, tx: watch::Sender<AwaitedAction>) {
        self.tx_map.insert(operation_id.clone(), tx);
    }

}

pub struct RedisAwaitedActionDb {
    store: Arc<RedisStore>,
    inner: Arc<Mutex<RedisAwaitedActionDbImpl>>,
    join_handle: JoinHandleDropGuard<()>,
    // recv from action
    drop_event_rx: mpsc::UnboundedReceiver<RedisActionEvent>,
    drop_event_tx: mpsc::UnboundedSender<RedisActionEvent>,
}

impl RedisAwaitedActionDb {
    pub fn new(
        _store: Arc<RedisStore>,
        _inner: Arc<Mutex<RedisAwaitedActionDbImpl>>
    ) -> Self {
        todo!()
        // let weak_inner = Arc::downgrade(&inner);
        // let join_handle = spawn!("redis_action_change_listener", async move {
        //     let pubsub_result = client.get_async_pubsub().await;
        //     let Ok(mut pubsub) = pubsub_result else {
        //         event!(Level::ERROR, "RedisAwaitedActionDb::new Failed to get pubsub");
        //         return
        //     };
        //     if let Err(e) = pubsub.subscribe("update:*").await {
        //         event!(Level::ERROR, ?e, "RedisAwaitedActionDb::new Failed to subscribe to channel");
        //         return
        //     }
        //     let mut stream = pubsub.into_on_message();
        //     loop {
        //         let msg = stream.next().await.unwrap();
        //         match weak_inner.upgrade() {
        //             Some(inner_mutex) => {
        //                 let state: AwaitedAction = AwaitedAction::try_from(msg.get_payload_bytes()).unwrap();
        //                 let mut inner_mut = inner_mutex.lock().await;
        //                 let tx = inner_mut.get_operation_sender(state.operation_id()).await.unwrap();
        //                 // Use send_replace so that we can send the update even when there are no recievers.
        //                 tx.send_replace(state);
        //             }
        //             None => {
        //                 event!(Level::ERROR, "RedisAwaitedActionDb - Failed to upgrade inner");
        //                 return
        //             }
        //         }
        //     }
        // });
        //
        // let (drop_event_tx, drop_event_rx) = mpsc::unbounded_channel();
        // Self {
        //     store,
        //     inner,
        //     join_handle,
        //     drop_event_rx,
        //     drop_event_tx,
        // }
    }
    async fn get_operation_subscriber_count(&self, operation_id: &OperationId) -> usize {
        let mut inner = self.inner.lock().await;
        inner.get_operation_subscriber_count(operation_id).await
    }

    async fn dec_operation_subscriber_count(&self, operation_id: &OperationId) {
        let mut inner = self.inner.lock().await;
        let existing_count = inner.get_operation_subscriber_count(operation_id).await;
        inner.set_operation_subscriber_count(operation_id, existing_count-1).await;
    }

    async fn subscribe_to_operation(&self, operation_id: &OperationId) -> Result<Option<RedisOperationSubscriber>, Error> {
        let mut inner = self.inner.lock().await;
        let Some(tx) = inner.get_operation_sender(operation_id).await else {
            return Ok(None)
        };
        Ok(Some(RedisOperationSubscriber {
            awaited_action_rx: tx.subscribe(),
            operation_id: operation_id.clone(),
            // Increment in here so that we know it has occured immediately prior to drop_tx becoming accessible.
            drop_tx: (async move {
                let drop_tx = self.drop_event_tx.clone();
                let existing_count = inner.get_operation_subscriber_count(operation_id).await;
                inner.set_operation_subscriber_count(operation_id, existing_count+1).await;
                drop_tx
            }).await
        }))
    }

    async fn get_operation_id_by_client_id(&self, client_id: &ClientOperationId) -> Result<Option<OperationId>, Error> {
        let result: Result<OperationId, Error> = {
            let key = format!("cid:{client_id}");
            let bytes = self.store.get_part_unchunked(key.as_str(), 0, None).await?;
            deserialize(&bytes).map_err(|e| {
                make_input_err!("Decoding bytes failed with error: {e}")
            })?
        };
        match result {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                match e.code {
                    Code::NotFound => Ok(None),
                    _ => Err(e)
                }
            }
        }
    }

    async fn get_operation_id_by_hash_key(&self, unique_qualifier: &ActionUniqueQualifier) -> Result<Option<OperationId>, Error> {
        let result: Result<OperationId, Error> = {
            let key = format!("ahk:{unique_qualifier}");
            let bytes = self.store.get_part_unchunked(key.as_str(), 0, None).await?;
            deserialize(&bytes).map_err(|e| {
                make_input_err!("Decoding bytes failed with error: {e}")
            })?
        };
        match result {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                match e.code {
                    Code::NotFound => Ok(None),
                    _ => Err(e)
                }
            }
        }
    }

    async fn get_awaited_action_by_operation_id(&self, operation_id: &OperationId) -> Result<Option<AwaitedAction>, Error> {
        let result: Result<AwaitedAction, Error> = {
            let key = format!("oid:{operation_id}");
            let bytes = self.store.get_part_unchunked(key.as_str(), 0, None).await?;
            deserialize(&bytes).map_err(|e| {
                make_input_err!("Decoding bytes failed with error: {e}")
            })?
        };
        match result {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                match e.code {
                    Code::NotFound => Ok(None),
                    _ => Err(e)
                }
            }
        }
    }

    async fn set_client_id(&self, client_id: &ClientOperationId, operation_id: &OperationId) -> Result<(), Error> {
        let key = format!("cid:{client_id}");
        self.store.update_oneshot(key.as_str(), operation_id.to_string().into()).await
    }

    async fn set_hashkey_operation(&self, hash_key: &ActionUniqueQualifier, operation_id: &OperationId) -> Result<(), Error> {
        let key = format!("ahk:{hash_key}");
        self.store.update_oneshot(key.as_str(), operation_id.to_string().into()).await
    }
}

impl RedisAwaitedActionDb {
    /// Get the AwaitedAction by the client operation id.
    pub async fn get_awaited_action_by_id(
        &self,
        client_operation_id: &ClientOperationId,
    ) -> Result<Option<RedisOperationSubscriber>, Error> {
        match self.get_operation_id_by_client_id(client_operation_id).await {
            Ok(Some(operation_id)) => {
                self.subscribe_to_operation(&operation_id).await
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e)
        }
    }

    /// Get the AwaitedAction by the operation id.
    pub async fn get_by_operation_id(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<RedisOperationSubscriber>, Error> {
        self.subscribe_to_operation(operation_id).await
    }

    /// Process a change changed AwaitedAction and notify any listeners.
    pub async fn update_awaited_action(
        &self,
        new_awaited_action: AwaitedAction,
    ) -> Result<(), Error> {
        todo!()
    }

    /// Add (or join) an action to the AwaitedActionDb and subscribe
    /// to changes.
    pub async fn add_action(
        &self,
        client_operation_id: ClientOperationId,
        action_info: Arc<ActionInfo>,
    ) -> Result<RedisOperationSubscriber, Error> {
        match self.get_operation_id_by_hash_key(&action_info.unique_qualifier).await {
            Ok(Some(operation_id)) => {
                let Some(sub) = self.subscribe_to_operation(&operation_id).await? else {
                    return Err(make_err!(Code::NotFound, "Existing operation was found in redis but was not present in db"))
                };
                let key = format!("cid:{client_operation_id}");
                self.store.update_oneshot(key.as_str(), operation_id.to_string().into()).await?;
                Ok(sub)
            },
            Ok(None) => {
                let operation_id = OperationId::new(action_info.unique_qualifier.clone());
                let sub = self.subscribe_to_operation(&operation_id).await;
                let key = format!("cid:{client_operation_id}");
                self.store.update_oneshot(key.as_str(), operation_id.to_string().into()).await?;
                // Doing this here saves us a `clone` call on `action_info`.
                let action_hash_key = format!("ahk:{}", &action_info.unique_qualifier);
                let action = AwaitedAction::new(operation_id.clone(), action_info);
                let operation_id_key = format!("oid:{operation_id}");
                serialize(&action).map_err(op)
                self.store.update_oneshot(operation_id_key.as_str(), .await?;
                self.store.update_oneshot(action_hash_key.as_str(), operation_id.to_string().into()).await?;
                sub
            }
            Err(e) => Err(e)
        }
    }

    // /// Get a range of AwaitedActions of a specific state in sorted order.
    // fn get_range_of_actions(
    //     &self,
    //     state: SortedAwaitedActionState,
    //     start: Bound<SortedAwaitedAction>,
    //     end: Bound<SortedAwaitedAction>,
    //     desc: bool,
    // ) -> impl Future<Output = impl Stream<Item = Result<Self::Subscriber, Error>> + Send + Sync> + Send + Sync;
    //
    // /// Get all AwaitedActions. This call should be avoided as much as possible.
    // fn get_all_awaited_actions(
    //     &self,
    // ) -> impl Future<Output = impl Stream<Item = Result<Self::Subscriber, Error>> + Send + Sync> + Send + Sync;
}
