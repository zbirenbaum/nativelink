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

// use std::num::NonZeroUsize;
// use std::time::{Duration, SystemTime};
//
// use nativelink_error::Error;
// use nativelink_scheduler::redis_adapter::RedisAdapter;
// use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey};
// use nativelink_util::common::DigestInfo;
// use nativelink_util::digest_hasher::DigestHasherFunc;
// use nativelink_util::platform_properties::PlatformProperties;
// use redis::Client;
//
// // TODO: This is extremely unsable in async context
// // use connection_manager and awaits to fix
// #[cfg(test)]
// mod redis_adapter_test {
//     use std::str::FromStr;
//
//     use pretty_assertions::assert_eq;
//     use redis::AsyncCommands;
//
//     use super::*; // Must be declared in every module.
//
//
//     #[tokio::test]
//     async fn enqueue_action() -> Result<(), Error> {
//         const SALT: u64 = 1000;
//         let action_digest = DigestInfo::new([3u8; 32], 10);
//         let action_info = ActionInfo {
//             command_digest: DigestInfo::new([1u8; 32], 10),
//             input_root_digest: DigestInfo::new([2u8; 32], 10),
//             timeout: Duration::from_secs(1),
//             platform_properties: PlatformProperties::default(),
//             priority: 0,
//             load_timestamp: SystemTime::UNIX_EPOCH,
//             insert_timestamp: SystemTime::UNIX_EPOCH,
//             unique_qualifier: ActionInfoHashKey {
//                 instance_name: "foo".to_string(),
//                 digest: action_digest,
//                 salt: SALT,
//             },
//             skip_cache_lookup: true,
//             digest_function: DigestHasherFunc::Sha256,
//         };
//         let client = Client::open("redis://localhost").unwrap();
//         let adapter = RedisAdapter::new(client);
//         let encoded = hex::encode(action_info.unique_qualifier.get_hash());
//         let enqueue_res = adapter.enqueue(&encoded).await;
//         println!("{:?}", &enqueue_res);
//         assert!(enqueue_res.is_ok());
//         let mut con = adapter.get_multiplex_connection().await?;
//         assert_eq!(
//             con.hget("active".to_string(), encoded.clone()).await,
//             Ok(encoded.clone())
//         );
//
//         let active_res = adapter.make_active(&encoded).await;
//         println!("{:?}", &active_res);
//         assert!(active_res.is_ok());
//         Ok(())
//     }
// }
