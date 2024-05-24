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

use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_state::type_wrappers::{OperationStageFlags, RedisOperation};
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionResult, ActionStage, ExecutionMetadata, OperationId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::platform_properties::PlatformProperties;

#[cfg(test)]
mod action_messages_tests {
    use std::time::{Duration, SystemTime};

    use pretty_assertions::assert_eq;

    use super::*; // Must be declared in every module.

    #[nativelink_test]

    async fn json_test() -> Result<(), Error> {
        const INSTANCE_NAME: &str = "foo";
        let action_digest = DigestInfo::new([3u8; 32], 10);
        const SALT: u64 = 0;
        let action_info = ActionInfo {
            command_digest: DigestInfo::new([1u8; 32], 10),
            input_root_digest: DigestInfo::new([2u8; 32], 10),
            timeout: Duration::from_secs(1),
            platform_properties: PlatformProperties::default(),
            priority: 0,
            load_timestamp: SystemTime::UNIX_EPOCH,
            insert_timestamp: SystemTime::UNIX_EPOCH,
            unique_qualifier: ActionInfoHashKey {
                instance_name: INSTANCE_NAME.to_string(),
                digest: action_digest,
                salt: SALT,
            },
            skip_cache_lookup: true,
            digest_function: DigestHasherFunc::Blake3,
        };
        let id = OperationId::new(action_info.unique_qualifier.clone());
        let operation = RedisOperation {
            stage: OperationStageFlags::Queued,
            operation_id: id.to_string(),
            worker_id: None,
            last_worker_update: SystemTime::now(),
            completed_at: None,
            last_client_update: None,
            action_info: action_info.into(),
        };
        let json = serde_json::to_string(&operation);
        println!("{:?}", json);
        Ok(())
    }
}
