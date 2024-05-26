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
use nativelink_state::type_wrappers::{JsonConvertable, OperationStageFlags, RedisOperation};
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionResult, ActionStage, ExecutionMetadata, OperationId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};

#[cfg(test)]
mod action_messages_tests {
    use std::any::Any;
    use std::time::{Duration, SystemTime};

    use nativelink_state::operation;
    use nativelink_state::type_wrappers::RedisOperationJson;
    use pretty_assertions::assert_eq;
    use redis::JsonAsyncCommands;

    use super::*; // Must be declared in every module.

    #[nativelink_test]

    async fn json_test() -> Result<(), Error> {
        const INSTANCE_NAME: &str = "foo";
        let action_digest = DigestInfo::new([3u8; 32], 10);
        const SALT: u64 = 0;

        let client = redis::Client::open("redis://127.0.0.1").unwrap();
        let mut con = client.get_multiplexed_async_connection().await?;
        redis::cmd("FLUSHALL").query_async(&mut con).await?;
        // Cant do digest info, platform props needs to be seperate index or platform props
        redis::cmd("FT.CREATE")
            .arg("stage")
            .arg("ON")
            .arg("JSON")
            .arg("PREFIX")
            .arg(1)
            .arg("operation:")
            .arg("SCHEMA")
            .arg("$.stage")
            .arg("AS")
            .arg("stage")
            .arg("TAG")
            // .arg("$.operation_id")
            // .arg("AS")
            // .arg("operation_id")
            // .arg("TEXT")
            // .arg("$.worker_id")
            // .arg("AS")
            // .arg("worker_id")
            // .arg("TAG")
            // .arg("$.last_worker_update")
            // .arg("AS")
            // .arg("last_worker_update")
            // .arg("NUMERIC")
            // .arg("$.completed_at")
            // .arg("AS")
            // .arg("completed_at")
            // .arg("NUMERIC")
            // .arg("$.last_client_update")
            // .arg("AS")
            // .arg("last_client_update")
            // .arg("NUMERIC")
            // .arg("$.timeout")
            // .arg("AS")
            // .arg("timeout")
            // .arg("NUMERIC")
            // .arg("$.priority")
            // .arg("AS")
            // .arg("priority")
            // .arg("NUMERIC")
            // .arg("$.load_timestamp")
            // .arg("AS")
            // .arg("load_timestamp")
            // .arg("NUMERIC")
            // .arg("$.insert_timestamp")
            // .arg("AS")
            // .arg("insert_timestamp")
            // .arg("NUMERIC")
            // .arg("$.unique_qualifier")
            // .arg("AS")
            // .arg("unique_qualifier")
            // .arg("TEXT")
            // .arg("$.skip_cache_lookup")
            // .arg("AS")
            // .arg("skip_cache_lookup")
            // .arg("TEXT")
            // .arg("$.digest_function")
            // .arg("AS")
            // .arg("digest_function")
            // .arg("TEXT")
            .query_async(&mut con)
            .await?;

        let action_info_1 = ActionInfo {
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
        let id = OperationId::new(action_info_1.unique_qualifier.clone());
        let operation = RedisOperation {
            stage: OperationStageFlags::Queued,
            operation_id: id.to_string(),
            worker_id: None,
            last_worker_update: SystemTime::now(),
            completed_at: None,
            last_client_update: None,
            action_info: action_info_1.into(),
        };

        let redis_json = RedisOperationJson::from(&operation);
        let key = format!("operation:{}", operation.operation_id);
        redis::cmd("JSON.SET")
            .arg(key)
            .arg("$")
            .arg(&redis_json.as_json_string())
            .query_async(&mut con)
            .await?;

        // let redis_json = RedisOperationJson::from(&operation);
        // con.json_set(key, "$.", &redis_json).await?;

        redis::cmd("FT.SEARCH");
        let res: Vec<String> = redis::cmd("FT.TAGVALS")
            .arg("stage")
            .arg("stage")
            .query_async(&mut con)
            .await?;
        println!("{:?}", res);
        // FT.SEARCH idx:bicycle "@price:[1000 +inf]"

        // .arg("$.stage AS stage TEXT")
        // .arg("$.last_update as last_update NUMERIC")
        // .arg("$.operation_id as operation_id TEXT")
        // .arg("$.worker_id as worker_id TEXT")
        // .arg("$.last_worker_update as last_worker_update TEXT")
        // .arg("$.completed_at as completed_at TEXT")
        // .arg("$.last_client_update as last_client_update TEXT")
        // .arg("$.action_info as action_info TEXT");
        Ok(())
    }
}
