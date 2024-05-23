use std::collections::HashMap;

use uuid::Uuid;
use nativelink_error::Error;
use serde::{Serialize, Deserialize};
use redis_macros::{ToRedisArgs, FromRedisValue};
use nativelink_util::{action_messages::{ActionInfoHashKey, DirectoryInfo, ExecutionMetadata, FileInfo, Id, SymlinkInfo}, common::DigestInfo};

#[derive(Eq, PartialEq, Hash, Clone, Serialize, Deserialize, FromRedisValue, ToRedisArgs)]
pub struct RedisId {
    id: Uuid,
    unique_qualifier: ActionInfoHashKey
}

impl From<Id> for RedisId {
    fn from(value: Id) -> Self {
        Self {
            id: value.id,
            unique_qualifier: value.unique_qualifier
        }
    }
}

impl From<RedisId> for Id {
    fn from(id: RedisId) -> Id {
        Id {
            id: id.id,
            unique_qualifier: id.unique_qualifier
        }
    }
}

impl TryFrom<&str> for RedisId {
    type Error = Error;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Ok(RedisId::from(Id::try_from(s)?))
    }
}

impl TryFrom<String> for RedisId {
    type Error = Error;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

impl std::fmt::Display for RedisId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id_str = format!("{}:{}", self.unique_qualifier.action_name(), self.id);
        write!(f, "{id_str}")
    }
}

impl std::fmt::Debug for RedisId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id_str = format!("{}:{}", self.unique_qualifier.action_name(), self.id);
        write!(f, "{id_str}")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromRedisValue, ToRedisArgs)]
pub struct RedisActionResult {
    pub output_files: Vec<FileInfo>,
    pub output_folders: Vec<DirectoryInfo>,
    pub output_directory_symlinks: Vec<SymlinkInfo>,
    pub output_file_symlinks: Vec<SymlinkInfo>,
    pub exit_code: i32,
    pub stdout_digest: DigestInfo,
    pub stderr_digest: DigestInfo,
    pub execution_metadata: ExecutionMetadata,
    pub server_logs: HashMap<String, DigestInfo>,
    pub error: Option<Error>,
    pub message: String,
}
pub struct RedisActionState {

}
