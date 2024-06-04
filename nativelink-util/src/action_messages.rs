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

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use blake3::Hasher as Blake3Hasher;
use nativelink_error::{error_if, make_input_err, Error, ResultExt};
use nativelink_proto::build::bazel::remote::execution::v2::{
    execution_stage, Action, ActionResult as ProtoActionResult, ExecuteOperationMetadata,
    ExecuteRequest, ExecuteResponse, ExecutedActionMetadata, FileNode, LogFile, OutputDirectory,
    OutputFile, OutputSymlink, SymlinkNode,
};
use nativelink_proto::google::longrunning::operation::Result as LongRunningResult;
use nativelink_proto::google::longrunning::Operation;
use nativelink_proto::google::rpc::Status;
use prost::bytes::Bytes;
use prost::Message;
use prost_types::Any;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::{DigestInfo, HashMapExt, VecExt};
use crate::digest_hasher::DigestHasherFunc;
use crate::metrics_utils::{CollectorState, MetricsComponent};
use crate::platform_properties::PlatformProperties;

/// Default priority remote execution jobs will get when not provided.
pub const DEFAULT_EXECUTION_PRIORITY: i32 = 0;

pub type WorkerTimestamp = u64;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationId {
    pub unique_qualifier: ActionInfoHashKey,
    pub id: Uuid,
}

// TODO: Eventually we should make this it's own hash rather than delegate to ActionInfoHashKey.
impl Hash for OperationId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        ActionInfoHashKey::hash(&self.unique_qualifier, state)
    }
}

impl OperationId {
    pub fn new(unique_qualifier: ActionInfoHashKey) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            unique_qualifier,
        }
    }

    /// Utility function used to make a unique hash of the digest including the salt.
    pub fn get_hash(&self) -> [u8; 32] {
        self.unique_qualifier.get_hash()
    }

    /// Returns the salt used for cache busting/hashing.
    #[inline]
    pub fn action_name(&self) -> String {
        self.unique_qualifier.action_name()
    }
}

impl TryFrom<&str> for OperationId {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Error> {
        let (unique_qualifier, id) = value
            .split_once(':')
            .err_tip(|| "Invalid Id string - {value}")?;
        Ok(Self {
            unique_qualifier: ActionInfoHashKey::try_from(unique_qualifier)?,
            id: Uuid::parse_str(id).map_err(|e| make_input_err!("Failed to parse {e} as uuid"))?,
        })
    }
}

impl std::fmt::Display for OperationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{}:{}",
            self.unique_qualifier.action_name(),
            self.id
        ))
    }
}

impl std::fmt::Debug for OperationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{}:{}",
            self.unique_qualifier.action_name(),
            self.id
        ))
    }
}

/// Unique id of worker.
#[derive(Eq, PartialEq, Hash, Copy, Clone, Serialize, Deserialize)]
pub struct WorkerId(pub Uuid);

impl std::fmt::Display for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf = Uuid::encode_buffer();
        let worker_id_str = self.0.hyphenated().encode_lower(&mut buf);
        f.write_fmt(format_args!("{worker_id_str}"))
    }
}

impl std::fmt::Debug for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf = Uuid::encode_buffer();
        let worker_id_str = self.0.hyphenated().encode_lower(&mut buf);
        f.write_fmt(format_args!("{worker_id_str}"))
    }
}

impl TryFrom<String> for WorkerId {
    type Error = Error;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        match Uuid::parse_str(&s) {
            Err(e) => Err(make_input_err!(
                "Failed to convert string to WorkerId : {} : {:?}",
                s,
                e
            )),
            Ok(my_uuid) => Ok(WorkerId(my_uuid)),
        }
    }
}
/// This is a utility struct used to make it easier to match `ActionInfos` in a
/// `HashMap` without needing to construct an entire `ActionInfo`.
/// Since the hashing only needs the digest and salt we can just alias them here
/// and point the original `ActionInfo` structs to reference these structs for
/// it's hashing functions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionInfoHashKey {
    /// Name of instance group this action belongs to.
    pub instance_name: String,
    /// The digest function this action expects.
    pub digest_function: DigestHasherFunc,
    /// Digest of the underlying `Action`.
    pub digest: DigestInfo,
    /// Salt that can be filled with a random number to ensure no `ActionInfo` will be a match
    /// to another `ActionInfo` in the scheduler. When caching is wanted this value is usually
    /// zero.
    pub salt: u64,
}

impl ActionInfoHashKey {
    /// Utility function used to make a unique hash of the digest including the salt.
    pub fn get_hash(&self) -> [u8; 32] {
        Blake3Hasher::new()
            .update(self.instance_name.as_bytes())
            .update(&i32::from(self.digest_function.proto_digest_func()).to_le_bytes())
            .update(&self.digest.packed_hash[..])
            .update(&self.digest.size_bytes.to_le_bytes())
            .update(&self.salt.to_le_bytes())
            .finalize()
            .into()
    }

    /// Returns the salt used for cache busting/hashing.
    #[inline]
    pub fn action_name(&self) -> String {
        format!(
            "{}/{}/{}-{}/{:X}",
            self.instance_name,
            self.digest_function,
            self.digest.hash_str(),
            self.digest.size_bytes,
            self.salt
        )
    }
}

impl TryFrom<&str> for ActionInfoHashKey {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let (instance_name, other) = value
            .split_once('/')
            .err_tip(|| "Invalid ActionInfoHashKey string - {value}")?;
        let (digest_function, other) = other
            .split_once('/')
            .err_tip(|| "Invalid ActionInfoHashKey string - {value}")?;
        let (digest_hash, other) = other
            .split_once('-')
            .err_tip(|| "Invalid ActionInfoHashKey string - {value}")?;
        let (digest_size, salt) = other
            .split_once('/')
            .err_tip(|| "Invalid ActionInfoHashKey string - {value}")?;
        let digest = DigestInfo::try_new(
            digest_hash,
            digest_size
                .parse::<u64>()
                .err_tip(|| "Expected digest size to be a number for ActionInfoHashKey")?,
        )?;
        let salt = u64::from_str_radix(salt, 16).err_tip(|| "Expected salt to be a hex string")?;
        Ok(Self {
            instance_name: instance_name.to_string(),
            digest_function: digest_function.try_into()?,
            digest,
            salt,
        })
    }
}

/// Information needed to execute an action. This struct is used over bazel's proto `Action`
/// for simplicity and offers a `salt`, which is useful to ensure during hashing (for dicts)
/// to ensure we never match against another `ActionInfo` (when a task should never be cached).
/// This struct must be 100% compatible with `ExecuteRequest` struct in `remote_execution.proto`
/// except for the salt field.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionInfo {
    /// Digest of the underlying `Command`.
    pub command_digest: DigestInfo,
    /// Digest of the underlying `Directory`.
    pub input_root_digest: DigestInfo,
    /// Timeout of the action.
    pub timeout: Duration,
    /// The properties rules that must be applied when finding a worker that can run this action.
    pub platform_properties: PlatformProperties,
    /// The priority of the action. Higher value means it should execute faster.
    pub priority: i32,
    /// When this action started to be loaded from the CAS.
    pub load_timestamp: SystemTime,
    /// When this action was created.
    pub insert_timestamp: SystemTime,

    /// Info used to uniquely identify this ActionInfo. Normally the hash function would just
    /// use the fields it needs and you wouldn't need to separate them, however we have a use
    /// case where we sometimes want to lookup an entry in a HashMap, but we don't have the
    /// info to construct an entire ActionInfo. In such case we construct only a ActionInfoHashKey
    /// then use that object to lookup the entry in the map. The root problem is that HashMap
    /// requires `ActionInfo :Borrow<ActionInfoHashKey>` in order for this to work, which means
    /// we need to be able to return a &ActionInfoHashKey from ActionInfo, but since we cannot
    /// return a temporary reference we must have an object tied to ActionInfo's lifetime and
    /// return it's reference.
    pub unique_qualifier: ActionInfoHashKey,

    /// Whether to try looking up this action in the cache.
    pub skip_cache_lookup: bool,
}

impl ActionInfo {
    #[inline]
    pub const fn instance_name(&self) -> &String {
        &self.unique_qualifier.instance_name
    }

    /// Returns the underlying digest of the `Action`.
    #[inline]
    pub const fn digest(&self) -> &DigestInfo {
        &self.unique_qualifier.digest
    }

    /// Returns the salt used for cache busting/hashing.
    #[inline]
    pub const fn salt(&self) -> &u64 {
        &self.unique_qualifier.salt
    }

    pub fn try_from_action_and_execute_request_with_salt(
        execute_request: ExecuteRequest,
        action: Action,
        salt: u64,
        load_timestamp: SystemTime,
        queued_timestamp: SystemTime,
    ) -> Result<Self, Error> {
        Ok(Self {
            command_digest: action
                .command_digest
                .err_tip(|| "Expected command_digest to exist on Action")?
                .try_into()?,
            input_root_digest: action
                .input_root_digest
                .err_tip(|| "Expected input_root_digest to exist on Action")?
                .try_into()?,
            timeout: action
                .timeout
                .unwrap_or_default()
                .try_into()
                .map_err(|_| make_input_err!("Failed convert proto duration to system duration"))?,
            platform_properties: action.platform.unwrap_or_default().into(),
            priority: execute_request.execution_policy.unwrap_or_default().priority,
            load_timestamp,
            insert_timestamp: queued_timestamp,
            unique_qualifier: ActionInfoHashKey {
                instance_name: execute_request.instance_name,
                digest_function: DigestHasherFunc::try_from(execute_request.digest_function)
                    .err_tip(|| format!("Could not find digest_function in try_from_action_and_execute_request_with_salt {:?}", execute_request.digest_function))?,
                digest: execute_request
                    .action_digest
                    .err_tip(|| "Expected action_digest to exist on ExecuteRequest")?
                    .try_into()?,
                salt,
            },
            skip_cache_lookup: execute_request.skip_cache_lookup,
        })
    }
}

impl From<ActionInfo> for ExecuteRequest {
    fn from(val: ActionInfo) -> Self {
        let digest = val.digest().into();
        Self {
            instance_name: val.unique_qualifier.instance_name,
            action_digest: Some(digest),
            skip_cache_lookup: true, // The worker should never cache lookup.
            execution_policy: None,  // Not used in the worker.
            results_cache_policy: None, // Not used in the worker.
            digest_function: val
                .unique_qualifier
                .digest_function
                .proto_digest_func()
                .into(),
        }
    }
}

// Note: Hashing, Eq, and Ord matching on this struct is unique. Normally these functions
// must play well with each other, but in our case the following rules apply:
// * Hash - Hashing must be unique on the exact command being run and must never match
//          when do_not_cache is enabled, but must be consistent between identical data
//          hashes.
// * Eq   - Same as hash.
// * Ord  - Used when sorting `ActionInfo` together. The only major sorting is priority and
//          insert_timestamp, everything else is undefined, but must be deterministic.
impl Hash for ActionInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        ActionInfoHashKey::hash(&self.unique_qualifier, state);
    }
}

impl PartialEq for ActionInfo {
    fn eq(&self, other: &Self) -> bool {
        ActionInfoHashKey::eq(&self.unique_qualifier, &other.unique_qualifier)
    }
}

impl Eq for ActionInfo {}

impl Ord for ActionInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        // Want the highest priority on top, but the lowest insert_timestamp.
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.insert_timestamp.cmp(&self.insert_timestamp))
            .then_with(|| self.salt().cmp(other.salt()))
            .then_with(|| self.digest().size_bytes.cmp(&other.digest().size_bytes))
            .then_with(|| self.digest().packed_hash.cmp(&other.digest().packed_hash))
            .then_with(|| {
                self.unique_qualifier
                    .digest_function
                    .cmp(&other.unique_qualifier.digest_function)
            })
    }
}

impl PartialOrd for ActionInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Borrow<ActionInfoHashKey> for Arc<ActionInfo> {
    #[inline]
    fn borrow(&self) -> &ActionInfoHashKey {
        &self.unique_qualifier
    }
}

impl Hash for ActionInfoHashKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Digest is unique, so hashing it is all we need.
        self.digest_function.hash(state);
        self.digest.hash(state);
        self.salt.hash(state);
    }
}

impl PartialEq for ActionInfoHashKey {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
            && self.salt == other.salt
            && self.digest_function == other.digest_function
    }
}

impl Eq for ActionInfoHashKey {}

/// Simple utility struct to determine if a string is representing a full path or
/// just the name of the file.
/// This is in order to be able to reuse the same struct instead of building different
/// structs when converting `FileInfo` -> {`OutputFile`, `FileNode`} and other similar
/// structs.
#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum NameOrPath {
    Name(String),
    Path(String),
}

impl PartialOrd for NameOrPath {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NameOrPath {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_lexical_name = match self {
            Self::Name(name) => name,
            Self::Path(path) => path,
        };
        let other_lexical_name = match other {
            Self::Name(name) => name,
            Self::Path(path) => path,
        };
        self_lexical_name.cmp(other_lexical_name)
    }
}

/// Represents an individual file and associated metadata.
/// This struct must be 100% compatible with `OutputFile` and `FileNode` structs
/// in `remote_execution.proto`.
#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub name_or_path: NameOrPath,
    pub digest: DigestInfo,
    pub is_executable: bool,
}

//TODO: Make this TryFrom.
impl From<FileInfo> for FileNode {
    fn from(val: FileInfo) -> Self {
        let NameOrPath::Name(name) = val.name_or_path else {
            panic!("Cannot return a FileInfo that uses a NameOrPath::Path(), it must be a NameOrPath::Name()");
        };
        Self {
            name,
            digest: Some((&val.digest).into()),
            is_executable: val.is_executable,
            node_properties: Option::default(), // Not supported.
        }
    }
}

impl TryFrom<OutputFile> for FileInfo {
    type Error = Error;

    fn try_from(output_file: OutputFile) -> Result<Self, Error> {
        Ok(Self {
            name_or_path: NameOrPath::Path(output_file.path),
            digest: output_file
                .digest
                .err_tip(|| "Expected digest to exist on OutputFile")?
                .try_into()?,
            is_executable: output_file.is_executable,
        })
    }
}

//TODO: Make this TryFrom.
impl From<FileInfo> for OutputFile {
    fn from(val: FileInfo) -> Self {
        let NameOrPath::Path(path) = val.name_or_path else {
            panic!("Cannot return a FileInfo that uses a NameOrPath::Name(), it must be a NameOrPath::Path()");
        };
        Self {
            path,
            digest: Some((&val.digest).into()),
            is_executable: val.is_executable,
            contents: Bytes::default(),
            node_properties: Option::default(), // Not supported.
        }
    }
}

/// Represents an individual symlink file and associated metadata.
/// This struct must be 100% compatible with `SymlinkNode` and `OutputSymlink`.
#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct SymlinkInfo {
    pub name_or_path: NameOrPath,
    pub target: String,
}

impl TryFrom<SymlinkNode> for SymlinkInfo {
    type Error = Error;

    fn try_from(symlink_node: SymlinkNode) -> Result<Self, Error> {
        Ok(Self {
            name_or_path: NameOrPath::Name(symlink_node.name),
            target: symlink_node.target,
        })
    }
}

// TODO: Make this TryFrom.
impl From<SymlinkInfo> for SymlinkNode {
    fn from(val: SymlinkInfo) -> Self {
        let NameOrPath::Name(name) = val.name_or_path else {
            panic!("Cannot return a SymlinkInfo that uses a NameOrPath::Path(), it must be a NameOrPath::Name()");
        };
        Self {
            name,
            target: val.target,
            node_properties: Option::default(), // Not supported.
        }
    }
}

impl TryFrom<OutputSymlink> for SymlinkInfo {
    type Error = Error;

    fn try_from(output_symlink: OutputSymlink) -> Result<Self, Error> {
        Ok(Self {
            name_or_path: NameOrPath::Path(output_symlink.path),
            target: output_symlink.target,
        })
    }
}

// TODO: Make this TryFrom.
impl From<SymlinkInfo> for OutputSymlink {
    fn from(val: SymlinkInfo) -> Self {
        let NameOrPath::Path(path) = val.name_or_path else {
            panic!("Cannot return a SymlinkInfo that uses a NameOrPath::Path(), it must be a NameOrPath::Name()");
        };
        Self {
            path,
            target: val.target,
            node_properties: Option::default(), // Not supported.
        }
    }
}

/// Represents an individual directory file and associated metadata.
/// This struct must be 100% compatible with `SymlinkNode` and `OutputSymlink`.
#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryInfo {
    pub path: String,
    pub tree_digest: DigestInfo,
}

impl TryFrom<OutputDirectory> for DirectoryInfo {
    type Error = Error;

    fn try_from(output_directory: OutputDirectory) -> Result<Self, Error> {
        Ok(Self {
            path: output_directory.path,
            tree_digest: output_directory
                .tree_digest
                .err_tip(|| "Expected tree_digest to exist in OutputDirectory")?
                .try_into()?,
        })
    }
}

impl From<DirectoryInfo> for OutputDirectory {
    fn from(val: DirectoryInfo) -> Self {
        Self {
            path: val.path,
            tree_digest: Some(val.tree_digest.into()),
            is_topologically_sorted: false,
        }
    }
}

/// Represents the metadata associated with the execution result.
/// This struct must be 100% compatible with `ExecutedActionMetadata`.
#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionMetadata {
    pub worker: String,
    pub queued_timestamp: SystemTime,
    pub worker_start_timestamp: SystemTime,
    pub worker_completed_timestamp: SystemTime,
    pub input_fetch_start_timestamp: SystemTime,
    pub input_fetch_completed_timestamp: SystemTime,
    pub execution_start_timestamp: SystemTime,
    pub execution_completed_timestamp: SystemTime,
    pub output_upload_start_timestamp: SystemTime,
    pub output_upload_completed_timestamp: SystemTime,
}

impl Default for ExecutionMetadata {
    fn default() -> Self {
        Self {
            worker: "".to_string(),
            queued_timestamp: SystemTime::UNIX_EPOCH,
            worker_start_timestamp: SystemTime::UNIX_EPOCH,
            worker_completed_timestamp: SystemTime::UNIX_EPOCH,
            input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
            input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
            execution_start_timestamp: SystemTime::UNIX_EPOCH,
            execution_completed_timestamp: SystemTime::UNIX_EPOCH,
            output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
            output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
        }
    }
}

impl From<ExecutionMetadata> for ExecutedActionMetadata {
    fn from(val: ExecutionMetadata) -> Self {
        Self {
            worker: val.worker,
            queued_timestamp: Some(val.queued_timestamp.into()),
            worker_start_timestamp: Some(val.worker_start_timestamp.into()),
            worker_completed_timestamp: Some(val.worker_completed_timestamp.into()),
            input_fetch_start_timestamp: Some(val.input_fetch_start_timestamp.into()),
            input_fetch_completed_timestamp: Some(val.input_fetch_completed_timestamp.into()),
            execution_start_timestamp: Some(val.execution_start_timestamp.into()),
            execution_completed_timestamp: Some(val.execution_completed_timestamp.into()),
            output_upload_start_timestamp: Some(val.output_upload_start_timestamp.into()),
            output_upload_completed_timestamp: Some(val.output_upload_completed_timestamp.into()),
            virtual_execution_duration: val
                .execution_completed_timestamp
                .duration_since(val.execution_start_timestamp)
                .ok()
                .and_then(|duration| prost_types::Duration::try_from(duration).ok()),
            auxiliary_metadata: Vec::default(),
        }
    }
}

impl TryFrom<ExecutedActionMetadata> for ExecutionMetadata {
    type Error = Error;

    fn try_from(eam: ExecutedActionMetadata) -> Result<Self, Error> {
        Ok(Self {
            worker: eam.worker,
            queued_timestamp: eam
                .queued_timestamp
                .err_tip(|| "Expected queued_timestamp to exist in ExecutedActionMetadata")?
                .try_into()?,
            worker_start_timestamp: eam
                .worker_start_timestamp
                .err_tip(|| "Expected worker_start_timestamp to exist in ExecutedActionMetadata")?
                .try_into()?,
            worker_completed_timestamp: eam
                .worker_completed_timestamp
                .err_tip(|| {
                    "Expected worker_completed_timestamp to exist in ExecutedActionMetadata"
                })?
                .try_into()?,
            input_fetch_start_timestamp: eam
                .input_fetch_start_timestamp
                .err_tip(|| {
                    "Expected input_fetch_start_timestamp to exist in ExecutedActionMetadata"
                })?
                .try_into()?,
            input_fetch_completed_timestamp: eam
                .input_fetch_completed_timestamp
                .err_tip(|| {
                    "Expected input_fetch_completed_timestamp to exist in ExecutedActionMetadata"
                })?
                .try_into()?,
            execution_start_timestamp: eam
                .execution_start_timestamp
                .err_tip(|| {
                    "Expected execution_start_timestamp to exist in ExecutedActionMetadata"
                })?
                .try_into()?,
            execution_completed_timestamp: eam
                .execution_completed_timestamp
                .err_tip(|| {
                    "Expected execution_completed_timestamp to exist in ExecutedActionMetadata"
                })?
                .try_into()?,
            output_upload_start_timestamp: eam
                .output_upload_start_timestamp
                .err_tip(|| {
                    "Expected output_upload_start_timestamp to exist in ExecutedActionMetadata"
                })?
                .try_into()?,
            output_upload_completed_timestamp: eam
                .output_upload_completed_timestamp
                .err_tip(|| {
                    "Expected output_upload_completed_timestamp to exist in ExecutedActionMetadata"
                })?
                .try_into()?,
        })
    }
}

/// Exit code sent if there is an internal error.
pub const INTERNAL_ERROR_EXIT_CODE: i32 = -178;

/// Represents the results of an execution.
/// This struct must be 100% compatible with `ActionResult` in `remote_execution.proto`.
#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct ActionResult {
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

impl Default for ActionResult {
    fn default() -> Self {
        ActionResult {
            output_files: Default::default(),
            output_folders: Default::default(),
            output_directory_symlinks: Default::default(),
            output_file_symlinks: Default::default(),
            exit_code: INTERNAL_ERROR_EXIT_CODE,
            stdout_digest: DigestInfo::new([0u8; 32], 0),
            stderr_digest: DigestInfo::new([0u8; 32], 0),
            execution_metadata: ExecutionMetadata {
                worker: "".to_string(),
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
            server_logs: Default::default(),
            error: None,
            message: String::new(),
        }
    }
}

// TODO(allada) Remove the need for clippy argument by making the ActionResult and ProtoActionResult
// a Box.
/// The execution status/stage. This should match `ExecutionStage::Value` in `remote_execution.proto`.
#[derive(PartialEq, Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum ActionStage {
    /// Stage is unknown.
    Unknown,
    /// Checking the cache to see if action exists.
    CacheCheck,
    /// Action has been accepted and waiting for worker to take it.
    Queued,
    // TODO(allada) We need a way to know if the job was sent to a worker, but hasn't begun
    // execution yet.
    /// Worker is executing the action.
    Executing,
    /// Worker completed the work with result.
    Completed(ActionResult),
    /// Result was found from cache, don't decode the proto just to re-encode it.
    CompletedFromCache(ProtoActionResult),
}

impl ActionStage {
    pub const fn has_action_result(&self) -> bool {
        match self {
            Self::Unknown | Self::CacheCheck | Self::Queued | Self::Executing => false,
            Self::Completed(_) | Self::CompletedFromCache(_) => true,
        }
    }

    /// Returns true if the worker considers the action done and no longer needs to be tracked.
    // Note: This function is separate from `has_action_result()` to not mix the concept of
    //       "finished" with "has a result".
    pub const fn is_finished(&self) -> bool {
        self.has_action_result()
    }
}

impl MetricsComponent for ActionStage {
    fn gather_metrics(&self, c: &mut CollectorState) {
        let (stage, maybe_exit_code) = match self {
            ActionStage::Unknown => ("Unknown", None),
            ActionStage::CacheCheck => ("CacheCheck", None),
            ActionStage::Queued => ("Queued", None),
            ActionStage::Executing => ("Executing", None),
            ActionStage::Completed(action_result) => ("Completed", Some(action_result.exit_code)),
            ActionStage::CompletedFromCache(proto_action_result) => {
                ("CompletedFromCache", Some(proto_action_result.exit_code))
            }
        };
        c.publish("stage", &stage.to_string(), "The state of the action.");
        if let Some(exit_code) = maybe_exit_code {
            c.publish("exit_code", &exit_code, "The exit code of the action.");
        }
    }
}

impl From<&ActionStage> for execution_stage::Value {
    fn from(val: &ActionStage) -> Self {
        match val {
            ActionStage::Unknown => Self::Unknown,
            ActionStage::CacheCheck => Self::CacheCheck,
            ActionStage::Queued => Self::Queued,
            ActionStage::Executing => Self::Executing,
            ActionStage::Completed(_) | ActionStage::CompletedFromCache(_) => Self::Completed,
        }
    }
}

pub fn to_execute_response(action_result: ActionResult) -> ExecuteResponse {
    fn logs_from(server_logs: HashMap<String, DigestInfo>) -> HashMap<String, LogFile> {
        let mut logs = HashMap::with_capacity(server_logs.len());
        for (k, v) in server_logs {
            logs.insert(
                k.clone(),
                LogFile {
                    digest: Some(v.into()),
                    human_readable: false,
                },
            );
        }
        logs
    }

    let status = Some(
        action_result
            .error
            .clone()
            .map_or_else(Status::default, |v| v.into()),
    );
    let message = action_result.message.clone();
    ExecuteResponse {
        server_logs: logs_from(action_result.server_logs.clone()),
        result: Some(action_result.into()),
        cached_result: false,
        status,
        message,
    }
}

impl From<ActionStage> for ExecuteResponse {
    fn from(val: ActionStage) -> Self {
        match val {
            // We don't have an execute response if we don't have the results. It is defined
            // behavior to return an empty proto struct.
            ActionStage::Unknown
            | ActionStage::CacheCheck
            | ActionStage::Queued
            | ActionStage::Executing => Self::default(),
            ActionStage::Completed(action_result) => to_execute_response(action_result),
            // Handled separately as there are no server logs and the action
            // result is already in Proto format.
            ActionStage::CompletedFromCache(proto_action_result) => Self {
                server_logs: HashMap::new(),
                result: Some(proto_action_result),
                cached_result: true,
                status: Some(Status::default()),
                message: String::new(), // Will be populated later if applicable.
            },
        }
    }
}

impl From<ActionResult> for ProtoActionResult {
    fn from(val: ActionResult) -> Self {
        let mut output_symlinks = Vec::with_capacity(
            val.output_file_symlinks.len() + val.output_directory_symlinks.len(),
        );
        output_symlinks.extend_from_slice(val.output_file_symlinks.as_slice());
        output_symlinks.extend_from_slice(val.output_directory_symlinks.as_slice());

        Self {
            output_files: val.output_files.into_iter().map(Into::into).collect(),
            output_file_symlinks: val
                .output_file_symlinks
                .into_iter()
                .map(Into::into)
                .collect(),
            output_symlinks: output_symlinks.into_iter().map(Into::into).collect(),
            output_directories: val.output_folders.into_iter().map(Into::into).collect(),
            output_directory_symlinks: val
                .output_directory_symlinks
                .into_iter()
                .map(Into::into)
                .collect(),
            exit_code: val.exit_code,
            stdout_raw: Bytes::default(),
            stdout_digest: Some(val.stdout_digest.into()),
            stderr_raw: Bytes::default(),
            stderr_digest: Some(val.stderr_digest.into()),
            execution_metadata: Some(val.execution_metadata.into()),
        }
    }
}

impl TryFrom<ProtoActionResult> for ActionResult {
    type Error = Error;

    fn try_from(val: ProtoActionResult) -> Result<Self, Error> {
        let output_file_symlinks = val
            .output_file_symlinks
            .into_iter()
            .map(|output_symlink| {
                SymlinkInfo::try_from(output_symlink)
                    .err_tip(|| "Output File Symlinks could not be converted to SymlinkInfo")
            })
            .collect::<Result<Vec<_>, _>>()?;

        let output_directory_symlinks = val
            .output_directory_symlinks
            .into_iter()
            .map(|output_symlink| {
                SymlinkInfo::try_from(output_symlink)
                    .err_tip(|| "Output File Symlinks could not be converted to SymlinkInfo")
            })
            .collect::<Result<Vec<_>, _>>()?;

        let output_files = val
            .output_files
            .into_iter()
            .map(|output_file| {
                output_file
                    .try_into()
                    .err_tip(|| "Output File could not be converted")
            })
            .collect::<Result<Vec<_>, _>>()?;

        let output_folders = val
            .output_directories
            .into_iter()
            .map(|output_directory| {
                output_directory
                    .try_into()
                    .err_tip(|| "Output File could not be converted")
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            output_files,
            output_folders,
            output_file_symlinks,
            output_directory_symlinks,
            exit_code: val.exit_code,
            stdout_digest: val
                .stdout_digest
                .err_tip(|| "Expected stdout_digest to be set on ExecuteResponse msg")?
                .try_into()?,
            stderr_digest: val
                .stderr_digest
                .err_tip(|| "Expected stderr_digest to be set on ExecuteResponse msg")?
                .try_into()?,
            execution_metadata: val
                .execution_metadata
                .err_tip(|| "Expected execution_metadata to be set on ExecuteResponse msg")?
                .try_into()?,
            server_logs: Default::default(),
            error: None,
            message: String::new(),
        })
    }
}

impl TryFrom<ExecuteResponse> for ActionStage {
    type Error = Error;

    fn try_from(execute_response: ExecuteResponse) -> Result<Self, Error> {
        let proto_action_result = execute_response
            .result
            .err_tip(|| "Expected result to be set on ExecuteResponse msg")?;
        let action_result = ActionResult {
            output_files: proto_action_result
                .output_files
                .try_map(TryInto::try_into)?,
            output_directory_symlinks: proto_action_result
                .output_directory_symlinks
                .try_map(TryInto::try_into)?,
            output_file_symlinks: proto_action_result
                .output_file_symlinks
                .try_map(TryInto::try_into)?,
            output_folders: proto_action_result
                .output_directories
                .try_map(TryInto::try_into)?,
            exit_code: proto_action_result.exit_code,

            stdout_digest: proto_action_result
                .stdout_digest
                .err_tip(|| "Expected stdout_digest to be set on ExecuteResponse msg")?
                .try_into()?,
            stderr_digest: proto_action_result
                .stderr_digest
                .err_tip(|| "Expected stderr_digest to be set on ExecuteResponse msg")?
                .try_into()?,
            execution_metadata: proto_action_result
                .execution_metadata
                .err_tip(|| "Expected execution_metadata to be set on ExecuteResponse msg")?
                .try_into()?,
            server_logs: execute_response.server_logs.try_map(|v| {
                v.digest
                    .err_tip(|| "Expected digest to be set on LogFile msg")?
                    .try_into()
            })?,
            error: execute_response.status.clone().and_then(|v| {
                if v.code == 0 {
                    None
                } else {
                    Some(v.into())
                }
            }),
            message: execute_response.message,
        };

        if execute_response.cached_result {
            return Ok(Self::CompletedFromCache(action_result.into()));
        }
        Ok(Self::Completed(action_result))
    }
}

// TODO: Should be able to remove this after tokio-rs/prost#299
trait TypeUrl: Message {
    const TYPE_URL: &'static str;
}

impl TypeUrl for ExecuteResponse {
    const TYPE_URL: &'static str =
        "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteResponse";
}

impl TypeUrl for ExecuteOperationMetadata {
    const TYPE_URL: &'static str =
        "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteOperationMetadata";
}

fn from_any<T>(message: &Any) -> Result<T, Error>
where
    T: TypeUrl + Default,
{
    error_if!(
        message.type_url != T::TYPE_URL,
        "Incorrect type when decoding Any. {} != {}",
        message.type_url,
        T::TYPE_URL.to_string()
    );
    Ok(T::decode(message.value.as_slice())?)
}

fn to_any<T>(message: &T) -> Any
where
    T: TypeUrl,
{
    Any {
        type_url: T::TYPE_URL.to_string(),
        value: message.encode_to_vec(),
    }
}

impl TryFrom<Operation> for ActionState {
    type Error = Error;

    fn try_from(operation: Operation) -> Result<ActionState, Error> {
        let metadata = from_any::<ExecuteOperationMetadata>(
            &operation
                .metadata
                .err_tip(|| "No metadata in upstream operation")?,
        )
        .err_tip(|| "Could not decode metadata in upstream operation")?;

        let stage = match execution_stage::Value::try_from(metadata.stage).err_tip(|| {
            format!(
                "Could not convert {} to execution_stage::Value",
                metadata.stage
            )
        })? {
            execution_stage::Value::Unknown => ActionStage::Unknown,
            execution_stage::Value::CacheCheck => ActionStage::CacheCheck,
            execution_stage::Value::Queued => ActionStage::Queued,
            execution_stage::Value::Executing => ActionStage::Executing,
            execution_stage::Value::Completed => {
                let execute_response = operation
                    .result
                    .err_tip(|| "No result data for completed upstream action")?;
                match execute_response {
                    LongRunningResult::Error(error) => ActionStage::Completed(ActionResult {
                        error: Some(error.into()),
                        ..ActionResult::default()
                    }),
                    LongRunningResult::Response(response) => {
                        // Could be Completed, CompletedFromCache or Error.
                        from_any::<ExecuteResponse>(&response)
                            .err_tip(|| {
                                "Could not decode result structure for completed upstream action"
                            })?
                            .try_into()?
                    }
                }
            }
        };

        // NOTE: This will error if we are forwarding an operation from
        // one remote execution system to another that does not use our operation name
        // format (ie: very unlikely, but possible).
        let id = OperationId::try_from(operation.name.as_str())?;
        Ok(Self { id, stage })
    }
}

/// Current state of the action.
/// This must be 100% compatible with `Operation` in `google/longrunning/operations.proto`.
#[derive(PartialEq, Debug, Clone)]
pub struct ActionState {
    pub stage: ActionStage,
    pub id: OperationId,
}

impl ActionState {
    #[inline]
    pub fn unique_qualifier(&self) -> &ActionInfoHashKey {
        &self.id.unique_qualifier
    }
    #[inline]
    pub fn action_digest(&self) -> &DigestInfo {
        &self.id.unique_qualifier.digest
    }
}

impl MetricsComponent for ActionState {
    fn gather_metrics(&self, c: &mut CollectorState) {
        c.publish("stage", &self.stage, "");
    }
}

impl From<ActionState> for Operation {
    fn from(val: ActionState) -> Self {
        let stage = Into::<execution_stage::Value>::into(&val.stage) as i32;
        let name = val.id.to_string();

        let result = if val.stage.has_action_result() {
            let execute_response: ExecuteResponse = val.stage.into();
            Some(LongRunningResult::Response(to_any(&execute_response)))
        } else {
            None
        };
        let digest = Some(val.id.unique_qualifier.digest.into());

        let metadata = ExecuteOperationMetadata {
            stage,
            action_digest: digest,
            // TODO(blaise.bruer) We should support stderr/stdout streaming.
            stdout_stream_name: String::default(),
            stderr_stream_name: String::default(),
            partial_execution_metadata: None,
        };

        Self {
            name,
            metadata: Some(to_any(&metadata)),
            done: result.is_some(),
            result,
        }
    }
}
