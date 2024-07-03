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

use std::borrow::Cow;
use std::cell::OnceCell;
use std::fmt::Display;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use fred::clients::RedisPool;
use fred::interfaces::{KeysInterface, TransactionInterface};
use fred::types::{Builder, ClusterDiscoveryPolicy, ReconnectPolicy, ReplicaConfig, RespVersion};
use futures::future::{ErrInto, FutureExt, Shared};
use futures::stream::FuturesOrdered;
use futures::{Future, TryFutureExt, TryStreamExt};
use nativelink_error::{error_if, make_err, make_input_err, Code, Error, ResultExt};
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::health_utils::{HealthRegistryBuilder, HealthStatus, HealthStatusIndicator};
use nativelink_util::metrics_utils::{Collector, CollectorState, MetricsComponent, Registry};
use nativelink_util::store_trait::{StoreDriver, StoreKey, UploadSizeInfo};
use serde::Serialize;
use fred::prelude::{RedisClient, RedisConfig, RedisKey, RedisValue};

use crate::cas_utils::is_zero_digest;

const READ_CHUNK_SIZE: isize = 64 * 1024;

/// A [`StoreDriver`] implementation that uses Redis as a backing store.
pub struct RedisStore {
    /// The connection to the underlying Redis instance(s).
    client_pool: RedisPool,

    /// A function used to generate names for temporary keys.
    temp_name_generator_fn: fn() -> String,
    pub_sub_channel: Option<String>,

    /// A common prefix to append to all keys before they are sent to Redis.
    ///
    /// See [`RedisStore::key_prefix`](`nativelink_config::stores::RedisStore::key_prefix`).
    key_prefix: String,
}

impl RedisStore {
    pub fn new(
        config: &nativelink_config::stores::RedisStore
    ) -> Result<Arc<Self>, Error> {
        let redis_config = RedisConfig::from_url(&config.address).map_err(|e|
            make_input_err!("Error while creating connection url: {}", e.to_string())
        )?;
        let pool = Builder::from_config(redis_config)
            .with_config(|config| {
              config.version = RespVersion::RESP3;
              config
                .server
                .set_cluster_discovery_policy(ClusterDiscoveryPolicy::ConfigEndpoint)
                .expect("Failed to set discovery policy.");
            })
            .with_connection_config(|config| {
              config.replica = ReplicaConfig {
                lazy_connections: true,
                primary_fallback: true,
                ..Default::default()
              };
            })
            .set_policy(ReconnectPolicy::new_exponential(0, 100, 30_000, 2))
            .build_pool(5).map_err(|e| {
                make_input_err!("Error while building client pool: {}", e.to_string())
            })?;

        Ok(Arc::new(
            RedisStore::new_with_conn_and_name_generator_and_prefix(
                pool,
                || uuid::Uuid::new_v4().to_string(),
                config.experimental_pub_sub_channel.clone(),
                config.key_prefix.clone(),
            )
        ))
    }

    #[inline]
    pub fn new_with_conn_and_name_generator(
        client_pool: RedisPool,
        temp_name_generator_fn: fn() -> String,
    ) -> Self {
        RedisStore::new_with_conn_and_name_generator_and_prefix(
            client_pool,
            temp_name_generator_fn,
            None,
            String::new(),
        )
    }


    #[inline]
    pub fn new_with_conn_and_name_generator_and_prefix(
        client_pool: RedisPool,
        temp_name_generator_fn: fn() -> String,
        pub_sub_channel: Option<String>,
        key_prefix: String,
    ) -> Self {
        RedisStore {
            client_pool,
            temp_name_generator_fn,
            pub_sub_channel,
            key_prefix,
        }
    }

    #[inline]
    pub async fn get_conn(&self) -> RedisClient {
        self.client_pool.next().clone()
    }
    /// Encode a [`StoreKey`] so it can be sent to Redis.
    fn encode_key(&self, key: StoreKey) -> String {
        if self.key_prefix.is_empty() {
          key.as_str().to_string()
        } else {
          let mut encoded_key = String::with_capacity(self.key_prefix.len() + key.as_str().len());
          encoded_key.push_str(&self.key_prefix);
          encoded_key.push_str(&key.as_str());
          encoded_key
        }
    }
}

#[async_trait]
impl StoreDriver for RedisStore
{
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<usize>],
    ) -> Result<(), Error> {
        if keys.len() == 1 && is_zero_digest(keys[0].borrow()) {
            results[0] = Some(0);
            return Ok(());
        }

        let mut zero_digest_indexes = Vec::new();

        let queries =
            keys.iter()
                .enumerate()
                .map(|(index, key)| {
                    if is_zero_digest(key.borrow()) {
                        zero_digest_indexes.push(index);
                    }
                    let encoded_key = self.encode_key(key.borrow());

                    async {
                        let conn = self.get_conn().await;
                        conn.strlen::<usize, _>(encoded_key)
                            .await
                            .map_err(from_redis_err)
                            .err_tip(|| "Error: Could not call strlen in has_with_results")
                    }
                })
                .collect::<FuturesOrdered<_>>();

        let digest_sizes = queries.try_collect::<Vec<_>>().await?;

        error_if!(
            digest_sizes.len() != results.len(),
            "Mismatch in digest sizes and results length"
        );

        digest_sizes
            .into_iter()
            .zip(results.iter_mut())
            .for_each(|(size, result)| {
                *result = if size == 0 { None } else { Some(size) };
            });

        zero_digest_indexes.into_iter().for_each(|index| {
            results[index] = Some(0);
        });

        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let temp_key = OnceCell::new();
        let final_key = self.encode_key(key.borrow());
        //
        // // While the name generation function can be supplied by the user, we need to have the curly
        // // braces in place in order to manage redis' hashing behavior and make sure that the temporary
        // // key name and the final key name are directed to the same cluster node. See
        // // https://redis.io/blog/redis-clustering-best-practices-with-keys/
        // //
        // // The TL;DR is that if we're in cluster mode and the names hash differently, we can't use request
        // // pipelining. By using these braces, we tell redis to only hash the part of the temporary key that's
        // // identical to the final key -- so they will always hash to the same node.
        // //
        // // TODO(caass): the stabilization PR for [`LazyCell`](`std::cell::LazyCell`) has been merged into rust-lang,
        // // so in the next stable release we can use LazyCell::new(|| { ... }) instead.
        let make_temp_name = || {
            format!(
                "temp-{}-{{{}}}",
                (self.temp_name_generator_fn)(),
                &final_key
            )
        };

        let tx = self.get_conn().await.with_cluster_node('x').multi();
        let mut watch_keys: Vec<RedisKey> = Vec::new();

        'outer: loop {
            let mut force_recv = true;

            while force_recv || !reader.is_empty() {
                let chunk = reader
                    .recv()
                    .await
                    .err_tip(|| "Failed to reach chunk in update in redis store")?;

                if chunk.is_empty() {
                    if is_zero_digest(key.borrow()) {
                        return Ok(());
                    }
                    if force_recv {
                        tx.append(&final_key, &chunk[..])
                            .await
                            .map_err(from_redis_err)
                            .err_tip(|| "In RedisStore::update() single chunk")?;
                    }

                    break 'outer;
                }

                tx.append(temp_key.get_or_init(make_temp_name), &chunk[..]);
                force_recv = false;

                // Give other tasks a chance to run to populate the reader's
                // buffer if possible.
                tokio::task::yield_now().await;
            }
            // TODO: Check what existing `abort_on_error=bool` behavior was
            tx.exec(true).await
                .map_err(from_redis_err)
                .err_tip(|| "In RedisStore::update::query_async")?;
        }

        let tx = self.get_conn().await.multi();

        tx.rename(temp_key.get_or_init(make_temp_name), &final_key).await;

        tx.query_async(&mut conn)
            .await
            .map_err(from_redis_err)
            .err_tip(|| "In RedisStore::update")?;

        if let Some(pub_sub_channel) = &self.pub_sub_channel {
            conn.publish(pub_sub_channel, final_key)
                .await
                .map_err(from_redis_err)
                .err_tip(|| "Failed to publish temp key value to configured channel")?
        }

        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        // To follow RBE spec we need to consider any digest's with
        // zero size to be existing.
        if is_zero_digest(key.borrow()) {
            writer
                .send_eof()
                .err_tip(|| "Failed to send zero EOF in redis store get_part")?;
            return Ok(());
        }

        let mut conn = self.get_conn().await;
        if length == Some(0) {
            let exists = conn
                .exists(self.encode_key(key.borrow()))
                .await
                .map_err(from_redis_err)
                .err_tip(|| "In RedisStore::get_part::zero_exists")?;
        }
        //     if !exists {
        //         return Err(make_err!(
        //             Code::NotFound,
        //             "Data not found in Redis store for digest: {key:?}"
        //         ));
        //     }
        //     writer
        //         .send_eof()
        //         .err_tip(|| "Failed to write EOF in redis store get_part")?;
        //     return Ok(());
        // }
        //
        // let mut current_start = isize::try_from(offset)
        //     .err_tip(|| "Cannot convert offset to isize in RedisStore::get_part()")?;
        // let max_length = isize::try_from(length.unwrap_or(isize::MAX as usize))
        //     .err_tip(|| "Cannot convert length to isize in RedisStore::get_part()")?;
        // let end_position = current_start.saturating_add(max_length);
        //
        // loop {
        //     // Note: Redis getrange is inclusive, so we need to subtract 1 from the end.
        //     let current_end =
        //         std::cmp::min(current_start.saturating_add(READ_CHUNK_SIZE), end_position) - 1;
        //     let chunk = conn
        //         .getrange::<_, Bytes>(self.encode_key(key.borrow()), current_start, current_end)
        //         .await
        //         .map_err(from_redis_err)
        //         .err_tip(|| "In RedisStore::get_part::getrange")?;
        //
        //     if chunk.is_empty() {
        //         writer
        //             .send_eof()
        //             .err_tip(|| "Failed to write EOF in redis store get_part")?;
        //         break;
        //     }
        //
        //     // Note: Redis getrange is inclusive, so we need to add 1 to the end.
        //     let was_partial_data = chunk.len() as isize != current_end - current_start + 1;
        //     current_start += chunk.len() as isize;
        //     writer
        //         .send(chunk)
        //         .await
        //         .err_tip(|| "Failed to write data in Redis store")?;
        //
        //     // If we got partial data or the exact requested number of bytes, we are done.
        //     if writer.get_bytes_written() as isize == max_length || was_partial_data {
        //         writer
        //             .send_eof()
        //             .err_tip(|| "Failed to write EOF in redis store get_part")?;
        //
        //         break;
        //     }
        //
        //     error_if!(
        //         writer.get_bytes_written() as isize > max_length,
        //         "Data received exceeds requested length"
        //     );
        // }
        //
        Ok(())
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn std::any::Any + Sync + Send) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send> {
        self
    }

    fn register_metrics(self: Arc<Self>, registry: &mut Registry) {
        registry.register_collector(Box::new(Collector::new(&self)));
    }

    fn register_health(self: Arc<Self>, registry: &mut HealthRegistryBuilder) {
        registry.register_indicator(self);
    }
}

impl MetricsComponent for RedisStore
{
    fn gather_metrics(&self, _c: &mut CollectorState) {}
}

#[async_trait]
impl HealthStatusIndicator for RedisStore
{
    fn get_name(&self) -> &'static str {
        "RedisStore"
    }

    async fn check_health(&self, namespace: Cow<'static, str>) -> HealthStatus {
        StoreDriver::check_health(Pin::new(self), namespace).await
    }
}

fn from_redis_err(call_res: fred::prelude::RedisError) -> Error {
    make_err!(Code::Internal, "Redis Error: {call_res}")
}
