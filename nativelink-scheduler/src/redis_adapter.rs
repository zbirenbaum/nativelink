use futures::future::{
    try_join, try_join3, try_join_all, BoxFuture, Future, FutureExt, TryFutureExt,
};
use nativelink_error::{make_err, Error};
use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey};
use redis::aio::{ConnectionManager, MultiplexedConnection};
use redis::{
    AsyncCommands, Client, Commands, Connection, FromRedisValue, PubSubCommands, RedisError,
    RedisResult, ToRedisArgs,
};
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::marker::{Send, Sync};
use std::num::NonZeroUsize;

#[derive(Debug, PartialEq, Serialize, Deserialize, FromRedisValue, ToRedisArgs)]
struct Container<T: ToRedisArgs + Serialize + Send + Sync + Clone> {
    inner: T,
}

impl<T: ToRedisArgs + Serialize + Clone + Send + Sync> Container<T> {
    pub async fn new(v: T) -> Self {
        Self { inner: v }
    }
}
pub struct RedisAdapter {
    client: Client,
}

impl RedisAdapter {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    // pub async async fn async_set<K: ToRedisArgs + FromRedisValue + Serialize + Clone + Send + Sync, V: ToRedisArgs + FromRedisValue + Serialize + Clone + Send + Sync>(&self, key: K, value: V) -> RedisResult<u128> {
    //     let connection_manager = &mut self.client.get_multiplexed_tokio_connection().await?;
    //     connection_manager.set(Container::new(key), Container::new(value)).await
    // }

    pub async fn lpush<
        K: ToRedisArgs + Serialize + Send + Sync + Clone,
        V: ToRedisArgs + Serialize + Send + Sync + Clone,
    >(
        &self,
        key: K,
        value: V,
    ) -> Result<u128, Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => con
                .lpush(key, value)
                .await
                .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call lpush")),
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn lpop<
        K: ToRedisArgs + Serialize + Send + Sync + Clone + Debug,
        V: FromRedisValue + DeserializeOwned,
    >(
        &self,
        key: K,
        num: Option<NonZeroUsize>,
    ) -> Result<V, Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => con
                .lpop(key, num)
                .await
                .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call lpop")),
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn get<
        K: ToRedisArgs + Serialize + Send + Sync + Clone,
        V: FromRedisValue + DeserializeOwned,
    >(
        &self,
        key: K,
    ) -> Result<V, Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => con
                .get(key)
                .await
                .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call get")),
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn set<
        K: ToRedisArgs + Serialize + Send + Sync + Clone,
        V: ToRedisArgs + Serialize + Send + Sync + Clone,
    >(
        &self,
        key: K,
        value: V,
    ) -> Result<u128, Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => con
                .set(key, value)
                .await
                .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call set")),
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn hget<
        K: ToRedisArgs + Serialize + Send + Sync + Clone,
        V: FromRedisValue + DeserializeOwned,
    >(
        &self,
        table: String,
        key: K,
    ) -> Result<V, Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => con
                .hget(table, key)
                .await
                .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call get")),
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn del<K: ToRedisArgs + Serialize + Send + Sync + Clone>(
        &self,
        key: K,
    ) -> Result<u128, Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => con
                .del(key)
                .await
                .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call del")),
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn enqueue<K: ToRedisArgs + Serialize + Send + Sync + Clone>(
        &self,
        key: K,
    ) -> Result<(), Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => {
                let mut pipe = redis::pipe();
                pipe.atomic()
                    .hset("queued", &key, &key)
                    .ignore()
                    .query_async(&mut con)
                    .await
                    .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call set"))
            }
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }

    pub async fn make_active<K: ToRedisArgs + Serialize + Send + Sync + Clone>(
        &self,
        key: K,
    ) -> Result<(), Error> {
        match self.client.get_multiplexed_tokio_connection().await {
            Ok(mut con) => {
                let mut pipe = redis::pipe();
                pipe.atomic()
                    .hdel("queued", &key)
                    .hset("active", &key, &key)
                    .ignore()
                    .query_async(&mut con)
                    .await
                    .map_err(|_| make_err!(nativelink_error::Code::Internal, "Failed to call set"))
            }
            Err(_) => Err(make_err!(
                nativelink_error::Code::Internal,
                "Failed to create connection"
            )),
        }
    }
}
