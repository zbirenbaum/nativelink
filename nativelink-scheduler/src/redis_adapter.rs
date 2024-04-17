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
struct Container<T: ToRedisArgs + Serialize> {
    inner: T,
}

impl<T: ToRedisArgs + Serialize> Container<T> {
    pub fn new(v: T) -> Self {
        Self { inner: v }
    }
}
pub struct RedisAdapter {
    client: Client,
}

fn lpush<K: ToRedisArgs + Serialize, V: ToRedisArgs + Serialize>(
    connection: &mut Connection,
    key: Container<K>,
    value: Container<V>,
) -> RedisResult<u128> {
    connection.lpush(key, value.inner)
}

fn lpop<K: ToRedisArgs + Serialize + Debug, V: FromRedisValue + DeserializeOwned>(
    connection: &mut Connection,
    key: Container<K>,
    num: Option<NonZeroUsize>,
) -> RedisResult<V> {
    println!("key: {:?}", key);
    connection.lpop(key, num)
}

fn get<K: ToRedisArgs + Serialize, V: FromRedisValue + DeserializeOwned>(
    connection: &mut Connection,
    key: Container<K>,
) -> RedisResult<V> {
    connection.get(key)
}

fn set<K: ToRedisArgs + Serialize, V: ToRedisArgs + Serialize>(
    connection: &mut Connection,
    key: Container<K>,
    value: Container<V>,
) -> RedisResult<u128> {
    connection.set(key, value.inner)
}

fn del<K: ToRedisArgs + Serialize>(
    connection: &mut Connection,
    key: Container<K>,
) -> RedisResult<u128> {
    connection.del(key)
}

impl RedisAdapter {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Acquire a permit from the open file semaphore and call a raw function.
    #[inline]
    pub fn call_with_connection<F, V>(&self, f: F) -> Result<V, Error>
    where
        F: FnOnce(&mut Connection) -> Result<V, RedisError>,
    {
        let connection = &mut self.client.get_connection().map_err(|_| {
            make_err!(
                nativelink_error::Code::Internal,
                "Error in call_with_connection"
            )
        })?;
        f(connection).map_err(|e| {
            println!("{:?}", e);
            make_err!(
                nativelink_error::Code::Internal,
                "Error in call_with_connection"
            )
        })
    }

    // pub async fn async_set<K: ToRedisArgs + FromRedisValue + Serialize + Clone + Send + Sync, V: ToRedisArgs + FromRedisValue + Serialize + Clone + Send + Sync>(&self, key: K, value: V) -> RedisResult<u128> {
    //     let connection_manager = &mut self.client.get_multiplexed_tokio_connection().await?;
    //     connection_manager.set(Container::new(key), Container::new(value)).await
    // }

    pub fn lpush<K: ToRedisArgs + Serialize, V: ToRedisArgs + Serialize>(
        &self,
        key: K,
        value: V,
    ) -> Result<u128, Error> {
        self.call_with_connection(|connection| {
            lpush(connection, Container::new(key), Container::new(value))
        })
    }

    pub fn lpop<K: ToRedisArgs + Serialize + Debug, V: FromRedisValue + DeserializeOwned>(
        &self,
        key: K,
        num: Option<NonZeroUsize>,
    ) -> Result<V, Error> {
        self.call_with_connection(|connection| lpop(connection, Container::new(key), num))
    }
    pub fn get<K: ToRedisArgs + Serialize, V: FromRedisValue + DeserializeOwned>(
        &self,
        key: K,
    ) -> Result<V, Error> {
        self.call_with_connection(|connection| get(connection, Container::new(key)))
    }

    pub fn set<K: ToRedisArgs + Serialize, V: ToRedisArgs + Serialize>(
        &self,
        key: K,
        value: V,
    ) -> Result<u128, Error> {
        self.call_with_connection(|connection| {
            set(connection, Container::new(key), Container::new(value))
        })
    }

    pub fn del<K: ToRedisArgs + Serialize>(&self, key: K) -> Result<u128, Error> {
        self.call_with_connection(|connection| del(connection, Container::new(key)))
    }
}
