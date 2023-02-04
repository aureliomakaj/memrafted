pub mod local;

use std::{collections::HashSet, time::SystemTime};

use async_raft::async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Type of the key in the cache
pub type KeyType = String;
/// Type that can be stored in the cache
pub type ValueType = String;

/// Struct that describe a cached entity
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FullType {
    /// The key of the entry
    pub key: KeyType,

    /// The value stored
    pub value: ValueType,

    /// Expiration time. Until when the data should be available
    #[serde(with = "serde_millis")]
    pub exp_time: SystemTime,
}

#[async_trait]
pub trait Cache: Send + Sync {
    /// Get the cached valued corresponding to key `k`, `None` if not present or expired
    async fn get(&mut self, k: &KeyType) -> Option<ValueType>;

    /// Set the value `v` in the cache for key `k` and stores it until `expire_time`
    async fn set(&mut self, k: &KeyType, v: ValueType, expire_time: SystemTime);

    /// Remove the cached calue corresponding to key `k`
    // fn drop(&mut self, k: &CacheKeyType);

    async fn value_set(&mut self) -> HashSet<FullType>;

    /// Wheter `entry` is expired or not
    fn is_expired(entry: &FullType) -> bool {
        SystemTime::now() > entry.exp_time
    }
}
