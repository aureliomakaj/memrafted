pub mod local;
pub mod orchestrator;
pub mod thread;

use std::collections::HashSet;

use async_raft::async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Type of the key in the cache
pub type KeyType = String;
/// Type that can be stored in the cache
pub type ValueType = String;
// Client representation of time
pub type Time = u64;

/// Struct that describe a cached entity
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FullType {
    /// The key of the entry
    pub key: KeyType,

    /// The value stored
    pub value: ValueType,

    /// Expiration time. Until when the data should be available
    pub exp_time: Time,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GetResult {
    Found(FullType),
    NotFound,
}

#[async_trait]
pub trait Cache {
    /// Get the cached valued corresponding to key `key`, `None` if not present or expired
    async fn get(&mut self, &now: Time, key: &KeyType) -> GetResult;

    /// Get all the key-value pairs in an hash_set
    async fn get_all(&mut self, &now: Time) -> HashSet<FullType>;

    /// Set the `value` in the cache for `key` and stores it until `exp_time`
    async fn set(&mut self, key: &KeyType, value: ValueType, exp_time: Time);

    /// Remove the cached calue corresponding to key `key`
    async fn drop(&mut self, key: &KeyType);
}
