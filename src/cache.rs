pub mod local;

use std::time::Instant;

use serde::{Serialize, Deserialize};

/// Type of the key in the cache
pub type KeyType = String;
/// Type that can be stored in the cache
pub type ValueType = String;

/// Struct that describe a cached entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedInfo {
    /// The value stored
    value: ValueType,

    /// Expiration time in seconds. How long the data should be available
    expiration: u64,

    /// Time of when the value was inserted or updated
    #[serde(with = "serde_millis")]
    creation: Instant,
}

pub trait Cache {
    /// Creates new empty cache
    fn new() -> Self;

    /// Get the cached valued corresponding to key `k`, `None` if not present or expired
    fn get(&mut self, k: &KeyType) -> Option<ValueType>;

    /// Set the value `v` in the cache for key `k` and stores it for `duration` seconds
    fn set(&mut self, k: &KeyType, v: ValueType, duration: u64);

    /// Remove the cached calue corresponding to key `k`
    // fn drop(&mut self, k: &CacheKeyType);

    /// Wheter `value` is expired or not
    fn is_expired(value: &CachedInfo) -> bool {
        let elapsed = value.creation.elapsed().as_secs();
        elapsed > value.expiration
    }
}
