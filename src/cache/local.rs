use std::{collections::HashMap, time::Instant};

use async_raft::async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{Cache, CachedInfo, KeyType, ValueType};

#[derive(Clone, Serialize, Deserialize)]
pub struct LocalCache {
    map: HashMap<String, CachedInfo>,
}

impl LocalCache {}

#[async_trait]
impl Cache for LocalCache {
    async fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    async fn get(&mut self, key: &KeyType) -> Option<ValueType> {
        // Get the value from the hashmap
        let in_cache = self.map.get(key);
        match in_cache {
            Some(value) => {
                // The value was present.
                // Check if the expiration hasn't been reached
                if LocalCache::is_expired(value) {
                    // Expiration reached. Remove the key from the hashmap
                    self.map.remove(key);
                    // Return None as the value wasn't valid anymore
                    None
                } else {
                    Some(value.value.clone())
                }
            }
            None => None,
        }
    }

    async fn set(&mut self, key: &KeyType, value: ValueType, expiration: u64) {
        self.map.insert(
            String::from(key),
            CachedInfo {
                value,
                expiration,
                creation: Instant::now(),
            },
        );
        ()
    }

    fn print_internally(&self) {
        for (key, value) in self.map.iter() {
            println!(
                "Key: {}, value: {}, expiration: {}",
                key, value.value, value.expiration
            );
        }
    }
}
