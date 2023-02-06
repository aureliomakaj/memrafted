use std::collections::{HashMap, HashSet};

use async_raft::async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::{Cache, FullType, GetResult, KeyType, Time, ValueType};

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct LocalCache {
    map: HashMap<String, FullType>,
}

impl LocalCache {}

#[async_trait]
impl Cache for LocalCache {
    async fn get(&mut self, now: Time, key: &KeyType) -> GetResult {
        // Get the value from the hashmap
        let in_cache = self.map.get(key);
        match in_cache {
            Some(value) => {
                // The value was present.
                // Check if the expiration hasn't been reached
                if value.exp_time < now {
                    // Expiration reached. Remove the key from the hashmap
                    self.map.remove(key);
                    debug!("Key {} expired", key);
                    // Return None as the value wasn't valid anymore
                    GetResult::NotFound
                } else {
                    GetResult::Found(value.clone())
                }
            }
            None => GetResult::NotFound,
        }
    }

    async fn get_all(&mut self, now: Time) -> HashSet<FullType> {
        self.map = self
            .map
            .drain()
            .filter(|(_, info)| -> bool { info.exp_time < now })
            .collect();

        self.map.clone().into_values().collect()
    }

    async fn set(&mut self, key: &KeyType, value: ValueType, exp_time: Time) {
        debug!("Setting {key} := {value}");
        self.map.insert(
            key.to_string(),
            FullType {
                key: key.to_string(),
                value,
                exp_time,
            },
        );
        ()
    }

    async fn drop(&mut self, key: &KeyType) {
        self.map.remove(key);
    }
}
