use std::{
    collections::{HashMap, HashSet},
    time::SystemTime,
};

use async_raft::async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::{Cache, FullType, KeyType, ValueType};

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct LocalCache {
    map: HashMap<String, FullType>,
}

impl LocalCache {}

#[async_trait]
impl Cache for LocalCache {
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
                    info!("Key {} expired", key);
                    // Return None as the value wasn't valid anymore
                    None
                } else {
                    Some(value.value.clone())
                }
            }
            None => None,
        }
    }

    async fn set(&mut self, key: &KeyType, value: ValueType, exp_time: SystemTime) {
        info!("Setting {key} := {value}");
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

    async fn value_set(&mut self) -> HashSet<FullType> {
        let now = SystemTime::now();

        self.map = self
            .map
            .drain()
            .filter(|(_, info)| -> bool { info.exp_time < now })
            .collect();

        self.map.clone().into_values().collect()
    }
}
