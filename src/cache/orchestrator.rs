use std::collections::{BTreeMap, HashMap, HashSet};

use async_raft::async_trait::async_trait;
use futures::future::JoinAll;
use tracing::info;

use crate::{
    cache::{Cache, FullType, GetResult, KeyType, Time, ValueType},
    hash::hash,
};

use super::thread::ThreadCache;

#[async_trait]
pub trait Orchestrator: Cache {
    async fn add_cache<T>(&mut self, name: String, cache: T)
    where
        T: Cache + Send + Sync + 'static;
    async fn remove_cache(&mut self, name: String);
}

// Pool that simulates a distributed cache.
// It can contain zero or more cache servers
pub struct HashOrchestrator {
    /// Map of server key with the actual server
    cache_map: HashMap<String, ThreadCache>,

    /// Map between the hashed server key and the server key itself
    ring: BTreeMap<u64, String>,
}

impl Default for HashOrchestrator {
    fn default() -> Self {
        Self {
            cache_map: Default::default(),
            ring: Default::default(),
        }
    }
}

impl HashOrchestrator {
    /// Implementation of "Ketama Consistent Hashing".
    /// The server key is hashed, and inserted in the ring, possibly multiple times.
    /// When we want to cache a key, we hash the key, and find in the ring the first
    /// server with the hashed key greater then the hash of the cached key.
    /// If there isn't one, get the first from the start
    fn get_cache_from_key(&self, hashed_key: u64) -> Option<&String> {
        if self.ring.keys().len() == 0 {
            return None;
        }

        // Find the server associated with the first hash greater then the hashed key
        let first_greater_hash = self.ring.keys().find(|elem| **elem > hashed_key);

        let cache_idx_opt = match first_greater_hash {
            Some(idx) => Some(self.ring.get(idx).unwrap()),
            None => {
                // If there isn't a hash greater then our key, then get the first
                // from the start
                let idx = self.ring.keys().min().unwrap();
                Some(self.ring.get(idx).unwrap())
            }
        };

        if let Some(idx) = cache_idx_opt {
            return Some(idx);
        }
        None
    }
}

#[async_trait]
impl Cache for HashOrchestrator {
    async fn get(&mut self, now: Time, key: &KeyType) -> GetResult {
        let hashed_key = hash(key);
        // Get the index of the server with the hashed name nearest to the hashed key
        let idx_opt = self.get_cache_from_key(hashed_key);
        match idx_opt {
            Some(index) => {
                // Get the memrafted instance of that index
                let cache_opt = self.cache_map.get_mut(&index.clone());

                // If the instance exists, try gettin the value
                if let Some(cache) = cache_opt {
                    cache.get(now, key).await
                } else {
                    GetResult::NotFound
                }
            }
            None => GetResult::NotFound,
        }
    }

    async fn get_all(&mut self, now: Time) -> HashSet<FullType> {
        let futures = self
            .cache_map
            .iter_mut()
            .map(|(_, cache)| cache.get_all(now))
            .collect::<Vec<_>>();
        JoinAll::from_iter(futures)
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    async fn set(&mut self, key: &KeyType, value: ValueType, exp_time: Time) {
        let hashed_key = hash(key);
        // Get the index of the server with the hashed name nearest to the hashed key
        let idx_opt = self.get_cache_from_key(hashed_key);
        match idx_opt {
            Some(index) => {
                info!("Saving {} to server with key {}", hashed_key, index);
                let cache_opt = self.cache_map.get_mut(&index.clone());
                if let Some(cache) = cache_opt {
                    cache.set(key, value, exp_time).await
                }
            }
            None => (),
        }
    }

    async fn drop(&mut self, key: &KeyType) {
        let hashed_key = hash(key);
        // Get the index of the server with the hashed name nearest to the hashed key
        let idx_opt = self.get_cache_from_key(hashed_key);
        match idx_opt {
            Some(index) => {
                info!("Saving {} to server with key {}", hashed_key, index);
                let cache_opt = self.cache_map.get_mut(&index.clone());
                if let Some(cache) = cache_opt {
                    cache.drop(key).await
                }
            }
            None => (),
        }
    }
}

#[async_trait]
impl Orchestrator for HashOrchestrator {
    ///Add a new cache to the pool
    async fn add_cache<T>(&mut self, name: String, cache: T)
    where
        T: Cache + Send + Sync + 'static,
    {
        let mut keys = vec![];
        for i in 0..100 {
            keys.push(format!("{}_{}", name, i));
        }

        let cloned = name.clone();
        // Create a new memrafted instace and map it to the server name
        self.cache_map.insert(name, ThreadCache::start(cache));
        for key in keys {
            self.ring.insert(hash(&key), cloned.clone());
        }
    }

    async fn remove_cache(&mut self, name: String) {
        let mut keys = vec![];
        for i in 0..100 {
            keys.push(format!("{}_{}", name, i));
        }
        self.cache_map.remove(&name);
        for key in keys {
            self.ring.remove(&hash(&key));
        }
    }
}
