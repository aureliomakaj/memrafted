use std::collections::{BTreeMap, HashMap};

use log::info;

use crate::{
    cache::{Cache, KeyType, ValueType},
    hash::hash,
};

use super::Orchestrator;

// Pool that simulates a distributed cache.
// It can contain zero or more cache servers
#[derive(Clone)]
pub struct HashOrchestrator<T>
where
    T: Cache,
{
    /// Map of server key with the actual server
    cache_map: HashMap<String, T>,

    /// Map between the hashed server key and the server key itself
    ring: BTreeMap<u64, String>,
}

impl<T> HashOrchestrator<T>
where
    T: Cache + Clone,
{
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

impl<T> Cache for HashOrchestrator<T>
where
    T: Cache + Clone,
{
    fn new() -> Self {
        HashOrchestrator {
            ring: BTreeMap::new(),
            cache_map: HashMap::new(),
        }
    }

    fn get(&mut self, key: &KeyType) -> Option<ValueType> {
        let hashed_key = hash(key);
        let cloned = self.clone();
        // Get the index of the server with the hashed name nearest to the hashed key
        let idx_opt = cloned.get_cache_from_key(hashed_key);
        match idx_opt {
            Some(index) => {
                // Get the memrafted instance of that index
                let cache_opt = self.cache_map.get_mut(index);

                // If the instance exists, try gettin the value
                if let Some(cache) = cache_opt {
                    cache.get(key)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    fn set(&mut self, key: &KeyType, value: ValueType, expiration: u64) {
        let hashed_key = hash(key);
        let cloned = self.clone();
        // Get the index of the server with the hashed name nearest to the hashed key
        let idx_opt = cloned.get_cache_from_key(hashed_key);
        match idx_opt {
            Some(index) => {
                info!("Saving {} to server with key {}", hashed_key, index);
                let cache_opt = self.cache_map.get_mut(index);
                if let Some(cache) = cache_opt {
                    cache.set(key, value, expiration)
                }
            }
            None => (),
        }
    }
}

impl<T> Orchestrator for HashOrchestrator<T>
where
    T: Cache + Clone,
{
    ///Add a new cache to the pool
    fn add_cache(&mut self, name: String) {
        let mut keys = vec![];
        for i in 0..100 {
            keys.push(format!("{}_{}", name, i));
        }
        // Compute the hash of the server name
        let cache_hash = hash(&name);
        let cloned = name.clone();

        info!(
            "Adding server {} with hashed <T> where T: Cachekey {}",
            cloned, cache_hash
        );
        // Create a new memrafted instace and map it to the server name
        self.cache_map.insert(name, T::new());
        for key in keys {
            self.ring.insert(hash(&key), cloned.clone());
        }
        // Map the hash of the server name to the server name
    }
}
