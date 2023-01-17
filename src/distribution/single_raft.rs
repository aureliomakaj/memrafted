use core::panic;
use std::{collections::{HashMap, BTreeMap}, sync::{mpsc::{Receiver, Sender, channel}, Arc, Mutex}, thread::{JoinHandle, sleep}, time::Duration};

use async_raft::{AppData, AppDataResponse, NodeId, RaftNetwork};
use log::info;
use serde::{Serialize, Deserialize};

use crate::{cache::{CachedInfo, Cache, local::LocalCache, ValueType, KeyType}, hash::hash};

use super::{server::{GetKeyQueryParams, SetKeyJsonBody}, Orchestrator};

pub enum NetworkRequest {
    GetKey(GetKeyQueryParams),
    SetKey(SetKeyJsonBody),

}

pub type NetworkResponse = Option<String>;


/// Simulation of a network of cache servers.
/// Each node is a thead and is mapped to an identifier.
/// The communication happens using two channels, one for the request and one for the response.
/// Sender<NetworkRequest> is used to send the network request (and is owned by Network), that is received by the thread
/// through Receiver<NetworkRequest> (owned by the thread). 
/// The thread elaborate the result and send it back through Sender<NetworkResponse> (owned by the thread), and the value
/// is received by the Network struct through Receiver<NetworkResponse> 
pub struct Network<T>
where
    T: NetworkNode
{
    pub nodes: HashMap<NodeId, T>,
    pub channels: HashMap<NodeId, (Sender<NetworkRequest>, Receiver<NetworkResponse>)>
}

impl<T> Network<T>
where
    T: NetworkNode
{
    pub fn new() -> Network<T> {
        Network { 
            nodes: HashMap::new(), 
            channels: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, id: NodeId) {
        let (req_send, req_receive ) = channel::<NetworkRequest>();
        let (resp_send, resp_receive) = channel::<NetworkResponse>();
        let node = NetworkNode::new(id, req_receive, resp_send);
        self.nodes.insert(id, node);
        self.channels.insert(id, (req_send, resp_receive));
    }

    pub fn send(&self, target: NodeId, req: NetworkRequest) -> NetworkResponse {
        let sender = self.channels.get(&target);
        match sender {
            Some(ch) => {
                (*ch).0.send(req).unwrap();
                (*ch).1.recv().unwrap()
            },
            None => panic!("Not a valid target")
        }
    }
} 

pub trait NetworkNode {
    fn new(id: NodeId, req_channel: Receiver<NetworkRequest>, resp_channel: Sender<NetworkResponse>) -> Self;
}

pub struct RaftNode {
    id: NodeId,
    thread: JoinHandle<()>
}

impl NetworkNode for RaftNode {
    fn new(id: NodeId, req_channel: Receiver<NetworkRequest>, resp_channel: Sender<NetworkResponse>) -> Self {
        let thread = std::thread::spawn(move || {
            let mut cache = LocalCache::new();
            while let Ok(request) = req_channel.recv() {
                match request {
                    NetworkRequest::GetKey(query_params) => {
                        let res = cache.get(&query_params.key);
                        info!("Getting key {} from node {}", query_params.key, id.clone());
                        resp_channel.send(res).unwrap();                        
                    },
                    NetworkRequest::SetKey(json_body) => {
                        let res = cache.set(&json_body.key, json_body.value, json_body.expiration);
                        info!("Setting key {} to node {}", json_body.key, id.clone());
                        resp_channel.send(None).unwrap();
                    }
                }
            }

            panic!("Panicked from node {}", id)
        });      
        RaftNode { id, thread }
    }
}



pub struct RaftOrchestrator<T>
where
    T: NetworkNode
{
    pub last_node: NodeId,
    pub network: Network<T>,
    /// Map of server key with the actual server
    // cache_map: HashMap<String, T>,
    pub nodes: HashMap<String, NodeId>,
    /// Map between the hashed server key and the server key itself
    pub ring: BTreeMap<u64, NodeId>,
}

impl<T> RaftOrchestrator<T>
where
    T: NetworkNode
{
    /// Implementation of "Ketama Consistent Hashing".
    /// The server key is hashed, and inserted in the ring, possibly multiple times.
    /// When we want to cache a key, we hash the key, and find in the ring the first
    /// server with the hashed key greater then the hash of the cached key.
    /// If there isn't one, get the first from the start
    fn get_target_node(&self, hashed_key: u64) -> Option<NodeId> {
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
            return Some(*idx);
        }
        None
    }
}

impl<T> Cache for RaftOrchestrator<T> 
where
    T: NetworkNode
{
    fn new() -> Self {
        RaftOrchestrator {
            last_node: 0,
            network: Network::new(),
            nodes: HashMap::new(),
            ring: BTreeMap::new(),
        }
    }

    fn get(&mut self, key: &KeyType) -> Option<ValueType> {
        let hashed_key = hash(key);
        let target = self.get_target_node(hashed_key);
        match target {
            Some(t) => {
                let req = GetKeyQueryParams { key: key.clone() };
                self.network.send(t, NetworkRequest::GetKey(req))
            },
            None => None
        }
    }

    fn set(&mut self, key: &KeyType, value: ValueType, expiration: u64) {
        let hashed_key = hash(key);
        let target = self.get_target_node(hashed_key);
        if let Some(t) = target {
            let req = SetKeyJsonBody { key: key.clone(), value, expiration };
            self.network.send(t, NetworkRequest::SetKey(req));
        }
    }
}

impl<T> Orchestrator for RaftOrchestrator<T>
where
    T: NetworkNode
{
    ///Add a new cache to the pool
    fn add_cache(&mut self, name: String) {
        let node_id = self.last_node + 1;
        self.network.add_node(node_id);
        self.nodes.insert(name.clone(), node_id);

        let mut keys = vec![];
        for i in 0..100 {
            keys.push(format!("{}_{}", name, i));
        }
        // Compute the hash of the server name
        let cache_hash = hash(&name);

        info!(
            "Adding server {} with hashed <T> where T: Cachekey {}",
            name, cache_hash
        );

        for key in keys {
            self.ring.insert(hash(&key), node_id);
        }
    }
}