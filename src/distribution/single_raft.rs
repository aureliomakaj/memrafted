use std::{collections::HashMap, sync::{mpsc::{Receiver, Sender, channel}, Arc, Mutex}, thread::{JoinHandle, self}};

use async_raft::{AppData, AppDataResponse, NodeId, RaftNetwork};
use serde::{Serialize, Deserialize};

use crate::cache::{CachedInfo, Cache, local::LocalCache};

use super::{server::{GetKeyQueryParams, SetKeyJsonBody}, Orchestrator};

pub enum NetworkRequest {
    GetKey(GetKeyQueryParams),
    SetKey(SetKeyJsonBody)
}

pub type NetworkResponse = Option<String>;

/// Simulation of a network of cache servers.
/// Each node is a thead and is mapped to an identifier.
/// The communication happens using two channels, one for the request and one for the response.
/// Sender<NetworkRequest> is used to send the network request (and is owned by Network), that is received by the thread
/// through Receiver<NetworkRequest> (owned by the thread). 
/// The thread elaborate the result and send it back through Sender<NetworkResponse> (owned by the thread), and the value
/// is received by the Network struct through Receiver<NetworkResponse> 
struct Network {
    nodes: HashMap<NodeId, NetworkNode>,
    channels: HashMap<NodeId, (Sender<NetworkRequest>, Receiver<NetworkResponse>)>
}

impl Network {
    pub fn new(ids: Vec<NodeId>) -> Network {
        let mut nodes = HashMap::new();
        let mut channels = HashMap::new();
        for n in ids.iter() {
            let (req_send, req_receive ) = channel::<NetworkRequest>();
            let (resp_send, resp_receive) = channel::<NetworkResponse>();
            let node = NetworkNode::new(n.clone(), req_receive, resp_send);
            nodes.insert(n.clone(), node);
            channels.insert(n.clone(), (req_send, resp_receive));
        }

        Network { nodes, channels }
    }
} 


pub struct NetworkNode {
    id: NodeId,
    thread: JoinHandle<()>
}

impl NetworkNode {
    pub fn new(id: NodeId, req_channel: Receiver<NetworkRequest>, resp_channel: Sender<NetworkResponse>) -> NetworkNode {
        let thread = thread::spawn(move || {
            let mut cache = LocalCache::new();
            while let Ok(request) = req_channel.recv() {
                match request {
                    NetworkRequest::GetKey(query_params) => {
                        let res = cache.get(&query_params.key);
                        resp_channel.send(res).unwrap();                        
                    },
                    NetworkRequest::SetKey(json_body) => {
                        let res = cache.set(&json_body.key, json_body.value, json_body.expiration);
                        resp_channel.send(None).unwrap();
                    }
                }
            }

            panic!("Panicked from node {}", id)
        });      
        NetworkNode { id, thread }
    }

}
