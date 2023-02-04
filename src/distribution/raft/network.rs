use core::panic;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Weak},
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use async_raft::{
    async_trait::async_trait,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ClientWriteRequest, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    Config, NodeId, Raft, RaftError, RaftNetwork,
};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::{
    api::{GetKeyQueryParams, SetKeyJsonBody},
    cache::{Cache, FullType, KeyType, ValueType},
};

use super::{storage::CacheStorage, CacheRequest, CacheResponse};

type CacheNode<T> = Raft<CacheRequest, CacheResponse, RwLock<CacheNetwork<T>>, CacheStorage<T>>;

pub(super) struct CacheNetwork<T>
where
    T: Cache + Default + 'static,
{
    name: String,
    nodes: HashMap<NodeId, (bool, CacheNode<T>)>,
    config: Arc<Config>,
    weak_self: Weak<RwLock<Self>>,
}

impl<T> CacheNetwork<T>
where
    T: Cache + Default + 'static,
{
    pub(crate) fn new(name: String) -> Arc<RwLock<Self>> {
        Arc::new_cyclic(|ws| {
            RwLock::new(Self {
                name: name.clone(),
                nodes: HashMap::new(),
                config: Arc::new(
                    Config::build(name)
                        .election_timeout_min(20 * 1000) // 20 secs
                        .election_timeout_max(50 * 1000) // 50 secs
                        .heartbeat_interval(5 * 1000)
                        .validate()
                        .unwrap()
                ),
                weak_self: ws.clone(),
            })
        })
    }

    pub(crate) async fn add_node(&mut self, id: NodeId) -> Result<()> {
        if self.nodes.contains_key(&id) {
            warn!(
                "Attempting to insert node {} in network {}. Node {} already exists!",
                id, self.name, id
            );
            return Err(anyhow!("Node {} already exists!", id));
        }
        let config = self.config.clone();
        let network = self.weak_self.upgrade().unwrap();
        let storage = Arc::new(CacheStorage::new(id));
        let node = CacheNode::new(id, config, network, storage);

        if self.nodes.is_empty() {
            let mut members = HashSet::new();
            members.insert(id);
            node.initialize(members).await.unwrap_or_default();
        }else{
            node.add_non_voter(id).await.unwrap_or_default();
            let mut members = HashSet::new();
            for (id_node, _) in self.nodes.iter() {
                members.insert(*id_node);
            }
            members.insert(id);
            let leader_opt = self.get_leader().await;
            if let Some(leader) = leader_opt {
                if let Some((true, leader_node)) = self.nodes.get(&leader) {
                    leader_node.change_membership(members).await.unwrap();
                }else{
                    panic!("Leader not available");
                }
            }else{
                panic!("Cannot add node {} to network {}. Leader not found", id, self.name);
            }
        }
        self.nodes.insert(id, (true, node));
        Ok(())
    }

    pub(crate) async fn remove_node(&mut self, id: NodeId) -> Result<()> {
        if !self.nodes.contains_key(&id) {
            warn!(
                "Attempting to remove node {} from network {}. Node {} does not exists!",
                id, self.name, id
            );
            return Err(anyhow!("Node {} does not exists!", id));
        }

        let (_, n) = self.nodes.remove(&id).unwrap();
        n.shutdown().await?;
        Ok(())
    }

    pub(crate) fn disconnect_node(&mut self, id: NodeId) -> Result<()> {
        match self.nodes.get(&id) {
            Some((true, _)) => {
                info!("Disconnecting node {} from network {}", id, self.name);
                self.nodes.entry(id).and_modify(|(c, _)| *c = false);
                Ok(())
            }
            Some((false, _)) => {
                info!(
                    "Node {} already disconnected from network {}",
                    id, self.name
                );
                Ok(())
            }
            None => {
                warn!(
                    "Attempting to disconnect node {} from network {}. Node {} doesn't exists!",
                    id, self.name, id
                );
                Err(anyhow!("Node {} doesn't exists!", id))
            }
        }
    }

    pub(crate) fn reconnect_node(&mut self, id: NodeId) -> Result<()> {
        match self.nodes.get(&id) {
            Some((false, _)) => {
                info!("Reconnecting node {} to network {}", id, self.name);
                self.nodes.entry(id).and_modify(|(c, _)| *c = true);
                Ok(())
            }
            Some((true, _)) => {
                info!("Node {} already connected to network {}", id, self.name);
                Ok(())
            }
            None => {
                warn!(
                    "Attempting to reconnect node {} to network {}. Node {} doesn't exists!",
                    id, self.name, id
                );
                Err(anyhow!("Node {} doesn't exists!", id))
            }
        }
    }

    async fn get_leader(&self) -> Option<NodeId> {
        let n = self
            .nodes
            .iter()
            .find_map(|(_, (c, n))| if *c { Some(n) } else { None });
        match n {
            Some(n) => n.current_leader().await,
            None => {
                warn!("Unable to get leader of netwrok {}", self.name);
                None
            }
        }
    }

    async fn write_to(&mut self, req: CacheRequest, to: NodeId) -> Result<CacheResponse> {
        let mut node = self.nodes.get(&to);
        while let Some((true, n)) = node {
            let req = ClientWriteRequest::new(req.clone());
            match n.client_write(req).await {
                Ok(resp) => return Ok(resp.data),
                Err(e) => match e {
                    async_raft::ClientWriteError::ForwardToLeader(_, Some(l)) => {
                        node = self.nodes.get(&l)
                    }
                    _ => (),
                },
            }
        }
        Err(anyhow!("Write error!"))
    }
}

#[async_trait]
impl<T> RaftNetwork<CacheRequest> for RwLock<CacheNetwork<T>>
where
    T: Cache + Default + 'static,
{
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<CacheRequest>,
    ) -> Result<AppendEntriesResponse> {
        info!("Append entries");
        match self.read().await.nodes.get(&target) {
            Some((true, n)) => {
                info!("AppendEntriesRequest for node {}", target);
                match n.append_entries(rpc).await {
                    Ok(o) => Ok(o),
                    Err(e) => match e {
                        RaftError::RaftStorage(_) => Err(anyhow!("Raft Storage error")),
                        RaftError::RaftNetwork(_) => Err(anyhow!("Raft Network error")),
                        RaftError::ShuttingDown => Err(anyhow!("Shuting down error")),
                        _ => panic!("This error shouldn't exists"),
                    },
                }
            }
            _ => {
                warn!("Node {} does not exists.", target);
                Err(anyhow!("Node {} does not exists.", target))
            }
        }
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        match self.read().await.nodes.get(&target) {
            Some((true, n)) => {
                info!("InstallSnapshotRequest for node {}", target);
                match n.install_snapshot(rpc).await {
                    Ok(o) => Ok(o),
                    Err(e) => match e {
                        RaftError::RaftStorage(_) => Err(anyhow!("Raft Storage error")),
                        RaftError::RaftNetwork(_) => Err(anyhow!("Raft Network error")),
                        RaftError::ShuttingDown => Err(anyhow!("Shuting down error")),
                        _ => panic!("This error shouldn't exists"),
                    },
                }
            }
            _ => {
                warn!("Node {} does not exists.", target);
                Result::Err(anyhow!("Node {} does not exists.", target))
            }
        }
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        match self.read().await.nodes.get(&target) {
            Some((true, n)) => {
                info!("VoteRequest for node {}", target);
                match n.vote(rpc).await {
                    Ok(o) => Ok(o),
                    Err(e) => match e {
                        RaftError::RaftStorage(_) => Err(anyhow!("Raft Storage error")),
                        RaftError::RaftNetwork(_) => Err(anyhow!("Raft Network error")),
                        RaftError::ShuttingDown => Err(anyhow!("Shuting down error")),
                        _ => panic!("This error shouldn't exists"),
                    },
                }
            }
            _ => {
                warn!("Node {} does not exists.", target);
                Result::Err(anyhow!("Node {} does not exists.", target))
            }
        }
    }
}

#[async_trait]
impl<T> Cache for CacheNetwork<T>
where
    T: Cache + Default + 'static,
{
    async fn get(&mut self, k: &KeyType) -> Option<ValueType> {
        let req = CacheRequest::GetKey(GetKeyQueryParams { key: k.to_string() });
        match self.get_leader().await {
            Some(l) => match self.write_to(req, l).await {
                Ok(CacheResponse::GetKey(vo)) => vo,
                _ => None,
            },
            None => None,
        }
    }

    async fn set(&mut self, k: &KeyType, value: ValueType, exp_time: SystemTime) {
        let req = CacheRequest::SetKey(SetKeyJsonBody {
            key: k.to_string(),
            value,
            exp_time,
        });
        match self.get_leader().await {
            Some(l) => {
                self.write_to(req, l)
                    .await
                    .unwrap_or(CacheResponse::SetKey());
            }
            None => (),
        };
    }

    async fn value_set(&mut self) -> HashSet<FullType> {
        let req = CacheRequest::Iter();
        match self.get_leader().await {
            Some(l) => match self.write_to(req, l).await {
                Ok(CacheResponse::Iter(m)) => m,
                _ => HashSet::new(),
            },
            None => HashSet::new(),
        }
    }
}
