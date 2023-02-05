use core::panic;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Weak},
};

use anyhow::{anyhow, Result};
use async_raft::{
    async_trait::async_trait,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ClientWriteRequest, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    ChangeConfigError, Config, NodeId, Raft, RaftError, RaftNetwork,
};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::{
    api::{GetKeyQueryParams, SetKeyJsonBody, DropKeyQueryParams},
    cache::{Cache, FullType, GetResult, KeyType, Time, ValueType},
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
                        .unwrap(),
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

        let mut members: HashSet<_> = self.nodes.iter().map(|(i, _)| *i).collect();
        members.insert(id);
        if members.len() == 1 {
            node.initialize(members.clone()).await.unwrap_or_default();
            self.nodes.insert(id, (true, node));
            return Ok(());
        }

        let leader_opt = self.get_leader().await;
        let mut res = Err(ChangeConfigError::NodeNotLeader(leader_opt));

        while let Err(ChangeConfigError::NodeNotLeader(Some(l))) = res {
            match self.nodes.get(&l) {
                Some((true, ln)) => {
                    res = ln.add_non_voter(id).await;
                    res = ln.change_membership(members.clone()).await;
                }
                Some((false, _)) => {
                    return Err(anyhow!(
                        "Suggested leader {} is not reachable in network {}",
                        l,
                        self.name
                    ))
                }
                None => {
                    return Err(anyhow!(
                        "Suggested leader {} does not exists in network {}",
                        l,
                        self.name
                    ))
                }
            }
        }
        
        self.nodes.insert(id, (true, node));
        match res {
            Ok(_) => Ok(()),
            Err(e) => match e {
                ChangeConfigError::RaftError(_) => todo!(),
                ChangeConfigError::ConfigChangeInProgress => Err(anyhow!(
                    "The cluster {} is already undergoing a configuration change.",
                    self.name
                )),
                ChangeConfigError::InoperableConfig => Err(anyhow!(
                    "Adding node {} would leave the cluster {} in an inoperable state.",
                    id,
                    self.name
                )),
                ChangeConfigError::Noop => Ok(()),
                e => Err(anyhow!(
                    "Unable to add node {} to network {}. {}",
                    id,
                    self.name,
                    e
                )),
            },
        }
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
    async fn get(&mut self, now: Time, k: &KeyType) -> GetResult {
        let req = CacheRequest::GetKey(GetKeyQueryParams {
            key: k.to_string(),
            now,
        });
        match self.get_leader().await {
            Some(l) => match self.write_to(req, l).await {
                Ok(CacheResponse::GetKey(vo)) => vo,
                _ => GetResult::NotFound,
            },
            None => GetResult::NotFound,
        }
    }

    async fn get_all(&mut self, now: Time) -> HashSet<FullType> {
        let req = CacheRequest::Iter(now);
        match self.get_leader().await {
            Some(l) => match self.write_to(req, l).await {
                Ok(CacheResponse::Iter(m)) => m,
                _ => HashSet::new(),
            },
            None => HashSet::new(),
        }
    }

    async fn set(&mut self, k: &KeyType, value: ValueType, exp_time: Time) {
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

    async fn drop(&mut self, key: &KeyType) {
        let req = CacheRequest::DropKey(DropKeyQueryParams {
            key: key.to_string()
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
}
