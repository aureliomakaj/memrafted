use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use async_raft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ClientWriteRequest, ClientWriteResponse,
        InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    ChangeConfigError, ClientWriteError, Config, InitializeError, NodeId, Raft, RaftError,
};
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::cache::Cache;

use super::{
    network_handle::CacheNetworkHandle, storage::CacheStorage, CacheRequest, CacheResponse,
};

type CacheRaft<T> =
    Raft<CacheRequest, CacheResponse, RwLock<CacheNetworkHandle<T>>, CacheStorage<T>>;

pub(super) struct CacheNode<T>
where
    T: Cache + Default + Send + Sync + 'static,
{
    id: NodeId,
    net: Arc<RwLock<CacheNetworkHandle<T>>>,
    raft: CacheRaft<T>,
}

impl<T> CacheNode<T>
where
    T: Cache + Default + Send + Sync + 'static,
{
    pub fn new(
        id: NodeId,
        config: Arc<Config>,
        network: Arc<RwLock<CacheNetworkHandle<T>>>,
        storage: Arc<CacheStorage<T>>,
    ) -> Self {
        Self {
            id,
            net: network.clone(),
            raft: CacheRaft::<T>::new(id, config, network, storage),
        }
    }

    pub async fn metrics(&self) {
        info!(">>> METRICS NODE {} <<<<<", self.id);
        let metrics = self.raft.metrics().borrow().clone();
        debug!("{:?}\n", metrics);
    }

    pub async fn disconnect(&self) {
        self.net.write().await.disconnect().await
    }

    pub async fn reconnect(&self) {
        self.net.write().await.reconnect().await
    }

    pub async fn initialize(&self, members: HashSet<NodeId>) -> Result<(), InitializeError> {
        self.raft.initialize(members).await
    }

    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    pub async fn change_membership(
        &self,
        members: HashSet<NodeId>,
    ) -> Result<(), ChangeConfigError> {
        self.raft.change_membership(members).await
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.raft.shutdown().await
    }

    pub async fn client_write(
        &self,
        rpc: ClientWriteRequest<CacheRequest>,
    ) -> Result<ClientWriteResponse<CacheResponse>, ClientWriteError<CacheRequest>> {
        self.raft.client_write(rpc).await
    }

    pub async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<CacheRequest>,
    ) -> Result<AppendEntriesResponse, RaftError> {
        self.raft.append_entries(rpc).await
    }

    pub async fn install_snapshot(
        &self,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.raft.install_snapshot(rpc).await
    }

    pub async fn vote(&self, rpc: VoteRequest) -> Result<VoteResponse, RaftError> {
        self.raft.vote(rpc).await
    }
}
