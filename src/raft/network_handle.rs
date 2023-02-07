use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use anyhow::{anyhow, Result};
use async_raft::{
    async_trait::async_trait,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    NodeId, RaftNetwork,
};
use tokio::{sync::RwLock, time::sleep};
use tracing::warn;

use crate::cache::Cache;

use super::{network::CacheNetwork, CacheRequest};

pub(super) struct CacheNetworkHandle<T>
where
    T: Cache + Default + Send + Sync + 'static,
{
    id: NodeId,
    connected: bool,
    net: Weak<RwLock<CacheNetwork<T>>>,
}

impl<T> CacheNetworkHandle<T>
where
    T: Cache + Default + Send + Sync + 'static,
{
    pub fn new(id: NodeId, net: Weak<RwLock<CacheNetwork<T>>>) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self {
            id,
            connected: true,
            net,
        }))
    }

    pub async fn disconnect(&mut self) {
        self.connected = false
    }

    pub async fn reconnect(&mut self) {
        self.connected = true
    }
}

#[async_trait]
impl<T> RaftNetwork<CacheRequest> for RwLock<CacheNetworkHandle<T>>
where
    T: Cache + Default + Send + Sync + 'static,
{
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<CacheRequest>,
    ) -> Result<AppendEntriesResponse> {
        if self.read().await.connected {
            self.read()
                .await
                .net
                .upgrade()
                .unwrap()
                .read()
                .await
                .append_entries(target, rpc)
                .await
        } else {
            sleep(Duration::from_secs(1)).await;
            let err = format!(
                "Node {} cannot reach network {}",
                self.read().await.id,
                self.read().await.net.upgrade().unwrap().read().await.name()
            );
            warn!(err);
            Err(anyhow!(err))
        }
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        if self.read().await.connected {
            self.read()
                .await
                .net
                .upgrade()
                .unwrap()
                .read()
                .await
                .install_snapshot(target, rpc)
                .await
        } else {
            let err = format!(
                "Node {} cannot reach network {}",
                self.read().await.id,
                self.read().await.net.upgrade().unwrap().read().await.name()
            );
            warn!(err);
            Err(anyhow!(err))
        }
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        if self.read().await.connected {
            self.read()
                .await
                .net
                .upgrade()
                .unwrap()
                .read()
                .await
                .vote(target, rpc)
                .await
        } else {
            let err = format!(
                "Node {} cannot reach network {}",
                self.read().await.id,
                self.read().await.net.upgrade().unwrap().read().await.name()
            );
            warn!(err);
            Err(anyhow!(err))
        }
    }
}
