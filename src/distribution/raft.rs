mod network;
mod storage;

use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use async_raft::{async_trait::async_trait, AppData, AppDataResponse, NodeId};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    api::{GetKeyQueryParams, SetKeyJsonBody},
    cache::{Cache, FullType, GetResult, KeyType, Time, ValueType},
};

use self::network::CacheNetwork;

// --- Message types ---
#[derive(Clone, Debug, Serialize, Deserialize)]
enum CacheRequest {
    GetKey(GetKeyQueryParams),
    SetKey(SetKeyJsonBody),
    Iter(Time),
}
impl AppData for CacheRequest {}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum CacheResponse {
    GetKey(GetResult),
    SetKey(),
    Iter(HashSet<FullType>),
}
impl AppDataResponse for CacheResponse {}

pub struct RaftManager<T>
where
    T: Cache + Default + 'static,
{
    net: Arc<RwLock<CacheNetwork<T>>>,
}

impl<T> RaftManager<T>
where
    T: Cache + Default + 'static,
{
    pub fn new(name: String) -> Self {
        Self {
            net: CacheNetwork::new(name),
        }
    }

    pub async fn add_node(&mut self, id: NodeId) -> Result<()> {
        self.net.write().await.add_node(id).await
    }

    pub async fn remove_node(&mut self, id: NodeId) -> Result<()> {
        self.net.write().await.remove_node(id).await
    }

    pub async fn disconnect_node(&mut self, id: NodeId) -> Result<()> {
        self.net.write().await.disconnect_node(id)
    }

    pub async fn reconnect_node(&mut self, id: NodeId) -> Result<()> {
        self.net.write().await.reconnect_node(id)
    }
}

#[async_trait]
impl<T> Cache for RaftManager<T>
where
    T: Cache + Default + 'static,
{
    async fn get(&mut self, now: Time, k: &KeyType) -> GetResult {
        self.net.write().await.get(now, k).await
    }

    async fn get_all(&mut self, now: Time) -> HashSet<FullType> {
        self.net.write().await.get_all(now).await
    }

    async fn set(&mut self, k: &KeyType, v: ValueType, exp_time: Time) {
        self.net.write().await.set(k, v, exp_time).await
    }
}
