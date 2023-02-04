mod network;
mod storage;

use std::{collections::HashSet, sync::Arc, time::SystemTime};

use anyhow::Result;
use async_raft::{async_trait::async_trait, AppData, AppDataResponse, NodeId};
use serde::{Deserialize, Serialize};

use crate::{
    api::{GetKeyQueryParams, SetKeyJsonBody},
    cache::{Cache, FullType, KeyType, ValueType},
};

use self::network::CacheNetwork;

// --- Message types ---
#[derive(Clone, Debug, Serialize, Deserialize)]
enum CacheRequest {
    GetKey(GetKeyQueryParams),
    SetKey(SetKeyJsonBody),
    Iter(),
}
impl AppData for CacheRequest {}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum CacheResponse {
    GetKey(Option<ValueType>),
    SetKey(),
    Iter(HashSet<FullType>),
}
impl AppDataResponse for CacheResponse {}

pub struct RaftManager<T>
where
    T: Cache + Default + 'static,
{
    net: Arc<CacheNetwork<T>>,
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
        Arc::get_mut(&mut self.net).unwrap().add_node(id).await
    }

    pub async fn remove_node(&mut self, id: NodeId) -> Result<()> {
        Arc::get_mut(&mut self.net).unwrap().remove_node(id).await
    }

    pub fn disconnect_node(&mut self, id: NodeId) -> Result<()> {
        Arc::get_mut(&mut self.net).unwrap().disconnect_node(id)
    }

    pub fn reconnect_node(&mut self, id: NodeId) -> Result<()> {
        Arc::get_mut(&mut self.net).unwrap().reconnect_node(id)
    }
}

#[async_trait]
impl<T> Cache for RaftManager<T>
where
    T: Cache + Default + 'static,
{
    async fn get(&mut self, k: &KeyType) -> Option<ValueType> {
        Arc::get_mut(&mut self.net).unwrap().get(k).await
    }

    async fn set(&mut self, k: &KeyType, v: ValueType, exp_time: SystemTime) {
        Arc::get_mut(&mut self.net)
            .unwrap()
            .set(k, v, exp_time)
            .await
    }

    async fn value_set(&mut self) -> HashSet<FullType> {
        Arc::get_mut(&mut self.net).unwrap().value_set().await
    }
}
