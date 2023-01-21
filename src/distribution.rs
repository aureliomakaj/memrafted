use async_raft::async_trait::async_trait;

use crate::cache::Cache;

pub mod orchestrator;
pub mod server;
pub mod single_raft;

#[async_trait]
pub trait Orchestrator: Cache {
    async fn add_cache(&mut self, name: String);
}
