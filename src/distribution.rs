use crate::cache::Cache;

pub mod orchestrator;
pub mod server;
pub mod single_raft;

pub trait Orchestrator: Cache {
    fn add_cache(&mut self, name: String);
}
