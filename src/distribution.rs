use crate::cache::Cache;

pub mod orchestrator;
pub mod server;

pub trait Orchestrator: Cache {
    fn add_cache(&mut self, name: String);
}
