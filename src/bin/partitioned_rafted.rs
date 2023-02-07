use memrafted::{
    cache::{
        local::LocalCache,
        orchestrator::{HashOrchestrator, Orchestrator},
    },
    raft::RaftManager,
    setup::start_server,
    test::{load_values, LoadConfig},
};
use std::io::Result;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let mut net0 = RaftManager::new("net_0".into());
    net0.add_node(0, LocalCache::default()).await.unwrap();
    net0.add_node(1, LocalCache::default()).await.unwrap();
    net0.add_node(2, LocalCache::default()).await.unwrap();

    let mut net1 = RaftManager::new("net_1".into());
    net1.add_node(0 + 3, LocalCache::default()).await.unwrap();
    net1.add_node(1 + 3, LocalCache::default()).await.unwrap();
    net1.add_node(2 + 3, LocalCache::default()).await.unwrap();

    let mut net2 = RaftManager::new("net_2".into());
    net2.add_node(0 + 6, LocalCache::default()).await.unwrap();
    net2.add_node(1 + 6, LocalCache::default()).await.unwrap();
    net2.add_node(2 + 6, LocalCache::default()).await.unwrap();

    let mut orc = HashOrchestrator::default();
    orc.add_cache("net_0".into(), net0).await;
    orc.add_cache("net_1".into(), net1).await;
    orc.add_cache("net_2".into(), net2).await;

    let addrs = "127.0.0.1:8081";

    let server_thread = tokio::spawn(async move { start_server(orc, addrs).await.unwrap().await });

    let http_addrs = "http://127.0.0.1:8081";
    let lcfg = LoadConfig {
        workers_n: 8,
        keys_n: 100,
        padding: std::iter::repeat("*").take(2).collect::<String>(),
    };

    info!("Loading values...");
    load_values(&String::from(http_addrs), &lcfg).await;

    info!("Starting server...");
    info!("Server ready. Listening on {:#?}", addrs);

    server_thread.await.unwrap()
}
