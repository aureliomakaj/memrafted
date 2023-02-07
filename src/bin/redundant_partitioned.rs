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

    let mut orc0 = HashOrchestrator::default();
    orc0.add_cache("cache_0_0".into(), LocalCache::default())
        .await;
    orc0.add_cache("cache_0_1".into(), LocalCache::default())
        .await;
    orc0.add_cache("cache_0_2".into(), LocalCache::default())
        .await;

    let mut orc1 = HashOrchestrator::default();
    orc1.add_cache("cache_1_0".into(), LocalCache::default())
        .await;
    orc1.add_cache("cache_1_1".into(), LocalCache::default())
        .await;
    orc1.add_cache("cache_1_2".into(), LocalCache::default())
        .await;

    let mut orc2 = HashOrchestrator::default();
    orc2.add_cache("cache_2_0".into(), LocalCache::default())
        .await;
    orc2.add_cache("cache_2_1".into(), LocalCache::default())
        .await;
    orc2.add_cache("cache_2_2".into(), LocalCache::default())
        .await;

    let mut net0 = RaftManager::new("net_0".into());
    net0.add_node(0, orc0).await.unwrap();
    net0.add_node(1, orc1).await.unwrap();
    net0.add_node(2, orc2).await.unwrap();

    let addrs = "127.0.0.1:8081";

    let server_thread = tokio::spawn(async move { start_server(net0, addrs).await.unwrap().await });

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
