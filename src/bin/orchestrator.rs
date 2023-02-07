use memrafted::{
    cache::{
        local::LocalCache,
        orchestrator::{HashOrchestrator, Orchestrator},
    },
    setup::start_server,
    test::{load_values, LoadConfig},
};
use std::io::Result;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let mut orc = HashOrchestrator::default();
    orc.add_cache("cache_0".into(), LocalCache::default()).await;
    orc.add_cache("cache_1".into(), LocalCache::default()).await;
    orc.add_cache("cache_2".into(), LocalCache::default()).await;

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
