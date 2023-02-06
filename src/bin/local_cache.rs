use memrafted::{
    cache::local::LocalCache,
    setup::start_server,
    test::{load_values, run_test, LoadConfig},
};
use std::{io::Result, time::Duration};
use tokio::time::sleep;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    //std::env::set_var("RUST_BACKTRACE", "1");
    env_logger::init();

    let cache = LocalCache::default();
    let addrs = "127.0.0.1:8081";
    let http_addrs = "http://127.0.0.1:8081";

    let server_thread =
        tokio::spawn(async move { start_server(cache, addrs).await.unwrap().await });

    let cfg = LoadConfig {
        workers_n: 8,
        keys_n: 100,
        padding: std::iter::repeat("*").take(2).collect::<String>(),
    };

    info!("Loading values...");
    load_values(&String::from(http_addrs), &cfg).await;
    info!("Values loaded.");
    run_test(&String::from(http_addrs), &cfg).await;

    info!("Starting server...");
    info!("Server ready. Listening on {:#?}", addrs);

    server_thread.abort();
    Ok(())
}
