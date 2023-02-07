use memrafted::{
    cache::local::LocalCache,
    setup::start_server,
    test::{load_values, LoadConfig},
    // test::{run_test, TestConfig},
};
use std::io::Result;
// use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    // std::env::set_var("RUST_BACKTRACE", "full");
    env_logger::init();

    let cache = LocalCache::default();
    let addrs = "127.0.0.1:8081";

    let server_thread =
        tokio::spawn(async move { start_server(cache, addrs).await.unwrap().await });

    let http_addrs = "http://127.0.0.1:8081";
    let lcfg = LoadConfig {
        workers_n: 8,
        keys_n: 100,
        padding: std::iter::repeat("*").take(2).collect::<String>(),
    };

    info!("Loading values...");
    load_values(&String::from(http_addrs), &lcfg).await;

    // let tcfg = TestConfig {
    //     time: Duration::from_secs(10),
    //     workers_n: 3,
    //     req_sec: 100,
    //     keys_n: 5,
    // };

    // info!("Values loaded.");
    // let res = run_test(&String::from(http_addrs), &tcfg).await;

    info!("Starting server...");
    info!("Server ready. Listening on {:#?}", addrs);

    server_thread.await.unwrap()
    // println!("Total requests: {}", res);
}
