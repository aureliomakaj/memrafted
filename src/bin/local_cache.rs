use futures::executor::block_on;
use memrafted::{cache::local::LocalCache, setup::start_server};
use std::io::Result;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let cache = LocalCache::default();
    let addrs = ("127.0.0.1", 8081);
    let server = block_on(start_server(cache, addrs)).unwrap();

    info!("Starting server...");
    info!("Server ready. Listening on {:#?}", addrs);

    server.await
}
