mod api;
mod cache;
mod distribution;
mod hash;

use std::io::Result;

use actix_web::{web, App, HttpServer};

use crate::api::{add_server, drop_key, get_key, remove_server, set_key, ServerState};
use crate::cache::local::LocalCache;
// use crate::distribution::orchestrator::HashOrchestrator;
use crate::distribution::raft::RaftManager;

#[actix_web::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    //std::env::set_var("RUST_BACKTRACE", "1");
    env_logger::init();

    let mut rm = RaftManager::<LocalCache>::new("Network".to_string());
    rm.add_node(0).await.unwrap();
    rm.add_node(1).await.unwrap();
    rm.add_node(2).await.unwrap();

    let appstate = web::Data::new(ServerState::new(rm).await);

    let server = HttpServer::new(move || {
        App::new()
            .app_data(appstate.clone())
            .route(
                "/get-key",
                web::get().to(get_key::<RaftManager<LocalCache>>),
            )
            .route(
                "/set-key",
                web::post().to(set_key::<RaftManager<LocalCache>>),
            )
            .route(
                "/drop-key",
                web::post().to(drop_key::<RaftManager<LocalCache>>),
            )
            // .route(
            //     "/add-cache",
            //     web::post().to(add_server::<RaftManager<LocalCache>>),
            // )
            // .route(
            //     "/remove-cache",
            //     web::post().to(remove_server::<RaftManager<LocalCache>>),
            // )
    })
    .bind(("127.0.0.1", 8081))?
    .run();

    println!("Starting server...");
    println!("Server ready. Listening on 127.0.0.1:8081");

    server.await
}
