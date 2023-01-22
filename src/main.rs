mod cache;
mod distribution;
mod hash;

use actix_web::{web, App, HttpServer};
//use cache::local::LocalCache;
//use distribution::orchestrator::HashOrchestrator;
use distribution::single_raft::{RaftNode, RaftOrchestrator};

use crate::distribution::server::{remove_server, add_server, get_key, set_key, ServerState};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    std::env::set_var("RUST_BACKTRACE", "1");
    env_logger::init();

    println!("Starting server...");
    println!("Server ready. Listening on 127.0.0.1:8081"); 

    let appstate = web::Data::new(ServerState::<RaftOrchestrator<RaftNode>>::new().await);
    
    HttpServer::new(move || {
        App::new()
            .app_data(appstate.clone())
            .route(
                "/get-key",
                web::get().to(get_key::<RaftOrchestrator<RaftNode>>),
            )
            .route(
                "/set-key",
                web::post().to(set_key::<RaftOrchestrator<RaftNode>>),
            )
            .route(
                "/add-cache",
                web::post().to(add_server::<RaftOrchestrator<RaftNode>>),
            )
            .route(
                "/remove-cache",
                web::post().to(remove_server::<RaftOrchestrator<RaftNode>>),
            )
        // .route("print-internally", web::get().to(print_internally<HashOrchestrator<LocalCache>>))
    })
    .bind(("127.0.0.1", 8081))?
    .run()
    .await
}
