mod cache;
mod distribution;
mod hash;

use actix_web::{web, App, HttpServer};
use cache::local::LocalCache;
use distribution::orchestrator::HashOrchestrator;

use crate::distribution::server::{add_server, get_key, set_key, ServerState};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    println!("Starting server...");
    println!("Server ready. Listening on 127.0.0.1:8081");

    let appstate = web::Data::new(ServerState::<HashOrchestrator<LocalCache>>::new());

    HttpServer::new(move || {
        App::new()
            .app_data(appstate.clone())
            .route(
                "/get-key",
                web::get().to(get_key::<HashOrchestrator<LocalCache>>),
            )
            .route(
                "/set-key",
                web::post().to(set_key::<HashOrchestrator<LocalCache>>),
            )
            .route(
                "/add-cache",
                web::post().to(add_server::<HashOrchestrator<LocalCache>>),
            )
        // .route("print-internally", web::get().to(print_internally<HashOrchestrator<LocalCache>>))
    })
    .bind(("127.0.0.1", 8081))?
    .run()
    .await
}
