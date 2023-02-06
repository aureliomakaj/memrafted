use std::fmt::Debug;
use std::io::Result;
use std::net::ToSocketAddrs;

use actix_web::dev::Server;
use actix_web::{web, App, HttpServer};

use crate::api::{drop_key, get_key, set_key, ServerState};
use crate::cache::Cache;

pub async fn start_server<T, A>(cache: T, addrs: A) -> Result<Server>
where
    T: Cache + 'static,
    A: ToSocketAddrs + Debug + Clone,
{
    std::env::set_var("RUST_LOG", "debug");
    //std::env::set_var("RUST_BACKTRACE", "1");
    env_logger::init();

    let appstate = web::Data::new(ServerState::new(cache).await);

    let server = HttpServer::new(move || {
        App::new()
            .app_data(appstate.clone())
            .route("/get-key", web::get().to(get_key::<T>))
            .route("/set-key", web::post().to(set_key::<T>))
            .route("/drop-key", web::post().to(drop_key::<T>))
    })
    .bind(addrs.clone())?
    .run();

    Ok(server)
}
