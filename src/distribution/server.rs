use std::sync::Mutex;

use actix_web::{web, Responder, Result};
use serde::{Deserialize, Serialize};

use crate::cache::{Cache, KeyType, ValueType};

use super::Orchestrator;

pub struct ServerState<T>
where
    T: Cache,
{
    inner_cache: Mutex<T>,
}

impl<T> ServerState<T>
where
    T: Cache,
{
    pub fn new() -> Self {
        Self {
            inner_cache: T::new().into(),
        }
    }
}

#[derive(Deserialize)]
pub struct GetKeyQueryParams {
    pub key: KeyType,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct SetKeyJsonBody {
    pub key: KeyType,
    pub value: ValueType,
    pub expiration: u64,
}

#[derive(Deserialize)]
pub struct AddServerJsonBody {
    name: String,
}

// #[get("/get-key")]
pub async fn get_key<T>(
    data: web::Data<ServerState<T>>,
    query_params: web::Query<GetKeyQueryParams>,
) -> Result<impl Responder>
where
    T: Cache,
{
    let mut inner = data.inner_cache.lock().unwrap();
    let res_opt = inner.get(&query_params.key);
    Ok(web::Json(res_opt))
}

// #[post("/set-key")]
pub async fn set_key<T>(
    data: web::Data<ServerState<T>>,
    json_req: web::Json<SetKeyJsonBody>,
) -> Result<impl Responder>
where
    T: Cache,
{
    let mut inner = data.inner_cache.lock().unwrap();
    inner.set(&json_req.key, json_req.value.clone(), json_req.expiration);
    Ok("Ok")
}

// #[post("/add-cache")]
pub async fn add_server<T>(
    data: web::Data<ServerState<T>>,
    json_req: web::Json<AddServerJsonBody>,
) -> Result<impl Responder>
where
    T: Orchestrator,
{
    let mut inner = data.inner_cache.lock().unwrap();
    inner.add_cache(json_req.name.clone());
    Ok("Ok")
}

// // #[get("/print-internally")]
// pub async fn print_internally<T>(data: web::Data<ServerState<T>>) -> Result<impl Responder>
// where
//     T: Cache,
// {
//     let inner = data.inner_cache.lock().unwrap();
//     println!("Dumping cached values");
//     for (key, server) in inner.server_map.iter() {
//         println!("Server: {}", key);
//         for (cached_key, cached_value) in server.map.iter() {
//             println!(
//                 "Key: {}, value: {}, expiration: {}",
//                 cached_key, cached_value.value, cached_value.expiration
//             );
//         }
//     }
//     Ok("Ok")
// }
