use std::{sync::Mutex, time::SystemTime};

use actix_web::{web, Responder, Result};
use serde::{Deserialize, Serialize};

use crate::{
    cache::{Cache, KeyType, ValueType},
    distribution::orchestrator::Orchestrator,
};

pub struct ServerState<T>
where
    T: Cache,
{
    cache: Mutex<T>,
}

impl<T> ServerState<T>
where
    T: Cache,
{
    pub async fn new(cache: T) -> Self {
        Self {
            cache: cache.into(),
        }
    }
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct GetKeyQueryParams {
    pub key: KeyType,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct SetKeyJsonBody {
    pub key: KeyType,
    pub value: ValueType,
    #[serde(with = "serde_millis")]
    pub exp_time: SystemTime,
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
    let mut inner = data.cache.lock().unwrap();
    let res_opt = inner.get(&query_params.key).await;
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
    let mut inner = data.cache.lock().unwrap();
    inner
        .set(&json_req.key, json_req.value.clone(), json_req.exp_time)
        .await;
    Ok("Ok")
}

// #[post("/add-cache")]
pub async fn add_server<T>(
    data: web::Data<ServerState<T>>,
    json_req: web::Json<AddServerJsonBody>,
) -> Result<impl Responder>
where
    T: Orchestrator,
    T::CacheType: Default,
{
    let mut inner = data.cache.lock().unwrap();
    inner
        .add_cache(json_req.name.clone(), T::CacheType::default())
        .await;
    Ok("Ok")
}

// #[post("/remove-cache")]
pub async fn remove_server<T>(
    data: web::Data<ServerState<T>>,
    json_req: web::Json<AddServerJsonBody>,
) -> Result<impl Responder>
where
    T: Orchestrator,
{
    let mut inner = data.cache.lock().unwrap();
    inner.remove_cache(json_req.name.clone()).await;
    Ok("Ok")
}
