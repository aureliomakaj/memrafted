use core::fmt;
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, HashMap},
    fmt::Display,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
    time::{Instant},
};

use actix_web::{get, post, web, App,  HttpServer, Responder, Result};
use log::info;
use serde::{Deserialize, Serialize};

// Enumeration of the possible type that can be stored in the cache
#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "variant", content = "content")]
enum CacheValueType {
    String(String),
    F64(f64),
}

impl Display for CacheValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *&self {
            CacheValueType::String(s) => write!(f, "{}", s),
            CacheValueType::F64(float) => write!(f, "{}", float),
        }
    }
}

// Struct that describe a cached entity
#[derive(Clone)]
struct CacheValue {
    // The value stored
    value: CacheValueType,

    // Expiration time in seconds. How much the data should be available
    expiration: u64,

    // Time of when the value was inserted or updated
    creation: Instant,
}

fn hash_function(key: &String) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    return hasher.finish();
}

// Pool that simulates a distributed cache.
// It can contain zero or more cache servers
struct Pool {
    // Map of server key with the actual server
    server_map: HashMap<String, Memrafted>,

    // Map between the hashed server key and the server key itself
    ring: BTreeMap<u64, String>,
}

impl Pool {
    // Get a new instance
    pub fn new() -> Self {
        Pool {
            ring: BTreeMap::new(),
            server_map: HashMap::new(),
        }
    }

    //Add a new server
    pub fn add_server(&mut self, server_id: String) {
        let mut keys = vec![];
        for i in 0..100 {
            keys.push(format!("{}_{}", server_id, i));
        }
        // Compute the hash of the server name
        let hashed_srv_id = hash_function(&server_id);
        let cloned = server_id.clone();

        info!("Adding server {} with hashed key {}", cloned, hashed_srv_id);
        // Create a new memrafted instace and map it to the server name
        self.server_map.insert(server_id, Memrafted::new());
        for key in keys {
            self.ring.insert(hash_function(&key), cloned.clone());
        }
        // Map the hash of the server name to the server name
        
    }

    // Get tthe value cached for that key
    pub fn get(&mut self, key: &String) -> Option<CacheValueType> {
        let hashed_key = hash_function(key);
        let cloned = self.clone();
        // Get the index of the server with the hashed name nearest to the hashed key
        let idx_opt = cloned.get_memrafted_server_key(hashed_key);
        match idx_opt {
            Some(index) => {
                // Get the memrafted instance of that index
                let memrafted_opt = self.server_map.get_mut(index);

                // If the instance exists, try gettin the value
                if let Some(memrafted) = memrafted_opt {
                    memrafted.get(key)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    // Cache a value for that key for tot expiration seconds
    pub fn set(&mut self, key: &String, value: CacheValueType, expiration: u64) {
        let hashed_key = hash_function(key);
        let cloned = self.clone();
        // Get the index of the server with the hashed name nearest to the hashed key
        let idx_opt = cloned.get_memrafted_server_key(hashed_key);
        match idx_opt {
            Some(index) => {
                info!("Saving {} to server with key {}", hashed_key, index);
                let memrafted_opt = self.server_map.get_mut(index);
                if let Some(memrafted) = memrafted_opt {
                    memrafted.set(key, value, expiration)
                }
            }
            None => (),
        }
    }

    // Implementation of "Ketama Consistent Hashing".
    // The server key is hashed, and inserted in the ring, possibly multiple times.
    // When we want to cache a key, we hash the key, and find in the ring the first
    // server with the hashed key greater then the hash of the cached key.
    // If there isn't one, get the first from the start
    fn get_memrafted_server_key(&self, hashed_key: u64) -> Option<&String> {
        if self.ring.keys().len() == 0 {
            return None;
        }

        // Find the server associated with the first hash greater then the hashed key
        let first_greater_hash = self.ring.keys().find(|elem| **elem > hashed_key);

        let memrafted_idx_opt = match first_greater_hash {
            Some(idx) => Some(self.ring.get(idx).unwrap()),
            None => {
                // If there isn't a hash greater then our key, then get the first
                // from the start
                let idx = self.ring.keys().min().unwrap();
                Some(self.ring.get(idx).unwrap())
            }
        };

        if let Some(idx) = memrafted_idx_opt {
            return Some(idx);
        }
        None
    }
}

impl Clone for Pool {
    fn clone(&self) -> Self {
        Self {
            ring: self.ring.clone(),
            server_map: self.server_map.clone(),
        }
    }
}

struct Memrafted {
    map: HashMap<String, CacheValue>,
}

impl Memrafted {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: &str) -> Option<CacheValueType> {
        // Get the value from the hashmap
        let in_cache = self.map.get(key);
        match in_cache {
            Some(value) => {
                // The value was present.
                // Check if the expiration hasn't been reached
                if Memrafted::is_expired(value) {
                    // Expiration reached. Remove the key from the hashmap
                    self.map.remove(key);
                    // Return None as the value wasn't valid anymore
                    None
                } else {
                    Some(value.value.clone())
                }
            }
            None => None,
        }
    }

    pub fn set(&mut self, key: &str, value: CacheValueType, expiration: u64) {
        self.map.insert(
            String::from(key),
            CacheValue {
                value,
                expiration,
                creation: Instant::now(),
            },
        );
        ()
    }

    fn is_expired(value: &CacheValue) -> bool {
        let elapsed = value.creation.elapsed().as_secs();
        elapsed > value.expiration
    }
}

impl Clone for Memrafted {
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
        }
    }
}

#[derive(Deserialize)]
struct GetKeyQueryParams {
    key: String,
}

#[derive(Deserialize)]
struct SetKeyJsonBody {
    key: String,
    value: CacheValueType,
    expiration: u64,
}

#[derive(Deserialize)]
struct AddServerJsonBody {
    name: String,
}

#[get("/get-key")]
async fn get_key(
    data: web::Data<PoolState>,
    query_params: web::Query<GetKeyQueryParams>,
) -> Result<impl Responder> {
    let mut pool = data.pool.lock().unwrap();
    let res_opt = pool.get(&query_params.key);
    Ok(web::Json(res_opt))
}

#[post("/set-key")]
async fn set_key(
    data: web::Data<PoolState>,
    json_req: web::Json<SetKeyJsonBody>,
) -> Result<impl Responder> {
    let mut pool = data.pool.lock().unwrap();
    pool.set(&json_req.key, json_req.value.clone(), json_req.expiration);
    Ok("Ok")
}

#[post("/add-server")]
async fn add_server(
    data: web::Data<PoolState>,
    json_req: web::Json<AddServerJsonBody>,
) -> Result<impl Responder> {
    let mut pool = data.pool.lock().unwrap();
    pool.add_server(json_req.name.clone());
    Ok("Ok")
}

#[get("/print-internally")]
async fn print_internally(data: web::Data<PoolState>) -> Result<impl Responder> {
    let pool = data.pool.lock().unwrap();
    println!("Dumping cached values");
    for (key, server) in pool.server_map.iter() {
        println!("Server: {}", key);
        for (cached_key, cached_value) in server.map.iter() {
            println!(
                "Key: {}, value: {}, expiration: {}",
                cached_key, cached_value.value, cached_value.expiration
            );
        }
    }
    Ok("Ok")
}

struct PoolState {
    pool: Mutex<Pool>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    println!("Starting server...");
    println!("Server ready. Listening on 127.0.0.1:8081");

    let appstate = web::Data::new(PoolState {
        pool: Mutex::new(Pool::new()),
    });

    HttpServer::new(move || {
        App::new()
            .app_data(appstate.clone())
            .service(get_key)
            .service(set_key)
            .service(add_server)
            .service(print_internally)
    })
    .bind(("127.0.0.1", 8081))?
    .run()
    .await
}
