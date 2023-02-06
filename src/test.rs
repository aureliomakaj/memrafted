use std::{
    collections::HashMap, fmt::Display, iter, net::ToSocketAddrs, sync::Arc, time::Duration,
};

use futures::{stream::{self, FuturesUnordered}, StreamExt, future::join_all, executor::block_on};
use log::info;
use reqwest::Client;
use tokio::{sync::Mutex, task::JoinHandle, time::sleep, join};

use crate::api::{SetKeyJsonBody, GetKeyQueryParams};

pub struct LoadConfig {
    pub workers_n: usize,
    pub keys_n: usize,
    pub padding: String,
}

pub async fn load_values(addrs: &String, cfg: &LoadConfig) {
    let client = &reqwest::Client::new();

    stream::iter(1..cfg.keys_n)
        .for_each_concurrent(Some(cfg.workers_n), |k| async move {
            let res = client
                .post(format!("{}/set-key", addrs))
                .json(&SetKeyJsonBody {
                    key: format!("k_{k}"),
                    value: format!("{}_{}", k, cfg.padding),
                    exp_time: 100,
                })
                .send();
            res.await.unwrap();
            info!("{}/{}", k, cfg.keys_n);
        })
        .await;
}

pub struct TestConfig {
    pub time: Duration,
    pub workers_n: usize,
    pub req_sec: usize,
    pub keys_n: usize,
}

fn get_random_key(c: Arc<Mutex<Client>>, a: &String) {
    //c.get(format!("{}/get-key", a)).build().unwrap().body();
}

async fn make_thread(d: Duration, addrs: String) -> JoinHandle<usize> {
    let count = Arc::new(Mutex::new(0));
    tokio::spawn(async move{
        let stop = sleep(d.clone());
        let client = Client::new();
        stream::repeat(addrs)
            .take_until(stop)
            .for_each(|a| async move{
                // get_random_key(c, &a);
                let client = Client::new();
                client.get(format!("{}/get-key", a))
                .query(&GetKeyQueryParams {
                    key: String::from("K_1"),
                    now: 1
                })
                .send()
                .await
                .unwrap();

                //*count.lock().await = *count.lock().await + 1;
            })
            .await;
        //let x = count.lock().await.to_owned();
        0
    })
}

pub async fn run_test(addrs: &String, cfg: &LoadConfig) {
    let duration = Duration::from_secs(5);
    let workers: HashMap<_, _> = iter::repeat(duration)
        .take(cfg.workers_n)
        .enumerate()
        .collect();

    let threads: HashMap<_, _> = workers
        .into_iter()
        .map(|(i, d)| {
            let t = block_on(make_thread(d, addrs.clone()));
            (i, t)
        })
        .collect();

    let a = join_all(threads.into_iter().map(|(k,v)| v)).await;
    
}
