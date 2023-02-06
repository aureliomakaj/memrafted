use std::{
    collections::HashMap, fmt::Display, iter, net::ToSocketAddrs, sync::Arc, time::Duration,
};

use futures::{stream, StreamExt};
use log::info;
use reqwest::Client;
use tokio::{sync::Mutex, task::JoinHandle, time::sleep};

use crate::api::SetKeyJsonBody;

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
    c.get(format!("{}/get-key", a)).build().unwrap().body();
}

async fn make_thread(d: Duration, addrs: String) -> JoinHandle<usize> {
    tokio::spawn(async {
        // let stop = sleep(d);
        let count = Arc::new(Mutex::new(0));
        let client = Client::new();
        stream::repeat((client, addrs))
            // .take_until(stop)
            .for_each(|(c, a)| async {
                // get_random_key(c, &a);
                *count.lock().await = *count.lock().await + 1;
            })
            .await;
        let x = count.lock().await.to_owned();
        x
    })
}

pub async fn run_test(addrs: &String, cfg: &TestConfig) {
    let workers: HashMap<_, _> = iter::repeat((cfg.time))
        .take(cfg.workers_n)
        .enumerate()
        .collect();

    let threads: HashMap<_, _> = workers
        .into_iter()
        .map(|(i, d)| {
            let t = make_thread(d, addrs.clone());
            (i, t)
        })
        .collect();
}
