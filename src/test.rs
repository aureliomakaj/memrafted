use std::{sync::Arc, time::Duration};

use futures::{executor::block_on, stream};
use futures::{future::join_all, StreamExt};
use log::info;
use rand::{distributions::Uniform, prelude::Distribution};
use reqwest::Client;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::api::{GetKeyQueryParams, SetKeyJsonBody};

pub struct LoadConfig {
    pub workers_n: u32,
    pub keys_n: u32,
    pub padding: String,
}

pub async fn load_values(addrs: &String, cfg: &LoadConfig) {
    let client = &reqwest::Client::new();

    stream::iter(1..cfg.keys_n)
        .for_each_concurrent(Some(cfg.workers_n as usize), |k| async move {
            let res = client
                .post(format!("{}/set-key", addrs))
                .json(&SetKeyJsonBody {
                    key: format!("k_{k}"),
                    value: format!("{}_{}", k, cfg.padding),
                    exp_time: 100,
                })
                .send();
            res.await.unwrap();
            sleep(Duration::from_secs_f32(0.5)).await;

            info!("{}/{}", k, cfg.keys_n);
        })
        .await;

    sleep(Duration::from_secs(2)).await;
}

pub struct TestConfig {
    pub time: Duration,
    pub workers_n: u32,
    pub req_sec: u32,
    pub keys_n: u32,
}

async fn get_key(c: &Client, a: &String, k: &u32) {
    let _ = c
        .get(format!("{}/get-key", a))
        .query(&GetKeyQueryParams {
            key: format!("k_{}", k),
            now: 50,
        })
        .send()
        .await;
}

fn thread_main(d: Duration, i: Duration, addrs: String, keys_n: u32) -> usize {
    let count = Arc::new(RwLock::new(0));
    let stop = sleep(d.clone());
    let client = Arc::new(RwLock::new(Client::new()));
    let rng = rand::thread_rng();
    let die = Uniform::from(1..keys_n);

    let keys = stream::iter(die.sample_iter(rng));
    let args = stream::repeat((client, addrs, count.clone()));

    let task = args.zip(keys).take_until(stop).for_each_concurrent(
        Some(keys_n as usize),
        |((c, a, count), k)| async move {
            info!("Executing");
            let sleep = sleep(i);
            get_key(&*c.read().await, &a, &k).await;
            let mut lock = count.write().await;
            *lock += 1;
            info!("Increased");
            sleep.await;
            info!("After I slept");
        },
    );
    info!("Before block on");
    block_on(task);
    info!("After block");
    let ret = *block_on(count.read());
    ret
}

pub async fn run_test(addrs: &String, cfg: &TestConfig) -> usize {
    let int = cfg.time.clone() / cfg.req_sec * cfg.workers_n;
    info!("Duration {:?}", int);

    let threads = stream::repeat((cfg.time, int, addrs.clone(), cfg.keys_n))
        .take(cfg.workers_n as usize)
        .map(|(d, i, a, k)| async move { tokio::spawn(async move { thread_main(d, i, a, k) }) })
        .collect::<Vec<_>>()
        .await;

    let handles = join_all(threads).await;

    stream::iter(handles)
        .fold(0, |sum, h| async move { sum + h.await.unwrap_or(0) })
        .await
}
