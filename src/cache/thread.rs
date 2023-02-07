use std::{collections::HashSet, sync::Arc, thread::spawn};

use async_raft::async_trait::async_trait;
use futures::executor::block_on;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    RwLock,
};

use super::{Cache, FullType, GetResult, KeyType, Time, ValueType};

#[derive(Debug)]
enum Request {
    Get(Time, KeyType),
    GetAll(Time),
    Set(KeyType, ValueType, Time),
    Drop(KeyType),
}

#[derive(Debug)]
enum Response {
    Get(GetResult),
    GetAll(HashSet<FullType>),
}

fn thread_main<T>(mut cache: T, mut req_rcv: Receiver<Request>, res_snd: Sender<Response>)
where
    T: Cache,
{
    while let Some(req) = req_rcv.blocking_recv() {
        match req {
            Request::Get(now, key) => {
                (res_snd.blocking_send(Response::Get(block_on(cache.get(now, &key))))).unwrap();
            }
            Request::GetAll(now) => {
                (res_snd.blocking_send(Response::GetAll(block_on(cache.get_all(now))))).unwrap();
            }
            Request::Set(key, value, exp_time) => {
                block_on(cache.set(&key, value, exp_time));
            }
            Request::Drop(key) => {
                block_on(cache.drop(&key));
            }
        }
    }
}

struct ThreadInnerCache {
    req_snd: Sender<Request>,
    res_rcv: Receiver<Response>,
}

impl ThreadInnerCache {
    pub fn start<T>(cache: T) -> Self
    where
        T: Cache + Send + 'static,
    {
        let (req_snd, req_rcv) = channel(1);
        let (res_snd, res_rcv) = channel(1);
        spawn(move || thread_main(cache, req_rcv, res_snd));
        Self { req_snd, res_rcv }
    }

    async fn get(&mut self, now: Time, key: &KeyType) -> GetResult {
        self.req_snd
            .send(Request::Get(now, key.clone()))
            .await
            .unwrap();

        match self.res_rcv.recv().await {
            Some(Response::Get(res)) => res,
            _ => GetResult::NotFound,
        }
    }

    async fn get_all(&mut self, now: Time) -> HashSet<FullType> {
        self.req_snd.send(Request::GetAll(now)).await.unwrap();

        match self.res_rcv.recv().await {
            Some(Response::GetAll(res)) => res,
            _ => HashSet::new(),
        }
    }

    async fn set(&mut self, key: &KeyType, value: ValueType, exp_time: Time) {
        self.req_snd
            .send(Request::Set(key.clone(), value, exp_time))
            .await
            .unwrap();
    }

    async fn drop(&mut self, key: &KeyType) {
        self.req_snd.send(Request::Drop(key.clone())).await.unwrap();
    }
}

pub struct ThreadCache {
    inner: Arc<RwLock<ThreadInnerCache>>,
}

impl ThreadCache {
    pub fn start<T>(cache: T) -> Self
    where
        T: Cache + Send + 'static,
    {
        Self {
            inner: Arc::new(RwLock::new(ThreadInnerCache::start(cache))),
        }
    }
}

#[async_trait]
impl Cache for ThreadCache {
    async fn get(&mut self, now: Time, key: &KeyType) -> GetResult {
        self.inner.write().await.get(now, key).await
    }

    async fn get_all(&mut self, now: Time) -> HashSet<FullType> {
        self.inner.write().await.get_all(now).await
    }

    async fn set(&mut self, key: &KeyType, value: ValueType, exp_time: Time) {
        self.inner.write().await.set(key, value, exp_time).await
    }

    async fn drop(&mut self, key: &KeyType) {
        ThreadInnerCache::drop(&mut *self.inner.write().await, key).await
    }
}
