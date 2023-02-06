use std::net::ToSocketAddrs;

use tokio::runtime;

struct TestConfig {
    pub workers_n: Option<u64>,
    pub keys_n: Option<u64>,
}

async fn load_values<A>(addrs: A, cfg: TestConfig)
where
    A: ToSocketAddrs,
{
    let threaded_rt = runtime::Runtime::new().unwrap();
}

fn run_test<A>(addrs: A)
where
    A: ToSocketAddrs,
{
}
