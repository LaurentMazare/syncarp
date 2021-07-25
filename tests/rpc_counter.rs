// TODO: use an ephemeral port rather than 8080.
use std::sync::{Arc, Mutex};
use syncarp::Rpc;

const DEBUG: bool = false;

struct GetUniqueIdImpl(Arc<Mutex<i64>>);

impl syncarp::JRpcImpl for GetUniqueIdImpl {
    type Q = ();
    type R = i64;
    type E = std::convert::Infallible;

    fn rpc_impl(&self, _q: Self::Q) -> Result<Self::R, Self::E> {
        let mut b = self.0.lock().unwrap();
        let result = *b;
        *b += 1;
        Ok(result)
    }
}

struct SetIdCounterImpl(Arc<Mutex<i64>>);

impl syncarp::JRpcImpl for SetIdCounterImpl {
    type Q = i64;
    type R = ();
    type E = std::convert::Infallible;

    fn rpc_impl(&self, q: Self::Q) -> Result<Self::R, Self::E> {
        let mut b = self.0.lock().unwrap();
        *b = q;
        Ok(())
    }
}

struct GetUniqueId;
struct SetIdCounter;

impl syncarp::Rpc for GetUniqueId {
    type Q = ();
    type R = i64;

    const RPC_NAME: &'static str = "get-unique-id";
    const RPC_VERSION: i64 = 0i64;
}

impl syncarp::Rpc for SetIdCounter {
    type Q = i64;
    type R = ();

    const RPC_NAME: &'static str = "set-id-counter";
    const RPC_VERSION: i64 = 1i64;
}

async fn run_rpc_server() -> Result<(), syncarp::Error> {
    let counter = Arc::new(Mutex::new(0));
    let get_unique_id_impl = GetUniqueIdImpl(counter.clone());
    let set_id_counter_impl = SetIdCounterImpl(counter.clone());
    syncarp::RpcServer::new()
        .add_rpc(
            GetUniqueId::RPC_NAME,
            GetUniqueId::RPC_VERSION,
            get_unique_id_impl,
        )
        .add_rpc(
            SetIdCounter::RPC_NAME,
            SetIdCounter::RPC_VERSION,
            set_id_counter_impl,
        )
        .start("127.0.0.1:8080")
        .await?;
    Ok(())
}

#[tokio::test]
async fn server_test() -> Result<(), syncarp::Error> {
    if DEBUG {
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::TRACE)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    }
    tokio::spawn(async move { run_rpc_server().await });

    // TODO: Rather than waiting for the server to start, tweak the
    // api to split the [bind] from the [accept] loop.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut rpc_client = syncarp::RpcClient::new("127.0.0.1:8080").await?;
    for i in 0..5i64 {
        let result = GetUniqueId::dispatch(&mut rpc_client, ()).await?;
        assert_eq!(result, i);
    }
    SetIdCounter::dispatch(&mut rpc_client, 42).await?;
    for i in 0..5i64 {
        let result = GetUniqueId::dispatch(&mut rpc_client, ()).await?;
        assert_eq!(result, 42 + i);
    }
    Ok(())
}
