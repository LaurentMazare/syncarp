use std::sync::{Arc, Mutex};
use syncarp::JRpc;

const DEBUG: bool = false;

struct GetUniqueId;
struct GetUniqueIdImpl(Arc<Mutex<i64>>);

impl JRpc for GetUniqueId {
    type Q = ();
    type R = i64;
    const RPC_NAME: &'static str = "get-unique-id";
    const RPC_VERSION: i64 = 0i64;
}

impl syncarp::JRpcImpl for GetUniqueIdImpl {
    type E = std::convert::Infallible;
    type JRpc = GetUniqueId;

    fn rpc_impl(&self, _q: <Self::JRpc as JRpc>::Q) -> Result<<Self::JRpc as JRpc>::R, Self::E> {
        let mut b = self.0.lock().unwrap();
        let result = *b;
        *b += 1;
        Ok(result)
    }
}

struct SetIdCounter;
struct SetIdCounterImpl(Arc<Mutex<i64>>);

impl JRpc for SetIdCounter {
    type Q = i64;
    type R = ();
    const RPC_NAME: &'static str = "set-id-counter";
    const RPC_VERSION: i64 = 1i64;
}

impl syncarp::JRpcImpl for SetIdCounterImpl {
    type E = std::convert::Infallible;
    type JRpc = SetIdCounter;

    fn rpc_impl(&self, q: <Self::JRpc as JRpc>::Q) -> Result<<Self::JRpc as JRpc>::R, Self::E> {
        let mut b = self.0.lock().unwrap();
        *b = q;
        Ok(())
    }
}

async fn rpc_server() -> Result<syncarp::RpcServer, syncarp::Error> {
    let counter = Arc::new(Mutex::new(0));
    let get_unique_id_impl = GetUniqueIdImpl(counter.clone());
    let set_id_counter_impl = SetIdCounterImpl(counter.clone());
    let rpc_server = syncarp::RpcServer::new("127.0.0.1:0")
        .await?
        .add_rpc(get_unique_id_impl)
        .add_rpc(set_id_counter_impl);
    Ok(rpc_server)
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
    let rpc_server = rpc_server().await?;
    let local_addr = rpc_server.local_addr()?;
    tokio::spawn(async move { rpc_server.run().await });

    let mut rpc_client = syncarp::RpcClient::new(local_addr).await?;
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
