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

struct GetUniqueIdV2;
impl JRpc for GetUniqueIdV2 {
    type Q = ();
    type R = i64;
    const RPC_NAME: &'static str = "get-unique-id";
    const RPC_VERSION: i64 = 2i64;
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

struct GetUniqueIdBroken;

// This uses the same rpc name and version than GetUniqueId but
// with a different type.
impl JRpc for GetUniqueIdBroken {
    type Q = i64;
    type R = i64;
    const RPC_NAME: &'static str = "get-unique-id";
    const RPC_VERSION: i64 = 0i64;
}

struct SetIdCounter;

impl JRpc for SetIdCounter {
    type Q = i64;
    type R = ();
    const RPC_NAME: &'static str = "set-id-counter";
    const RPC_VERSION: i64 = 1i64;
}

async fn rpc_server() -> Result<syncarp::RpcServer, syncarp::Error> {
    let counter = Arc::new(Mutex::new(0));
    let get_unique_id_impl = GetUniqueIdImpl(counter);
    let rpc_server = syncarp::RpcServer::new("127.0.0.1:0").await?.add_rpc(get_unique_id_impl);
    Ok(rpc_server)
}

#[tokio::test]
async fn server_error_test() -> Result<(), syncarp::Error> {
    if DEBUG {
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::TRACE)
            .finish();

        let _test = tracing::subscriber::set_global_default(subscriber);
    }
    let rpc_server = rpc_server().await?;
    let local_addr = rpc_server.local_addr()?;
    tokio::spawn(async move { rpc_server.run().await });

    let mut rpc_client = syncarp::RpcClient::new(local_addr).await?;
    let result = GetUniqueId::dispatch(&mut rpc_client, ()).await?;
    assert_eq!(result, 0);
    let result = SetIdCounter::dispatch(&mut rpc_client, 42).await;
    assert!(result.is_err());
    let result = GetUniqueId::dispatch(&mut rpc_client, ()).await?;
    assert_eq!(result, 1);
    let result = GetUniqueIdV2::dispatch(&mut rpc_client, ()).await;
    assert!(result.is_err());
    let result = GetUniqueId::dispatch(&mut rpc_client, ()).await?;
    assert_eq!(result, 2);
    Ok(())
}

#[tokio::test]
async fn type_error_test() -> Result<(), syncarp::Error> {
    if DEBUG {
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::TRACE)
            .finish();

        let _test = tracing::subscriber::set_global_default(subscriber);
    }
    let rpc_server = rpc_server().await?;
    let local_addr = rpc_server.local_addr()?;
    tokio::spawn(async move { rpc_server.run().await });

    let mut rpc_client = syncarp::RpcClient::new(local_addr).await?;
    let result = GetUniqueId::dispatch(&mut rpc_client, ()).await?;
    assert_eq!(result, 0);
    // This works because the binprot representation of unit is 0.
    let result = GetUniqueIdBroken::dispatch(&mut rpc_client, 0).await?;
    assert_eq!(result, 1);
    // This fail as an unexpected value is received.
    // Currently this gets stuck though.
    let result = GetUniqueIdBroken::dispatch(&mut rpc_client, 1).await;
    assert!(result.is_err());

    Ok(())
}
