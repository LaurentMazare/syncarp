use syncarp::JRpc;

struct GetUniqueId;
struct SetIdCounter;

impl syncarp::JRpc for GetUniqueId {
    type Q = ();
    type R = i64;

    const RPC_NAME: &'static str = "get-unique-id";
    const RPC_VERSION: i64 = 0i64;
}

impl syncarp::JRpc for SetIdCounter {
    type Q = i64;
    type R = ();

    const RPC_NAME: &'static str = "set-id-counter";
    const RPC_VERSION: i64 = 1i64;
}

#[tokio::main]
async fn main() -> Result<(), syncarp::Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let mut rpc_client = syncarp::RpcClient::new("127.0.0.1:8080").await?;
    for i in 1..5 {
        let result = GetUniqueId::dispatch(&mut rpc_client, ()).await;
        println!("Received: {} {:?}", i, result);
    }
    SetIdCounter::dispatch(&mut rpc_client, 42).await?;
    for i in 1..5 {
        let result = GetUniqueId::dispatch(&mut rpc_client, ()).await;
        println!("Received: {} {:?}", i, result);
    }
    Ok(())
}
