#[tokio::main]
async fn main() -> Result<(), syncarp::Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let mut rpc_client = syncarp::RpcClient::new("127.0.0.1:8080").await?;
    let result = rpc_client
        .dispatch::<(), i64>("get-unique-id".to_string(), 0, ())
        .await;
    println!("Received: {:?}", result);
    Ok(())
}
