#[tokio::main]
async fn main() -> Result<(), syncarp::Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let _rpc_client = syncarp::RpcClient::new("127.0.0.1:8080").await?;
    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    Ok(())
}
