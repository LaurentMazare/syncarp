use std::sync::{Arc, Mutex};

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

#[tokio::main]
async fn main() -> Result<(), syncarp::Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let counter = Arc::new(Mutex::new(0));
    let get_unique_id_impl = GetUniqueIdImpl(counter.clone());
    let set_id_counter_impl = SetIdCounterImpl(counter.clone());
    syncarp::RpcServer::new("127.0.0.1:8080")
        .await?
        .add_rpc("get-unique-id", 0, get_unique_id_impl)
        .add_rpc("set-id-counter", 1, set_id_counter_impl)
        .run()
        .await?;
    Ok(())
}
