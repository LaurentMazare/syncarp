# syncarp
An async RPC implementation based on tokio and compatible with OCaml
[Async_rpc](https://github.com/janestreet/async/tree/master/async_rpc).

This uses [tokio](https://tokio.rs/) and relies on
[binprot-rs](https://github.com/LaurentMazare/binprot-rs) for message
serialization.

A simple server matching the [OCaml
example](https://github.com/janestreet/async/tree/master/async_rpc/example) can
be implemented as follows: 

```rust
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

#[tokio::main]
async fn main() -> Result<(), syncarp::Error> {
    let get_unique_id_impl = GetUniqueIdImpl(Mutex::new(0));
    syncarp::RpcServer::new("127.0.0.1")
        .await?
        .add_rpc(get_unique_id_impl)
        .run()
        .await?;
    Ok(())
}
```
