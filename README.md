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
struct GetUniqueIdImpl(Mutex<i64>);

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

#[tokio::main]
async fn main() -> Result<(), syncarp::Error> {
    let get_unique_id_impl = GetUniqueIdImpl(Mutex::new(0));
    syncarp::RpcServer::new()
        .add_rpc("get-unique-id", /*version=*/0, get_unique_id_impl)
        .start("127.0.0.1:8080")
        .await?;
    Ok(())
}
```
