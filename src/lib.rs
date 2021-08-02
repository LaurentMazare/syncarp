// RPC Client and cerver compatible with https://github.com/janestreet/async_rpc_kernel

// TODO: Maybe switch to BufReader/BufWriter + handling async in binprot
// TODO: Add some failure when reading more or less data than expected.
mod error;
mod protocol;
mod read_write;
mod rpc_client;
mod rpc_server;
mod sexp;
mod traits;

pub use crate::error::Error;
pub use crate::rpc_client::RpcClient;
pub use crate::rpc_server::RpcServer;
pub use crate::sexp::Sexp;
pub use crate::traits::{JRpc, JRpcImpl};
