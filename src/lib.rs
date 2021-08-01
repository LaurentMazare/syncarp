// RPC Client and cerver compatible with https://github.com/janestreet/async_rpc_kernel

// TODO: Maybe switch to BufReader/BufWriter + handling async in binprot
// TODO: Add some failure when reading more or less data than expected.
mod error;
mod protocol;
mod read_write;
mod rpc_client;
mod rpc_server;
mod sexp;

pub use crate::error::Error;
pub use crate::rpc_client::*;
pub use crate::rpc_server::*;
pub use crate::sexp::Sexp;
