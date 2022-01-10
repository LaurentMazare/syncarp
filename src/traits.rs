use async_trait::async_trait;
use binprot::{BinProtRead, BinProtWrite};

use crate::error::Error;
use crate::rpc_client::RpcClient;

#[async_trait]
pub trait JRpc {
    type Q; // Query
    type R; // Response

    const RPC_NAME: &'static str;
    const RPC_VERSION: i64;

    async fn dispatch(rpc_client: &mut RpcClient, q: Self::Q) -> Result<Self::R, Error>
    where
        Self::Q: BinProtWrite + Send + Sync,
        Self::R: BinProtRead + Send + Sync,
    {
        rpc_client.dispatch(Self::RPC_NAME, Self::RPC_VERSION, q).await
    }
}

pub trait JRpcImpl {
    type E; // Error
    type JRpc: JRpc;

    fn rpc_impl(
        &self,
        q: <Self::JRpc as JRpc>::Q,
    ) -> std::result::Result<<Self::JRpc as JRpc>::R, Self::E>;
}
