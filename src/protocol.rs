use binprot::{BinProtRead, BinProtWrite};
use binprot_derive::{BinProtRead, BinProtWrite};

use crate::sexp::Sexp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferWithLen(pub Vec<u8>);

impl BinProtRead for BufferWithLen {
    fn binprot_read<R: std::io::Read + ?Sized>(r: &mut R) -> Result<Self, binprot::Error>
    where
        Self: Sized,
    {
        let len = binprot::Nat0::binprot_read(r)?;
        let mut buf: Vec<u8> = vec![0u8; len.0 as usize];
        r.read_exact(&mut buf)?;
        Ok(BufferWithLen(buf))
    }
}

impl BinProtWrite for BufferWithLen {
    fn binprot_write<W: std::io::Write>(&self, w: &mut W) -> Result<(), std::io::Error> {
        let nat0 = binprot::Nat0(self.0.len() as u64);
        nat0.binprot_write(w)?;
        w.write_all(&self.0)?;
        Ok(())
    }
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq, Eq)]
#[polymorphic_variant]
pub enum Version {
    Version(i64),
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq, Eq)]
pub enum RpcError {
    BinIoExn(Sexp),
    ConnectionClosed,
    WriteError(Sexp),
    UncaughtExn(Sexp),
    UnimplementedRpc((String, Version)),
    UnknownQueryId(String),
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq, Eq)]
pub struct Handshake(pub Vec<i64>);

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq, Eq)]
pub struct Query<RpcTag, T> {
    pub rpc_tag: RpcTag,
    pub version: i64,
    pub id: i64,
    pub data: binprot::WithLen<T>,
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq, Eq)]
pub enum RpcResult<T> {
    Ok(binprot::WithLen<T>),
    Error(RpcError),
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq, Eq)]
pub struct Response<T> {
    pub id: i64,
    pub data: RpcResult<T>,
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq, Eq)]
pub struct ServerQuery {
    pub rpc_tag: String,
    pub version: i64,
    pub id: i64,
    pub data: BufferWithLen,
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq, Eq)]
pub enum ServerMessage<R> {
    Heartbeat,
    Query(ServerQuery),
    Response(R),
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq, Eq)]
pub enum ClientRpcResult {
    Ok(BufferWithLen),
    Error(RpcError),
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq, Eq)]
pub struct ClientResponse {
    pub id: i64,
    pub data: ClientRpcResult,
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage<RpcTag, Q> {
    Heartbeat,
    Query(Query<RpcTag, Q>),
    ClientResponse(ClientResponse),
}

pub const RPC_MAGIC_NUMBER: i64 = 4_411_474;
