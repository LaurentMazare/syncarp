// RPC Client and cerver compatible with https://github.com/janestreet/async_rpc_kernel
// RPC magic number 4_411_474

// TODO: Pre-allocate buffers or switch to BufReader/BufWriter + handling async in binprot
// TODO: Specific error type.
// TODO: Handle heartbeating.
// TODO: Handle the RPC Version.
// TODO: Make a RpcClient.
use async_trait::async_trait;
use binprot::{BinProtRead, BinProtSize, BinProtWrite};
use binprot_derive::{BinProtRead, BinProtWrite};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq)]
struct Handshake(Vec<i64>);

#[derive(BinProtRead, BinProtWrite, Clone, PartialEq)]
enum Sexp {
    Atom(String),
    List(Vec<Sexp>),
}

pub type Error = binprot::Error;

// Dummy formatter, escaping is not handled properly.
impl std::fmt::Debug for Sexp {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Sexp::Atom(atom) => {
                if atom.contains(|c: char| !c.is_alphanumeric()) {
                    fmt.write_str("\"")?;
                    for c in atom.escape_default() {
                        std::fmt::Write::write_char(fmt, c)?;
                    }
                    fmt.write_str("\"")?;
                } else {
                    fmt.write_str(&atom)?;
                }
                Ok(())
            }
            Sexp::List(list) => {
                fmt.write_str("(")?;
                for (index, sexp) in list.iter().enumerate() {
                    if index > 0 {
                        fmt.write_str(" ")?;
                    }
                    sexp.fmt(fmt)?;
                }
                fmt.write_str(")")?;
                Ok(())
            }
        }
    }
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq)]
struct Query<T> {
    rpc_tag: String,
    version: i64,
    id: i64,
    data: binprot::WithLen<T>,
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq)]
#[polymorphic_variant]
enum Version {
    Version(i64),
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq)]
enum RpcError {
    BinIoExn(Sexp),
    ConnectionClosed,
    WriteError(Sexp),
    UncaughtExn(Sexp),
    UnimplementedRpc((String, Version)),
    UnknownQueryId(String),
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq)]
enum RpcResult<T> {
    Ok(binprot::WithLen<T>),
    Error(RpcError),
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq)]
struct Response<T> {
    id: i64,
    data: RpcResult<T>,
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq)]
struct ServerQuery {
    rpc_tag: String,
    version: i64,
    id: i64,
    data: binprot::Nat0,
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq)]
enum ServerMessage<R> {
    Heartbeat,
    Query(ServerQuery),
    Response(R),
}

async fn read_bin_prot<T: BinProtRead>(s: &mut TcpStream) -> Result<T, Error> {
    let mut recv_bytes = [0u8; 8];
    s.read_exact(&mut recv_bytes).await?;
    let recv_len = i64::from_le_bytes(recv_bytes);
    // TODO: pre-allocate the buffer or use a BufReader
    let mut buf = vec![0u8; recv_len as usize];
    s.read_exact(&mut buf).await?;
    let mut slice = buf.as_slice();
    let data = T::binprot_read(&mut slice)?;
    Ok(data)
}

async fn write_bin_prot<T: BinProtWrite>(s: &mut TcpStream, v: &T) -> std::io::Result<()> {
    let len = v.binprot_size() as i64;
    s.write_all(&len.to_le_bytes()).await?;
    // TODO: pre-allocate the buffer or use a BufWriter
    let mut buf = vec![];
    v.binprot_write(&mut buf)?;
    s.write_all(&buf).await?;
    Ok(())
}

pub trait JRpcImpl {
    type Q; // Query
    type R; // Response
    type E; // Error

    fn rpc_impl(&self, q: Self::Q) -> std::result::Result<Self::R, Self::E>;
}

#[async_trait]
trait ErasedJRpcImpl {
    async fn erased_rpc_impl(
        &self,
        stream: &mut TcpStream,
        id: i64,
        mut payload: &[u8],
    ) -> Result<(), Error>;
}

//impl<Q, R, E> ErasedJRpcImpl for dyn JRpcImpl<Q = Q, R = R, E = E>
//where
//   Q: BinProtRead,
//    R: BinProtWrite,
//    E: std::error::Error,
#[async_trait]
impl<T> ErasedJRpcImpl for T
where
    T: JRpcImpl + Send + Sync,
    T::Q: BinProtRead + Send + Sync,
    T::R: BinProtWrite + Send + Sync,
    T::E: std::error::Error + Send,
{
    async fn erased_rpc_impl(
        &self,
        s: &mut TcpStream,
        id: i64,
        mut payload: &[u8],
    ) -> Result<(), Error> {
        let query = T::Q::binprot_read(&mut payload)?;
        let rpc_result = match self.rpc_impl(query) {
            Ok(response) => RpcResult::Ok(binprot::WithLen(response)),
            Err(error) => {
                let sexp = Sexp::Atom(error.to_string());
                RpcResult::Error(RpcError::UncaughtExn(sexp))
            }
        };
        let response = Response {
            id,
            data: rpc_result,
        };
        write_bin_prot(s, &ServerMessage::Response(response)).await?;
        Ok(())
    }
}

pub struct RpcServer {
    rpc_impls: BTreeMap<String, Box<dyn ErasedJRpcImpl + Send + Sync>>,
}

impl RpcServer {
    pub fn new() -> Self {
        RpcServer {
            rpc_impls: BTreeMap::new(),
        }
    }

    pub fn add_rpc<T: 'static>(mut self, rpc_name: &str, impl_: T) -> Self
    where
        T: JRpcImpl + Send + Sync,
        T::Q: BinProtRead + Send + Sync,
        T::R: BinProtWrite + Send + Sync,
        T::E: std::error::Error + Send,
    {
        let impl_: Box<dyn ErasedJRpcImpl + Send + Sync> = Box::new(impl_);
        self.rpc_impls.insert(rpc_name.to_string(), impl_);
        self
    }

    async fn handle_connection(
        &self,
        mut stream: TcpStream,
        addr: std::net::SocketAddr,
    ) -> Result<(), Error> {
        tracing::debug!("accepted connection {:?}", addr);
        write_bin_prot(&mut stream, &Handshake(vec![4411474, 1])).await?;
        let handshake: Handshake = read_bin_prot(&mut stream).await?;
        tracing::debug!("Handshake: {:?}", handshake);

        let mut recv_bytes = [0u8; 8];
        // TODO: use a BufReader
        let mut buf = vec![0u8; 128];

        loop {
            match stream.read_exact(&mut recv_bytes).await {
                Ok(_) => {}
                Err(err) => match err.kind() {
                    std::io::ErrorKind::UnexpectedEof => return Ok(()),
                    _ => return Err(err.into()),
                },
            };
            let recv_len = i64::from_le_bytes(recv_bytes);
            buf.resize(recv_len as usize, 0u8);
            // This reads both the ServerMessage and the payload.
            stream.read_exact(&mut buf).await?;
            let mut slice = buf.as_slice();
            let msg = ServerMessage::binprot_read(&mut slice)?;
            tracing::debug!("Received: {:?}", msg);
            match msg {
                ServerMessage::Heartbeat => {}
                ServerMessage::Query(query) => match self.rpc_impls.get(&query.rpc_tag) {
                    None => {
                        let err = RpcError::UnimplementedRpc((
                            query.rpc_tag,
                            Version::Version(query.version),
                        ));
                        let message = ServerMessage::Response(Response::<()> {
                            id: query.id,
                            data: RpcResult::Error(err),
                        });
                        write_bin_prot(&mut stream, &message).await?
                    }
                    Some(r) => {
                        // TODO: test query_data.0, and have a dedicated error.
                        let payload_offset = recv_len - (query.data.0 as i64);
                        let payload = &mut buf.as_mut_slice()[payload_offset as usize..];
                        let future = r.erased_rpc_impl(&mut stream, query.id, payload);
                        future.await?
                    }
                },
                ServerMessage::Response(()) => unimplemented!(),
            };
        }
    }

    pub async fn start<A: tokio::net::ToSocketAddrs>(self, addr: A) -> Result<(), Error> {
        let rc = Arc::new(self);
        let listener = TcpListener::bind(addr).await?;
        tracing::debug!("listening");
        loop {
            let rc = rc.clone();
            let (stream, addr) = listener.accept().await?;
            tokio::spawn(async move {
                if let Err(e) = (*rc).handle_connection(stream, addr).await {
                    tracing::info!("error handling connection {:?} {:?}", addr, e);
                }
            });
        }
    }
}

impl Default for RpcServer {
    fn default() -> Self {
        Self::new()
    }
}