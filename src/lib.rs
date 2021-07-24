// RPC Client and cerver compatible with https://github.com/janestreet/async_rpc_kernel

// TODO: Pre-allocate buffers or switch to BufReader/BufWriter + handling async in binprot
mod error;

use binprot::{BinProtRead, BinProtWrite};
use binprot_derive::{BinProtRead, BinProtWrite};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

pub use crate::error::Error;

const RPC_MAGIC_NUMBER: i64 = 4_411_474;

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq)]
struct Handshake(Vec<i64>);

#[derive(BinProtRead, BinProtWrite, Clone, PartialEq)]
enum Sexp {
    Atom(String),
    List(Vec<Sexp>),
}

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

async fn read_bin_prot<T: BinProtRead, R: AsyncReadExt + Unpin>(
    r: &mut R,
    buf: &mut Vec<u8>,
) -> Result<T, Error> {
    let mut recv_bytes = [0u8; 8];
    r.read_exact(&mut recv_bytes).await?;
    let recv_len = i64::from_le_bytes(recv_bytes);
    buf.resize(recv_len as usize, 0u8);
    r.read_exact(buf).await?;
    let mut slice = buf.as_slice();
    let data = T::binprot_read(&mut slice)?;
    Ok(data)
}

async fn write_with_size(
    w: &Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    buf: &[u8],
) -> std::io::Result<()> {
    let mut w = w.lock().await;
    let len = buf.len() as i64;
    w.write_all(&len.to_le_bytes()).await?;
    w.write_all(&buf).await?;
    Ok(())
}

async fn write_bin_prot<T: BinProtWrite>(
    w: &Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    v: &T,
    buf: &mut Vec<u8>,
) -> std::io::Result<()> {
    buf.clear();
    v.binprot_write(buf)?;
    write_with_size(w, buf).await?;
    Ok(())
}

pub trait JRpcImpl {
    type Q; // Query
    type R; // Response
    type E; // Error

    fn rpc_impl(&self, q: Self::Q) -> std::result::Result<Self::R, Self::E>;
}

trait ErasedJRpcImpl {
    fn erased_rpc_impl(&self, id: i64, payload: &[u8], buf: &mut Vec<u8>) -> Result<(), Error>;
}

impl<T> ErasedJRpcImpl for T
where
    T: JRpcImpl + Send + Sync,
    T::Q: BinProtRead + Send + Sync,
    T::R: BinProtWrite + Send + Sync,
    T::E: std::error::Error + Send,
{
    fn erased_rpc_impl(&self, id: i64, mut payload: &[u8], buf: &mut Vec<u8>) -> Result<(), Error> {
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
        ServerMessage::Response(response).binprot_write(buf)?;
        Ok(())
    }
}

pub struct RpcServer {
    rpc_impls: BTreeMap<(String, i64), Box<dyn ErasedJRpcImpl + Send + Sync>>,
}

fn spawn_heartbeat_thread(s: Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>) {
    tokio::spawn(async move {
        let mut buf = vec![0u8; 64];
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            tracing::debug!("heartbeating");
            let res = write_bin_prot(&s, &ServerMessage::Heartbeat::<()>, &mut buf).await;
            if let Err(error) = res {
                match error.kind() {
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::UnexpectedEof => break,
                    _ => {
                        tracing::error!("heartbeat failure {:?}", error);
                        break;
                    }
                }
            }
        }
    });
}

impl RpcServer {
    pub fn new() -> Self {
        RpcServer {
            rpc_impls: BTreeMap::new(),
        }
    }

    pub fn add_rpc<T: 'static>(mut self, rpc_name: &str, rpc_version: i64, impl_: T) -> Self
    where
        T: JRpcImpl + Send + Sync,
        T::Q: BinProtRead + Send + Sync,
        T::R: BinProtWrite + Send + Sync,
        T::E: std::error::Error + Send,
    {
        let impl_: Box<dyn ErasedJRpcImpl + Send + Sync> = Box::new(impl_);
        self.rpc_impls
            .insert((rpc_name.to_string(), rpc_version), impl_);
        self
    }

    async fn handle_connection(
        &self,
        stream: TcpStream,
        addr: std::net::SocketAddr,
    ) -> Result<(), Error> {
        tracing::debug!("accepted connection {:?}", addr);
        let (mut read, write) = stream.into_split();
        let write = Arc::new(Mutex::new(write));
        // TODO: use a BufReader
        let mut buf = vec![0u8; 128];
        let mut buf2 = vec![0u8; 128];
        write_bin_prot(&write, &Handshake(vec![RPC_MAGIC_NUMBER, 1]), &mut buf).await?;
        let handshake: Handshake = read_bin_prot(&mut read, &mut buf).await?;
        tracing::debug!("Handshake: {:?}", handshake);
        if handshake.0.is_empty() {
            return Err(error::Error::NoMagicNumberInHandshake);
        }
        if handshake.0[0] != RPC_MAGIC_NUMBER {
            return Err(error::Error::UnexpectedMagicNumber(handshake.0[0]));
        }

        let mut recv_bytes = [0u8; 8];
        spawn_heartbeat_thread(write.clone());

        loop {
            match read.read_exact(&mut recv_bytes).await {
                Ok(_) => {}
                Err(err) => match err.kind() {
                    std::io::ErrorKind::UnexpectedEof => return Ok(()),
                    _ => return Err(err.into()),
                },
            };
            let recv_len = i64::from_le_bytes(recv_bytes);
            buf.resize(recv_len as usize, 0u8);
            // This reads both the ServerMessage and the payload.
            read.read_exact(&mut buf).await?;
            let mut slice = buf.as_slice();
            let msg = ServerMessage::binprot_read(&mut slice)?;
            tracing::debug!("Received: {:?}", msg);
            match msg {
                ServerMessage::Heartbeat => {}
                ServerMessage::Query(q) => {
                    let rpc_key = (q.rpc_tag, q.version);
                    match self.rpc_impls.get(&rpc_key) {
                        None => {
                            let err = RpcError::UnimplementedRpc((
                                rpc_key.0,
                                Version::Version(rpc_key.1),
                            ));
                            let message = ServerMessage::Response(Response::<()> {
                                id: q.id,
                                data: RpcResult::Error(err),
                            });
                            write_bin_prot(&write, &message, &mut buf).await?
                        }
                        Some(r) => {
                            let payload_len = q.data.0 as i64;
                            if payload_len < 0 {
                                return Err(Error::IncorrectPayloadLength(payload_len));
                            }
                            let payload_offset = recv_len - payload_len;
                            let payload = &mut buf.as_mut_slice()[payload_offset as usize..];
                            buf2.clear();
                            r.erased_rpc_impl(q.id, payload, &mut buf2)?;
                            write_with_size(&write, &buf2).await?;
                        }
                    }
                }
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

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq)]
enum ClientRpcResult {
    Ok(binprot::Nat0),
    Error(RpcError),
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq)]
struct ClientResponse {
    id: i64,
    data: ClientRpcResult,
}

#[derive(BinProtRead, BinProtWrite, Debug, Clone, PartialEq)]
enum ClientMessage<Q> {
    Heartbeat,
    Query(Query<Q>),
    ClientResponse,
}

pub struct RpcClient {
    pub w: Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    pub buf: Vec<u8>,
    pub counter: std::sync::Mutex<i64>,
}

impl RpcClient {
    pub async fn new<A: tokio::net::ToSocketAddrs>(addr: A) -> Result<Self, Error> {
        let stream = TcpStream::connect(addr).await?;
        let (mut r, w) = stream.into_split();
        let w = Arc::new(Mutex::new(w));
        let mut buf = vec![0u8; 128];
        let handshake: Handshake = read_bin_prot(&mut r, &mut buf).await?;
        tracing::debug!("Handshake: {:?}", handshake);
        write_bin_prot(&w, &Handshake(vec![RPC_MAGIC_NUMBER, 1]), &mut buf).await?;

        let counter = std::sync::Mutex::new(0);
        spawn_heartbeat_thread(w.clone());

        let mut recv_bytes = [0u8; 8];
        tokio::spawn(async move {
            loop {
                match r.read_exact(&mut recv_bytes).await {
                    Ok(_) => {}
                    Err(err) => {
                        tracing::error!("socket read error: {:?}", err);
                        break;
                    }
                };
                let recv_len = i64::from_le_bytes(recv_bytes);
                buf.resize(recv_len as usize, 0u8);
                // This reads both the ClientMessage and the payload.
                if let Err(err) = r.read_exact(&mut buf).await {
                    tracing::error!("socket read error: {:?}", err);
                    break;
                }
                let mut slice = buf.as_slice();
                let msg = match ClientMessage::<()>::binprot_read(&mut slice) {
                    Err(err) => {
                        tracing::error!("unexpected message format: {:?}", err);
                        break;
                    }
                    Ok(msg) => msg,
                };
                tracing::debug!("Client received: {:?}", msg);
            }
        });
        let buf = vec![0u8; 128];
        Ok(RpcClient { w, buf, counter })
    }

    pub fn get_id(&mut self) -> i64 {
        let mut counter = self.counter.lock().unwrap();
        let res = *counter;
        *counter += 1;
        res
    }

    pub async fn dispatch<Q, R>(&mut self, rpc_tag: String, version: i64, q: Q) -> Result<Q, Error>
    where
        Q: BinProtWrite + Send + Sync,
        R: BinProtRead + Send + Sync,
    {
        let id = self.get_id();
        let message = ClientMessage::Query(Query::<Q> {
            rpc_tag,
            version,
            id,
            data: binprot::WithLen(q),
        });
        write_bin_prot(&self.w, &message, &mut self.buf).await?;
        unimplemented!()
    }
}
