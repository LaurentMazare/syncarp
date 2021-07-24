// RPC Client and cerver compatible with https://github.com/janestreet/async_rpc_kernel

// TODO: Pre-allocate buffers or switch to BufReader/BufWriter + handling async in binprot
mod error;
mod protocol;
mod sexp;

use async_trait::async_trait;
use binprot::{BinProtRead, BinProtWrite};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Mutex};

pub use crate::error::Error;
use crate::protocol::*;
pub use crate::sexp::Sexp;

const RPC_MAGIC_NUMBER: i64 = 4_411_474;

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
                            buf2.clear();
                            r.erased_rpc_impl(q.id, &q.data.0, &mut buf2)?;
                            write_with_size(&write, &buf2).await?;
                        }
                    }
                }
                ServerMessage::Response(()) => {
                    tracing::error!("server received a response message")
                }
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

type OneShots = BTreeMap<i64, oneshot::Sender<ClientRpcResult>>;

pub struct RpcClient {
    w: Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    buf: Vec<u8>,
    id_and_oneshots: Arc<Mutex<(i64, OneShots)>>,
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

        let oneshots: OneShots = BTreeMap::new();
        let id_and_oneshots = Arc::new(Mutex::new((0i64, oneshots)));
        spawn_heartbeat_thread(w.clone());

        let mut recv_bytes = [0u8; 8];
        let id_and_os = id_and_oneshots.clone();
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
                let msg = match ClientMessage::<String, ()>::binprot_read(&mut slice) {
                    Err(err) => {
                        tracing::error!("unexpected message format: {:?}", err);
                        break;
                    }
                    Ok(msg) => msg,
                };
                tracing::debug!("Client received: {:?}", msg);
                match msg {
                    ClientMessage::Heartbeat => {}
                    ClientMessage::Query(_) => {
                        tracing::error!("client received an unexpected query");
                    }
                    ClientMessage::ClientResponse(response) => {
                        let mut id_and_oneshots = id_and_os.lock().await;
                        let (_id, oneshots) = &mut *id_and_oneshots;
                        match oneshots.remove(&response.id) {
                            None => {
                                tracing::debug!(
                                    "Client received an unexpected id: {}",
                                    response.id
                                );
                            }
                            Some(tx) => match tx.send(response.data) {
                                Ok(()) => {}
                                Err(err) => {
                                    tracing::debug!(
                                        "Client cannot communicate with original thread: {:?}",
                                        err
                                    )
                                }
                            },
                        }
                    }
                }
            }
        });
        let buf = vec![0u8; 128];
        Ok(RpcClient {
            w,
            buf,
            id_and_oneshots,
        })
    }

    // Registers a fresh id and get back both the id and the
    // reader.
    async fn register_new_id(&mut self) -> (i64, oneshot::Receiver<ClientRpcResult>) {
        let mut id_and_oneshots = self.id_and_oneshots.lock().await;
        let id_and_oneshots = &mut *id_and_oneshots;
        let (tx, rx) = oneshot::channel();
        let fresh_id = id_and_oneshots.0;
        id_and_oneshots.1.insert(fresh_id, tx);
        id_and_oneshots.0 += 1;
        (fresh_id, rx)
    }

    pub async fn dispatch<Q, R>(&mut self, rpc_tag: &str, version: i64, q: Q) -> Result<R, Error>
    where
        Q: BinProtWrite,
        R: BinProtRead + Send + Sync,
    {
        let (id, rx) = self.register_new_id().await;
        let message = ClientMessage::Query(Query::<&str, Q> {
            rpc_tag,
            version,
            id,
            data: binprot::WithLen(q),
        });
        write_bin_prot(&self.w, &message, &mut self.buf).await?;
        let rpc_result = rx.await?;
        match rpc_result {
            ClientRpcResult::Ok(buffer_with_len) => {
                let mut slice = buffer_with_len.0.as_slice();
                let result = R::binprot_read(&mut slice)?;
                Ok(result)
            }
            ClientRpcResult::Error(err) => Err(err.into()),
        }
    }
}

#[async_trait]
pub trait Rpc {
    type Q; // Query
    type R; // Response

    const RPC_NAME: &'static str;
    const RPC_VERSION: i64;

    async fn dispatch(rpc_client: &mut RpcClient, q: Self::Q) -> Result<Self::R, Error>
    where
        Self::Q: BinProtWrite + Send + Sync,
        Self::R: BinProtRead + Send + Sync,
    {
        rpc_client
            .dispatch(&Self::RPC_NAME, Self::RPC_VERSION, q)
            .await
    }
}
