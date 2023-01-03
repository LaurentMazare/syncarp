use binprot::{BinProtRead, BinProtWrite};
use std::collections::BTreeMap;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::error::Error;
use crate::protocol::*;
use crate::read_write::*;
use crate::sexp::Sexp;
use crate::traits::{JRpc, JRpcImpl};

trait ErasedJRpcImpl {
    fn erased_rpc_impl(&self, id: i64, payload: &[u8], buf: &mut Vec<u8>) -> Result<(), Error>;
}

impl<T> ErasedJRpcImpl for T
where
    T: JRpcImpl + Send + Sync,
    <T::JRpc as JRpc>::Q: BinProtRead + Send + Sync,
    <T::JRpc as JRpc>::R: BinProtWrite + Send + Sync,
    T::E: std::error::Error + Send,
{
    fn erased_rpc_impl(&self, id: i64, mut payload: &[u8], buf: &mut Vec<u8>) -> Result<(), Error> {
        let query = <T::JRpc as JRpc>::Q::binprot_read(&mut payload)?;
        let rpc_result = match self.rpc_impl(query) {
            Ok(response) => RpcResult::Ok(binprot::WithLen(response)),
            Err(error) => {
                let sexp = Sexp::Atom(error.to_string());
                RpcResult::Error(RpcError::UncaughtExn(sexp))
            }
        };
        let response = Response { id, data: rpc_result };
        ServerMessage::Response(response).binprot_write(buf)?;
        Ok(())
    }
}

pub struct RpcServer {
    rpc_impls: BTreeMap<(String, i64), Box<dyn ErasedJRpcImpl + Send + Sync>>,
    listener: TcpListener,
}

impl RpcServer {
    pub fn local_addr(&self) -> Result<std::net::SocketAddr, std::io::Error> {
        self.listener.local_addr()
    }

    pub fn add_rpc<T: 'static>(mut self, impl_: T) -> Self
    where
        T: JRpcImpl + Send + Sync,
        <T::JRpc as JRpc>::Q: BinProtRead + Send + Sync,
        <T::JRpc as JRpc>::R: BinProtWrite + Send + Sync,
        T::E: std::error::Error + Send,
    {
        let impl_: Box<dyn ErasedJRpcImpl + Send + Sync> = Box::new(impl_);
        self.rpc_impls.insert((T::JRpc::RPC_NAME.to_string(), T::JRpc::RPC_VERSION), impl_);
        self
    }

    async fn handle_connection_(
        rpc_impls: &BTreeMap<(String, i64), Box<dyn ErasedJRpcImpl + Send + Sync>>,
        mut read: tokio::net::tcp::OwnedReadHalf,
        write: Arc<Mutex<WriteOrClosed>>,
        addr: std::net::SocketAddr,
    ) -> Result<(), Error> {
        tracing::debug!("accepted connection {:?}", addr);
        let mut buf = vec![0u8; 128];
        let mut buf2 = vec![0u8; 128];
        write_bin_prot(&write, &Handshake(vec![RPC_MAGIC_NUMBER, 1]), &mut buf).await?;
        let handshake: Handshake = read_bin_prot(&mut read, &mut buf).await?;
        tracing::debug!("handshake: {:?}", handshake);
        if handshake.0.is_empty() {
            return Err(Error::NoMagicNumberInHandshake);
        }
        if handshake.0[0] != RPC_MAGIC_NUMBER {
            return Err(Error::UnexpectedMagicNumber(handshake.0[0]));
        }

        let mut recv_bytes = [0u8; 8];
        let heartbeat_done = spawn_heartbeat_thread(write.clone());

        loop {
            if let Err(err) = read.read_exact(&mut recv_bytes).await {
                match err.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        heartbeat_done.store(true, std::sync::atomic::Ordering::Relaxed);
                        tracing::debug!("connection closed {:?}", addr);
                        return Ok(());
                    }
                    _ => return Err(err.into()),
                }
            };
            let recv_len = i64::from_le_bytes(recv_bytes);
            buf.resize(recv_len as usize, 0u8);
            read.read_exact(&mut buf).await?;
            let msg = ServerMessage::binprot_read(&mut buf.as_slice())?;
            tracing::debug!("Received: {:?}", msg);
            match msg {
                ServerMessage::Heartbeat => {}
                ServerMessage::Query(q) => {
                    let rpc_key = (q.rpc_tag, q.version);
                    match rpc_impls.get(&rpc_key) {
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

    async fn handle_connection(
        rpc_impls: &BTreeMap<(String, i64), Box<dyn ErasedJRpcImpl + Send + Sync>>,
        stream: TcpStream,
        addr: std::net::SocketAddr,
    ) -> Result<(), Error> {
        tracing::debug!("accepted connection {:?}", addr);
        let (read, write) = stream.into_split();
        let write = Arc::new(Mutex::new(WriteOrClosed::Write(write)));
        let res = RpcServer::handle_connection_(rpc_impls, read, write.clone(), addr).await;
        let mut write_or_closed = write.lock().await;
        let write_or_closed = write_or_closed.deref_mut();
        // Extracting the writer results in it being closed if this
        // was not already the case.
        let _writer = std::mem::replace(write_or_closed, WriteOrClosed::Closed);
        res
    }

    pub async fn new<A: tokio::net::ToSocketAddrs>(addr: A) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr).await?;
        tracing::debug!("listening");
        let rpc_server = RpcServer { rpc_impls: BTreeMap::new(), listener };
        Ok(rpc_server)
    }

    pub async fn run(self) -> Result<(), Error> {
        let rc = Arc::new(self.rpc_impls);
        loop {
            let rc = rc.clone();
            let (stream, addr) = self.listener.accept().await?;
            tokio::spawn(async move {
                if let Err(e) = RpcServer::handle_connection(&rc, stream, addr).await {
                    tracing::info!("error handling connection {:?} {:?}", addr, e);
                }
            });
        }
    }
}
