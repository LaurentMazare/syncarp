use binprot::{BinProtRead, BinProtWrite};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex};

use crate::error::Error;
use crate::protocol::*;
use crate::read_write::*;

type OneShots = BTreeMap<i64, oneshot::Sender<Result<ClientRpcResult, Error>>>;

pub struct RpcClient {
    w: Arc<Mutex<WriteOrClosed>>,
    buf: Vec<u8>,
    id_and_oneshots: Arc<Mutex<(i64, OneShots)>>,
}

impl RpcClient {
    pub async fn new<A: tokio::net::ToSocketAddrs>(addr: A) -> Result<Self, Error> {
        let stream = TcpStream::connect(addr).await?;
        let (mut r, w) = stream.into_split();
        let w = Arc::new(Mutex::new(WriteOrClosed::Write(w)));
        let mut buf = vec![0u8; 128];
        let handshake: Handshake = read_bin_prot(&mut r, &mut buf).await?;
        tracing::debug!("Handshake: {:?}", handshake);
        write_bin_prot(&w, &Handshake(vec![RPC_MAGIC_NUMBER, 1]), &mut buf).await?;

        let oneshots: OneShots = BTreeMap::new();
        let id_and_oneshots = Arc::new(Mutex::new((0i64, oneshots)));
        let _heartbeat_done = spawn_heartbeat_thread(w.clone());

        let id_and_os = id_and_oneshots.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 128];
            let mut recv_bytes = [0u8; 8];
            loop {
                if let Err(err) = r.read_exact(&mut recv_bytes).await {
                    tracing::error!("socket read error: {:?}", err);
                    break;
                };
                let recv_len = i64::from_le_bytes(recv_bytes);
                buf.resize(recv_len as usize, 0u8);
                if let Err(err) = r.read_exact(&mut buf).await {
                    tracing::error!("socket read error: {:?}", err);
                    break;
                }
                let msg = match ClientMessage::<String, ()>::binprot_read(&mut buf.as_slice()) {
                    Err(err) => {
                        tracing::error!("unexpected message format: {:?}", err);
                        break;
                    }
                    Ok(msg) => msg,
                };
                tracing::debug!("client received: {:?}", msg);
                match msg {
                    ClientMessage::Heartbeat => {}
                    ClientMessage::Query(_) => {
                        tracing::error!("client received an unexpected query");
                    }
                    ClientMessage::ClientResponse(resp) => {
                        let mut id_and_oneshots = id_and_os.lock().await;
                        let (_id, oneshots) = &mut *id_and_oneshots;
                        match oneshots.remove(&resp.id) {
                            None => {
                                tracing::error!("client received an unexpected id: {}", resp.id);
                            }
                            Some(tx) => {
                                if let Err(err) = tx.send(Ok(resp.data)) {
                                    tracing::error!("client tx error: {:?}", err)
                                }
                            }
                        }
                    }
                }
            }
            let mut id_and_oneshots = id_and_os.lock().await;
            let id = id_and_oneshots.0;
            let (_id, oneshots) = std::mem::replace(&mut *id_and_oneshots, (id, BTreeMap::new()));
            for (_key, tx) in oneshots.into_iter() {
                let error = std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "connection closed by server",
                );
                if let Err(err) = tx.send(Err(error.into())) {
                    tracing::error!("client tx error: {:?}", err)
                }
            }
        });
        Ok(RpcClient {
            w,
            buf,
            id_and_oneshots,
        })
    }

    // Registers a fresh id and get back both the id and the
    // reader.
    async fn register_new_id(
        &mut self,
    ) -> (i64, oneshot::Receiver<Result<ClientRpcResult, Error>>) {
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
        let rpc_result = rx.await??;
        match rpc_result {
            ClientRpcResult::Ok(buffer_with_len) => {
                let result = R::binprot_read(&mut buffer_with_len.0.as_slice())?;
                Ok(result)
            }
            ClientRpcResult::Error(err) => Err(err.into()),
        }
    }
}
