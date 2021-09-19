use binprot::{BinProtRead, BinProtWrite};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::error::Error;
use crate::protocol::ServerMessage;

pub async fn read_bin_prot<T: BinProtRead, R: AsyncReadExt + Unpin>(
    r: &mut R,
    buf: &mut Vec<u8>,
) -> Result<T, Error> {
    let mut recv_bytes = [0u8; 8];
    r.read_exact(&mut recv_bytes).await?;
    let recv_len = i64::from_le_bytes(recv_bytes);
    buf.resize(recv_len as usize, 0u8);
    r.read_exact(buf).await?;
    let data = T::binprot_read(&mut buf.as_slice())?;
    Ok(data)
}

// WriteOrClosed is a wrapper around the writer so that it is possible
// to close an Arc<Mutex<WriteOrClosed>> that is shared between multiple
// threads.
// This is useful as the writer is passed to the thread handling the
// heartbeating.
pub enum WriteOrClosed {
    Write(tokio::net::tcp::OwnedWriteHalf),
    Closed,
}

pub async fn write_with_size(w: &Arc<Mutex<WriteOrClosed>>, buf: &[u8]) -> std::io::Result<()> {
    let mut w = w.lock().await;
    match &mut *w {
        WriteOrClosed::Write(w) => {
            let len = buf.len() as i64;
            w.write_all(&len.to_le_bytes()).await?;
            w.write_all(buf).await?;
            Ok(())
        }
        WriteOrClosed::Closed => {
            let error = std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "connection closed by server",
            );
            Err(error)
        }
    }
}

pub async fn write_bin_prot<T: BinProtWrite>(
    w: &Arc<Mutex<WriteOrClosed>>,
    v: &T,
    buf: &mut Vec<u8>,
) -> std::io::Result<()> {
    buf.clear();
    v.binprot_write(buf)?;
    write_with_size(w, buf).await?;
    Ok(())
}

pub fn spawn_heartbeat_thread(s: Arc<Mutex<WriteOrClosed>>) {
    tokio::spawn(async move {
        let mut buf = vec![0u8; 64];
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            tracing::debug!("heartbeating");
            let res = write_bin_prot(&s, &ServerMessage::Heartbeat::<()>, &mut buf).await;
            if let Err(error) = res {
                match error.kind() {
                    std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionAborted => break,
                    _ => {
                        tracing::error!("heartbeat failure {:?}", error);
                        break;
                    }
                }
            }
        }
    });
}
