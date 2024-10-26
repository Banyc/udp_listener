use std::{
    collections::HashMap,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use bytes::BytesMut;
use primitive::obj_pool::{ArcObjectPool, ObjectScoped};
use tokio::net::UdpSocket;

pub const BUFFER_LENGTH: usize = 2_usize.pow(16);
const OBJ_POOL_SHARDS: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(4) };

pub type Packet = ObjectScoped<BytesMut>;

pub type Dispatch<K, V> = Arc<dyn Fn(SocketAddr, Packet) -> Option<(K, V)> + Sync + Send + 'static>;

type ConnTable<K, V> = Arc<Mutex<HashMap<K, tokio::sync::mpsc::Sender<V>>>>;

/// Manage user-defined sub-connections under a UDP socket.
pub struct UdpListener<K, V> {
    is_udp_connected: bool,
    udp: Arc<UdpSocket>,
    conn_table: ConnTable<K, V>,
    pkt_buf_pool: ArcObjectPool<BytesMut>,
    dispatcher_buffer_size: NonZeroUsize,
    dispatch: Dispatch<K, V>,
}
impl UdpListener<SocketAddr, Packet> {
    /// Construct a TCP-like listener using peer addresses as dispatch keys.
    pub fn new_identity_dispatch(
        unconnected_udp: UdpSocket,
        dispatcher_buffer_size: NonZeroUsize,
    ) -> Self {
        let dispatch = |addr: SocketAddr, packet: Packet| Some((addr, packet));
        UdpListener::new(unconnected_udp, dispatcher_buffer_size, Arc::new(dispatch))
    }
}
impl<K, V> UdpListener<K, V> {
    pub fn new(
        udp: UdpSocket,
        dispatcher_buffer_size: NonZeroUsize,
        dispatch: Dispatch<K, V>,
    ) -> Self {
        let pkt_buf_pool = ArcObjectPool::new(
            usize::try_from(u32::MAX).unwrap(),
            OBJ_POOL_SHARDS,
            || BytesMut::with_capacity(BUFFER_LENGTH),
            |buf| buf.clear(),
        );
        Self {
            is_udp_connected: udp.peer_addr().is_ok(),
            udp: Arc::new(udp),
            conn_table: Arc::new(Mutex::new(HashMap::new())),
            pkt_buf_pool,
            dispatcher_buffer_size,
            dispatch,
        }
    }
}
impl<K, V> UdpListener<K, V>
where
    K: Clone + core::hash::Hash + Eq + Sync + Send + 'static,
    V: Sync + Send + 'static,
{
    /// Side-effect: This method also dispatches packets to all the accepted sub-connections.
    ///
    /// You should keep this method in a loop.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn accept(&self) -> std::io::Result<Conn<K, V>> {
        loop {
            let mut pkt_buf = self.pkt_buf_pool.take_scoped();
            let (n, addr) = if self.is_udp_connected {
                let n = self.udp.recv_buf(&mut *pkt_buf).await?;
                let addr = self.udp.peer_addr()?;
                (n, addr)
            } else {
                self.udp.recv_buf_from(&mut *pkt_buf).await?
            };
            if n == BUFFER_LENGTH {
                continue;
            }

            let Some((key, mut value)) = (self.dispatch)(addr, pkt_buf) else {
                continue;
            };

            let mut conn_table = self.conn_table.lock().unwrap();

            if let Some(tx) = conn_table.get(&key) {
                let Err(e) = tx.try_send(value) else {
                    continue;
                };
                match e {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => {
                        continue;
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(v) => value = v,
                }
            }

            let (tx, rx) = tokio::sync::mpsc::channel(self.dispatcher_buffer_size.get());
            tx.try_send(value).unwrap();
            conn_table.insert(key.clone(), tx);

            drop(conn_table);

            return Ok(self.wrap_handle(key, rx, addr));
        }
    }

    /// This method is intended to open a sub-connection under a connected UDP socket.
    ///
    /// You still need to put [`Self::accept()`] in a loop to drive the packet dispatch among the sub-connections.
    ///
    /// Return [`None`] if either:
    ///
    /// - The UDP socket is unconnected;
    /// - The `conn_key` has already been registered in the connection table.
    pub fn open(&self, conn_key: K) -> Option<Conn<K, V>> {
        let peer_addr = self.udp.peer_addr().ok()?;
        let mut conn_table = self.conn_table.lock().unwrap();
        if conn_table.get(&conn_key).is_some() {
            return None;
        }
        let (tx, rx) = tokio::sync::mpsc::channel(self.dispatcher_buffer_size.get());
        conn_table.insert(conn_key.clone(), tx);
        drop(conn_table);
        Some(self.wrap_handle(conn_key, rx, peer_addr))
    }

    /// Pass in `peer_addr` as [`None`] iff the underlying UDP socket is connected.
    fn wrap_handle(
        &self,
        conn_key: K,
        rx: tokio::sync::mpsc::Receiver<V>,
        peer_addr: SocketAddr,
    ) -> Conn<K, V> {
        let close_token = ConnCloseToken {
            conn_key: conn_key.clone(),
            conn_table: self.conn_table.clone(),
        };
        let close_token = Arc::new(close_token);
        let read = ConnRead {
            recv: rx,
            _close_token: close_token.clone(),
        };
        let udp_to = if self.is_udp_connected {
            assert_eq!(peer_addr, self.udp.peer_addr().unwrap());
            None
        } else {
            Some(peer_addr)
        };
        let write = ConnWrite {
            udp: Arc::clone(&self.udp),
            peer: udp_to,
            _close_token: close_token,
        };
        Conn {
            read,
            write,
            conn_key,
        }
    }
}
impl<K, V> core::fmt::Debug for UdpListener<K, V>
where
    K: core::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpListener")
            .field("udp", &self.udp)
            .field("accepted", &self.conn_table)
            .field("dispatcher_buffer_size", &self.dispatcher_buffer_size)
            .finish()
    }
}

trait StaticDrop: Sync + Send + 'static {}
impl<K, V> StaticDrop for ConnCloseToken<K, V>
where
    K: Clone + core::hash::Hash + Eq + Sync + Send + 'static,
    V: Sync + Send + 'static,
{
}

struct ConnCloseToken<K, V>
where
    K: Clone + core::hash::Hash + Eq,
{
    conn_key: K,
    conn_table: ConnTable<K, V>,
}
impl<K, V> Drop for ConnCloseToken<K, V>
where
    K: Clone + core::hash::Hash + Eq,
{
    fn drop(&mut self) {
        let mut conn_table = self.conn_table.lock().unwrap();
        conn_table.remove(&self.conn_key);
    }
}
impl<K, V> core::fmt::Debug for ConnCloseToken<K, V>
where
    K: core::fmt::Debug + Clone + core::hash::Hash + Eq,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceptedUdpCloseToken")
            .field("dispatch_key", &self.conn_key)
            .finish()
    }
}

/// A sub-connection derived from a UDP listener
pub struct Conn<K, V> {
    read: ConnRead<V>,
    write: ConnWrite,
    conn_key: K,
}
impl<K, V> Conn<K, V> {
    pub fn read(&mut self) -> &mut ConnRead<V> {
        &mut self.read
    }

    pub fn write(&self) -> &ConnWrite {
        &self.write
    }

    pub fn conn_key(&self) -> &K {
        &self.conn_key
    }

    pub fn split(self) -> (ConnRead<V>, ConnWrite) {
        (self.read, self.write)
    }
}
impl<K: core::fmt::Debug, V> core::fmt::Debug for Conn<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Conn")
            .field("read", &self.read)
            .field("write", &self.write)
            .field("conn_key", &self.conn_key)
            .finish()
    }
}

pub struct ConnRead<V> {
    recv: tokio::sync::mpsc::Receiver<V>,
    _close_token: Arc<dyn StaticDrop>,
}
impl<V> ConnRead<V> {
    pub fn recv(&mut self) -> &mut tokio::sync::mpsc::Receiver<V> {
        &mut self.recv
    }
}
impl<V> core::fmt::Debug for ConnRead<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnRead")
            .field("recv.len()", &self.recv.len())
            .finish()
    }
}

#[derive(Clone)]
pub struct ConnWrite {
    udp: Arc<UdpSocket>,
    peer: Option<SocketAddr>,
    _close_token: Arc<dyn StaticDrop>,
}
impl ConnWrite {
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.udp.local_addr()
    }

    pub fn peer_addr(&self) -> SocketAddr {
        match self.peer {
            Some(x) => x,
            None => self.udp.peer_addr().unwrap(),
        }
    }

    pub async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        match self.peer {
            Some(peer) => self.udp.send_to(buf, peer).await,
            None => self.udp.send(buf).await,
        }
    }

    pub fn try_send(&self, buf: &[u8]) -> std::io::Result<usize> {
        match self.peer {
            Some(peer) => self.udp.try_send_to(buf, peer),
            None => self.udp.try_send(buf),
        }
    }
}
impl core::fmt::Debug for ConnWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnWrite")
            .field("udp", &self.udp)
            .field("peer", &self.peer)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use bytes::Buf;
    use futures::{future::maybe_done, pin_mut};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_listener() {
        let dispatcher_buffer_size = NonZeroUsize::new(2).unwrap();
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = udp.local_addr().unwrap();
        let listener = UdpListener::new_identity_dispatch(udp, dispatcher_buffer_size);
        let send_msg_1 = b"hello";
        let send_msg_2 = b"world";
        let client_recv_msg = Arc::new(tokio::sync::Notify::new());
        let second_accept = Arc::new(tokio::sync::Notify::new());
        tokio::spawn({
            let client_recv_msg = client_recv_msg.clone();
            let second_accept = second_accept.clone();
            async move {
                let mut client = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let msg = client.read().recv().recv().await.unwrap();
                    assert_eq!(msg.as_ref(), send_msg_1);
                    let msg = client.read().recv().recv().await.unwrap();
                    assert_eq!(msg.as_ref(), send_msg_2);
                    drop(client);
                    client_recv_msg.notify_waiters();
                });
                listener.accept().await.unwrap();
                second_accept.notify_waiters();
            }
        });
        let client_recv_msg = client_recv_msg.notified();
        let second_accept = second_accept.notified();

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.connect(listen_addr).await.unwrap();
        client.send(send_msg_1).await.unwrap();
        client.send(send_msg_2).await.unwrap();
        client_recv_msg.await;

        // Second accept has not happened yet
        let second_accept = maybe_done(second_accept);
        pin_mut!(second_accept);
        assert!(second_accept.as_mut().take_output().is_none());

        client.send(send_msg_1).await.unwrap();
        second_accept.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_open() {
        let key = 42;
        let msg = b"hello world";
        let dispatcher_buffer_size = NonZeroUsize::new(2).unwrap();
        let dispatch = |_addr: SocketAddr, mut packet: Packet| -> Option<(u8, Packet)> {
            let mut key_buf = [0; 1];
            let mut rdr = std::io::Cursor::new(packet.as_ref());
            rdr.read_exact(&mut key_buf).ok()?;
            packet.advance(1);
            Some((key_buf[0], packet))
        };
        let dispatch = Arc::new(dispatch);

        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = server.local_addr().unwrap();
        let server: UdpListener<u8, Packet> =
            UdpListener::new(server, dispatcher_buffer_size, dispatch.clone());

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.connect(listen_addr).await.unwrap();
        let client: UdpListener<u8, Packet> =
            UdpListener::new(client, dispatcher_buffer_size, dispatch.clone());

        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            let server = Arc::new(server);
            let mut conn = server.accept().await.unwrap();
            tokio::spawn({
                let server = server.clone();
                async move {
                    loop {
                        let _ = server.accept().await;
                    }
                }
            });
            assert_eq!(*conn.conn_key(), key);
            let packet = conn.read().recv().recv().await.unwrap();
            assert_eq!(packet.as_ref(), msg);
            let buf = [key].iter().chain(msg).copied().collect::<Vec<u8>>();
            conn.write().send(&buf).await.unwrap();
        });
        tasks.spawn(async move {
            let client = Arc::new(client);
            let mut conn = client.open(key).unwrap();
            tokio::spawn({
                let client = client.clone();
                async move {
                    loop {
                        let _ = client.accept().await;
                    }
                }
            });
            assert_eq!(*conn.conn_key(), key);
            let buf = [key].iter().chain(msg).copied().collect::<Vec<u8>>();
            conn.write().send(&buf).await.unwrap();
            let packet = conn.read().recv().recv().await.unwrap();
            assert_eq!(packet.as_ref(), msg);
        });
        while let Some(res) = tasks.join_next().await {
            res.unwrap();
        }
    }
}
