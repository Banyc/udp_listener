use std::{
    collections::HashMap,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use bytes::BytesMut;
use lockfree_object_pool::{LinearObjectPool, LinearOwnedReusable};
use tokio::net::UdpSocket;

pub const BUFFER_LENGTH: usize = 2_usize.pow(16);

pub type Packet = LinearOwnedReusable<BytesMut>;

pub type Dispatch<K, V> = Arc<dyn Fn(SocketAddr, Packet) -> Option<(K, V)> + Sync + Send + 'static>;

type ConnTable<K, V> = Arc<Mutex<HashMap<K, tokio::sync::mpsc::Sender<V>>>>;

/// Manage user-defined sub-connections under a UDP socket.
pub struct UdpListener<K, V> {
    connected: bool,
    udp: Arc<UdpSocket>,
    conn_table: ConnTable<K, V>,
    buf_pool: Arc<LinearObjectPool<BytesMut>>,
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
        let accepted = Arc::new(Mutex::new(HashMap::new()));
        let buf_pool = Arc::new(LinearObjectPool::new(
            || BytesMut::with_capacity(BUFFER_LENGTH),
            |buf| buf.clear(),
        ));
        let connected = udp.peer_addr().is_ok();
        Self {
            connected,
            udp: Arc::new(udp),
            conn_table: accepted,
            buf_pool,
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
    /// Side-effect: This method also dispatches packets to all the accepted UDP sockets.
    ///
    /// You should keep this method in a loop.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn accept(&self) -> std::io::Result<AcceptedUdp<K, V>> {
        loop {
            let mut buf = self.buf_pool.pull_owned();
            let (n, addr) = if self.connected {
                let n = self.udp.recv_buf(&mut *buf).await?;
                let addr = self.udp.peer_addr()?;
                (n, addr)
            } else {
                self.udp.recv_buf_from(&mut *buf).await?
            };
            if n == BUFFER_LENGTH {
                continue;
            }

            let Some((key, mut value)) = (self.dispatch)(addr, buf) else {
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
    /// - The `dispatch_key` has already been registered in the connection table.
    pub fn open(&self, dispatch_key: K) -> Option<AcceptedUdp<K, V>> {
        let peer_addr = self.udp.peer_addr().ok()?;
        let mut conn_table = self.conn_table.lock().unwrap();
        if conn_table.get(&dispatch_key).is_some() {
            return None;
        }
        let (tx, rx) = tokio::sync::mpsc::channel(self.dispatcher_buffer_size.get());
        conn_table.insert(dispatch_key.clone(), tx);
        drop(conn_table);
        Some(self.wrap_handle(dispatch_key, rx, peer_addr))
    }

    /// Pass in `peer_addr` as [`None`] iff the underlying UDP socket is connected.
    fn wrap_handle(
        &self,
        dispatch_key: K,
        rx: tokio::sync::mpsc::Receiver<V>,
        peer_addr: SocketAddr,
    ) -> AcceptedUdp<K, V> {
        let close_token = AcceptedUdpCloseToken {
            dispatch_key: dispatch_key.clone(),
            accepted: self.conn_table.clone(),
        };
        let close_token = Arc::new(close_token);
        let read = AcceptedUdpRead {
            recv: rx,
            _close_token: close_token.clone(),
        };
        let udp_to = if self.connected {
            assert_eq!(peer_addr, self.udp.peer_addr().unwrap());
            None
        } else {
            Some(peer_addr)
        };
        let write = AcceptedUdpWrite {
            udp: Arc::clone(&self.udp),
            peer: udp_to,
            _close_token: close_token,
        };
        AcceptedUdp {
            read,
            write,
            dispatch_key,
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
impl<K, V> StaticDrop for AcceptedUdpCloseToken<K, V>
where
    K: Clone + core::hash::Hash + Eq + Sync + Send + 'static,
    V: Sync + Send + 'static,
{
}

struct AcceptedUdpCloseToken<K, V>
where
    K: Clone + core::hash::Hash + Eq,
{
    dispatch_key: K,
    accepted: ConnTable<K, V>,
}
impl<K, V> Drop for AcceptedUdpCloseToken<K, V>
where
    K: Clone + core::hash::Hash + Eq,
{
    fn drop(&mut self) {
        let mut accepted = self.accepted.lock().unwrap();
        accepted.remove(&self.dispatch_key);
    }
}
impl<K, V> core::fmt::Debug for AcceptedUdpCloseToken<K, V>
where
    K: core::fmt::Debug + Clone + core::hash::Hash + Eq,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceptedUdpCloseToken")
            .field("dispatch_key", &self.dispatch_key)
            .finish()
    }
}

pub struct AcceptedUdp<K, V> {
    read: AcceptedUdpRead<V>,
    write: AcceptedUdpWrite,
    dispatch_key: K,
}
impl<K, V> AcceptedUdp<K, V> {
    pub fn read(&mut self) -> &mut AcceptedUdpRead<V> {
        &mut self.read
    }

    pub fn write(&self) -> &AcceptedUdpWrite {
        &self.write
    }

    pub fn dispatch_key(&self) -> &K {
        &self.dispatch_key
    }

    pub fn split(self) -> (AcceptedUdpRead<V>, AcceptedUdpWrite) {
        (self.read, self.write)
    }
}
impl<K: core::fmt::Debug, V> core::fmt::Debug for AcceptedUdp<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceptedUdp")
            .field("read", &self.read)
            .field("write", &self.write)
            .field("dispatch_key", &self.dispatch_key)
            .finish()
    }
}

pub struct AcceptedUdpRead<V> {
    recv: tokio::sync::mpsc::Receiver<V>,
    _close_token: Arc<dyn StaticDrop>,
}
impl<V> AcceptedUdpRead<V> {
    pub fn recv(&mut self) -> &mut tokio::sync::mpsc::Receiver<V> {
        &mut self.recv
    }
}
impl<V> core::fmt::Debug for AcceptedUdpRead<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceptedUdpRead")
            .field("recv.len()", &self.recv.len())
            .finish()
    }
}

#[derive(Clone)]
pub struct AcceptedUdpWrite {
    udp: Arc<UdpSocket>,
    peer: Option<SocketAddr>,
    _close_token: Arc<dyn StaticDrop>,
}
impl AcceptedUdpWrite {
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
impl core::fmt::Debug for AcceptedUdpWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceptedUdpWrite")
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
            assert_eq!(*conn.dispatch_key(), key);
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
            assert_eq!(*conn.dispatch_key(), key);
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
