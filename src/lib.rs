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

type AcceptedMap<K, V> = Arc<Mutex<HashMap<K, tokio::sync::mpsc::Sender<V>>>>;

pub struct UdpListener<K, V> {
    udp: Arc<UdpSocket>,
    accepted: AcceptedMap<K, V>,
    buf_pool: Arc<LinearObjectPool<BytesMut>>,
    dispatcher_buffer_size: NonZeroUsize,
    dispatch: Dispatch<K, V>,
}
impl UdpListener<SocketAddr, Packet> {
    pub fn new_identity_dispatch(udp: UdpSocket, dispatcher_buffer_size: NonZeroUsize) -> Self {
        let dispatch = |addr: SocketAddr, packet: Packet| Some((addr, packet));
        UdpListener::new(udp, dispatcher_buffer_size, Arc::new(dispatch))
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
        Self {
            udp: Arc::new(udp),
            accepted,
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
            let (n, addr) = self.udp.recv_buf_from(&mut *buf).await?;
            if n == BUFFER_LENGTH {
                continue;
            }

            let Some((key, mut value)) = (self.dispatch)(addr, buf) else {
                continue;
            };

            let mut accepted = self.accepted.lock().unwrap();

            if let Some(tx) = accepted.get(&key) {
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
            accepted.insert(key.clone(), tx);

            drop(accepted);

            let close_token = AcceptedUdpCloseToken {
                dispatch_key: key.clone(),
                accepted: self.accepted.clone(),
            };
            let close_token = Arc::new(close_token);
            let read = AcceptedUdpRead {
                recv: rx,
                _close_token: close_token.clone(),
            };
            let write = AcceptedUdpWrite {
                udp: Arc::clone(&self.udp),
                peer: addr,
                _close_token: close_token,
            };
            return Ok(AcceptedUdp {
                read,
                write,
                dispatch_key: key,
            });
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
            .field("accepted", &self.accepted)
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
    accepted: AcceptedMap<K, V>,
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
    peer: SocketAddr,
    _close_token: Arc<dyn StaticDrop>,
}
impl AcceptedUdpWrite {
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.udp.local_addr()
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer
    }

    pub async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.udp.send_to(buf, self.peer).await
    }

    pub fn try_send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.udp.try_send_to(buf, self.peer)
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
    use futures::{future::maybe_done, pin_mut};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_listener() {
        let dispatcher_buffer_size = NonZeroUsize::new(2).unwrap();
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = udp.local_addr().unwrap();
        let listener: UdpListener<SocketAddr, LinearOwnedReusable<BytesMut>> =
            UdpListener::new_identity_dispatch(udp, dispatcher_buffer_size);
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
}
