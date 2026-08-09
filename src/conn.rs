use std::{io::IoSlice, net::SocketAddr, sync::Arc};

use crate::{ConnTable, UnreliableTransmit};

pub(crate) trait AnySendSyncStatic: Sync + Send + 'static {}
impl<K, V> AnySendSyncStatic for ConnCloseToken<K, V>
where
    K: Clone + core::hash::Hash + Eq + Sync + Send + 'static,
    V: Sync + Send + 'static,
{
}

pub(crate) struct ConnCloseToken<K, V>
where
    K: Clone + core::hash::Hash + Eq,
{
    pub(crate) conn_key: K,
    pub(crate) conn_table: ConnTable<K, V>,
    pub(crate) tx: tokio::sync::mpsc::Sender<V>,
    pub(crate) idle: tokio::sync::watch::Sender<bool>,
}
impl<K, V> core::fmt::Debug for ConnCloseToken<K, V>
where
    K: core::fmt::Debug + Clone + core::hash::Hash + Eq,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnCloseToken")
            .field("conn_key", &self.conn_key)
            .finish()
    }
}
impl<K, V> Drop for ConnCloseToken<K, V>
where
    K: Clone + core::hash::Hash + Eq,
{
    fn drop(&mut self) {
        let mut conn_table = self.conn_table.lock().unwrap();
        let is_still_ours = conn_table
            .get(&self.conn_key)
            .is_some_and(|tx| tx.same_channel(&self.tx));
        if is_still_ours {
            conn_table.remove(&self.conn_key);
            if conn_table.is_empty() {
                let _ = self.idle.send(true);
            }
        }
    }
}

/// A sub-connection derived from a unreliable transmission listener
pub struct Conn<Utp, K, V>
where
    Utp: UnreliableTransmit,
{
    pub(crate) read: ConnRead<V>,
    pub(crate) write: ConnWrite<Utp>,
    pub(crate) conn_key: K,
}
impl<Utp, K: core::fmt::Debug, V> core::fmt::Debug for Conn<Utp, K, V>
where
    Utp: UnreliableTransmit + core::fmt::Debug,
    SocketAddr: core::fmt::Debug,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Conn")
            .field("read", &self.read)
            .field("write", &self.write)
            .field("conn_key", &self.conn_key)
            .finish()
    }
}
impl<Utp, K, V> Conn<Utp, K, V>
where
    Utp: UnreliableTransmit,
{
    pub fn read_half(&mut self) -> &mut ConnRead<V> {
        &mut self.read
    }
    pub fn write(&self) -> &ConnWrite<Utp> {
        &self.write
    }
    pub fn conn_key(&self) -> &K {
        &self.conn_key
    }
    pub fn split(self) -> (ConnRead<V>, ConnWrite<Utp>) {
        (self.read, self.write)
    }
}

pub struct ConnRead<V> {
    pub(crate) recv: tokio::sync::mpsc::Receiver<V>,
    pub(crate) _close_token: Arc<dyn AnySendSyncStatic>,
}
impl<V> core::fmt::Debug for ConnRead<V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ConnRead")
            .field("recv.len()", &self.recv.len())
            .finish()
    }
}
impl<V> ConnRead<V> {
    pub fn read_half(&mut self) -> &mut tokio::sync::mpsc::Receiver<V> {
        &mut self.recv
    }
}

pub struct ConnWrite<Utp>
where
    Utp: UnreliableTransmit,
{
    pub(crate) utp: Arc<Utp>,
    pub(crate) peer: Option<SocketAddr>,
    pub(crate) _close_token: Arc<dyn AnySendSyncStatic>,
}
impl<Utp> core::fmt::Debug for ConnWrite<Utp>
where
    Utp: UnreliableTransmit + core::fmt::Debug,
    SocketAddr: core::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnWrite")
            .field("utp", &self.utp)
            .field("peer", &self.peer)
            .finish()
    }
}
impl<Utp> Clone for ConnWrite<Utp>
where
    Utp: UnreliableTransmit,
{
    fn clone(&self) -> Self {
        Self {
            utp: Arc::clone(&self.utp),
            peer: self.peer,
            _close_token: Arc::clone(&self._close_token),
        }
    }
}
impl<Utp> ConnWrite<Utp>
where
    Utp: UnreliableTransmit,
{
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.utp.local_addr()
    }
    pub fn peer_addr(&self) -> SocketAddr {
        match &self.peer {
            Some(x) => *x,
            None => self.utp.peer_addr().unwrap(),
        }
    }
    pub async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        match &self.peer {
            Some(peer) => self.utp.send_to(buf, peer).await,
            None => self.utp.send(buf).await,
        }
    }
    pub async fn send_vectored(&self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        match &self.peer {
            Some(peer) => self.utp.send_to_vectored(bufs, peer).await,
            None => self.utp.send_vectored(bufs).await,
        }
    }
    pub fn try_send(&self, buf: &[u8]) -> std::io::Result<usize> {
        match &self.peer {
            Some(peer) => self.utp.try_send_to(buf, peer),
            None => self.utp.try_send(buf),
        }
    }
}

#[cfg(test)]
mod tests {
    use core::{net::SocketAddr, num::NonZeroUsize};
    use std::{io::IoSlice, sync::Arc};

    use crate::{Classified, DispatchPolicy, Packet, UtpListener};

    #[tokio::test(flavor = "multi_thread")]
    async fn a_dead_connection_does_not_evict_its_successor() {
        let dispatcher_buffer_size = NonZeroUsize::new(2).unwrap();
        let udp = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let listen_addr = udp.local_addr().unwrap();
        let listener = UtpListener::new_identity_dispatch(udp, dispatcher_buffer_size);
        let client = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        client.connect(listen_addr).await.unwrap();
        client.send(b"a").await.unwrap();
        let (read_a, write_a) = listener.poll_next_conn().await.unwrap().split();
        drop(read_a);
        client.send(b"b").await.unwrap();
        let mut conn_b = listener.poll_next_conn().await.unwrap();
        assert_eq!(
            conn_b
                .read_half()
                .read_half()
                .recv()
                .await
                .unwrap()
                .as_ref(),
            b"b"
        );
        drop(write_a);
        client.send(b"c").await.unwrap();
        let dispatched = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::select! {
                _ = listener.poll_next_conn() => None,
                msg = conn_b.read_half().read_half().recv() => msg,
            }
        })
        .await
        .expect("neither a dispatch nor an accept happened");
        let dispatched = dispatched
            .expect("the successor was evicted: its packet accepted a third connection instead");
        assert_eq!(dispatched.as_ref(), b"c");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_dead_connection_does_not_block_reopening_its_key() {
        const KEY: u8 = 7;
        let dispatcher_buffer_size = NonZeroUsize::new(2).unwrap();
        let udp = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let peer = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        udp.connect(peer.local_addr().unwrap()).await.unwrap();
        let dispatch = |_addr: &SocketAddr, pkt: Packet| {
            Some(Classified {
                key: KEY,
                value: pkt,
                policy: DispatchPolicy::Create,
            })
        };
        let listener: UtpListener<tokio_udp::UdpSocket, u8, Packet> =
            UtpListener::new(udp, dispatcher_buffer_size, Arc::new(dispatch));
        let (read, write) = listener
            .register_conn(KEY)
            .expect("the key was free")
            .split();
        drop(read);
        let reopened = listener
            .register_conn(KEY)
            .expect("a dead connection's leftover entry refused the key");
        let (mut reopened_read, _reopened_write) = reopened.split();
        peer.send_to_vectored(&[IoSlice::new(b"x")], &listener.utp.local_addr().unwrap())
            .await
            .unwrap();
        let dispatched = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::select! {
                _ = listener.poll_next_conn() => None,
                msg = reopened_read.read_half().recv() => msg,
            }
        })
        .await
        .expect("neither a dispatch nor an accept happened")
        .expect("the reopened connection never got the packet");
        assert_eq!(dispatched.as_ref(), b"x");
        drop(write);
    }
}
