use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::BytesMut;
use expiring_hash_map::{ExpiringHashMap, SharedClone};
use lockfree_object_pool::{LinearObjectPool, LinearOwnedReusable};
use tokio::net::UdpSocket;

mod expiring_hash_map;

pub const BUFFER_LENGTH: usize = 2_usize.pow(16);

pub type Packet = LinearOwnedReusable<BytesMut>;

pub struct UdpListener {
    udp: Arc<UdpSocket>,
    accepted: ExpiringHashMap<SocketAddr, tokio::sync::mpsc::Sender<Packet>>,
    buf_pool: Arc<LinearObjectPool<BytesMut>>,
    dispatcher_buffer_size: NonZeroUsize,
}
impl UdpListener {
    pub fn new(
        udp: UdpSocket,
        idle_timeout: Duration,
        dispatcher_buffer_size: NonZeroUsize,
    ) -> Self {
        let accepted = ExpiringHashMap::new(idle_timeout);
        let buf_pool = Arc::new(LinearObjectPool::new(
            || BytesMut::with_capacity(BUFFER_LENGTH),
            |buf| buf.clear(),
        ));
        Self {
            udp: Arc::new(udp),
            accepted,
            buf_pool,
            dispatcher_buffer_size,
        }
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn accept(&mut self) -> std::io::Result<AcceptedUdp> {
        let mut buf = self.buf_pool.pull_owned();
        loop {
            let (n, addr) = self.udp.recv_buf_from(&mut *buf).await?;
            if n == BUFFER_LENGTH {
                continue;
            }

            if let Some(tx) = self.accepted.get(&addr) {
                let Err(e) = tx.try_send(buf) else {
                    buf = self.buf_pool.pull_owned();
                    continue;
                };
                match e {
                    tokio::sync::mpsc::error::TrySendError::Full(b) => {
                        buf = b;
                        continue;
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(b) => buf = b,
                }
            }

            let (tx, rx) = tokio::sync::mpsc::channel(self.dispatcher_buffer_size.get());
            tx.try_send(buf).unwrap();
            let shared_instant = SharedClone::new(Instant::now());
            self.accepted.insert(addr, tx, shared_instant.clone());
            return Ok(AcceptedUdp::new(
                rx,
                Arc::clone(&self.udp),
                addr,
                shared_instant,
            ));
        }
    }
}
impl core::fmt::Debug for UdpListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpListener")
            .field("udp", &self.udp)
            .field("accepted", &self.accepted)
            .field("dispatcher_buffer_size", &self.dispatcher_buffer_size)
            .finish()
    }
}

#[derive(Debug)]
pub struct AcceptedUdp {
    recv: tokio::sync::mpsc::Receiver<Packet>,
    udp: Arc<UdpSocket>,
    peer: SocketAddr,
    last_sent: SharedClone<Instant>,
}
impl AcceptedUdp {
    pub(crate) fn new(
        recv: tokio::sync::mpsc::Receiver<Packet>,
        udp: Arc<UdpSocket>,
        peer: SocketAddr,
        last_sent: SharedClone<Instant>,
    ) -> Self {
        Self {
            recv,
            udp,
            peer,
            last_sent,
        }
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.udp.local_addr()
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer
    }

    pub async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.last_sent.set(Instant::now());
        self.udp.send_to(buf, self.peer).await
    }

    pub fn try_send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.last_sent.set(Instant::now());
        self.udp.try_send_to(buf, self.peer)
    }

    pub fn recv(&mut self) -> &mut tokio::sync::mpsc::Receiver<Packet> {
        &mut self.recv
    }
}

#[cfg(test)]
mod tests {
    use futures::{future::maybe_done, pin_mut};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_listener() {
        let idle_timeout = Duration::from_millis(500);
        let dispatcher_buffer_size = NonZeroUsize::new(2).unwrap();
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = udp.local_addr().unwrap();
        let mut listener = UdpListener::new(udp, idle_timeout, dispatcher_buffer_size);
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
                    let msg = client.recv().recv().await.unwrap();
                    assert_eq!(msg.as_ref(), send_msg_1);
                    let msg = client.recv().recv().await.unwrap();
                    assert_eq!(msg.as_ref(), send_msg_2);
                    client_recv_msg.notify_waiters();
                });
                listener.accept().await.unwrap();
                second_accept.notify_waiters()
            }
        });
        let client_recv_msg = client_recv_msg.notified();
        let second_accept = second_accept.notified();

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.connect(listen_addr).await.unwrap();
        client.send(send_msg_1).await.unwrap();
        client.send(send_msg_2).await.unwrap();
        client_recv_msg.await;

        tokio::time::sleep(idle_timeout).await;

        // Second accept has not happened yet
        let second_accept = maybe_done(second_accept);
        pin_mut!(second_accept);
        assert!(second_accept.as_mut().take_output().is_none());

        client.send(send_msg_1).await.unwrap();
        second_accept.await;
    }
}
