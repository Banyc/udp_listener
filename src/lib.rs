use core::net::SocketAddr;
use core::num::NonZeroUsize;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use bytes::BytesMut;
use primitive::arena::obj_pool::{ArcObjPool, ObjScoped};
use tokio::sync::watch;

mod conn;
mod transmit;

use conn::ConnCloseToken;
pub use conn::{Conn, ConnRead, ConnWrite};
pub use transmit::UnreliableTransmit;

pub const PACKET_BUFFER_LENGTH: usize = 2_usize.pow(16);
/// Capacity of the bounded queue of newly opened sub-connections waiting for
/// [`UtpListener::accept_next`]. `dispatch_next` must not block (it is the
/// process-lifetime dispatch loop), so an overflowed accept queue refuses the
/// new flow and counts it in `accepts_dropped_queue_full` instead.
const ACCEPT_QUEUE_CAPACITY: usize = 256;
const OBJ_POOL_SHARDS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

pub type Packet = ObjScoped<BytesMut>;

/// Outcome of a single [`UtpListener::dispatch_next`] datagram read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dispatch {
    /// The datagram was routed to an existing sub-connection (or dropped).
    Routed,
    /// The datagram opened a new sub-connection, queued for [`UtpListener::accept_next`].
    Accepted,
}

/// Drop/dispatch accounting for a [`UtpListener`] accept loop.
///
/// The dispatch loop used to fold every silent-drop reason into one
/// `continue`; each counter below tracks one distinct path so that packet
/// loss is observable instead of vanishing into the loop.
pub struct ListenerStats {
    /// Datagrams read into the accept loop, before any drop decision.
    pub packets_received: AtomicU64,
    /// Datagrams placed into a sub-connection's buffer.
    pub packets_dispatched: AtomicU64,
    /// Datagrams dropped because the dispatch closure returned `None`.
    ///
    /// In a keyed setup this is where a keyed-packet decode/decrypt failure
    /// surfaces (e.g. `rtp::keyed_udp::dispatch` rejects an undecodable
    /// datagram); the crate itself has no `CryptoError` type.
    pub packets_dropped_rejected: AtomicU64,
    /// Datagrams dropped because the sub-connection's dispatcher buffer was full.
    pub packets_dropped_dispatcher_full: AtomicU64,
    /// Datagrams dropped because they filled the whole packet buffer.
    pub packets_dropped_pkt_buf_overflow: AtomicU64,
    /// Newly opened sub-connections dropped because the bounded accept queue
    /// was full. The datagram that opened the flow is consumed either way; only
    /// the flow's acceptance is refused under overload.
    pub accepts_dropped_queue_full: AtomicU64,
    /// Sub-connections created by [`UtpListener::poll_next_conn`] and [`UtpListener::register_conn`].
    pub connections_opened: AtomicU64,
}
impl ListenerStats {
    fn new() -> Self {
        Self {
            packets_received: AtomicU64::new(0),
            packets_dispatched: AtomicU64::new(0),
            packets_dropped_rejected: AtomicU64::new(0),
            packets_dropped_dispatcher_full: AtomicU64::new(0),
            packets_dropped_pkt_buf_overflow: AtomicU64::new(0),
            accepts_dropped_queue_full: AtomicU64::new(0),
            connections_opened: AtomicU64::new(0),
        }
    }
}

/// Coarse rate limiter so high-frequency drop paths do not spam the log.
struct RateLimiter {
    cooldown: Duration,
    last: Mutex<Option<Instant>>,
}
impl RateLimiter {
    fn new(cooldown: Duration) -> Self {
        Self {
            cooldown,
            last: Mutex::new(None),
        }
    }
    /// Returns `true` if the caller should emit the guarded log line.
    fn fire(&self) -> bool {
        let now = Instant::now();
        let mut last = self.last.lock().unwrap();
        match *last {
            Some(prev) if now.duration_since(prev) < self.cooldown => false,
            _ => {
                *last = Some(now);
                true
            }
        }
    }
}

pub type Classify<Addr, K, V> =
    Arc<dyn Fn(&Addr, Packet) -> Option<(K, V)> + Sync + Send + 'static>;

pub(crate) type ConnTable<K, V> = Arc<Mutex<HashMap<K, tokio::sync::mpsc::Sender<V>>>>;

/// Manage user-defined sub-connections under a unreliable transmission socket.
pub struct UtpListener<Utp, K, V>
where
    Utp: UnreliableTransmit,
{
    is_utp_connected: bool,
    utp: Arc<Utp>,
    conn_table: ConnTable<K, V>,
    pkt_buf_pool: ArcObjPool<BytesMut>,
    dispatcher_buffer_size: NonZeroUsize,
    dispatch: Classify<SocketAddr, K, V>,
    stats: ListenerStats,
    crypto_warn_limiter: RateLimiter,
    accept_queue_tx: tokio::sync::mpsc::Sender<Conn<Utp, K, V>>,
    accept_queue_rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Conn<Utp, K, V>>>,
    /// Watch signalling whether any live sub-connections remain (`true` when
    /// the connection table is empty). Lets a process-scoped dispatcher stop
    /// once a removed listener's surviving flows have drained.
    idle: watch::Sender<bool>,
}
impl<Utp, K, V> core::fmt::Debug for UtpListener<Utp, K, V>
where
    Utp: UnreliableTransmit + core::fmt::Debug,
    K: core::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UtpListener")
            .field("utp", &self.utp)
            .field("conn_table", &self.conn_table)
            .field("dispatcher_buffer_size", &self.dispatcher_buffer_size)
            .finish()
    }
}
impl<Utp> UtpListener<Utp, SocketAddr, Packet>
where
    Utp: UnreliableTransmit,
{
    /// Construct a TCP-like listener using peer addresses as dispatch keys.
    pub fn new_identity_dispatch(socket: Utp, dispatcher_buffer_size: NonZeroUsize) -> Self {
        let dispatch = |addr: &SocketAddr, packet: Packet| Some((*addr, packet));
        UtpListener::new(socket, dispatcher_buffer_size, Arc::new(dispatch))
    }
}
impl<Utp, K, V> UtpListener<Utp, K, V>
where
    Utp: UnreliableTransmit,
{
    pub fn new(
        utp: Utp,
        dispatcher_buffer_size: NonZeroUsize,
        dispatch: Classify<SocketAddr, K, V>,
    ) -> Self {
        let pkt_buf_pool = ArcObjPool::new(
            None,
            OBJ_POOL_SHARDS,
            || BytesMut::with_capacity(PACKET_BUFFER_LENGTH),
            |buf| {
                buf.clear();
                buf.reserve(PACKET_BUFFER_LENGTH);
            },
        );
        let (accept_queue_tx, accept_queue_rx) = tokio::sync::mpsc::channel(ACCEPT_QUEUE_CAPACITY);
        let (idle, _) = watch::channel(true);
        Self {
            is_utp_connected: utp.peer_addr().is_ok(),
            utp: Arc::new(utp),
            conn_table: Arc::new(Mutex::new(HashMap::new())),
            pkt_buf_pool,
            dispatcher_buffer_size,
            dispatch,
            stats: ListenerStats::new(),
            crypto_warn_limiter: RateLimiter::new(Duration::from_secs(1)),
            accept_queue_tx,
            accept_queue_rx: tokio::sync::Mutex::new(accept_queue_rx),
            idle,
        }
    }
}
impl<Utp, K, V> UtpListener<Utp, K, V>
where
    Utp: UnreliableTransmit,
    SocketAddr: core::fmt::Debug + PartialEq,
    K: Clone + core::hash::Hash + Eq + Sync + Send + 'static,
    V: Sync + Send + 'static,
{
    /// Combined accept-and-dispatch loop, kept for backward compatibility.
    ///
    /// Side-effect: This method also dispatches packets to all the accepted sub-connections.
    ///
    /// You should keep this method in a loop.
    ///
    /// This is the combined form of [`Self::dispatch_next`] + [`Self::accept_next`]:
    /// use the split form when packet dispatch and accepted-flow ownership need
    /// separate owners.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn poll_next_conn(&self) -> std::io::Result<Conn<Utp, K, V>> {
        loop {
            match self.dispatch_next().await? {
                Dispatch::Routed => continue,
                Dispatch::Accepted => {
                    return self.accept_next().await.ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::BrokenPipe, "accept queue closed")
                    })
                }
            }
        }
    }

    /// Read ONE datagram and dispatch it, without accepting new sub-connections.
    ///
    /// Returns [`Dispatch::Routed`] when the datagram was handed to an existing
    /// sub-connection (or dropped by a dispatch/drop path); returns
    /// [`Dispatch::Accepted`] when the datagram opened a new sub-connection,
    /// which is queued internally for [`Self::accept_next`].
    ///
    /// You should keep this method in a loop to drive packet dispatch among the
    /// accepted sub-connections. Combined with [`Self::accept_next`] this is
    /// the split form of [`Self::poll_next_conn`].
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn dispatch_next(&self) -> std::io::Result<Dispatch> {
        let mut pkt_buf = self.pkt_buf_pool.take_scoped();
        let (n, addr) = if self.is_utp_connected {
            let n = self.utp.recv_buf(&mut *pkt_buf).await?;
            let addr = self.utp.peer_addr()?;
            (n, addr)
        } else {
            self.utp.recv_buf_from(&mut *pkt_buf).await?
        };
        self.stats.packets_received.fetch_add(1, Ordering::Relaxed);
        if n == PACKET_BUFFER_LENGTH {
            self.stats
                .packets_dropped_pkt_buf_overflow
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Dispatch::Routed);
        }

        let Some((key, mut value)) = (self.dispatch)(&addr, pkt_buf) else {
            self.stats
                .packets_dropped_rejected
                .fetch_add(1, Ordering::Relaxed);
            if self.crypto_warn_limiter.fire() {
                tracing::warn!(
                    ?addr,
                    packets_dropped_rejected =
                        self.stats.packets_dropped_rejected.load(Ordering::Relaxed),
                    "dropping packet rejected by dispatch (possible keyed decode failure)"
                );
            }
            return Ok(Dispatch::Routed);
        };

        let mut conn_table = self.conn_table.lock().unwrap();

        if let Some(tx) = conn_table.get(&key) {
            match tx.try_send(value) {
                Ok(_) => {
                    self.stats
                        .packets_dispatched
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(Dispatch::Routed);
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    self.stats
                        .packets_dropped_dispatcher_full
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(Dispatch::Routed);
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(v)) => value = v,
            }
        }

        let (tx, rx) = tokio::sync::mpsc::channel(self.dispatcher_buffer_size.get());
        tx.try_send(value).unwrap();
        conn_table.insert(key.clone(), tx.clone());
        self.stats
            .packets_dispatched
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .connections_opened
            .fetch_add(1, Ordering::Relaxed);

        drop(conn_table);

        let conn = self.conn_from_parts(key, tx, rx, addr);
        match self.accept_queue_tx.try_send(conn) {
            Ok(_) => {
                let _ = self.idle.send(false);
                Ok(Dispatch::Accepted)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                // The accept side is not draining its bounded queue; refuse the
                // new flow rather than buffer unboundedly.
                self.stats
                    .accepts_dropped_queue_full
                    .fetch_add(1, Ordering::Relaxed);
                Ok(Dispatch::Routed)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                self.stats
                    .accepts_dropped_queue_full
                    .fetch_add(1, Ordering::Relaxed);
                Ok(Dispatch::Routed)
            }
        }
    }

    /// Receive the next newly opened sub-connection queued by [`Self::dispatch_next`].
    ///
    /// Returns `None` when the listener has been dropped (accept queue closed).
    pub async fn accept_next(&self) -> Option<Conn<Utp, K, V>> {
        self.accept_queue_rx.lock().await.recv().await
    }

    /// Non-blocking variant of [`Self::accept_next`] for draining a removed
    /// listener's accept queue.
    pub fn try_accept_next(&self) -> Option<Conn<Utp, K, V>> {
        self.accept_queue_rx.try_lock().ok()?.try_recv().ok()
    }

    /// Snapshot access to the listener's drop/dispatch counters.
    pub fn stats(&self) -> &ListenerStats {
        &self.stats
    }

    /// Watch signalling whether any live sub-connections remain. The receiver
    /// starts with the current state and updates to `true` when the last flow
    /// closes, so a process-scoped dispatcher can stop once a removed
    /// listener's surviving flows have drained.
    pub fn idle(&self) -> watch::Receiver<bool> {
        self.idle.subscribe()
    }

    /// This method is intended to open a sub-connection under a connected unreliable transmission socket.
    ///
    /// You still need to put [`Self::poll_next_conn()`] in a loop to drive the packet dispatch among the sub-connections.
    ///
    /// Return [`None`] if either:
    ///
    /// - The unreliable transmission socket is unconnected;
    /// - The `conn_key` has already been registered in the connection table.
    pub fn register_conn(&self, conn_key: K) -> Option<Conn<Utp, K, V>> {
        let peer_addr = self.utp.peer_addr().ok()?;
        let mut conn_table = self.conn_table.lock().unwrap();
        if conn_table.get(&conn_key).is_some_and(|tx| !tx.is_closed()) {
            return None;
        }
        let (tx, rx) = tokio::sync::mpsc::channel(self.dispatcher_buffer_size.get());
        conn_table.insert(conn_key.clone(), tx.clone());
        drop(conn_table);
        let _ = self.idle.send(false);
        self.stats
            .connections_opened
            .fetch_add(1, Ordering::Relaxed);
        Some(self.conn_from_parts(conn_key, tx, rx, peer_addr))
    }

    /// Pass in `peer_addr` as [`None`] iff the underlying unreliable transmission socket is connected.
    fn conn_from_parts(
        &self,
        conn_key: K,
        tx: tokio::sync::mpsc::Sender<V>,
        rx: tokio::sync::mpsc::Receiver<V>,
        peer_addr: SocketAddr,
    ) -> Conn<Utp, K, V> {
        let close_token = ConnCloseToken {
            conn_key: conn_key.clone(),
            conn_table: self.conn_table.clone(),
            tx,
            idle: self.idle.clone(),
        };
        let close_token = Arc::new(close_token);
        let read = ConnRead {
            recv: rx,
            _close_token: close_token.clone(),
        };
        let udp_to = if self.is_utp_connected {
            assert_eq!(peer_addr, self.utp.peer_addr().unwrap());
            None
        } else {
            Some(peer_addr)
        };
        let write = ConnWrite {
            utp: Arc::clone(&self.utp),
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

#[cfg(test)]
mod tests {
    use std::io::Read;

    use bytes::Buf;
    use futures::{future::maybe_done, pin_mut};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_pooled_buffer_does_not_shrink_across_reuses() {
        const HEADER_LEN: usize = 8_000;
        const PAYLOAD_LEN: usize = 8_192;
        const BODY_LEN: usize = PAYLOAD_LEN - HEADER_LEN;
        let dispatcher_buffer_size = NonZeroUsize::new(64).unwrap();
        let udp = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let listen_addr = udp.local_addr().unwrap();
        let dispatch = |addr: &SocketAddr, mut pkt: Packet| {
            let header_len = HEADER_LEN.min(pkt.len());
            pkt.advance(header_len);
            Some((*addr, pkt))
        };
        let listener = UtpListener::new(udp, dispatcher_buffer_size, Arc::new(dispatch));
        let client = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        client.connect(listen_addr).await.unwrap();
        let payload = vec![0xABu8; PAYLOAD_LEN];
        client.send(&payload).await.unwrap();
        let mut conn = listener.poll_next_conn().await.unwrap();
        assert_eq!(
            conn.read_half().read_half().recv().await.unwrap().len(),
            BODY_LEN
        );
        for i in 0..64 {
            client.send(&payload).await.unwrap();
            let pkt = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::select! {
                    _ = listener.poll_next_conn() => None,
                    pkt = conn.read_half().read_half().recv() => pkt,
                }
            })
            .await
            .expect("the packet was never dispatched")
            .expect("the connection was dropped");
            assert_eq!(
                pkt.len(),
                BODY_LEN,
                "packet {i} was truncated: the pooled buffer shrank"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_listener() {
        let dispatcher_buffer_size = NonZeroUsize::new(2).unwrap();
        let udp = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let listen_addr = udp.local_addr().unwrap();
        let listener = UtpListener::new_identity_dispatch(udp, dispatcher_buffer_size);
        let send_msg_1 = b"hello";
        let send_msg_2 = b"world";
        let client_recv_msg = Arc::new(tokio::sync::Notify::new());
        let second_accept = Arc::new(tokio::sync::Notify::new());
        tokio::spawn({
            let client_recv_msg = client_recv_msg.clone();
            let second_accept = second_accept.clone();
            async move {
                let mut client = listener.poll_next_conn().await.unwrap();
                tokio::spawn(async move {
                    let msg = client.read_half().read_half().recv().await.unwrap();
                    assert_eq!(msg.as_ref(), send_msg_1);
                    let msg = client.read_half().read_half().recv().await.unwrap();
                    assert_eq!(msg.as_ref(), send_msg_2);
                    drop(client);
                    client_recv_msg.notify_waiters();
                });
                listener.poll_next_conn().await.unwrap();
                second_accept.notify_waiters();
            }
        });
        let client_recv_msg = client_recv_msg.notified();
        let second_accept = second_accept.notified();

        let client = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
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
        let dispatch = |_addr: &SocketAddr, mut packet: Packet| -> Option<(u8, Packet)> {
            let mut key_buf = [0; 1];
            let mut rdr = std::io::Cursor::new(packet.as_ref());
            rdr.read_exact(&mut key_buf).ok()?;
            packet.advance(1);
            Some((key_buf[0], packet))
        };
        let dispatch = Arc::new(dispatch);

        let server = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let listen_addr = server.local_addr().unwrap();
        let server: UtpListener<tokio_udp::UdpSocket, u8, Packet> =
            UtpListener::new(server, dispatcher_buffer_size, dispatch.clone());

        let client = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        client.connect(listen_addr).await.unwrap();
        let client: UtpListener<tokio_udp::UdpSocket, u8, Packet> =
            UtpListener::new(client, dispatcher_buffer_size, dispatch.clone());

        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            let server = Arc::new(server);
            let mut conn = server.poll_next_conn().await.unwrap();
            tokio::spawn({
                let server = server.clone();
                async move {
                    loop {
                        let _ = server.poll_next_conn().await;
                    }
                }
            });
            assert_eq!(*conn.conn_key(), key);
            let packet = conn.read_half().read_half().recv().await.unwrap();
            assert_eq!(packet.as_ref(), msg);
            let buf = [key].iter().chain(msg).copied().collect::<Vec<u8>>();
            conn.write().send(&buf).await.unwrap();
        });
        tasks.spawn(async move {
            let client = Arc::new(client);
            let mut conn = client.register_conn(key).unwrap();
            tokio::spawn({
                let client = client.clone();
                async move {
                    loop {
                        let _ = client.poll_next_conn().await;
                    }
                }
            });
            assert_eq!(*conn.conn_key(), key);
            let buf = [key].iter().chain(msg).copied().collect::<Vec<u8>>();
            conn.write().send(&buf).await.unwrap();
            let packet = conn.read_half().read_half().recv().await.unwrap();
            assert_eq!(packet.as_ref(), msg);
        });
        while let Some(res) = tasks.join_next().await {
            res.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn counters_distinguish_rejected_from_dispatched_packets() {
        let dispatcher_buffer_size = NonZeroUsize::new(4).unwrap();
        let udp = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let listen_addr = udp.local_addr().unwrap();
        // A packet whose key byte is b'x' cannot be decoded: the dispatch
        // rejects it (this is where a keyed decode/decrypt failure surfaces).
        let dispatch = |addr: &SocketAddr, mut packet: Packet| {
            if packet.first() == Some(&b'x') {
                return None;
            }
            packet.advance(1);
            Some((*addr, packet))
        };
        let listener = UtpListener::new(udp, dispatcher_buffer_size, Arc::new(dispatch));
        let client = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        client.connect(listen_addr).await.unwrap();
        client.send(b"xrejected").await.unwrap();
        client.send(b"aaccepted").await.unwrap();
        let mut conn = listener.poll_next_conn().await.unwrap();
        assert_eq!(
            conn.read_half().read_half().recv().await.unwrap().as_ref(),
            b"accepted"
        );
        assert_eq!(listener.stats().packets_received.load(Ordering::Relaxed), 2);
        assert_eq!(
            listener
                .stats()
                .packets_dropped_rejected
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            listener.stats().packets_dispatched.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            listener.stats().connections_opened.load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn counters_distinguish_dispatcher_buffer_overflows() {
        let dispatcher_buffer_size = NonZeroUsize::new(1).unwrap();
        let udp = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let listen_addr = udp.local_addr().unwrap();
        let dispatch = |_addr: &SocketAddr, pkt: Packet| Some((0u8, pkt));
        let listener = Arc::new(UtpListener::new(
            udp,
            dispatcher_buffer_size,
            Arc::new(dispatch),
        ));
        let client = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        client.connect(listen_addr).await.unwrap();
        client.send(b"a").await.unwrap();
        let mut conn = listener.poll_next_conn().await.unwrap();
        // Now that key 0's 1-slot channel holds b"a" and no reader is draining
        // it, a second packet must be dropped as a dispatcher overflow rather
        // than silently folded into the success path.
        let driver = {
            let listener = Arc::clone(&listener);
            tokio::spawn(async move {
                loop {
                    let _ = listener.poll_next_conn().await;
                }
            })
        };
        client.send(b"b").await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if listener
                    .stats()
                    .packets_dropped_dispatcher_full
                    .load(Ordering::Relaxed)
                    == 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the second packet was never dropped as a dispatcher overflow");
        let pkt = conn
            .read_half()
            .read_half()
            .recv()
            .await
            .expect("the connection was closed");
        assert_eq!(pkt.as_ref(), b"a");
        assert_eq!(
            listener.stats().packets_dispatched.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            listener
                .stats()
                .packets_dropped_dispatcher_full
                .load(Ordering::Relaxed),
            1
        );
        driver.abort();
    }
}
