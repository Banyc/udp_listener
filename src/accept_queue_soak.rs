//! Capacity-boundary and counter-consistency checks for the accept queue.
//!
//! These live inside the crate because the queue bound and the lock-free
//! length counter are private: the boundary is only assertable where the
//! `VecDeque` and the counter can be read together. [`try_accept_next`] treats
//! a zero counter as "the queue is empty", so the counter must never read low:
//! a read of zero while a flow is queued strands that flow with no error
//! anywhere. The tests here drive the queue to its bound and across it, and
//! check the counter against the queue after every step.
//!
//! [`try_accept_next`]: crate::UtpListener::try_accept_next

use bytes::BufMut;
use core::net::SocketAddr;
use core::num::NonZeroUsize;
use std::{
    collections::VecDeque,
    io::IoSlice,
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

use crate::{
    ACCEPT_QUEUE_CAPACITY, Classified, Classify, Dispatch, DispatchPolicy, Packet,
    UnreliableTransmit, UtpListener,
};

type Listener = UtpListener<tokio_udp::UdpSocket, u64, Packet>;

/// The counter and the queue are two views of one fact; at a quiescent point
/// they must agree exactly.
fn assert_queue_consistent(listener: &Listener) {
    let queued = listener.accept_queue.lock().unwrap().len();
    let counted = listener.accept_queue_len.load(Ordering::Acquire);
    assert_eq!(
        counted, queued,
        "the accept-queue length counter says {counted} while the queue holds {queued}"
    );
}

/// A payload-keyed listener whose dispatch order is recorded, so each
/// `dispatch_next` outcome can be attributed to the exact key it concerned.
async fn recording_listener() -> (Listener, Arc<Mutex<Vec<u64>>>) {
    let udp = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&order);
    let dispatch: Classify<SocketAddr, u64, Packet> = Arc::new(
        move |_addr: &SocketAddr, pkt: Packet| -> Option<Classified<u64, Packet>> {
            let key = u64::from_be_bytes(pkt.as_ref().try_into().ok()?);
            recorded.lock().unwrap().push(key);
            Some(Classified {
                key,
                value: pkt,
                policy: DispatchPolicy::Create,
            })
        },
    );
    let listener = UtpListener::new(udp, NonZeroUsize::new(1).unwrap(), dispatch);
    (listener, order)
}

/// Driving the queue past its bound must refuse exactly the overflow, count
/// each refusal, and leave the length counter equal to the queue length: an
/// under-count disables the fast path and can strand a queued flow, while a
/// refusal that still increments would skew the counter forever.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_flow_leaves_the_queue_bound_and_the_length_counter_intact() {
    const OVERFLOW: usize = 17;
    const TOTAL: usize = ACCEPT_QUEUE_CAPACITY + OVERFLOW;
    let (listener, order) = recording_listener().await;
    let listen_addr = listener.utp.local_addr().unwrap();
    let dialer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();

    // Queue every datagram before the first dispatch, so the dispatch order is
    // the dial order and the accept side never drains anything.
    for key in 1..=TOTAL as u64 {
        dialer
            .send_to(&key.to_be_bytes(), listen_addr)
            .await
            .unwrap();
    }

    let mut accepted_keys = Vec::new();
    let mut refused_keys = Vec::new();
    for _ in 0..TOTAL {
        let outcome = listener.dispatch_next().await.unwrap();
        let key = *order.lock().unwrap().last().unwrap();
        match outcome {
            Dispatch::Accepted => accepted_keys.push(key),
            Dispatch::Routed => refused_keys.push(key),
        }
        assert_queue_consistent(&listener);
    }

    assert_eq!(
        accepted_keys.len(),
        ACCEPT_QUEUE_CAPACITY,
        "exactly the queue bound may be accepted"
    );
    assert_eq!(refused_keys.len(), OVERFLOW, "the overflow must be refused");
    assert_eq!(
        listener
            .stats()
            .accepts_dropped_queue_full
            .load(Ordering::Relaxed),
        OVERFLOW as u64,
        "every refused flow must be counted"
    );
    assert_eq!(
        listener.stats().connections_opened.load(Ordering::Relaxed),
        TOTAL as u64
    );

    // The refusal must not have hidden a queued flow: the queue holds exactly
    // the accepted flows, in the order they were opened, and none of the
    // refused keys.
    let mut drained = Vec::new();
    while let Some(conn) = listener.try_accept_next() {
        drained.push(*conn.conn_key());
        drop(conn);
        assert_queue_consistent(&listener);
    }
    assert_eq!(
        drained, accepted_keys,
        "the queued flows are not the ones that were accepted"
    );
    for refused in &refused_keys {
        assert!(
            !drained.contains(refused),
            "refused flow {refused} was queued anyway"
        );
    }
    assert_eq!(
        listener.accept_queue_len.load(Ordering::Acquire),
        0,
        "draining the whole queue must leave the counter at zero"
    );
    assert!(
        listener.conn_table.lock().unwrap().is_empty(),
        "a refused flow left its key in the connection table"
    );

    // The bound must not have poisoned the fast path for the next dial.
    let fresh = TOTAL as u64 + 1;
    dialer
        .send_to(&fresh.to_be_bytes(), listen_addr)
        .await
        .unwrap();
    assert_eq!(listener.dispatch_next().await.unwrap(), Dispatch::Accepted);
    assert_queue_consistent(&listener);
    let conn = listener
        .try_accept_next()
        .expect("the fresh dial was queued");
    assert_eq!(*conn.conn_key(), fresh);
    drop(conn);
    assert!(listener.try_accept_next().is_none());
    assert_queue_consistent(&listener);
}

/// Many tasks dequeuing with no enqueue in their own call stack: the
/// cross-task read of the counter is the only thing that can let them skip
/// the lock, so none of the flows may be left behind and the counter must
/// return to zero once the queue is drained.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_try_accept_next_drains_every_queued_flow() {
    const DIALERS: usize = 8;
    const DIALS: u64 = 200;
    assert!(DIALS as usize <= ACCEPT_QUEUE_CAPACITY);
    let (listener, _order) = recording_listener().await;
    let listener = Arc::new(listener);
    let listen_addr = listener.utp.local_addr().unwrap();
    let dialer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (stop, stop_rx) = tokio::sync::watch::channel(false);

    let dequeued = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..DIALERS {
        let listener = Arc::clone(&listener);
        let dequeued = Arc::clone(&dequeued);
        let mut stop_rx = stop_rx.clone();
        tasks.spawn(async move {
            loop {
                if *stop_rx.borrow() {
                    return;
                }
                if let Some(conn) = listener.try_accept_next() {
                    dequeued.lock().unwrap().push(*conn.conn_key());
                    drop(conn);
                    continue;
                }
                tokio::select! {
                    _ = stop_rx.changed() => return,
                    _ = tokio::task::yield_now() => {}
                }
            }
        });
    }
    let _dispatcher = {
        let listener = Arc::clone(&listener);
        let mut stop_rx = stop_rx.clone();
        tasks.spawn(async move {
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => return,
                    result = listener.dispatch_next() => {
                        result.unwrap();
                    }
                }
            }
        })
    };

    for key in 1..=DIALS {
        dialer
            .send_to(&key.to_be_bytes(), listen_addr)
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        while dequeued.lock().unwrap().len() < DIALS as usize {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the concurrent dequeuers left a queued flow behind");
    stop.send_replace(true);
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    let mut keys = dequeued.lock().unwrap().clone();
    keys.sort_unstable();
    let deduped = {
        let mut unique = keys.clone();
        unique.dedup();
        unique
    };
    assert_eq!(keys, deduped, "a queued flow was handed to two dequeuers");
    assert_eq!(
        keys.len(),
        DIALS as usize,
        "not every dialled flow was dequeued"
    );
    assert!(listener.try_accept_next().is_none());
    assert_queue_consistent(&listener);
}

/// One datagram queued for the next reader: its source and its bytes.
type Datagram = (SocketAddr, Vec<u8>);
/// The gated transport's inbox.
type Inbox = Arc<Mutex<VecDeque<Datagram>>>;

/// An in-memory transport whose read *checks the queue and then waits*, with
/// the wait released by the test. That is what makes the concurrent-dispatcher
/// handover reproducible: while a pending read is parked on the gate, the test
/// can feed one datagram and consume it with its own `dispatch_next`, which
/// enqueues a flow without the parked read ever completing. A read that held a
/// mutex across its wait (as the pooling test's double does) could not be
/// driven that way.
#[derive(Clone)]
struct GatedTransport {
    local: SocketAddr,
    queue: Inbox,
    available: Arc<tokio::sync::Notify>,
}

impl GatedTransport {
    fn new(local: SocketAddr) -> Self {
        Self {
            local,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            available: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Hand one datagram to the next reader.
    fn push(&self, from: SocketAddr, data: Vec<u8>) {
        self.queue.lock().unwrap().push_back((from, data));
        // `notify_one` stores a permit when nothing is waiting, so a push
        // between the queue check and the wait cannot be lost.
        self.available.notify_one();
    }
}

impl UnreliableTransmit for GatedTransport {
    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        Ok(self.local)
    }
    fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "gated transport is unconnected",
        ))
    }
    async fn recv_buf(&self, _buf: &mut impl BufMut) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "gated transport delivers from addresses",
        ))
    }
    async fn recv_buf_from(&self, buf: &mut impl BufMut) -> std::io::Result<(usize, SocketAddr)> {
        loop {
            if let Some((from, data)) = self.queue.lock().unwrap().pop_front() {
                let n = data.len();
                buf.put_slice(&data);
                return Ok((n, from));
            }
            self.available.notified().await;
        }
    }
    async fn send(&self, _buf: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "no sends",
        ))
    }
    async fn send_to(&self, _buf: &[u8], _target: &SocketAddr) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "no sends",
        ))
    }
    fn try_send(&self, _buf: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "no sends",
        ))
    }
    fn try_send_to(&self, _buf: &[u8], _target: &SocketAddr) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "no sends",
        ))
    }
    async fn send_vectored(&self, _bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "no sends",
        ))
    }
    async fn send_to_vectored(
        &self,
        _bufs: &[IoSlice<'_>],
        _target: &SocketAddr,
    ) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "no sends",
        ))
    }
    fn supports_send_vectored(&self) -> bool {
        false
    }
}

/// `poll_next_conn` must hand back a flow queued by a *different* task's
/// `dispatch_next`.
///
/// The combined accept loop reads datagrams itself, so it can be parked in a
/// read when another task — a shared dispatcher, which the split
/// `dispatch_next`/`accept_next` form exists to allow — opens and queues a
/// flow. Waiting only on the datagram read leaves that flow queued until a
/// later datagram happens to arrive, which at the end of a dialling burst means
/// indefinitely: a connection that is never accepted, with no error anywhere.
///
/// The gate makes that deterministic rather than load-dependent: the parked
/// accept future never receives the datagram, and the flow is enqueued by a
/// separate `dispatch_next` call before the future is polled again.
#[tokio::test(flavor = "current_thread")]
async fn a_flow_enqueued_by_a_concurrent_dispatch_wakes_the_combined_accept() {
    const KEY: u64 = 7;
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let transport = GatedTransport::new(addr);
    let dispatch = |_addr: &SocketAddr, pkt: Packet| -> Option<Classified<u64, Packet>> {
        let key = u64::from_be_bytes(pkt.as_ref().try_into().ok()?);
        Some(Classified {
            key,
            value: pkt,
            policy: DispatchPolicy::Create,
        })
    };
    let listener = UtpListener::new(
        transport.clone(),
        NonZeroUsize::new(4).unwrap(),
        Arc::new(dispatch),
    );

    // Park the combined accept loop in its datagram read: the queue is empty,
    // so nothing else can be returned.
    let mut accept = Box::pin(listener.poll_next_conn());
    assert!(
        futures::poll!(accept.as_mut()).is_pending(),
        "the combined accept must park while the queue is empty"
    );

    // A different call opens a flow. Its datagram is consumed by this call, so
    // the parked accept loop has nothing to read and cannot observe the flow
    // through its datagram arm.
    transport.push(addr, KEY.to_be_bytes().to_vec());
    assert_eq!(listener.dispatch_next().await.unwrap(), Dispatch::Accepted);
    assert_eq!(
        listener.conn_table.lock().unwrap().len(),
        1,
        "the flow must be open and waiting to be accepted"
    );

    let conn = match futures::poll!(accept.as_mut()) {
        std::task::Poll::Ready(Ok(conn)) => conn,
        std::task::Poll::Ready(Err(err)) => panic!("poll_next_conn failed: {err}"),
        std::task::Poll::Pending => panic!(
            "the combined accept loop stayed parked in its datagram read instead of \
             handing back the flow another task enqueued"
        ),
    };
    assert_eq!(
        *conn.conn_key(),
        KEY,
        "the combined accept loop handed back the wrong flow"
    );
}
