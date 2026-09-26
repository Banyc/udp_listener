//! Capacity-boundary, counter-consistency and accept-wakeup liveness checks.
//!
//! The capacity tests live inside the crate because the queue bound and the
//! lock-free length counter are private: the boundary is only assertable where
//! the `VecDeque` and the counter can be read together. [`try_accept_next`]
//! treats a zero counter as "the queue is empty", so the counter must never
//! read low: a read of zero while a flow is queued strands that flow with no
//! error anywhere. Those tests drive the queue to its bound and across it, and
//! check the counter against the queue after every step.
//!
//! The ordering test below asserts the enqueue's *commit-before-wake* order:
//! the queue's two mutations happen under its lock and the wake follows both, so
//! a woken waiter can always read the committed flow — a wake delivered before
//! the commit would let it read an empty queue and park again.
//!
//! The wakeup tests below assert the *liveness* side of the same queue: an
//! await on [`accept_next`] is woken only by the enqueue's `notify_one`, and a
//! `Notify` holds at most one permit, so a wake consumed by a waiter that never
//! takes the flow — or by a waiter that is cancelled — leaves the flow queued
//! with no error anywhere. Each wakeup test therefore drives one interleaving
//! and asserts the flow still comes back, which is what a lost wake violates.
//!
//! [`try_accept_next`]: crate::UtpListener::try_accept_next
//! [`accept_next`]: crate::UtpListener::accept_next

use bytes::BufMut;
use core::net::SocketAddr;
use core::num::NonZeroUsize;
use core::pin::Pin;
use std::{
    collections::VecDeque,
    io::IoSlice,
    sync::{
        Arc, Mutex, TryLockError, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

use crate::{
    ACCEPT_QUEUE_CAPACITY, Classified, Classify, Conn, Dispatch, DispatchPolicy, Packet,
    UnreliableTransmit, UtpListener,
};

type Listener = UtpListener<tokio_udp::UdpSocket, u64, Packet>;

/// The counter and the queue are two views of one fact; at a quiescent point
/// they must agree exactly. Shared with the teardown cells, because a drain
/// that leaves the counter and the queue disagreeing strands a queued flow.
pub(crate) fn assert_queue_consistent<Utp, K, V>(listener: &UtpListener<Utp, K, V>)
where
    Utp: UnreliableTransmit,
{
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

// ===== accept-wakeup liveness =====

/// A listener over the gated in-memory transport, keyed by an 8-byte payload
/// so a flow can be opened without a UDP socket: the wakeup cells are about
/// the accept queue's notify, and a real socket adds only port churn and
/// load-sensitivity to that.
type GatedListener = UtpListener<GatedTransport, u64, Packet>;

fn gated_listener(transport: GatedTransport) -> GatedListener {
    let dispatch = |_addr: &SocketAddr, pkt: Packet| -> Option<Classified<u64, Packet>> {
        Some(Classified {
            key: u64::from_be_bytes(pkt.as_ref().try_into().ok()?),
            value: pkt,
            policy: DispatchPolicy::Create,
        })
    };
    UtpListener::new(transport, NonZeroUsize::new(4).unwrap(), Arc::new(dispatch))
}

pub(crate) fn wake_env(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<usize>()
            .unwrap_or_else(|err| panic!("{name}={raw:?} is not parseable: {err}")),
        Err(_) => default,
    }
}

/// Open one flow: hand the transport the key's datagram, then dispatch it.
async fn open_flow(
    transport: &GatedTransport,
    listener: &GatedListener,
    addr: SocketAddr,
    key: u64,
) {
    transport.push(addr, key.to_be_bytes().to_vec());
    assert_eq!(
        listener.dispatch_next().await.unwrap(),
        Dispatch::Accepted,
        "dial {key} did not open a flow"
    );
}

/// M waiters parked on an empty queue, then N > M enqueues.
///
/// A `Notify` holds at most one permit, so the M enqueues that arrive while
/// the waiters are registered wake M distinct waiters and everything after
/// that leaves at most one permit — the surplus flows reach the accept side
/// only because a woken waiter's caller re-checks the queue at its loop head.
/// Two properties are pinned here, and they are different claims:
///
/// - every waiter that parked is woken (a parked waiter is never served
///   otherwise, and its caller waits forever); and
/// - every flow comes back exactly once, whatever the permit accounting did —
///   which is why the surplus must not be assumed to ride the permit.
#[tokio::test(flavor = "current_thread")]
async fn m_parked_waiters_and_more_enqueues_than_waiters_lose_no_flow() {
    const WAITERS: usize = 4;
    const ENQUEUES: u64 = 9;
    assert!(WAITERS as u64 <= ENQUEUES);
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let transport = GatedTransport::new(addr);
    let listener = gated_listener(transport.clone());

    let mut waiters: Vec<_> = (0..WAITERS)
        .map(|_| Box::pin(listener.accept_next()))
        .collect();
    for (i, waiter) in waiters.iter_mut().enumerate() {
        assert!(
            futures::poll!(waiter.as_mut()).is_pending(),
            "waiter {i} did not park on the empty queue"
        );
    }

    for key in 0..ENQUEUES {
        open_flow(&transport, &listener, addr, key).await;
    }

    let mut handed = Vec::new();
    for (i, waiter) in waiters.iter_mut().enumerate() {
        match futures::poll!(waiter.as_mut()) {
            Poll::Ready(Some(conn)) => handed.push(*conn.conn_key()),
            Poll::Ready(None) => panic!("waiter {i} returned None"),
            Poll::Pending => panic!(
                "waiter {i} stayed parked although {ENQUEUES} flows were enqueued after it \
                 parked: {} are still queued behind it",
                listener.accept_queue.lock().unwrap().len()
            ),
        }
    }
    while let Some(conn) = listener.try_accept_next() {
        handed.push(*conn.conn_key());
    }
    handed.sort_unstable();
    assert_eq!(
        handed,
        (0..ENQUEUES).collect::<Vec<_>>(),
        "the parked waiters did not hand back every enqueued flow exactly once"
    );
    assert_queue_consistent(&listener);
}

/// The surviving waiter must hand back the queued flow. Its own readiness is
/// not the evidence — a re-polled waiter recovers through the queue check at its
/// loop head whatever the notify did — so this only checks that the flow the
/// cancelled waiter left behind is still deliverable.
fn assert_hands_back<F>(waiter: &mut Pin<Box<F>>, waker: &Waker, expected: u64)
where
    F: core::future::Future<Output = Option<Conn<GatedTransport, u64, Packet>>>,
{
    match waiter.as_mut().poll(&mut Context::from_waker(waker)) {
        Poll::Ready(Some(conn)) => assert_eq!(*conn.conn_key(), expected),
        Poll::Ready(None) => panic!("the surviving waiter returned None"),
        Poll::Pending => panic!(
            "the surviving waiter stayed parked although it was woken and the flow is queued"
        ),
    }
}

/// A waker that counts the wakes it receives, so a *delivery* can be asserted
/// separately from the wait's outcome: a parked waiter that is polled again will
/// find a queued flow through its loop-head check whatever the notify did, so
/// only the wake count can tell a delivered wake from a lost one.
struct WakeCounter(Arc<AtomicUsize>);

/// A waker that counts the wakes it receives, with the counter it reports them
/// to. The count is what distinguishes a delivered wake from a lost one.
fn counting_waker() -> (Arc<AtomicUsize>, Waker) {
    let count = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(WakeCounter(Arc::clone(&count))));
    (count, waker)
}
impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// A wake consumed by a waiter that is then cancelled must reach another parked
/// waiter, not vanish with it.
///
/// `notify_one` wakes exactly one waiter and stores no permit when one *is*
/// registered, so an acceptor that is woken by an enqueue and then dropped
/// before it polls — a `select!` arm that loses to another arm, which is what
/// the cancellation modes and every caller's teardown race do — would consume
/// the queued flow's only wake. The flow would still be queued, so nothing
/// reports an error: the accept side would simply wait for an event that has
/// already been spent.
///
/// The wake count is the assertion (a re-polled waiter recovers through its
/// queue check regardless, so the wait's outcome alone cannot see this): after
/// the cancel, the surviving parked waiter's waker must have fired.
#[tokio::test(flavor = "current_thread")]
async fn a_cancelled_waiter_does_not_consume_another_waiter_wake() {
    const KEY: u64 = 11;
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let transport = GatedTransport::new(addr);
    let listener = gated_listener(transport.clone());
    let (first_wakes, first_waker) = counting_waker();
    let (second_wakes, second_waker) = counting_waker();
    let mut first = Box::pin(listener.accept_next());
    let mut second = Box::pin(listener.accept_next());
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending(),
        "the first waiter must park"
    );
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending(),
        "the second waiter must park"
    );

    open_flow(&transport, &listener, addr, KEY).await;

    let first_count = first_wakes.load(Ordering::SeqCst);
    let second_count = second_wakes.load(Ordering::SeqCst);
    assert_eq!(
        first_count + second_count,
        1,
        "the enqueue must deliver exactly one wake: two would not exercise the cancelled \
         waiter and none would mean nothing was delivered to cancel"
    );
    // Cancel the woken waiter without polling it, and check the survivor.
    if first_count == 1 {
        drop(first);
        assert!(
            second_wakes.load(Ordering::SeqCst) >= 1,
            "the cancelled waiter kept the only wake: the surviving waiter is parked, the flow \
             is still queued ({} queued), and that enqueue's wake has been spent",
            listener.accept_queue_len.load(Ordering::Acquire)
        );
        assert_hands_back(&mut second, &second_waker, KEY);
    } else {
        drop(second);
        assert!(
            first_wakes.load(Ordering::SeqCst) >= 1,
            "the cancelled waiter kept the only wake: the surviving waiter is parked, the flow \
             is still queued ({} queued), and that enqueue's wake has been spent",
            listener.accept_queue_len.load(Ordering::Acquire)
        );
        assert_hands_back(&mut first, &first_waker, KEY);
    }
}

/// One burst-before-registration round: open `items` flows with no waiter
/// registered, then drain with `waiters` accept-only tasks and `combined`
/// combined `poll_next_conn` tasks, each taking one flow and returning to
/// waiting. Returns how long the drain took and the identity multiset it
/// handed back.
async fn burst_round(
    label: &str,
    items: u64,
    waiters: usize,
    combined: usize,
    bound: Duration,
) -> (Duration, Vec<u64>) {
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let transport = GatedTransport::new(addr);
    let listener = Arc::new(gated_listener(transport.clone()));
    for key in 0..items {
        open_flow(&transport, &listener, addr, key).await;
    }
    assert_eq!(
        listener.accept_queue_len.load(Ordering::Acquire),
        items as usize,
        "{label}: the burst must be queued whole before the accept side runs"
    );

    let accepted = Arc::new(Mutex::new(Vec::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..waiters {
        let listener = Arc::clone(&listener);
        let accepted = Arc::clone(&accepted);
        let count = Arc::clone(&count);
        tasks.spawn(async move {
            loop {
                let Some(conn) = listener.accept_next().await else {
                    return;
                };
                accepted.lock().unwrap().push(*conn.conn_key());
                count.fetch_add(1, Ordering::Relaxed);
            }
        });
    }
    for _ in 0..combined {
        let listener = Arc::clone(&listener);
        let accepted = Arc::clone(&accepted);
        let count = Arc::clone(&count);
        tasks.spawn(async move {
            loop {
                let conn = listener.poll_next_conn().await.expect("the accept loop");
                accepted.lock().unwrap().push(*conn.conn_key());
                count.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    let started = Instant::now();
    let drained = tokio::time::timeout(bound * 4, async {
        while count.load(Ordering::Relaxed) < items as usize {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let elapsed = started.elapsed();
    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        // An aborted (or finished) task is fine; a panic is not.
        if let Err(err) = result {
            assert!(err.is_cancelled(), "an acceptor task panicked: {err}");
        }
    }
    let mut handed = accepted.lock().unwrap().clone();
    handed.sort_unstable();
    if drained.is_err() {
        let queued = listener.accept_queue.lock().unwrap().len();
        panic!(
            "HANG ({label}): {} of {items} flows were accepted in {:?}, and {queued} are still \
             queued: a queued flow was never handed to any waiter",
            handed.len(),
            elapsed
        );
    }
    if elapsed > bound {
        // A late drain is its own outcome: a scheduling delay, not a loss, and
        // it must not be silently scored as either.
        println!("SOAK_WAKE_LATE {label} elapsed_ms={}", elapsed.as_millis());
    }
    assert_queue_consistent(&listener);
    assert_eq!(
        listener.accept_queue_len.load(Ordering::Acquire),
        0,
        "{label}: the queue must be empty once every dial is handed back"
    );
    (elapsed, handed)
}

/// A burst of N flows enqueued before any waiter exists, then tasks racing to
/// drain it.
///
/// This is the burst-before-registration interleaving: while the burst is
/// enqueued no waiter is registered, so every flow can only be handed back by
/// a task that reaches the queue afterwards, and each task takes one flow and
/// returns to waiting. The verdict is an identity multiset, so a duplicate
/// handover cannot balance a lost one, and the drain must finish inside a
/// bound: an overrun is reported as LATE (a scheduling delay, not a loss) and a
/// drain that never finishes as HANG, with the queue's own length in the panic
/// so a stranded flow is distinguished from a lost one.
///
/// The two rounds differ in who drains: the second keeps only `accept_next`
/// tasks, so the burst can be handed back only by the queue check at the top
/// of each wait — the property that makes the surplus flows in
/// [`m_parked_waiters_and_more_enqueues_than_waiters_lose_no_flow` independent
/// of the notify's permit accounting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_burst_enqueued_before_any_waiter_is_handed_back_once_and_in_bound() {
    let items = wake_env("SOAK_WAKE_ITEMS", 64) as u64;
    let waiters = wake_env("SOAK_WAKE_WAITERS", 3);
    let combined = wake_env("SOAK_WAKE_COMBINED", 2);
    let bound = Duration::from_millis(wake_env("SOAK_WAKE_BOUND_MS", 5_000) as u64);
    assert!(items > 0 && waiters > 0);

    let cases = [
        ("accept+combined", waiters, combined),
        // Only `accept_next` tasks: the burst can then be handed back only by
        // the queue check at the top of each wait.
        ("accept-only", waiters, 0),
    ];
    for (label, n_waiters, n_combined) in cases {
        let (_, handed) = burst_round(label, items, n_waiters, n_combined, bound).await;
        assert_eq!(
            handed,
            (0..items).collect::<Vec<_>>(),
            "{label}: the burst did not come back exactly once per dial"
        );
    }
}

/// A waker that snapshots the accept queue at the instant the wake is
/// delivered, so the enqueue's ordering (commit before notify) is *observable*
/// rather than argued.
///
/// The waker is the instrument an inline-running executor would put in place of
/// a scheduler: it runs synchronously inside the enqueue's `notify_one`, so what
/// it reads is exactly what a waiter whose waker polls immediately — or a
/// cross-task `try_accept_next` that runs right then — would observe. A wake
/// delivered before the commit point would let such a waiter read an empty
/// queue, park again, and never be woken by that enqueue.
struct CommitWaker {
    /// Weak, so the instrument cannot itself keep the listener alive.
    listener: Weak<GatedListener>,
    wakes: AtomicUsize,
    /// The accept queue's length counter when the wake was delivered.
    counter_at_wake: AtomicUsize,
    /// The queue's length when the wake was delivered, or `usize::MAX` if the
    /// queue could not be read.
    queue_len_at_wake: AtomicUsize,
    /// Whether the wake was delivered while the enqueue still held the queue.
    queue_locked_at_wake: AtomicBool,
}
impl CommitWaker {
    fn new(listener: Weak<GatedListener>) -> Self {
        Self {
            listener,
            wakes: AtomicUsize::new(0),
            counter_at_wake: AtomicUsize::new(usize::MAX),
            queue_len_at_wake: AtomicUsize::new(usize::MAX),
            queue_locked_at_wake: AtomicBool::new(false),
        }
    }
    fn record(&self) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        let Some(listener) = self.listener.upgrade() else {
            return;
        };
        self.counter_at_wake.store(
            listener.accept_queue_len.load(Ordering::Acquire),
            Ordering::SeqCst,
        );
        match listener.accept_queue.try_lock() {
            Ok(queue) => {
                self.queue_len_at_wake.store(queue.len(), Ordering::SeqCst);
            }
            Err(TryLockError::WouldBlock) => {
                self.queue_locked_at_wake.store(true, Ordering::SeqCst);
            }
            Err(TryLockError::Poisoned(_)) => {}
        }
    }
}
impl Wake for CommitWaker {
    fn wake(self: Arc<Self>) {
        self.record();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.record();
    }
}

/// The enqueue must be **committed before the wake is delivered**.
///
/// `dispatch_next` publishes the queue length before pushing the connection, and
/// notifies only after both, with the queue lock released. A wake delivered
/// earlier — announcing the reservation rather than the commit — can be polled
/// inline by an executor whose waker runs immediately, and the woken waiter
/// would find an empty queue, park again, and not be woken by that enqueue:
/// a flow queued with no further event to deliver it. The wait's queue check at
/// its loop head recovers from a wake that arrives *late*; nothing recovers from
/// one that arrives *early*.
///
/// The waker's snapshot is the assertion: at the instant the parked waiter is
/// woken, the queue must already hold the flow and must not still be locked by
/// the enqueue.
#[tokio::test(flavor = "current_thread")]
async fn the_accept_queue_is_committed_before_the_wake_is_delivered() {
    const KEY: u64 = 3;
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let transport = GatedTransport::new(addr);
    let listener = Arc::new(gated_listener(transport.clone()));
    let mut waiter = Box::pin(listener.accept_next());
    let probe = Arc::new(CommitWaker::new(Arc::downgrade(&listener)));
    let waker = Waker::from(Arc::clone(&probe));
    let mut context = Context::from_waker(&waker);
    assert!(
        waiter.as_mut().poll(&mut context).is_pending(),
        "the waiter must park on the empty queue, or its wake is not the one observed"
    );

    open_flow(&transport, &listener, addr, KEY).await;

    assert_eq!(
        probe.wakes.load(Ordering::SeqCst),
        1,
        "the enqueue delivered no wake to the parked waiter, so the ordering cannot be observed"
    );
    assert_eq!(
        probe.counter_at_wake.load(Ordering::SeqCst),
        1,
        "the wake was delivered before the enqueued flow was counted: a waiter polled at that \
         instant would read an empty queue and park again"
    );
    assert!(
        !probe.queue_locked_at_wake.load(Ordering::SeqCst),
        "the wake was delivered while the enqueue still held the accept queue: an executor whose \
         waker runs inline would run the woken waiter inside the enqueue's critical section"
    );
    assert_eq!(
        probe.queue_len_at_wake.load(Ordering::SeqCst),
        1,
        "the accept queue did not hold the flow at the instant the waiter was woken"
    );
    match waiter.as_mut().poll(&mut context) {
        Poll::Ready(Some(conn)) => assert_eq!(*conn.conn_key(), KEY),
        Poll::Ready(None) => panic!("the awakened waiter returned None"),
        Poll::Pending => panic!(
            "the awakened waiter parked again although the wake was delivered after the commit"
        ),
    }
}

/// An enqueue that finds no registered waiter must leave the wait deliverable
/// without a further event.
///
/// This pins the *permit* half of the wakeup contract, which is the half a
/// `Notify` wait can silently lose: `notify_one` stores a permit when no waiter
/// is registered, `notify_waiters` stores none. The accept path needs it
/// because its wait is entered only after the queue check, so a flow enqueued
/// in the window between that check and the wait's registration is delivered by
/// the stored permit alone — a waiter already parked is woken either way, which
/// is why no other cell in this crate can see the difference.
///
/// A wake substituted for `notify_one` must therefore either keep storing a
/// permit, or register the wait before the queue check (tokio's documented
/// `enable`-then-`try_recv` order).
#[tokio::test(flavor = "current_thread")]
async fn an_enqueue_with_no_registered_waiter_stores_a_permit() {
    const KEY: u64 = 1;
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let transport = GatedTransport::new(addr);
    let listener = gated_listener(transport.clone());

    // No waiter exists anywhere while the flow is opened.
    open_flow(&transport, &listener, addr, KEY).await;
    assert_eq!(
        listener.accept_queue_len.load(Ordering::Acquire),
        1,
        "the flow must still be queued: the permit is what is under test, not the queue"
    );

    let mut waiter = Box::pin(listener.accept_notify.notified());
    assert_eq!(
        futures::poll!(waiter.as_mut()),
        Poll::Ready(()),
        "the enqueue left no permit: a waiter registering after it would need a later \
         event to be woken, and the accept path sends none"
    );
}
