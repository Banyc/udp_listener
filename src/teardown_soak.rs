//! Teardown-with-parked-waiters liveness checks for the accept path.
//!
//! The accept path has no closed state: [`UtpListener::accept_next`] borrows the
//! listener and its returned [`Option`] is always `Some`, so a waiter that is
//! parked cannot be woken by a listener teardown — only by a queue change, by a
//! transport error (the combined [`UtpListener::poll_next_conn`] form), or by
//! the caller's own signal. The cells here pin what that means for teardown:
//!
//! - The **idle watch** is the crate's teardown signal (a process-scoped
//!   dispatcher stops once a removed listener's surviving flows have drained).
//!   Its documented contract is that *the receiver starts with the current
//!   state*, which must hold even when the state changed while no receiver was
//!   subscribed — otherwise a drain armed at removal time reads a stale state
//!   and either stops dispatching live flows or never stops.
//! - A **drain** that releases its signal must leave no waiter parked, must not
//!   lose a queued flow to the cancelled waits, must release the listener (its
//!   transport, and on a real socket the bound port), and must let a waiter
//!   that arrives *during* the teardown observe the signal on its first poll.
//!   A waiter parked while the listener has no other owner is the one shape that
//!   keeps a listener and its bound port alive with no task able to end it, so
//!   the cell counts parked waiters and asserts that count drains to zero.
//! - The combined form's **transport error** is its only close/error result: a
//!   parked combined waiter must observe it, and observing it must not consume
//!   a datagram the failed read never received.

use core::net::SocketAddr;
use core::num::NonZeroUsize;
use std::{
    collections::VecDeque,
    io::IoSlice,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::{Duration, Instant},
};

use bytes::BufMut;
use futures::FutureExt;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::{
    Classified, Classify, Conn, Dispatch, DispatchPolicy, Packet, UnreliableTransmit, UtpListener,
    accept_queue_soak::{assert_queue_consistent, wake_env},
};

/// The error a read armed by [`TeardownTransport::fail_reads`] reports; the
/// cells assert on it so a failure is attributed to the transport rather than
/// to any error a fabricated path might invent.
const READ_FAILURE: &str = "the teardown transport failed this read";

/// One datagram handed to the transport: its source and its bytes.
type Datagram = (SocketAddr, Vec<u8>);
/// The transport's inbox of not-yet-read datagrams.
type Inbox = Arc<Mutex<VecDeque<Datagram>>>;
/// A listener over [`TeardownTransport`], keyed by an eight-byte payload.
type Listener = UtpListener<TeardownTransport, u64, Packet>;
/// The accept wait returned by the split or the combined form.
type WaitedConn = Conn<TeardownTransport, u64, Packet>;

/// An in-memory transport for the teardown cells: datagrams are handed in by
/// the test (no socket, no kernel buffer, no scheduling race) and a **live-clone
/// census** makes "the listener released its transport" assertable rather than
/// assumed.
///
/// The census counts live handles: the test holds one, so a census of one means
/// the listener's own copy is gone.
struct TeardownTransport {
    local: SocketAddr,
    inbox: Inbox,
    available: Arc<tokio::sync::Notify>,
    live: Arc<AtomicUsize>,
    /// Reads armed to fail, so a teardown cell can drive the transport-error
    /// path deterministically instead of closing a socket and racing.
    fail_reads: Arc<AtomicUsize>,
}
impl TeardownTransport {
    fn new(local: SocketAddr) -> Self {
        Self {
            local,
            inbox: Arc::new(Mutex::new(VecDeque::new())),
            available: Arc::new(tokio::sync::Notify::new()),
            live: Arc::new(AtomicUsize::new(1)),
            fail_reads: Arc::new(AtomicUsize::new(0)),
        }
    }
    /// Hand one datagram to the next read.
    fn push(&self, from: SocketAddr, data: Vec<u8>) {
        self.inbox.lock().unwrap().push_back((from, data));
        self.available.notify_one();
    }
    /// Make the next `n` reads fail, before the inbox is consulted: an armed
    /// failure must not be masked by a datagram that happens to be queued, and
    /// the datagram must still be there for the read after it.
    fn fail_reads(&self, n: usize) {
        self.fail_reads.store(n, Ordering::SeqCst);
    }
    fn inbox_len(&self) -> usize {
        self.inbox.lock().unwrap().len()
    }
    /// Live handles: the test holds one, so `live() == 1` means no listener
    /// holds this transport any more.
    fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }
}
impl Clone for TeardownTransport {
    fn clone(&self) -> Self {
        self.live.fetch_add(1, Ordering::SeqCst);
        Self {
            local: self.local,
            inbox: Arc::clone(&self.inbox),
            available: Arc::clone(&self.available),
            live: Arc::clone(&self.live),
            fail_reads: Arc::clone(&self.fail_reads),
        }
    }
}
impl Drop for TeardownTransport {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}
impl UnreliableTransmit for TeardownTransport {
    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        Ok(self.local)
    }
    fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "the teardown transport is unconnected",
        ))
    }
    async fn recv_buf(&self, _buf: &mut impl BufMut) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "the teardown transport delivers from addresses",
        ))
    }
    async fn recv_buf_from(&self, buf: &mut impl BufMut) -> std::io::Result<(usize, SocketAddr)> {
        loop {
            if self.fail_reads.load(Ordering::SeqCst) > 0 {
                self.fail_reads.fetch_sub(1, Ordering::SeqCst);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    READ_FAILURE,
                ));
            }
            if let Some((from, data)) = self.inbox.lock().unwrap().pop_front() {
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

/// A flow keyed by the datagram's leading eight bytes.
fn key_dispatch() -> Classify<SocketAddr, u64, Packet> {
    Arc::new(
        |_addr: &SocketAddr, pkt: Packet| -> Option<Classified<u64, Packet>> {
            let key = u64::from_be_bytes(pkt.as_ref().try_into().ok()?);
            Some(Classified {
                key,
                value: pkt,
                policy: DispatchPolicy::Create,
            })
        },
    )
}

/// The idle watch must reflect the current state to a subscriber that arrives
/// after that state changed while nobody was subscribed.
///
/// `watch::Sender::send` does not make its value available to future receivers
/// when no receiver exists (`send_replace`/`send_modify`/`send_if_modified` do),
/// so the not-idle state of a flow that opened with no subscriber is discarded,
/// and a drain that arms its wait when the listener is removed — the documented
/// use — reads `idle` while a flow is live: it stops dispatching the very flows
/// it exists to drain. The symmetric direction is worse: the *close* is
/// discarded when the last receiver leaves before the last flow closes, so the
/// next subscriber reads not-idle forever and the drain never stops.
///
/// Both directions are asserted here because both are the same contract, and
/// each is preceded by the census assertion that proves the cell is actually
/// exercising the no-subscriber window (an instrument that cannot reach its
/// window is not coverage).
#[tokio::test(flavor = "current_thread")]
async fn a_late_idle_subscriber_observes_the_current_state() {
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let transport = TeardownTransport::new(addr);
    let listener = UtpListener::new(
        transport.clone(),
        NonZeroUsize::new(4).unwrap(),
        key_dispatch(),
    );

    // Direction 1: a flow opens with no subscriber at all.
    assert_eq!(
        listener.idle.receiver_count(),
        0,
        "the cell must start with no idle subscriber"
    );
    transport.push(addr, 1u64.to_be_bytes().to_vec());
    assert_eq!(listener.dispatch_next().await.unwrap(), Dispatch::Accepted);
    assert_eq!(
        listener.idle.receiver_count(),
        0,
        "the flow must have opened while nobody was subscribed, or the window is not exercised"
    );

    let late = listener.idle();
    assert!(
        !*late.borrow(),
        "a subscriber that starts while a flow is live read `idle`: a drain armed when the \
         listener is removed would stop dispatching the flow it exists to drain"
    );

    // The close a live subscriber does observe must be the close it reports.
    let conn = listener.try_accept_next().expect("the flow is queued");
    drop(conn);
    assert!(
        *late.borrow(),
        "the subscriber did not observe the last flow closing"
    );
    drop(late);

    // Direction 2: the last flow of a *later* round closes with no subscriber.
    assert_eq!(listener.idle.receiver_count(), 0);
    transport.push(addr, 2u64.to_be_bytes().to_vec());
    assert_eq!(listener.dispatch_next().await.unwrap(), Dispatch::Accepted);
    {
        let live = listener.idle();
        assert!(
            !*live.borrow(),
            "a subscriber that starts while a flow is live read `idle`"
        );
    }
    assert_eq!(
        listener.idle.receiver_count(),
        0,
        "the close below must happen with nobody subscribed, or the window is not exercised"
    );
    let conn = listener.try_accept_next().expect("the flow is queued");
    drop(conn);
    assert!(
        listener.conn_table.lock().unwrap().is_empty(),
        "the last flow must really be closed before its state is read"
    );

    let after = listener.idle();
    assert!(
        *after.borrow(),
        "a subscriber that starts after the last flow closed read not-idle: a drain armed when \
         the listener is removed would never stop, pinning the listener and its socket"
    );
}

/// The combined accept form's only close/error result is the transport's own
/// error, so a parked combined waiter must return it — and returning it must
/// not be achieved by consuming the datagram the failed read never received,
/// nor by poisoning the accept queue for the retry.
///
/// A caller that treats a transport error as fatal stops accepting; a caller
/// that treats it as transient retries. Either way it needs to *see* the error:
/// a combined wait that swallowed it and kept waiting would report a dead
/// socket as silence, and one that swallowed it and returned a flow would
/// report it as traffic.
#[tokio::test(flavor = "current_thread")]
async fn a_parked_combined_waiter_observes_the_transports_error() {
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let transport = TeardownTransport::new(addr);
    let listener = UtpListener::new(
        transport.clone(),
        NonZeroUsize::new(4).unwrap(),
        key_dispatch(),
    );

    let mut waiter = Box::pin(listener.poll_next_conn());
    assert!(
        futures::poll!(waiter.as_mut()).is_pending(),
        "the combined accept must park while the queue is empty"
    );

    // One armed read failure, with the flow's datagram queued behind it.
    transport.fail_reads(1);
    transport.push(addr, 5u64.to_be_bytes().to_vec());

    match futures::poll!(waiter.as_mut()) {
        Poll::Ready(Err(err)) => {
            assert_eq!(
                err.kind(),
                std::io::ErrorKind::ConnectionReset,
                "the waiter reported an error that is not the transport's: {err}"
            );
            assert!(
                err.to_string().contains(READ_FAILURE),
                "the waiter reported a different error than the transport's: {err}"
            );
        }
        Poll::Ready(Ok(conn)) => panic!(
            "the parked combined waiter returned flow {:?} instead of the transport's error: a \
             failed read is reported as traffic",
            conn.conn_key()
        ),
        Poll::Pending => panic!(
            "HANG: the parked combined waiter stayed parked although its transport read failed: \
             {} datagram(s) are queued and {} remain unread in the transport",
            listener.accept_queue_len.load(Ordering::Acquire),
            transport.inbox_len()
        ),
    }
    assert_eq!(
        transport.inbox_len(),
        1,
        "the failed read consumed the datagram it never received"
    );
    assert_eq!(
        listener.stats().packets_received.load(Ordering::Relaxed),
        0,
        "a read that failed must not be counted as a received datagram"
    );
    assert_queue_consistent(&listener);

    // The error is transient, not poisoning: the datagram the failed read left
    // behind is handed back by the retry, and it is the only flow.
    let mut conn = listener
        .poll_next_conn()
        .await
        .expect("the retry after a transport error failed");
    assert_eq!(*conn.conn_key(), 5);
    assert_eq!(
        listener.stats().packets_received.load(Ordering::Relaxed),
        1,
        "the retry must read exactly the datagram the failed read left behind"
    );
    assert_eq!(
        listener.stats().connections_opened.load(Ordering::Relaxed),
        1
    );
    assert!(
        listener.try_accept_next().is_none(),
        "the retry left a second flow queued"
    );
    assert_eq!(
        conn.read_half().read_half().recv().await.unwrap().as_ref(),
        5u64.to_be_bytes(),
        "the accepted flow did not carry the datagram it was opened by"
    );
    drop(conn);
    assert!(
        listener.conn_table.lock().unwrap().is_empty(),
        "closing the retried flow left its conntrack entry behind"
    );
    assert_queue_consistent(&listener);
}

/// Which accept form a teardown cell parks in.
#[derive(Clone, Copy)]
enum Waiter {
    /// `accept_next` — the split form, which never reads the transport.
    Split,
    /// `poll_next_conn` — the combined form, which reads the transport too.
    Combined,
}
impl Waiter {
    /// The next accepted flow, normalized over both forms so one loop can drive
    /// either. `None` is the accept queue's (unreachable today) closed state.
    async fn next(self, listener: &Listener) -> std::io::Result<Option<WaitedConn>> {
        match self {
            Waiter::Split => Ok(listener.accept_next().await),
            Waiter::Combined => listener.poll_next_conn().await.map(Some),
        }
    }
}

/// What one bracketed accept wait produced.
enum Waited {
    /// A flow was handed back.
    Conn(WaitedConn),
    /// The caller's teardown signal landed first.
    Stopped,
    /// The transport failed the read the combined form was parked in.
    Error(std::io::Error),
}

/// One accept wait on the caller's teardown signal, with the **parked census**
/// bracketing the wait: the count is raised before the wait is polled and
/// lowered once it returns, so it is the number of waiters currently parked on
/// the accept path — the quantity a teardown must drain to zero, asserted
/// rather than assumed.
async fn wait_once(
    waiter: Waiter,
    listener: &Listener,
    shutdown: &mut watch::Receiver<bool>,
    parked: &AtomicUsize,
) -> Waited {
    let next = waiter.next(listener);
    tokio::pin!(next);
    parked.fetch_add(1, Ordering::SeqCst);
    let outcome = tokio::select! {
        biased;
        _ = shutdown.changed() => Waited::Stopped,
        result = &mut next => match result {
            Ok(Some(conn)) => Waited::Conn(conn),
            Ok(None) => Waited::Stopped,
            Err(err) => Waited::Error(err),
        },
    };
    parked.fetch_sub(1, Ordering::SeqCst);
    outcome
}

/// One waiter task: accept flows until the caller's signal lands, then exit.
async fn run_waiter(
    waiter: Waiter,
    listener: Arc<Listener>,
    mut shutdown: watch::Receiver<bool>,
    parked: Arc<AtomicUsize>,
    exited: Arc<AtomicUsize>,
    handled: Arc<AtomicUsize>,
) {
    loop {
        match wait_once(waiter, &listener, &mut shutdown, &parked).await {
            Waited::Stopped => break,
            Waited::Error(err) => {
                panic!("this cell never arms a read failure, yet a read failed: {err}")
            }
            Waited::Conn(_conn) => {
                handled.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    exited.fetch_add(1, Ordering::SeqCst);
}

/// Wait until an atomic counter equals `target`, reporting **HANG** (never
/// reached, with the count it did reach) separately from **LATE** (reached, but
/// after the configurable bound, which is a scheduling delay rather than a
/// defect) so a slow host does not read as a lost waiter.
async fn await_eq(counter: &AtomicUsize, target: usize, bound: Duration, what: &str) {
    let started = Instant::now();
    let reached = tokio::time::timeout(bound * 4, async {
        while counter.load(Ordering::SeqCst) != target {
            tokio::task::yield_now().await;
        }
    })
    .await;
    if reached.is_err() {
        panic!(
            "HANG ({what}): the counter reads {} where {target} is required, {:?} after the wait \
             began, with the bound {bound:?} (SOAK_WAKE_BOUND_MS)",
            counter.load(Ordering::SeqCst),
            started.elapsed(),
        );
    }
    let elapsed = started.elapsed();
    if elapsed > bound {
        println!("SOAK_WAKE_LATE {what} elapsed_ms={}", elapsed.as_millis());
    }
}

/// A teardown must drain every parked waiter, lose no queued flow to the
/// cancelled waits, leave no task parked, and release the listener.
///
/// The crate has no listener-side close: a waiter parked in `accept_next` can be
/// moved only by a queue change, and one parked in `poll_next_conn` also by a
/// transport error. Teardown is therefore the caller's signal, raced against the
/// accept exactly as the consumers do, and what this cell asserts is that the
/// crate's await points make that teardown complete: the **parked census**
/// (raised and lowered around each wait) reaches zero, every waiter task is
/// joined rather than aborted, a flow queued afterwards is still handed back
/// exactly once, the listener's transport is released, and — on a real socket —
/// the bound port becomes bindable only once the last parked waiter has ended.
///
/// The parked census is the leak detector: waiters that a teardown failed to
/// release would leave it above zero, and the tasks holding the listener's last
/// handles would leave the port pinned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_teardown_drains_every_parked_waiter_and_releases_the_listener() {
    const SPLIT: usize = 3;
    const COMBINED: usize = 2;
    const WAITERS: usize = SPLIT + COMBINED;
    let bound = Duration::from_millis(wake_env("SOAK_WAKE_BOUND_MS", 5_000) as u64);

    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let transport = TeardownTransport::new(addr);
    let listener = Arc::new(UtpListener::new(
        transport.clone(),
        NonZeroUsize::new(4).unwrap(),
        key_dispatch(),
    ));
    let parked = Arc::new(AtomicUsize::new(0));
    let exited = Arc::new(AtomicUsize::new(0));
    let handled = Arc::new(AtomicUsize::new(0));
    let (shutdown, shutdown_rx) = watch::channel(false);
    let mut tasks = JoinSet::new();
    for waiter in [Waiter::Split; SPLIT]
        .into_iter()
        .chain([Waiter::Combined; COMBINED])
    {
        tasks.spawn(run_waiter(
            waiter,
            Arc::clone(&listener),
            shutdown_rx.clone(),
            Arc::clone(&parked),
            Arc::clone(&exited),
            Arc::clone(&handled),
        ));
    }

    // Every waiter is parked, and nothing but the caller's signal (or a queue
    // change) can move them: the queue is empty and no read is armed to fail.
    await_eq(&parked, WAITERS, bound, "every waiter parked").await;
    assert_queue_consistent(&listener);
    assert_eq!(listener.accept_queue_len.load(Ordering::Acquire), 0);
    assert!(listener.conn_table.lock().unwrap().is_empty());
    assert_eq!(
        Arc::strong_count(&listener),
        1 + WAITERS,
        "each parked waiter must own one handle, or this cell is not parking what it counts"
    );
    assert_eq!(
        transport.live(),
        2,
        "the test and the listener must be the transport's only handles"
    );

    // Teardown. A waiter that arrives *during* it must observe the signal on its
    // first poll instead of parking on the accept path — a signal that a late
    // waiter cannot observe is a teardown that strands one.
    shutdown.send_replace(true);
    assert!(
        shutdown_rx.clone().changed().now_or_never().is_some(),
        "a waiter arriving during the teardown would park instead of observing the signal"
    );
    tasks.spawn(run_waiter(
        Waiter::Split,
        Arc::clone(&listener),
        shutdown_rx.clone(),
        Arc::clone(&parked),
        Arc::clone(&exited),
        Arc::clone(&handled),
    ));

    await_eq(&parked, 0, bound, "every parked waiter drained").await;
    // Join, never abort: a waiter that panicked must be observed by the owner.
    while let Some(result) = tasks.join_next().await {
        result.expect("a waiter task panicked");
    }
    assert_eq!(exited.load(Ordering::SeqCst), WAITERS + 1);
    assert_eq!(parked.load(Ordering::SeqCst), 0);
    assert_eq!(handled.load(Ordering::SeqCst), 0, "no flow was dialled");
    assert_eq!(
        Arc::strong_count(&listener),
        1,
        "a waiter task still holds the listener after the teardown"
    );
    assert_eq!(
        transport.live(),
        2,
        "the listener must still hold the transport while this handle does"
    );

    // The cancelled waits ate nothing: a flow queued afterwards comes back
    // exactly once, and the queue's two views still agree.
    transport.push(addr, 1u64.to_be_bytes().to_vec());
    assert_eq!(listener.dispatch_next().await.unwrap(), Dispatch::Accepted);
    let mut conn = listener
        .try_accept_next()
        .expect("a flow queued after the teardown was not handed back");
    assert_eq!(*conn.conn_key(), 1);
    assert_eq!(
        conn.read_half().read_half().recv().await.unwrap().as_ref(),
        1u64.to_be_bytes()
    );
    drop(conn);
    assert!(listener.try_accept_next().is_none());
    assert!(listener.conn_table.lock().unwrap().is_empty());
    assert_queue_consistent(&listener);
    drop(listener);
    assert_eq!(
        transport.live(),
        1,
        "dropping the last handle did not release the listener's transport"
    );

    // On a real socket the same teardown is observable as an OS resource: the
    // listener's socket is kept alive by the parked waiter, so the port stays
    // bound until that waiter ends.
    let udp = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind the listener socket");
    let listen_addr = udp.local_addr().expect("listener local addr");
    let real = Arc::new(UtpListener::new_identity_dispatch(
        udp,
        NonZeroUsize::new(4).unwrap(),
    ));
    let real_parked = Arc::new(AtomicUsize::new(0));
    let (real_shutdown, real_shutdown_rx) = watch::channel(false);
    let mut real_tasks = JoinSet::new();
    {
        let listener = Arc::clone(&real);
        let parked = Arc::clone(&real_parked);
        let mut shutdown = real_shutdown_rx;
        real_tasks.spawn(async move {
            loop {
                let accept = listener.accept_next();
                tokio::pin!(accept);
                parked.fetch_add(1, Ordering::SeqCst);
                let outcome = tokio::select! {
                    biased;
                    _ = shutdown.changed() => None,
                    conn = &mut accept => conn,
                };
                parked.fetch_sub(1, Ordering::SeqCst);
                match outcome {
                    Some(conn) => drop(conn),
                    None => break,
                }
            }
        });
    }
    await_eq(&real_parked, 1, bound, "the real-socket waiter parked").await;
    let owner = Arc::clone(&real);
    drop(real);
    assert_eq!(
        Arc::strong_count(&owner),
        2,
        "the parked waiter must be the listener's only other owner"
    );
    match tokio_udp::UdpSocket::bind(listen_addr).await {
        Ok(_) => panic!(
            "the port was bindable while a parked waiter still held the listener: the socket \
             was released under a live accept wait"
        ),
        Err(err) => assert_eq!(
            err.kind(),
            std::io::ErrorKind::AddrInUse,
            "the second bind failed for a reason other than the port being held: {err}"
        ),
    }
    real_shutdown.send_replace(true);
    await_eq(&real_parked, 0, bound, "the real-socket waiter drained").await;
    while let Some(result) = real_tasks.join_next().await {
        result.expect("the real-socket waiter panicked");
    }
    assert_eq!(Arc::strong_count(&owner), 1);
    drop(owner);
    tokio_udp::UdpSocket::bind(listen_addr)
        .await
        .unwrap_or_else(|err| {
            panic!("the port was not released after the last parked waiter ended: {err}")
        });
}
