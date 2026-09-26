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
//! - The combined form observes a **transport error** as its only close/error
//!   result, and that error must not consume the accept queue.
//! - A **drain** that releases its signal must leave no waiter parked, must not
//!   lose a queued flow to the cancelled waits, must release the listener (its
//!   transport, and on a real socket the bound port), and must let a waiter
//!   that arrives *during* the teardown observe the signal on its first poll.
//! - The accept queue must be **committed before the wake is delivered**.

use core::net::SocketAddr;
use core::num::NonZeroUsize;
use std::{
    collections::VecDeque,
    io::IoSlice,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::BufMut;

use crate::{
    Classified, Classify, Dispatch, DispatchPolicy, Packet, UnreliableTransmit, UtpListener,
};

/// One datagram handed to the transport: its source and its bytes.
type Datagram = (SocketAddr, Vec<u8>);
/// The transport's inbox of not-yet-read datagrams.
type Inbox = Arc<Mutex<VecDeque<Datagram>>>;

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
}
impl TeardownTransport {
    fn new(local: SocketAddr) -> Self {
        Self {
            local,
            inbox: Arc::new(Mutex::new(VecDeque::new())),
            available: Arc::new(tokio::sync::Notify::new()),
            live: Arc::new(AtomicUsize::new(1)),
        }
    }
    /// Hand one datagram to the next read.
    fn push(&self, from: SocketAddr, data: Vec<u8>) {
        self.inbox.lock().unwrap().push_back((from, data));
        self.available.notify_one();
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
