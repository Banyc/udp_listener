//! Attribution of a dispatcher overflow to the flow whose buffer filled.
//!
//! `UtpListener::dispatch_next` is non-blocking by design: it `try_send`s each
//! classified datagram into the addressed flow's bounded channel, and when that
//! channel is full the datagram is dropped. Dropping under overload is the
//! correct policy for a reliable transport over UDP — the peer repairs it over a
//! round trip — so this arm does not test the policy.
//!
//! What it tests is that the drop is **attributable**. At the moment a path is
//! most degraded the datagrams that were dropped are exactly the ones carrying
//! no information about why, so a listener that counts overflow only in
//! aggregate leaves a reader able to say *that* something dropped but not
//! *which* flow to look at, and `rtp` repairs per flow. The arm offers a burst
//! into a channel whose reader is parked, then drains it, and requires the
//! every-datagram identity `offered == delivered + dropped` to hold on the flow's
//! own counters as well as on the listener's totals.
//!
//! One varying dimension from the crate's dispatch baseline
//! (`dispatch_delay::the_dispatch_path_adds_no_floor_to_a_lone_datagram`, a
//! ping-pong against a bare socket) is not enough to produce an overflow: the
//! offer has to exceed the drain *and* the channel has to be small. Both are
//! named in the cell this arm declares.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tokio_udp::UdpSocket;
use udp_listener::{Packet, UtpListener};

/// The per-flow channel bound. Small because the point of the arm is to overflow
/// it with a burst a loopback socket buffer absorbs whole: sizing it small keeps
/// the burst — and so the arm — small.
const CHANNEL_CAPACITY: usize = 4;
/// Datagrams offered after the opener — well above what the parked reader
/// drains, which is nothing.
const BURST: u64 = 64;
/// Slots already taken when the burst begins: the channel is created by the
/// opening datagram, which is still in it when the reader has not drained.
const IN_FLIGHT_AT_BURST: u64 = 1;
const QUIESCE_BOUND: Duration = Duration::from_secs(10);

type OverflowListener = UtpListener<UdpSocket, SocketAddr, Packet>;

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// Wait until every received datagram has resolved its `try_send`.
///
/// `packets_received` is counted before the send, and `packets_dispatched` /
/// `packets_dropped_dispatcher_full` after it, so the identity
/// `dispatched + dropped == received` holds only once the last datagram's
/// outcome is in — which is what makes the reads below race-free rather than
/// merely usually-race-free.
async fn quiesce(listener: &OverflowListener, offered: u64, bound: Duration) {
    let deadline = Instant::now() + bound;
    loop {
        let received = listener.stats().packets_received.load(Relaxed);
        let dispatched = listener.stats().packets_dispatched.load(Relaxed);
        let dropped = listener
            .stats()
            .packets_dropped_dispatcher_full
            .load(Relaxed);
        if received == offered && dispatched + dropped == offered {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the dispatch loop never accounted for all {offered} offered datagram(s): \
             received={received} dispatched={dispatched} dropped_dispatcher_full={dropped}"
        );
        tokio::task::yield_now().await;
    }
}

/// Offer a burst above the channel's drain and require the overflow to be
/// counted against the flow that dropped, not only against the listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dispatcher_overflow_is_attributed_to_the_flow_that_dropped() {
    let socket = UdpSocket::bind(loopback()).await.unwrap();
    let addr = socket.local_addr().unwrap();
    let listener = Arc::new(UtpListener::new_identity_dispatch(
        socket,
        NonZeroUsize::new(CHANNEL_CAPACITY).unwrap(),
    ));

    let mut tasks: JoinSet<()> = JoinSet::new();
    tasks.spawn({
        let listener = Arc::clone(&listener);
        async move {
            loop {
                // The production shape: one datagram per call, driven by
                // whichever task owns the socket.
                listener
                    .dispatch_next()
                    .await
                    .expect("the listener socket failed");
            }
        }
    });

    let client = UdpSocket::bind(loopback()).await.unwrap();
    client.connect(addr).await.unwrap();
    // The opener creates the flow; accepting it does not drain it, so the
    // channel keeps this slot for the whole of the burst below.
    client.send(&0u64.to_be_bytes()).await.unwrap();
    let mut conn = listener.accept_next().await.expect("never None");

    // The reader is parked for the whole burst: the channel fills, not the
    // socket, so every drop below is this layer's own overflow drop.
    for seq in 1..=BURST {
        client.send(&seq.to_be_bytes()).await.unwrap();
    }
    let offered = IN_FLIGHT_AT_BURST + BURST;
    quiesce(&listener, offered, QUIESCE_BOUND).await;

    let received = listener.stats().packets_received.load(Relaxed);
    assert_eq!(
        received, offered,
        "the arm offered {offered} datagram(s) and the listener read {received}: a datagram lost \
         below this layer makes the accounting below inconclusive, so it is a failure rather than \
         a smaller sample"
    );

    let per_flow_reading = conn.stats().packets_dropped_dispatcher_full.load(Relaxed);
    let aggregate_dropped = listener
        .stats()
        .packets_dropped_dispatcher_full
        .load(Relaxed);

    let mut delivered = 0u64;
    while conn.read_half().read_half().try_recv().is_ok() {
        delivered += 1;
    }
    let per_flow_dropped = conn.stats().packets_dropped_dispatcher_full.load(Relaxed);
    assert_eq!(
        per_flow_dropped, per_flow_reading,
        "draining the flow changed its recorded overflow count ({per_flow_reading} then \
         {per_flow_dropped}); receiving a datagram is not a drop"
    );

    let (read, _write) = conn.split();
    assert_eq!(
        read.stats().packets_dropped_dispatcher_full.load(Relaxed),
        per_flow_dropped,
        "the split read half reports a different overflow count than the connection it came from"
    );

    let expected_dropped = BURST - (CHANNEL_CAPACITY as u64 - IN_FLIGHT_AT_BURST);
    let drop_rate = per_flow_dropped as f64 / offered as f64;
    println!(
        "DISPATCH_OVERFLOW_STATS offered={offered} received={received} delivered={delivered} \
         dropped_dispatcher_full_per_flow={per_flow_dropped} \
         dropped_dispatcher_full_aggregate={aggregate_dropped} \
         channel_capacity={CHANNEL_CAPACITY} drop_rate={drop_rate:.3} flows=1"
    );

    // Delivery: the channel's whole capacity arrives at the flow, so the drop is
    // an overflow of a working path and not a blackhole.
    assert_eq!(
        delivered, CHANNEL_CAPACITY as u64,
        "the flow's channel delivered {delivered} of its {CHANNEL_CAPACITY} slots while {offered} \
         datagram(s) were offered and {per_flow_dropped} were counted as overflow drops"
    );
    assert!(
        delivered > 0,
        "the arm delivered nothing to the flow, so it measured an outage rather than an overload"
    );

    // Attribution: the drop is on the flow's own counter, with the count the
    // offered burst minus the slots the channel had.
    assert_eq!(
        per_flow_dropped, expected_dropped,
        "the flow's own overflow count is {per_flow_dropped}, not {expected_dropped}: offered \
         {offered} datagram(s), delivered {delivered}, aggregate overflow \
         {aggregate_dropped}, channel capacity {CHANNEL_CAPACITY}"
    );
    assert!(
        per_flow_dropped > 0,
        "no overflow was recorded at all (offered {offered}, delivered {delivered}, aggregate \
         {aggregate_dropped}): the arm never overflowed the channel it exists to overflow"
    );

    // The identity a reader uses to trust either number: with one flow, the
    // listener's total is this flow's count, and every offered datagram is
    // either delivered or dropped.
    assert_eq!(
        aggregate_dropped, per_flow_dropped,
        "one flow carried the whole burst, but the listener totals {aggregate_dropped} overflow \
         drop(s) against the flow's own {per_flow_dropped}"
    );
    assert_eq!(
        offered,
        delivered + per_flow_dropped,
        "offered {offered} datagram(s) are not delivered ({delivered}) plus dropped \
         ({per_flow_dropped}): {} datagram(s) were counted into no path",
        offered as i64 - (delivered + per_flow_dropped) as i64
    );

    drop(client);
    tasks.shutdown().await;
}
