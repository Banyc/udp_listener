//! Which of the two receive-side buffers is the binding drop site.
//!
//! `rtp` reaches the network through two buffers in series beneath it: the
//! kernel's receive queue, sized by `tokio_udp::UdpSocket::set_recv_buffer_size`
//! (exposed but, in this workspace, never called by a consumer), and
//! `udp_listener`'s per-flow dispatcher channel, sized by the
//! `dispatcher_buffer_size` argument to `UtpListener::new`. A datagram can be
//! refused at either. Only the second refusal is attributable: the dispatcher
//! counts it against the flow whose channel filled
//! (`ConnStats::packets_dropped_dispatcher_full`), while a datagram the kernel
//! refused before `recv` was never seen by this crate at all, so it is
//! indistinguishable from path loss.
//!
//! The question this arm answers is whether sizing the *kernel* buffer reduces
//! the *dispatcher* drop. The mechanism says no, and this arm measures it: the
//! dispatch loop never blocks on the channel (`try_send`, drop on `Full`), so
//! once it is running it drains the kernel queue at full rate and the kernel
//! buffer is irrelevant. The kernel buffer matters only while the dispatch loop
//! is not running — a scheduling stall — and there it decides how many of the
//! arrivals reach the dispatcher at all. So the two sites are related by an
//! identity, not by a trade the operator can win:
//!
//! ```text
//! offered = received + kernel_dropped
//! received = channel_accepted + dispatcher_dropped
//! channel_accepted = min(received, channel_capacity)
//! ```
//!
//! Sizing the kernel buffer moves a datagram from `kernel_dropped` into
//! `dispatcher_dropped`; it reduces *total* loss only while `received` is below
//! the channel capacity, and never reduces `dispatcher_dropped` (which rises
//! monotonically with the buffer). The knee is therefore where the kernel buffer
//! holds as many datagrams as the channel — `received == channel_capacity`.
//!
//! The load shape is a burst offered while the dispatch task is deliberately not
//! polled, which is the shape a scheduling stall produces and the only shape in
//! which the kernel buffer can fill at all. On a live dispatch loop the kernel
//! queue never accumulates. The channel capacity is `rtp`'s own
//! `DISPATCHER_BUF_SIZE` (`rtp/src/udp.rs:71`), so the knee this arm finds is the
//! one the deployed transport sits on.
//!
//! This arm is a refusal arm: it does not change a default. It is `#[ignore]`d
//! (the `standard` tier) because it stages ten bursts of `BURST` datagrams, which
//! is more than the crate's one-second default budget.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tokio_udp::UdpSocket;
use udp_listener::{Packet, UtpListener};

/// The deployed transport's per-flow channel bound (`rtp/src/udp.rs:71`
/// `DISPATCHER_BUF_SIZE = 1024`, plus the data-settings slack rtp adds). The
/// knee is where the kernel buffer holds this many datagrams.
const CHANNEL_CAPACITY: usize = 1024;

/// Datagrams offered per row after the opener. Above every kernel capacity on
/// this host, so each row's `received` is the kernel buffer's own capacity in
/// datagrams rather than the burst's length.
const BURST: usize = 32_768;

/// `rtp`'s wire datagram is a chunk of the MTU; the interactive lane is small
/// and the bulk lane is near MTU, so both payload sizes are swept.
const DATAGRAM_SIZES: [usize; 2] = [256, 1200];

/// The five receive-buffer budgets, `None` being the kernel's own default.
/// `linux-default` is the deployed target's `net.core.rmem_default`; this host's
/// unsized default is larger, so it is the *weaker* instrument and the explicit
/// Linux budget is carried for that reason.
const BUDGETS: [(Option<usize>, &str); 5] = [
    (Some(4 << 10), "floor-4KiB"),
    (Some(212_992), "linux-default"),
    (None, "host-default"),
    (Some(1 << 20), "1MiB"),
    (Some(4 << 20), "4MiB"),
];

type SweepListener = UtpListener<UdpSocket, SocketAddr, Packet>;

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// One row of the sweep: the counters after a burst against one buffer budget.
#[derive(Debug, Clone, Copy)]
struct Row {
    label: &'static str,
    effective: usize,
    offered: u64,
    received: u64,
    channel_accepted: u64,
    dispatcher_dropped: u64,
    delivered: u64,
}

impl Row {
    fn kernel_dropped(&self) -> u64 {
        self.offered - self.received
    }
    fn total_lost(&self) -> u64 {
        self.offered - self.delivered
    }
}

/// Drain the socket into the dispatcher until the counters stop moving.
///
/// `packets_received` is incremented before the `try_send`, and the dispatch is
/// the only writer of both `packets_dispatched` and
/// `packets_dropped_dispatcher_full`, so a run of unchanged reads means the
/// dispatch loop has caught up with everything the kernel keeps.
async fn quiesce(listener: &SweepListener, bound: Duration) -> (u64, u64, u64) {
    let deadline = Instant::now() + bound;
    let mut last = (u64::MAX, u64::MAX, u64::MAX);
    let mut stable = 0u32;
    loop {
        let now = (
            listener.stats().packets_received.load(Relaxed),
            listener.stats().packets_dispatched.load(Relaxed),
            listener
                .stats()
                .packets_dropped_dispatcher_full
                .load(Relaxed),
        );
        if now == last {
            stable += 1;
            if stable >= 25 {
                return now;
            }
        } else {
            stable = 0;
            last = now;
        }
        assert!(
            Instant::now() < deadline,
            "the dispatch loop never fell quiet within {bound:?}: {now:?}"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Offer `BURST` datagrams while nothing reads the socket, then start the
/// dispatch loop and read the two drop sites.
async fn one_row(size: usize, request: Option<usize>, label: &'static str) -> Row {
    let socket = UdpSocket::bind(loopback()).await.unwrap();
    if let Some(bytes) = request {
        socket.set_recv_buffer_size(bytes).unwrap();
    }
    let effective = socket.recv_buffer_size().unwrap();
    let addr = socket.local_addr().unwrap();
    let listener = Arc::new(UtpListener::new_identity_dispatch(
        socket,
        NonZeroUsize::new(CHANNEL_CAPACITY).unwrap(),
    ));

    let client = UdpSocket::bind(loopback()).await.unwrap();
    client.connect(addr).await.unwrap();
    let payload = vec![0x5Au8; size];

    // The opener creates the flow. `poll_next_conn` drives the one datagram,
    // so the flow is open and its channel holds the opener before the burst.
    client.send(&payload).await.unwrap();
    let mut conn = listener.poll_next_conn().await.expect("never None");

    // The burst lands while no task is reading the socket: whatever the kernel
    // receive queue does not hold is refused before `udp_listener` can count it.
    for seq in 0..BURST {
        client
            .send(&payload)
            .await
            .unwrap_or_else(|e| panic!("send {seq} of {BURST} failed at size {size}: {e}"));
    }
    // Let the last arrivals reach the queue and the overflow be applied.
    tokio::time::sleep(Duration::from_millis(2)).await;

    // Now the dispatch loop runs and drains the kernel queue into the channel.
    let mut drainer: JoinSet<()> = JoinSet::new();
    drainer.spawn({
        let listener = Arc::clone(&listener);
        async move {
            loop {
                if listener.dispatch_next().await.is_err() {
                    break;
                }
            }
        }
    });

    let (received, channel_accepted, dispatcher_dropped) =
        quiesce(&listener, Duration::from_secs(15)).await;

    // The per-flow counter is the feature this arm exists to exercise; the
    // aggregate and the flow's own reading must be the same drop counted twice.
    let per_flow_dropped = conn.stats().packets_dropped_dispatcher_full.load(Relaxed);
    assert_eq!(
        per_flow_dropped, dispatcher_dropped,
        "size {size} {label}: the flow's own overflow count is {per_flow_dropped} but the listener \
         totals {dispatcher_dropped} over its one flow"
    );

    // `channel_accepted` is every successful `try_send`; draining the channel
    // must hand back exactly those and nothing else.
    let mut delivered = 0u64;
    while conn.read_half().read_half().try_recv().is_ok() {
        delivered += 1;
    }
    assert_eq!(
        conn.stats().packets_dropped_dispatcher_full.load(Relaxed),
        per_flow_dropped,
        "size {size} {label}: draining the flow changed its recorded overflow count"
    );
    drainer.abort_all();

    let offered = (BURST + 1) as u64; // the opener is offered too

    // The instrument's own sanity, per row: every offered datagram is either
    // read by this layer or refused below it, and every one this layer read is
    // either handed to the channel or counted against it.
    assert!(
        received <= offered,
        "size {size} {label}: the listener read {received} of {offered} offered datagrams"
    );
    assert_eq!(
        channel_accepted + dispatcher_dropped,
        received,
        "size {size} {label}: received {received} != accepted {channel_accepted} + dispatcher \
         dropped {dispatcher_dropped}"
    );
    assert_eq!(
        delivered, channel_accepted,
        "size {size} {label}: the channel accepted {channel_accepted} datagrams but handed back \
         {delivered}: a datagram entered no drop counter"
    );
    assert!(
        delivered <= CHANNEL_CAPACITY as u64,
        "size {size} {label}: the channel delivered {delivered} datagrams over its \
         {CHANNEL_CAPACITY}-slot bound"
    );
    assert_eq!(
        channel_accepted,
        received.min(CHANNEL_CAPACITY as u64),
        "size {size} {label}: the channel accepted {channel_accepted} of {received} received \
         datagrams, not min(received, {CHANNEL_CAPACITY})"
    );

    Row {
        label,
        effective,
        offered,
        received,
        channel_accepted,
        dispatcher_dropped,
        delivered,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "stages ten bursts of 32768 datagrams; the `standard` tier, declared in GATE.md"]
async fn sizing_the_receive_buffer_moves_a_drop_between_sites_and_the_knee_is_the_channel() {
    for size in DATAGRAM_SIZES {
        let mut rows = Vec::new();
        for (request, label) in BUDGETS {
            let row = one_row(size, request, label).await;
            println!(
                "RECV_BUFFER_DROP_SITES size={size:<5} label={label:<14} want={:<11} \
                 effective={:<9} offered={:<6} received={:<6} channel_accepted={:<6} \
                 dispatcher_dropped={:<6} kernel_dropped={:<7} delivered={:<6} total_lost={:<7} \
                 channel_capacity={CHANNEL_CAPACITY}",
                request.map_or("none".to_string(), |v| v.to_string()),
                row.effective,
                row.offered,
                row.received,
                row.channel_accepted,
                row.dispatcher_dropped,
                row.kernel_dropped(),
                row.delivered,
                row.total_lost(),
            );
            rows.push(row);
        }

        // Order by the value the kernel actually accepted, so the sweep's
        // direction is the buffer's and not the request's.
        rows.sort_by_key(|r| r.effective);

        // The setter must reach the kernel before anything below means
        // anything: two different requests cannot land at one effective size,
        // and the largest effective size must exceed the smallest. If this
        // fails, every row below measured the same buffer and a "no effect"
        // reading would be the fixture, not the socket.
        assert!(
            rows.windows(2).all(|w| w[0].effective < w[1].effective),
            "size {size}: the sweep's {}-row budget series did not produce strictly increasing \
             effective buffer sizes: {:?}",
            rows.len(),
            rows.iter()
                .map(|r| (r.label, r.effective))
                .collect::<Vec<_>>()
        );

        // Monotone in the buffer: more held, fewer refused below the layer,
        // more reaching the dispatcher, more delivered, less lost in total.
        for w in rows.windows(2) {
            let (lo, hi) = (w[0], w[1]);
            assert!(
                hi.received >= lo.received,
                "size {size}: {hi:?} held fewer datagrams than {lo:?}"
            );
            assert!(
                hi.kernel_dropped() <= lo.kernel_dropped(),
                "size {size}: a larger buffer refused more below the layer: {lo:?} -> {hi:?}"
            );
            assert!(
                hi.delivered >= lo.delivered,
                "size {size}: a larger buffer delivered fewer datagrams: {lo:?} -> {hi:?}"
            );
            assert!(
                hi.total_lost() <= lo.total_lost(),
                "size {size}: a larger buffer lost more in total: {lo:?} -> {hi:?}"
            );
            assert!(
                hi.dispatcher_dropped >= lo.dispatcher_dropped,
                "size {size}: a larger buffer *reduced* the dispatcher drops, which is the \
                 direction this arm exists to falsify: {lo:?} -> {hi:?}"
            );
        }

        // The knee: a row whose whole kernel capacity is below the channel, and
        // a row whose capacity exceeds it. Below the knee the dispatcher drops
        // nothing and total loss is entirely kernel-side; above it the total is
        // pinned at `offered - channel_capacity` and the kernel refusal has
        // become an attributable dispatcher drop.
        let below = rows
            .iter()
            .find(|r| r.received < CHANNEL_CAPACITY as u64)
            .copied()
            .expect("no row held fewer datagrams than the channel; the burst is too small");
        let above = rows
            .iter()
            .find(|r| r.received > CHANNEL_CAPACITY as u64)
            .copied()
            .expect("no row held more datagrams than the channel; the largest budget is too small");
        assert_eq!(
            below.dispatcher_dropped, 0,
            "the below-knee row {below:?} already overflowed the channel"
        );
        assert!(
            below.delivered < CHANNEL_CAPACITY as u64,
            "the below-knee row {below:?} filled the channel, so it is not below the knee"
        );
        assert!(
            above.dispatcher_dropped > 0,
            "the above-knee row {above:?} recorded no dispatcher drop"
        );
        assert_eq!(
            above.delivered, CHANNEL_CAPACITY as u64,
            "the above-knee row {above:?} did not fill the channel"
        );
        assert!(
            above.total_lost() < below.total_lost(),
            "size {size}: raising the buffer to the above-knee row {above:?} did not reduce total \
             loss against the below-knee row {below:?}, so the knee is not where this says it is"
        );
        // The refusal this arm exists to deliver: the dispatcher drop is the
        // channel's, and it does not fall as the kernel buffer grows.
        assert!(
            above.kernel_dropped() < below.kernel_dropped(),
            "size {size}: the above-knee row {above:?} refused no less below the layer than the \
             below-knee row {below:?}"
        );
        // Above the knee the channel is saturated, so total loss is pinned at
        // `offered - channel_capacity` whatever the kernel buffer holds: the
        // extra buffer depth buys delivery only up to the channel and no more.
        // This is the arithmetic that says the binding site is the channel.
        for row in rows.iter().filter(|r| r.received > CHANNEL_CAPACITY as u64) {
            assert_eq!(
                row.total_lost(),
                row.offered - CHANNEL_CAPACITY as u64,
                "size {size}: above-knee row {row:?} lost {} of {} offered, but a saturated \
                 {CHANNEL_CAPACITY}-slot channel pins total loss at {}",
                row.total_lost(),
                row.offered,
                row.offered - CHANNEL_CAPACITY as u64,
            );
        }

        println!(
            "RECV_BUFFER_DROP_SITES_KNEE size={size} channel_capacity={CHANNEL_CAPACITY} \
             below={} (received={} effective={} kernel_dropped={} total_lost={}) \
             above={} (received={} effective={} dispatcher_dropped={} kernel_dropped={} \
             total_lost={})",
            below.label,
            below.received,
            below.effective,
            below.kernel_dropped(),
            below.total_lost(),
            above.label,
            above.received,
            above.effective,
            above.dispatcher_dropped,
            above.kernel_dropped(),
            above.total_lost(),
        );
    }
}
