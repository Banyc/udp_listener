//! The per-flow dispatcher channel's own capacity: the sweep, the knee, the cost.
//!
//! The sibling arm (`recv_buffer_drop_sites`) established that above the knee
//! `channel_accepted = min(received, channel_capacity)` and that the residual
//! loss after that is the channel's — sizing the *kernel* receive buffer only
//! relocates a refusal onto this crate's attributable counter. This arm sweeps
//! the **channel's own capacity** instead: the `dispatcher_buffer_size`
//! argument to `UtpListener::new`, which sizes the per-flow
//! `tokio::sync::mpsc::channel` this crate creates in `src/lib.rs` (`:479` on
//! the dispatch path, `:589` in `register_conn`). `rtp` is the caller that
//! picks the value (`rtp/src/udp.rs:274`, `keyed_udp.rs:107,215`,
//! `mpudp.rs:30`), but the channel and its capacity are this crate's seam, so
//! the sweep needs no change in `rtp`.
//!
//! One varying dimension per arm:
//!
//! * The **stalled-consumer** sweep holds the capacity as the varying
//!   dimension against a fixed burst while nothing drains the flow. It answers
//!   whether loss falls to zero at some capacity (the channel only has to be
//!   as deep as the burst a stalled consumer accumulates) and measures the
//!   memory that depth retains.
//! * The **live-consumer** arm holds the capacity at `rtp`'s own
//!   `DISPATCHER_BUF_SIZE` (1024) and varies only whether the consumer drains,
//!   offering far more than the channel holds. It measures the drain rate
//!   beside the offer rate, which is the dimension that decides whether the
//!   residual is a burst bound or a rate mismatch.
//!
//! The dispatch loop is live in both arms from the first datagram, so the
//! *kernel* receive queue never accumulates and the channel is the only buffer
//! under test. That is deliberately the opposite of the sibling arm's
//! unpolled-dispatch shape: here `received` is every datagram the wire
//! delivered, so the channel sees the whole burst. This arm therefore inherits
//! the sibling's coverage gap the other way — a *dispatch-loop* stall (the
//! shape `tokio_udp`'s `rcvbuf_cliff` measures) is not a cell here, and with a
//! live dispatch loop a kernel drop is as attributable as it is in production,
//! i.e. not at all.
//!
//! Both arms are the `standard` tier (opt-in).

use std::alloc::{GlobalAlloc, Layout, System};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tokio_udp::UdpSocket;
use udp_listener::{PACKET_BUFFER_LENGTH, Packet, UtpListener};

/// Counting global allocator, so the channel's retained memory is measured and
/// not asserted from the pool's construction.
///
/// A pooled `Packet` is an `ObjScoped<BytesMut>` whose `BytesMut` was
/// allocated with `PACKET_BUFFER_LENGTH` (65 536 B) of capacity and is *not*
/// shrunk to the datagram after `recv_buf_from` writes into it, so the number
/// of bytes the process holds because the channel holds N datagrams is a
/// quantity worth reading rather than reasoning about.
struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            LIVE.fetch_add(layout.size(), Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Relaxed);
        unsafe { System.dealloc(p, layout) };
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, layout, new_size) };
        if !q.is_null() {
            if new_size >= layout.size() {
                LIVE.fetch_add(new_size - layout.size(), Relaxed);
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Relaxed);
            }
        }
        q
    }
}
#[global_allocator]
static ALLOC: Counting = Counting;

/// Datagrams offered after the opener. Above every capacity in the sweep, so
/// the small-capacity rows overflow and the large-capacity rows absorb the
/// whole burst — the knee has to be *inside* the sweep for the reading to mean
/// anything.
const BURST: usize = 4_096;
/// The interactive lane's payload size. The pooled buffer is
/// `PACKET_BUFFER_LENGTH` regardless, so this is the datum the *wire* carries,
/// not the memory it commits.
const DATAGRAM_SIZE: usize = 256;
/// The channels swept, geometric so the knee's neighbourhood is resolved
/// cheaply. `rtp`'s own `DISPATCHER_BUF_SIZE` (1024) is one of them, and the
/// largest (8192) exceeds `BURST + 1`, so at least one row must absorb it all.
const CAPACITIES: [usize; 6] = [64, 256, 1024, 2048, 4096, 8192];
/// A kernel receive buffer that holds more than the whole burst, so a row's
/// `received` is the wire's delivery and not the kernel's capacity — the point
/// of this arm is to size the channel, and a kernel-limited `received` would
/// confound the two.
const RECV_BUF: usize = 4 << 20;
const QUIESCE_BOUND: Duration = Duration::from_secs(15);

/// The two arms in this binary share the process-global counting allocator, so
/// their `bytes` and `leaked` readings are only each other's if they do not run
/// at the same time. `cargo test` runs test functions concurrently, and either
/// arm's in-flight buffers inflate the other's subtraction (measured on the
/// unmodified tree: the stalled arm's `leaked` check failed while the live arm
/// held its own buffers). This guard serializes them; the arms, their windows,
/// capacities and thresholds are unchanged.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

type SweepListener = UtpListener<UdpSocket, SocketAddr, Packet>;

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// Wait until `packets_received` and its resolution (`dispatched` /
/// `dispatcher_dropped`) stop moving. `packets_received` is incremented before
/// the `try_send`, so a run of unchanged reads is the dispatch loop having
/// caught up with everything the kernel keeps.
///
/// The reads are spaced by a real millisecond, as the sibling arm's quiesce is:
/// a bare `yield_now` between reads lets a burst still in the kernel queue
/// arrive after the window closes, which showed up as a channel draining more
/// datagrams than it had accepted. The window only *proposes*; the caller stops
/// the dispatch task before it reads the counters, so no `try_send` can land
/// between the reading and the drain.
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

/// One row: counters and retained memory after a burst against one capacity.
#[derive(Debug, Clone, Copy)]
struct Row {
    capacity: usize,
    offered: u64,
    received: u64,
    accepted: u64,
    dropped: u64,
    delivered: u64,
    bytes: usize,
    drain_ns: u128,
}

impl Row {
    fn kernel_dropped(&self) -> u64 {
        self.offered - self.received
    }
    fn bytes_per_slot(&self) -> f64 {
        self.bytes as f64 / self.delivered as f64
    }
    /// A tight consumer's drain rate over this row's channel contents: how fast
    /// the mpsc receiver can be emptied once the offer has stopped.
    fn drain_rate(&self) -> f64 {
        self.delivered as f64 / (self.drain_ns as f64 / 1e9)
    }
}

/// Offer the burst against one capacity with a live dispatch loop and a
/// stalled consumer, then read both counters and the retained memory.
async fn stalled_row(capacity: usize) -> Row {
    let socket = UdpSocket::bind(loopback()).await.unwrap();
    socket.set_recv_buffer_size(RECV_BUF).unwrap();
    let addr = socket.local_addr().unwrap();
    let listener = Arc::new(UtpListener::new_identity_dispatch(
        socket,
        NonZeroUsize::new(capacity).unwrap(),
    ));
    let client = UdpSocket::bind(loopback()).await.unwrap();
    client.connect(addr).await.unwrap();
    let payload = [0x5Au8; DATAGRAM_SIZE];

    // Live from the first datagram: the kernel queue never accumulates, so what
    // the channel is offered is what the wire delivered.
    let mut tasks: JoinSet<()> = JoinSet::new();
    tasks.spawn({
        let listener = Arc::clone(&listener);
        async move {
            loop {
                if listener.dispatch_next().await.is_err() {
                    break;
                }
            }
        }
    });

    // The opener creates the flow. It is accepted but not drained, so the
    // consumer is stalled for the whole burst.
    client.send(&payload).await.unwrap();
    let mut conn = listener.accept_next().await.expect("never None");

    let before = LIVE.load(Relaxed);
    for seq in 0..BURST {
        client
            .send(&payload)
            .await
            .unwrap_or_else(|e| panic!("send {seq} of {BURST} failed at capacity {capacity}: {e}"));
    }
    let _ = quiesce(&listener, QUIESCE_BOUND).await;
    let bytes = LIVE.load(Relaxed).saturating_sub(before);

    // Stop the dispatch loop before reading the counters: quiesce only proposes
    // that the burst has landed, and a `try_send` landing between the counter
    // read and the drain would make `delivered > accepted` — which is exactly
    // what the first run of this arm measured at capacities 256 and 2048.
    tasks.shutdown().await;
    let received = listener.stats().packets_received.load(Relaxed);
    let accepted = listener.stats().packets_dispatched.load(Relaxed);
    let dropped = listener
        .stats()
        .packets_dropped_dispatcher_full
        .load(Relaxed);

    let drain_started = Instant::now();
    let mut delivered = 0u64;
    while conn.read_half().read_half().try_recv().is_ok() {
        delivered += 1;
    }
    let drain_ns = drain_started.elapsed().as_nanos();
    drop(conn);
    drop(listener);
    drop(client);

    Row {
        capacity,
        offered: (BURST + 1) as u64,
        received,
        accepted,
        dropped,
        delivered,
        bytes,
        drain_ns,
    }
}

/// Sweep the channel's own capacity against a fixed burst with the flow never
/// drained: find the knee and the memory it costs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "sweeps six channel capacities against a 4096-datagram burst; the `standard` tier, declared in GATE.md"]
async fn the_channel_capacity_sets_the_burst_a_stalled_consumer_accumulates() {
    let _serial = SERIAL.lock().await;
    let baseline = LIVE.load(Relaxed);
    let mut rows = Vec::new();
    for capacity in CAPACITIES {
        let row = stalled_row(capacity).await;
        println!(
            "CHANNEL_CAPACITY_SWEEP capacity={:<6} offered={:<6} received={:<6} accepted={:<6} \
             dispatcher_dropped={:<6} kernel_dropped={:<6} delivered={:<6} bytes={:<10} \
             bytes_per_slot={:.0} drain_datagrams_per_s={:.0}",
            row.capacity,
            row.offered,
            row.received,
            row.accepted,
            row.dropped,
            row.kernel_dropped(),
            row.delivered,
            row.bytes,
            row.bytes_per_slot(),
            row.drain_rate(),
        );
        rows.push(row);
    }
    let leaked = LIVE.load(Relaxed).saturating_sub(baseline);
    println!("CHANNEL_CAPACITY_SWEEP_RETURNED_TO_BASELINE leaked_bytes={leaked}");

    // The channel's own identity, per row: every received datagram was either
    // handed to the channel or counted against it, and the channel accepted
    // exactly its capacity once it was full. This is the sibling arm's identity,
    // now with the capacity as the variable.
    for row in &rows {
        assert_eq!(
            row.accepted + row.dropped,
            row.received,
            "capacity {}: received {} != accepted {} + dispatcher dropped {}",
            row.capacity,
            row.received,
            row.accepted,
            row.dropped,
        );
        assert_eq!(
            row.accepted,
            row.received.min(row.capacity as u64),
            "capacity {}: the channel accepted {} of {} received datagrams, not \
             min(received, {})",
            row.capacity,
            row.accepted,
            row.received,
            row.capacity,
        );
        assert_eq!(
            row.delivered, row.accepted,
            "capacity {}: the channel accepted {} but handed back {} on drain",
            row.capacity, row.accepted, row.delivered,
        );
        assert_eq!(
            row.dropped,
            row.received.saturating_sub(row.capacity as u64),
            "capacity {}: dispatcher drops {} are not the overflow of {} received over \
             {} slots",
            row.capacity,
            row.dropped,
            row.received,
            row.capacity,
        );
    }

    // Reachability: the smallest capacity row must have overflowed (the arm
    // actually filled the channel) and the largest must have absorbed the whole
    // burst (the knee is inside the sweep). A sweep that never filled the
    // channel measures nothing; one that never emptied it either finds no knee.
    let smallest = rows.first().expect("sweep produced no rows");
    assert!(
        smallest.dropped > 0,
        "the {}-slot row recorded no dispatcher drop ({smallest:?}); the burst never filled \
         the channel and the sweep's direction measures nothing",
        smallest.capacity,
    );
    let largest = rows.last().expect("sweep produced no rows");
    assert_eq!(
        largest.dropped, 0,
        "the {}-slot row still dropped {} — the largest capacity in the sweep does not absorb \
         the burst, so the knee is outside the sweep: {largest:?}",
        largest.capacity, largest.dropped,
    );

    // Loss falls to zero with capacity: at the knee every received datagram is
    // delivered, so the residual loss in the knee row (and every row above it)
    // is the kernel's alone. This is the reading that says a stalled consumer's
    // accumulation is bounded by the burst, not by a rate.
    let knee = rows
        .iter()
        .find(|r| r.dropped == 0)
        .expect("no row reached zero dispatcher drops");
    assert!(
        knee.received > 0,
        "the knee row {knee:?} received nothing, so its zero drop is vacuous"
    );
    assert_eq!(
        knee.delivered, knee.received,
        "the knee row {knee:?} did not deliver every received datagram"
    );
    println!(
        "CHANNEL_CAPACITY_SWEEP_KNEE first_capacity_with_zero_drops={} received={} \
         delivered={} kernel_dropped={} bytes_per_slot={:.0}",
        knee.capacity,
        knee.received,
        knee.delivered,
        knee.kernel_dropped(),
        knee.bytes_per_slot(),
    );

    // Memory: a channel slot retains a pooled `PACKET_BUFFER_LENGTH` buffer, not
    // the datagram, so the retained bytes are the slots times that constant and
    // not the slots times the wire payload. Read, not assumed — the lower bound
    // is the pooled capacity the datum sits in, and a fixture that stopped
    // retaining would fall below it.
    for row in rows.iter().filter(|r| r.delivered > 0) {
        assert!(
            row.bytes as u64 >= row.delivered * PACKET_BUFFER_LENGTH as u64,
            "capacity {}: {} delivered datagrams retained only {} bytes, under one \
             PACKET_BUFFER_LENGTH ({}) each — the channel is holding less than the pooled \
             buffer, so the cost reading is a fixture",
            row.capacity,
            row.delivered,
            row.bytes,
            PACKET_BUFFER_LENGTH,
        );
        assert!(
            row.bytes as u64 <= row.delivered * PACKET_BUFFER_LENGTH as u64 * 2,
            "capacity {}: {} delivered datagrams retained {} bytes, over twice one \
             PACKET_BUFFER_LENGTH each — an unexpected second copy",
            row.capacity,
            row.delivered,
            row.bytes,
        );
    }

    assert!(
        leaked <= 8 * 1024 * 1024,
        "the sweep leaked {leaked} bytes past every row's listener being dropped: a channel \
         that does not release its pooled buffers on close is a real defect, not a fixture"
    );
}

/// The channels swept with a live consumer: the product's own 1024, and two
/// smaller ones so the reading has a direction. Capacity 1 must overflow — its
/// one slot cannot absorb a whole offer even with a draining consumer — which is
/// the arm's reachability control.
const LIVE_CAPACITIES: [usize; 3] = [1, 64, 1024];
/// Datagrams offered per live row. Far above every capacity, so each row's
/// channel is loaded and the drops are the channel's.
const LIVE_OFFER: usize = 50_000;

/// One live-consumer row: the channel's counters while a consumer drains.
#[derive(Debug, Clone, Copy)]
struct LiveRow {
    capacity: usize,
    offered: u64,
    received: u64,
    accepted: u64,
    dropped: u64,
    delivered: u64,
    offered_elapsed: Duration,
    total_elapsed: Duration,
}

impl LiveRow {
    fn arrive_datagrams_per_s(&self) -> f64 {
        self.received as f64 / self.offered_elapsed.as_secs_f64()
    }
    fn consumer_datagrams_per_s(&self) -> f64 {
        self.delivered as f64 / self.total_elapsed.as_secs_f64()
    }
}

/// Offer `LIVE_OFFER` datagrams at one capacity while a consumer drains the
/// flow as fast as it can.
async fn live_row(capacity: usize) -> LiveRow {
    let socket = UdpSocket::bind(loopback()).await.unwrap();
    socket.set_recv_buffer_size(RECV_BUF).unwrap();
    let addr = socket.local_addr().unwrap();
    let listener = Arc::new(UtpListener::new_identity_dispatch(
        socket,
        NonZeroUsize::new(capacity).unwrap(),
    ));
    let client = UdpSocket::bind(loopback()).await.unwrap();
    client.connect(addr).await.unwrap();
    let payload = [0x5Au8; DATAGRAM_SIZE];

    let mut tasks: JoinSet<()> = JoinSet::new();
    tasks.spawn({
        let listener = Arc::clone(&listener);
        async move {
            loop {
                if listener.dispatch_next().await.is_err() {
                    break;
                }
            }
        }
    });

    client.send(&payload).await.unwrap();
    let conn = listener.accept_next().await.expect("never None");

    // The live consumer owns the flow's read half: a tight drain, the fastest a
    // consumer can take datagrams out of this channel. `rtp`'s own consumer does
    // strictly more per datagram, so this is an upper bound on its rate.
    let stop = Arc::new(AtomicBool::new(false));
    let drained = Arc::new(AtomicUsize::new(0));
    let (mut read, _write) = conn.split();
    tasks.spawn({
        let stop = Arc::clone(&stop);
        let drained = Arc::clone(&drained);
        async move {
            while !stop.load(Relaxed) {
                let mut n = 0;
                while read.read_half().try_recv().is_ok() {
                    n += 1;
                }
                drained.fetch_add(n, Relaxed);
                if n == 0 {
                    tokio::task::yield_now().await;
                }
            }
        }
    });

    let started = Instant::now();
    for seq in 0..LIVE_OFFER {
        client
            .send(&payload)
            .await
            .unwrap_or_else(|e| panic!("send {seq} of {LIVE_OFFER} at capacity {capacity}: {e}"));
    }
    let offered_elapsed = started.elapsed();
    let _ = quiesce(&listener, QUIESCE_BOUND).await;

    // Let the live consumer finish what is queued, bounded: the drain claim is
    // only meaningful if the consumer actually emptied the channel. `accepted`
    // is re-read each turn because a datagram still in the kernel can arrive
    // after quiesce's window closes.
    let drain_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let accepted_now = listener.stats().packets_dispatched.load(Relaxed);
        if drained.load(Relaxed) as u64 >= accepted_now {
            break;
        }
        assert!(
            Instant::now() < drain_deadline,
            "capacity {capacity}: the live consumer took {} of {accepted_now} accepted \
             datagrams and then stalled",
            drained.load(Relaxed),
        );
        tokio::task::yield_now().await;
    }
    stop.store(true, Relaxed);
    tasks.shutdown().await;

    let received = listener.stats().packets_received.load(Relaxed);
    let accepted = listener.stats().packets_dispatched.load(Relaxed);
    let dropped = listener
        .stats()
        .packets_dropped_dispatcher_full
        .load(Relaxed);
    let delivered = drained.load(Relaxed) as u64;

    LiveRow {
        capacity,
        offered: (LIVE_OFFER + 1) as u64,
        received,
        accepted,
        dropped,
        delivered,
        offered_elapsed,
        total_elapsed: started.elapsed(),
    }
}

/// With a consumer draining, how much capacity does the channel actually need?
/// The product's own value is the largest swept; the reading that decides the
/// recommendation is the smallest capacity at which the drop reaches zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "sweeps three channel capacities under a live drain; the `standard` tier, declared in GATE.md"]
async fn a_live_consumer_capacity_sweep_shows_the_stall_coverage() {
    let _serial = SERIAL.lock().await;
    let mut rows = Vec::new();
    for capacity in LIVE_CAPACITIES {
        let row = live_row(capacity).await;
        println!(
            "CHANNEL_CAPACITY_LIVE capacity={:<6} offered={:<6} received={:<6} accepted={:<6} \
             dispatcher_dropped={:<6} delivered={:<6} arrive_datagrams_per_s={:.0} \
             consumer_datagrams_per_s={:.0}",
            row.capacity,
            row.offered,
            row.received,
            row.accepted,
            row.dropped,
            row.delivered,
            row.arrive_datagrams_per_s(),
            row.consumer_datagrams_per_s(),
        );
        rows.push(row);
    }

    // Each row: the channel's own identity while a consumer takes datagrams out,
    // and the consumer having taken everything that was accepted. The drain
    // claim is only meaningful if the drain happened.
    for row in &rows {
        assert_eq!(
            row.accepted + row.dropped,
            row.received,
            "capacity {}: received {} != accepted {} + dispatcher dropped {}",
            row.capacity,
            row.received,
            row.accepted,
            row.dropped,
        );
        assert_eq!(
            row.delivered, row.accepted,
            "capacity {}: the live consumer took {} of {} accepted datagrams after a bounded \
             drain, so its 'live' name is a fixture",
            row.capacity, row.delivered, row.accepted,
        );
        assert!(
            row.received > (row.capacity as u64) * 4,
            "capacity {}: the row read only {} datagrams, under 4x its channel: it did not \
             load the channel and its drop reading would be vacuous",
            row.capacity,
            row.received,
        );
    }

    // Reachability: the one-slot row must overflow even with a live consumer, so
    // the sweep has a direction; and the product's own capacity must not, which
    // is the reading this arm exists for.
    let smallest = rows.first().expect("sweep produced no rows");
    assert!(
        smallest.dropped > 0,
        "the {}-slot row dropped nothing while {} datagrams were offered: the live consumer \
         kept up with a one-slot channel, so the sweep has no overloaded row to read against \
         and its zero-drop rows prove nothing",
        smallest.capacity,
        smallest.offered,
    );
    let largest = rows.last().expect("sweep produced no rows");
    assert_eq!(
        largest.dropped,
        0,
        "the {}-slot channel dropped {} of {} datagrams with a live consumer draining: if this \
         is a rate mismatch (arrive {:.0}/s, consume {:.0}/s) then no capacity removes it, and \
         the numbers above are the evidence",
        largest.capacity,
        largest.dropped,
        largest.received,
        largest.arrive_datagrams_per_s(),
        largest.consumer_datagrams_per_s(),
    );
    // Monotone: a deeper channel cannot overflow more, so the reading attributes
    // the drop to depth and not to noise.
    for pair in rows.windows(2) {
        assert!(
            pair[1].dropped <= pair[0].dropped,
            "a deeper channel ({}) dropped more ({}) than a shallower one ({}): {pair:?}",
            pair[1].capacity,
            pair[1].dropped,
            pair[0].capacity,
        );
    }
    println!(
        "CHANNEL_CAPACITY_LIVE_SWEEP smallest_overloading_capacity={} drops_at_smallest={} \
         product_capacity={} product_dropped={} product_consumer_datagrams_per_s={:.0} \
         product_arrive_datagrams_per_s={:.0}",
        smallest.capacity,
        smallest.dropped,
        largest.capacity,
        largest.dropped,
        largest.consumer_datagrams_per_s(),
        largest.arrive_datagrams_per_s(),
    );
}
