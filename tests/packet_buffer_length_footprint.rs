//! What one occupied dispatcher-channel slot retains, measured, for a slot
//! sized to a 2048-byte bound and for the 64 KiB default.
//!
//! The sibling arm (`packet_buffer_length`) pins that the slot's buffer *is* the
//! configured bound; this arm reads the memory that buffer costs in the
//! operator's terms (bytes per occupied slot), so the win is a measurement and
//! not a construction. The counting allocator is global to this test binary,
//! and this file holds ONE test, so nothing else allocates between the two
//! snapshots.

use std::alloc::{GlobalAlloc, Layout, System};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tokio_udp::UdpSocket;
use udp_listener::{PACKET_BUFFER_LENGTH, PacketBufferLength, UtpListener};

/// A counting global allocator, so the channel's retained memory is read rather
/// than assumed from the pool's construction.
struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, layout) };
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, layout, new_size) };
        if !q.is_null() {
            if new_size >= layout.size() {
                LIVE.fetch_add(new_size - layout.size(), Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        q
    }
}
#[global_allocator]
static ALLOC: Counting = Counting;

/// The configured bound under test. 2048 bytes carries the deployed rtp MSS
/// (1424) with room for the codec/FEC/nonce overheads, and sits 32x below the
/// default.
const BOUND: usize = 2048;
/// The interactive lane's payload size; the pooled buffer is the bound
/// regardless, so this is the datum the *wire* carries, not the memory it
/// commits.
const DATAGRAM_SIZE: usize = 256;
/// Datagrams offered after the opener: above the channel capacity, so the
/// channel is full and every occupied slot is retained.
const BURST: usize = 256;
/// The channel capacity for the footprint reading: small enough to fill
/// cheaply, large enough that per-slot bytes dominate the allocator's own
/// bookkeeping.
const SLOTS: usize = 64;
const QUIESCE_BOUND: Duration = Duration::from_secs(15);

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// Retained bytes per datagram the channel delivered, for a slot sized to
/// `bound`. The dispatch loop is live (so `received` is the wire's delivery and
/// the channel is the only buffer under test) and the consumer is stalled (so
/// every slot is occupied); the delta in the counting allocator's live bytes
/// over the burst is divided by the datagrams the channel accepted.
async fn bytes_per_slot(bound: usize) -> f64 {
    let socket = UdpSocket::bind(loopback()).await.unwrap();
    socket.set_recv_buffer_size(4 << 20).unwrap();
    let addr = socket.local_addr().unwrap();
    let listener = Arc::new(UtpListener::new_identity_dispatch_with_packet_buffer(
        socket,
        NonZeroUsize::new(SLOTS).unwrap(),
        PacketBufferLength::new(NonZeroUsize::new(bound).unwrap()),
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
    let mut conn = listener.accept_next().await.expect("never None");
    let before = LIVE.load(Ordering::Relaxed);
    for _ in 0..BURST {
        client.send(&payload).await.unwrap();
    }
    // Quiesce: received and dispatched stop moving. A run of stable reads is
    // the dispatch loop having caught up; the loop is stopped before the
    // counters are read, so no `try_send` should land between reading and drain.
    let deadline = Instant::now() + QUIESCE_BOUND;
    let mut last = (u64::MAX, u64::MAX);
    let mut stable = 0u32;
    loop {
        let now = (
            listener.stats().packets_received.load(Ordering::Relaxed),
            listener.stats().packets_dispatched.load(Ordering::Relaxed),
        );
        if now == last {
            stable += 1;
            if stable >= 10 {
                break;
            }
        } else {
            stable = 0;
            last = now;
        }
        assert!(
            Instant::now() < deadline,
            "the dispatch loop never fell quiet within {QUIESCE_BOUND:?}: {now:?}",
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let bytes = LIVE.load(Ordering::Relaxed).saturating_sub(before);
    tasks.shutdown().await;
    let delivered = listener.stats().packets_dispatched.load(Ordering::Relaxed);
    let opener_present = conn.read_half().read_half().try_recv().is_ok();
    drop(conn);
    drop(listener);
    drop(client);

    assert!(opener_present, "the opener must be readable");
    assert!(
        delivered >= SLOTS as u64,
        "the burst never filled the {SLOTS}-slot channel: only {delivered} datagrams were accepted",
    );
    assert_eq!(
        delivered, SLOTS as u64,
        "a {SLOTS}-slot channel must accept exactly {SLOTS} of a {BURST}-datagram burst",
    );
    bytes as f64 / delivered as f64
}

/// A slot sized to a 2048-byte bound retains roughly the bound, where the
/// 64 KiB default retains `PACKET_BUFFER_LENGTH`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slot_sized_to_the_bound_retains_the_bound_not_the_default() {
    let small = bytes_per_slot(BOUND).await;
    let default = bytes_per_slot(PACKET_BUFFER_LENGTH).await;
    println!(
        "PACKET_BUFFER_SLOT bound={BOUND} bytes_per_slot={small:.0} \
         per_flow_at_1024_slots={:.1} MiB; default_bytes_per_slot={default:.0} \
         default_per_flow_at_1024_slots={:.1} MiB",
        small * 1024.0 / (1024.0 * 1024.0),
        default * 1024.0 / (1024.0 * 1024.0),
    );
    assert!(
        small >= BOUND as f64,
        "the {BOUND}-byte slot retained only {small:.0} bytes per occupied slot, under the \
         configured bound — the reading is a fixture",
    );
    assert!(
        small < (BOUND * 2) as f64,
        "the {BOUND}-byte slot retained {small:.0} bytes per occupied slot, over twice the \
         bound — the pool is not sized to the configured bound",
    );
    assert!(
        default >= PACKET_BUFFER_LENGTH as f64,
        "the default slot retained only {default:.0} bytes per occupied slot, under \
         PACKET_BUFFER_LENGTH ({PACKET_BUFFER_LENGTH})",
    );
}
