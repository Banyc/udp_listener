//! The per-datagram receive buffer is a knob, and an oversized datagram is a
//! counted refusal rather than a silent truncation.
//!
//! A dispatcher-channel slot retains a pooled buffer of the configured capacity
//! whatever the datagram's size, so the default [`PACKET_BUFFER_LENGTH`]
//! (64 KiB, the UDP datagram ceiling) costs 64 KiB for a 256-byte datagram. A
//! caller whose peer is bounded well below that passes its own bound through
//! [`PacketBufferLength`]. This arm pins both sides: a datagram within the bound
//! is delivered intact in a buffer of exactly the configured capacity, and a
//! datagram over the bound is dropped and counted on
//! `packets_dropped_pkt_buf_overflow` — never delivered as a truncated
//! `bound`-byte packet. (`packet_buffer_length_footprint` measures the memory
//! the slot retains.)
//!
//! The oversize shape is what separates a refusal from a truncation. The kernel
//! copies at most the buffer's spare capacity into it and discards the rest, so
//! `recv` returns the buffer length for an oversized datagram; if the
//! `n == packet_buffer_length` guard in `dispatch_next` is made to compare
//! against the wrong constant (the default `PACKET_BUFFER_LENGTH` while the
//! buffer is the configured bound), that truncated read is dispatched. This arm
//! then reddens naming `packets_dropped_pkt_buf_overflow = 0` where one refusal
//! was owed, before the flow's next within-bound datagram arrives.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinSet;
use tokio_udp::UdpSocket;
use udp_listener::{PACKET_BUFFER_LENGTH, Packet, PacketBufferLength, UtpListener};

/// The configured bound: the slot is 2048 B, so the largest *delivered*
/// datagram is 2047 B. The oversize probe (4096) sits between the bound and the
/// 64 KiB default, so it is a refusal only because the slot was sized down.
const BOUND: usize = 2048;
const WITHIN: usize = 256;
const OVERSIZED: usize = 4096;

type BoundListener = UtpListener<UdpSocket, SocketAddr, Packet>;

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// Wait until `f` holds, bounded, yielding between reads. A predicate that never
/// holds fails with the counters, so a dispatch that never reached the awaited
/// state is a named failure rather than a hang.
async fn until(listener: &BoundListener, bound: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + bound;
    while !f() {
        assert!(
            Instant::now() < deadline,
            "the dispatch path never reached the awaited state within {bound:?}: \
             received={} dispatched={} pkt_buf_overflow={}",
            listener.stats().packets_received.load(Ordering::Relaxed),
            listener.stats().packets_dispatched.load(Ordering::Relaxed),
            listener
                .stats()
                .packets_dropped_pkt_buf_overflow
                .load(Ordering::Relaxed),
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slot_sized_to_the_bound_delivers_within_it_and_refuses_over_it() {
    let socket = UdpSocket::bind(loopback()).await.unwrap();
    let addr = socket.local_addr().unwrap();
    let listener = Arc::new(UtpListener::new_identity_dispatch_with_packet_buffer(
        socket,
        NonZeroUsize::new(4).unwrap(),
        PacketBufferLength::new(NonZeroUsize::new(BOUND).unwrap()),
    ));
    let client = UdpSocket::bind(loopback()).await.unwrap();
    client.connect(addr).await.unwrap();

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

    // A within-bound datagram opens the flow and is delivered whole, in a slot
    // whose buffer is the configured bound — the memory property, read off the
    // buffer the consumer actually holds.
    client.send(&[0x11u8; WITHIN]).await.unwrap();
    let mut conn = listener.accept_next().await.expect("never None");
    let opener = conn.read_half().read_half().recv().await.unwrap();
    assert_eq!(
        opener.len(),
        WITHIN,
        "the within-bound datagram must be delivered whole",
    );
    assert_eq!(
        opener.capacity(),
        BOUND,
        "the channel slot must hold a buffer of the configured bound, not the \
         {PACKET_BUFFER_LENGTH}-byte default",
    );

    // Over the bound: the kernel copies at most `BOUND` bytes and discards the
    // rest, so a delivered `BOUND`-byte packet would be a silent shortening.
    client.send(&[0x22u8; OVERSIZED]).await.unwrap();
    until(&listener, Duration::from_secs(5), || {
        listener.stats().packets_received.load(Ordering::Relaxed) >= 2
    })
    .await;
    assert_eq!(
        listener
            .stats()
            .packets_dropped_pkt_buf_overflow
            .load(Ordering::Relaxed),
        1,
        "an over-bound datagram must be counted as a buffer-overflow refusal",
    );
    assert!(
        matches!(
            conn.read_half().read_half().try_recv(),
            Err(TryRecvError::Empty)
        ),
        "the over-bound datagram must not be delivered — not even truncated to {BOUND} bytes",
    );

    // The refusal costs the datagram, not the flow: a within-bound datagram on
    // the same flow is delivered intact afterwards.
    client.send(&[0x33u8; WITHIN]).await.unwrap();
    let after = tokio::time::timeout(Duration::from_secs(5), conn.read_half().read_half().recv())
        .await
        .expect("a within-bound datagram after the refusal never arrived")
        .expect("the refusal closed the flow");
    assert_eq!(
        after.as_ref(),
        &[0x33u8; WITHIN],
        "the post-refusal datagram must be delivered intact",
    );

    tasks.shutdown().await;
}
