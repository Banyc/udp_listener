//! Whether a datagram the kernel refused is separable from one the path lost.
//!
//! `udp_listener` accounts for the datagrams it read and for the ones its own
//! per-flow channels refused. A datagram the *kernel* refused, because the
//! socket's receive queue was full when it arrived, appears in neither: it never
//! reached `recv`, so from inside the crate it is the same as a datagram the
//! path never delivered. [`UtpListener::kernel_refused`] is the reading that
//! separates them, and this arm is its evidence.
//!
//! ```text
//! peer_offered     = packets_received + kernel_refused
//! packets_received = packets_dispatched + the drop counters in ListenerStats
//! ```
//!
//! Both identities are exercised on a small and a host-default receive buffer:
//! the first row refuses almost everything below the layer and drops nothing in
//! it, the second fills the dispatcher channel too, so both self-inflicted sites
//! carry a non-zero count at once and the two counters still sum to what was
//! offered.
//!
//! The load shape is the only one in which the kernel queue can fill: a burst
//! offered while nothing reads the socket (a scheduling stall). A live dispatch
//! loop drains the queue as fast as it fills, so nothing would be refused.
//!
//! # What this demonstrates, and where
//!
//! The per-socket count is Linux's — the `drops` column of `/proc/net/udp{,6}`,
//! which is `sk_drops`. macOS keeps no per-socket counter, so its reading is
//! [`KernelRefused::NotPerSocket`] and the identity above is not computable
//! there. What macOS does keep is one host-wide `dropped due to full socket
//! buffers` total, and this arm measures it move by at least the refusals it
//! caused, against a quiet baseline. `GATE.md` records which half ran on which
//! host.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tokio_udp::UdpSocket;
use udp_listener::{KernelRefused, Packet, UtpListener};

/// The deployed transport's per-flow channel bound (`rtp/src/udp.rs:71`
/// `DISPATCHER_BUF_SIZE`), so the dispatcher refusal this arm reads is the one
/// the product's own channel produces.
const CHANNEL_CAPACITY: usize = 1024;

/// Datagrams offered after the opener, above every receive buffer on this host,
/// so `received` is the kernel's own capacity and not the burst's length.
const BURST: usize = 32_768;

/// The interactive lane's small datagram.
const DATAGRAM: usize = 256;

/// The two receive-buffer budgets: below the channel, so the loss is entirely
/// the kernel's, and the host default, which also fills the channel.
const BUDGETS: [(Option<usize>, &str); 2] = [(Some(4 << 10), "floor-4KiB"), (None, "host-default")];

type ArmListener = UtpListener<UdpSocket, SocketAddr, Packet>;

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// The host-wide count `netstat -s -p udp` reports as `dropped due to full
/// socket buffers` — every UDP socket on the machine, not this arm's.
///
/// macOS keeps no per-socket equivalent, which is exactly why the product
/// reports `NotPerSocket` here; this arm reads the host-wide total only to show
/// it moves with the refusals the arm caused.
#[cfg(target_os = "macos")]
fn host_udp_refusals() -> u64 {
    let output = std::process::Command::new("/usr/sbin/netstat")
        .args(["-s", "-p", "udp"])
        .output()
        .expect("netstat is needed to read the host-wide UDP refusal counter");
    assert!(output.status.success(), "netstat exited {}", output.status);
    let text = String::from_utf8_lossy(&output.stdout);
    parse_full_socket_buffers(&text).unwrap_or_else(|| {
        panic!("no `dropped due to full socket buffers` line in netstat output:\n{text}")
    })
}

/// The leading count of the `dropped due to full socket buffers` line.
#[cfg(target_os = "macos")]
fn parse_full_socket_buffers(netstat: &str) -> Option<u64> {
    netstat.lines().find_map(|line| {
        let (count, label) = line.trim_start().split_once(char::is_whitespace)?;
        label
            .trim_start()
            .starts_with("dropped due to full socket buffers")
            .then(|| count.parse().ok())
            .flatten()
    })
}

/// The counters of one stalled-burst row.
struct Row {
    label: &'static str,
    effective: usize,
    before: KernelRefused,
    after: KernelRefused,
    offered: u64,
    received: u64,
    dispatched: u64,
    self_refused: u64,
    delivered: u64,
}

/// Drain the socket into the dispatcher until the counters stop moving, and
/// return how many datagrams the layer read.
async fn quiesce(listener: &ArmListener, bound: Duration) -> u64 {
    let deadline = Instant::now() + bound;
    let mut last = (u64::MAX, u64::MAX);
    let mut stable = 0u32;
    loop {
        let now = (
            listener.stats().packets_received.load(Relaxed),
            listener.stats().packets_dispatched.load(Relaxed),
        );
        if now == last {
            stable += 1;
            if stable >= 25 {
                return now.0;
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

/// Offer `BURST` datagrams while nothing reads the socket, then read both drop
/// sites plus the kernel's own count.
async fn one_row(request: Option<usize>, label: &'static str) -> Row {
    let socket = UdpSocket::bind(loopback()).await.unwrap();
    if let Some(bytes) = request {
        socket.set_recv_buffer_size(bytes).unwrap();
    }
    let effective = socket.recv_buffer_size().unwrap();
    let addr = socket.local_addr().unwrap();
    let listener = Arc::new(ArmListener::new_identity_dispatch(
        socket,
        NonZeroUsize::new(CHANNEL_CAPACITY).unwrap(),
    ));

    // Before any datagram: whatever the kernel counts, a socket that has
    // received nothing has refused nothing.
    let before = listener.kernel_refused();

    let client = UdpSocket::bind(loopback()).await.unwrap();
    client.connect(addr).await.unwrap();
    let payload = vec![0x5Au8; DATAGRAM];
    client.send(&payload).await.unwrap();
    let mut conn = listener.poll_next_conn().await.expect("never None");

    for seq in 0..BURST {
        client
            .send(&payload)
            .await
            .unwrap_or_else(|e| panic!("{label}: send {seq} of {BURST} failed: {e}"));
    }
    tokio::time::sleep(Duration::from_millis(2)).await;

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
    let received = quiesce(&listener, Duration::from_secs(15)).await;
    drainer.abort_all();

    let after = listener.kernel_refused();
    let offered = (BURST + 1) as u64;
    let dispatched = listener.stats().packets_dispatched.load(Relaxed);
    let self_refused = received.checked_sub(dispatched).expect(
        "the layer dispatched more datagrams than it read, so one entered no accounting path",
    );
    let mut delivered = 0u64;
    while conn.read_half().read_half().try_recv().is_ok() {
        delivered += 1;
    }

    // The second identity, per row: everything this layer read was either handed
    // to a flow or refused by one of its own counters.
    assert_eq!(
        self_refused + dispatched,
        received,
        "{label}: received {received} != dispatched {dispatched} + self-refused {self_refused}"
    );
    assert_eq!(
        delivered, dispatched,
        "{label}: {dispatched} datagrams were dispatched but {delivered} came back out of the flow"
    );

    Row {
        label,
        effective,
        before,
        after,
        offered,
        received,
        dispatched,
        self_refused,
        delivered,
    }
}

/// The platform decision, pinned cheaply on every run: the reading is either a
/// real per-socket count or a named absence. A platform that has no per-socket
/// count must say so — never a host-wide total, and never a zero that reads as
/// "nothing was refused".
#[tokio::test]
async fn the_per_socket_answer_is_the_platforms_and_is_never_a_substitute() {
    let socket = UdpSocket::bind(loopback()).await.unwrap();
    let listener = ArmListener::new_identity_dispatch(socket, NonZeroUsize::new(64).unwrap());
    match listener.kernel_refused() {
        KernelRefused::PerSocket { refused, source } => {
            #[cfg(not(target_os = "linux"))]
            panic!(
                "no per-socket refusal count exists on {}, yet one was reported: {refused} from \
                 {source:?}",
                std::env::consts::OS,
            );
            #[cfg(target_os = "linux")]
            {
                println!(
                    "KERNEL_REFUSAL platform={} fresh_socket per_socket refused={refused} source={source:?}",
                    std::env::consts::OS
                );
                assert_eq!(
                    refused, 0,
                    "a socket that has received nothing has refused nothing"
                );
            }
        }
        KernelRefused::NotPerSocket { reason } => {
            #[cfg(target_os = "linux")]
            panic!(
                "Linux keeps a per-socket refusal count (the `drops` column of `/proc/net/udp` and \
                 `/proc/net/udp6`), yet none was read: {reason}"
            );
            #[cfg(not(target_os = "linux"))]
            println!(
                "KERNEL_REFUSAL platform={} fresh_socket not_per_socket reason={reason}",
                std::env::consts::OS
            );
        }
        KernelRefused::Unidentified { reason } => panic!(
            "a fresh socket over a real UdpSocket must be identifiable in the kernel's table: \
             {reason}"
        ),
    }
}

/// The receive side reconciles three ways when the kernel's own count is
/// readable, and the host-wide counter macOS does keep moves with the refusals
/// when it is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "stages two 32768-datagram bursts; the `standard` tier, declared in GATE.md"]
async fn a_kernel_refusal_is_separable_and_the_receive_side_reconciles() {
    // A baseline window the arm's own traffic cannot reach: the host-wide
    // counter must not have been moved by anything this arm did, so that what it
    // moves during a burst is attributable to the burst.
    #[cfg(target_os = "macos")]
    let host_quiet = {
        let first = host_udp_refusals();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let quiet = host_udp_refusals();
        println!(
            "KERNEL_REFUSAL platform=macos quiet_window_ms=200 host_wide_drift={}",
            quiet as i64 - first as i64
        );
        quiet
    };

    for (request, label) in BUDGETS {
        #[cfg(target_os = "macos")]
        let host_before_row = host_udp_refusals();
        let row = one_row(request, label).await;
        let sender_derived = row.offered - row.received;
        assert!(
            sender_derived > 0,
            "{label}: the burst was not refused below the layer at all (received {} of {}), so this \
             row cannot show a kernel refusal",
            row.received,
            row.offered
        );

        match (row.before, row.after) {
            (
                KernelRefused::PerSocket {
                    refused: start,
                    source,
                },
                KernelRefused::PerSocket { refused: end, .. },
            ) => {
                let measured = end - start;
                println!(
                    "KERNEL_REFUSAL platform=linux label={} effective={} offered={} received={} \
                     dispatched={} self_refused={} delivered={} kernel_refused={measured} \
                     sender_derived={sender_derived} source={source:?}",
                    row.label,
                    row.effective,
                    row.offered,
                    row.received,
                    row.dispatched,
                    row.self_refused,
                    row.delivered,
                );
                // The identity the operator cannot compute today: what the peer
                // offered is what this layer read plus what the kernel refused.
                assert_eq!(
                    measured, sender_derived,
                    "{label}: the kernel counted {measured} refusals but the sender's own \
                     arithmetic gives {sender_derived} of {} offered and {} read",
                    row.offered, row.received
                );
                assert_eq!(
                    row.received + measured,
                    row.offered,
                    "{label}: received {} + kernel-refused {measured} != offered {}",
                    row.received,
                    row.offered
                );
            }
            (KernelRefused::NotPerSocket { reason }, KernelRefused::NotPerSocket { .. }) => {
                #[cfg(target_os = "linux")]
                panic!(
                    "Linux reads these counts from the `drops` column of `/proc/net/udp` and \
                     `/proc/net/udp6`: {reason}"
                );
                #[cfg(target_os = "macos")]
                {
                    let host_delta = host_udp_refusals() - host_before_row;
                    println!(
                        "KERNEL_REFUSAL platform=macos label={} effective={} offered={} received={} \
                         dispatched={} self_refused={} delivered={} sender_derived_kernel_dropped={sender_derived} \
                         host_wide_delta={host_delta} host_wide_quiet_baseline={} reason={reason}",
                        row.label,
                        row.effective,
                        row.offered,
                        row.received,
                        row.dispatched,
                        row.self_refused,
                        row.delivered,
                        host_quiet,
                    );
                    // The host-wide total counts every socket, so it is an upper
                    // bound on this socket's refusals, never an exact reading;
                    // what makes it evidence here is that a quiet window moved it
                    // by nothing while the burst moved it by at least the
                    // refusals the burst caused.
                    assert!(
                        host_delta >= sender_derived,
                        "{label}: the host-wide counter moved by {host_delta} over a burst the sender \
                         says was refused {sender_derived} times, so it is not counting this socket's \
                         refusals"
                    );
                }
                #[cfg(all(not(target_os = "linux"), not(target_os = "macos")))]
                panic!(
                    "this platform reports no per-socket count ({reason}) and has no host-wide \
                     counter this arm knows how to read, so it demonstrates nothing"
                );
            }
            (before, after) => panic!(
                "{label}: the platform's answer changed between two readings of the same socket: \
                 {before:?} then {after:?}"
            ),
        }
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::parse_full_socket_buffers;

    /// The parser must read the count of the one line it is for, and refuse
    /// every other line of the same report rather than picking the first
    /// integer it sees.
    #[test]
    fn only_the_full_socket_buffer_line_is_read() {
        let report = "\t1472953904 datagrams received\n\
                      \t\t29585530 dropped due to no socket\n\
                      \t\t894526590 dropped due to full socket buffers\n\
                      \t\t548707369 delivered\n";
        assert_eq!(parse_full_socket_buffers(report), Some(894_526_590));
        assert_eq!(
            parse_full_socket_buffers("\t1472953904 datagrams received\n"),
            None
        );
        assert_eq!(parse_full_socket_buffers(""), None);
    }
}
