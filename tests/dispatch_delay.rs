//! Loopback measurement of the per-datagram delay `udp_listener`'s dispatch
//! path adds on top of a plain socket.
//!
//! `rtp` reads every datagram through this crate: a socket task calls
//! `UtpListener::dispatch_next`, which takes a pooled buffer, runs the dispatch
//! closure, and `try_send`s the packet into a per-flow `mpsc` channel that the
//! session's read half drains. That is a measurable amount of machinery per
//! datagram — a pooled-buffer take/put, a `HashMap` lookup under a mutex, a
//! channel hop and a task wakeup — and a *floor* on a round trip would have to
//! be one of these paying a fixed toll on essentially every datagram.
//!
//! The measurement is a differential one, so no absolute expectation is baked
//! in: the same echo is served twice, once by a raw `tokio_udp::UdpSocket` and
//! once by a `UtpListener` with a dispatcher loop and a per-flow echoer. The
//! client side is byte-identical between the arms, so the difference is this
//! crate's dispatch path and nothing else.
//!
//! The window sweep is what separates a constant cost from a load-dependent one:
//! the same measurement at 1, 4, 16 and 64 datagrams in flight reports the
//! *achieved* datagrams per second beside the round trip. A fixed per-datagram
//! toll holds its median across all four; a queue is visible as a median that
//! grows with the depth. Every arm prints its own distribution and byte counts,
//! and the dispatch drop counters are read from the listener itself, so a loss
//! this path causes is reported rather than absorbed by a timeout.

use std::io::IoSlice;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tokio_udp::UdpSocket;
use udp_listener::{Packet, UtpListener};

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    assert!(
        !sorted.is_empty(),
        "no samples: the percentile is undefined"
    );
    assert!(
        (0.0..=1.0).contains(&p),
        "a percentile outside 0..=1 is a caller error"
    );
    let idx = (((sorted.len() - 1) as f64) * p).round() as usize;
    sorted[idx]
}

/// A round-trip distribution that cannot be built from zero samples, so an arm
/// that measured nothing cannot be read as a fast arm.
#[derive(Debug)]
struct Dist {
    samples: usize,
    p50: Duration,
    p99: Duration,
    max: Duration,
}

impl Dist {
    fn from(mut values: Vec<Duration>) -> Self {
        assert!(
            !values.is_empty(),
            "an arm that collected no samples has measured nothing"
        );
        values.sort();
        Self {
            samples: values.len(),
            p50: percentile(&values, 0.50),
            p99: percentile(&values, 0.99),
            max: *values.last().expect("non-empty"),
        }
    }
}

/// One window's measurement: its round trip, how many datagrams were never
/// echoed, and the throughput the window achieved.
struct WindowRun {
    dist: Dist,
    lost: usize,
    achieved_per_sec: f64,
}

fn window_line(label: &str, window: usize, run: &WindowRun) -> String {
    format!(
        "DISPATCH_ARM {label:<22} window={window:<3} n={:<5} p50={:>8.3}ms p99={:>8.3}ms \
         max={:>8.3}ms lost={:<4} achieved={:>10.0}/s",
        run.dist.samples,
        ms(run.dist.p50),
        ms(run.dist.p99),
        ms(run.dist.max),
        run.lost,
        run.achieved_per_sec,
    )
}

/// Send `total` datagrams to `server`, keeping at most `window` unacknowledged,
/// and time each echo. The client is already connected and warmed up.
///
/// A datagram that is never echoed is counted as lost rather than waited on
/// forever: the dispatch path's own drop counter is read by the caller, so a
/// stall is attributed rather than hidden.
async fn measure_window(
    client: &UdpSocket,
    window: usize,
    total: usize,
    stall_bound: Duration,
) -> WindowRun {
    assert!(window >= 1, "a zero window cannot send anything");
    let mut sent_at: Vec<Option<Instant>> = vec![None; total];
    let mut acked: Vec<bool> = vec![false; total];
    let mut rtts = Vec::with_capacity(total);
    let mut outstanding = 0usize;
    let mut next = 0usize;
    let mut buf = [0u8; 64];
    let started = Instant::now();

    while next < total || outstanding > 0 {
        while next < total && outstanding < window {
            let payload = (next as u64).to_be_bytes();
            client.send(&payload).await.expect("client send failed");
            sent_at[next] = Some(Instant::now());
            next += 1;
            outstanding += 1;
        }
        match tokio::time::timeout(stall_bound, client.recv(&mut buf)).await {
            Ok(Ok(n)) => {
                assert_eq!(n, 8, "a short echo is not this arm's datagram");
                let seq = u64::from_be_bytes(buf[..n].try_into().expect("8 bytes")) as usize;
                assert!(seq < total, "an echo for a datagram never sent");
                assert!(
                    !acked[seq],
                    "datagram {seq} was echoed twice: the accounting would double-count"
                );
                acked[seq] = true;
                outstanding -= 1;
                let sent = sent_at[seq].expect("a received datagram was sent");
                rtts.push(sent.elapsed());
            }
            Ok(Err(e)) => panic!("client recv failed: {e}"),
            Err(_) => break,
        }
    }
    let elapsed = started.elapsed();
    let delivered = rtts.len();
    WindowRun {
        dist: Dist::from(rtts),
        lost: total - delivered,
        achieved_per_sec: delivered as f64 / elapsed.as_secs_f64(),
    }
}

/// Warm a flow up and return the connected client, so no measured datagram pays
/// for flow creation or for the first readiness registration.
async fn warmed_client(server_addr: SocketAddr) -> UdpSocket {
    let client = UdpSocket::bind(loopback()).await.unwrap();
    client.connect(server_addr).await.unwrap();
    let mut buf = [0u8; 64];
    for i in 0..5u64 {
        let payload = i.to_be_bytes();
        client.send(&payload).await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(5), client.recv(&mut buf))
            .await
            .expect("warm-up echo never came back")
            .unwrap();
        assert_eq!(&buf[..n], &payload);
    }
    client
}

/// The baseline: an echo served by a bare socket, with no dispatch layer.
async fn raw_echo_server(tasks: &mut JoinSet<()>) -> SocketAddr {
    let server = Arc::new(UdpSocket::bind(loopback()).await.unwrap());
    let addr = server.local_addr().unwrap();
    tasks.spawn(async move {
        let mut buf = [0u8; 64];
        loop {
            let (n, src) = server.recv_from(&mut buf).await.unwrap();
            server
                .send_to_vectored(&[IoSlice::new(&buf[..n])], &src)
                .await
                .unwrap();
        }
    });
    addr
}

type EchoListener = UtpListener<UdpSocket, SocketAddr, Packet>;

/// The measured path: a listener whose dispatcher reads every datagram and
/// whose accepted flows echo them back. Every task is owned by `tasks`, so
/// shutting the arm down aborts and awaits the whole set.
async fn listener_echo_server(tasks: &mut JoinSet<()>) -> (Arc<EchoListener>, SocketAddr) {
    let socket = UdpSocket::bind(loopback()).await.unwrap();
    let addr = socket.local_addr().unwrap();
    let listener = Arc::new(UtpListener::new_identity_dispatch(
        socket,
        NonZeroUsize::new(256).unwrap(),
    ));
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
    tasks.spawn({
        let listener = Arc::clone(&listener);
        async move {
            // The per-flow tasks are owned by this acceptor, so aborting the
            // acceptor aborts them with it.
            let mut flows = JoinSet::new();
            loop {
                let conn = listener.accept_next().await.expect("never None");
                flows.spawn(async move {
                    let (mut read, write) = conn.split();
                    while let Some(pkt) = read.read_half().recv().await {
                        if write.send(pkt.as_ref()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        }
    });
    (listener, addr)
}

/// The listener's own counters, read as one snapshot so a difference between two
/// reads cannot be a leftover from another arm.
fn snapshot(listener: &EchoListener) -> (u64, u64, u64, u64, u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    let stats = listener.stats();
    (
        stats.packets_received.load(Relaxed),
        stats.packets_dispatched.load(Relaxed),
        stats.packets_dropped_dispatcher_full.load(Relaxed),
        stats.packets_dropped_existing_only.load(Relaxed),
        stats.packets_dropped_pkt_buf_overflow.load(Relaxed),
        stats.connections_opened.load(Relaxed),
    )
}

/// The floor: the delay this dispatch path adds to a datagram that never queues
/// behind another one, measured against a bare socket serving the same echo.
///
/// `rtp` reaches the network only through this crate, so a 190 ms floor under it
/// would have to be visible here. The assertion is a tripwire at a few
/// milliseconds — three orders of magnitude below that — and the printed
/// distributions are the evidence it rests on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_dispatch_path_adds_no_floor_to_a_lone_datagram() {
    const SAMPLES: usize = 400;
    const STALL: Duration = Duration::from_secs(2);

    let mut raw_tasks = JoinSet::new();
    let raw_addr = raw_echo_server(&mut raw_tasks).await;
    let raw_client = warmed_client(raw_addr).await;
    let raw = measure_window(&raw_client, 1, SAMPLES, STALL).await;
    drop(raw_client);
    raw_tasks.shutdown().await;

    let mut listener_tasks = JoinSet::new();
    let (listener, listener_addr) = listener_echo_server(&mut listener_tasks).await;
    let listener_client = warmed_client(listener_addr).await;
    let dispatched = measure_window(&listener_client, 1, SAMPLES, STALL).await;
    let stats = snapshot(&listener);
    drop(listener_client);
    listener_tasks.shutdown().await;

    println!("{}", window_line("bare-socket", 1, &raw));
    println!("{}", window_line("udp_listener", 1, &dispatched));
    println!(
        "DISPATCH_STATS received={} dispatched={} dropped_dispatcher_full={} \
         dropped_existing_only={} dropped_pkt_buf_overflow={} flows_opened={}",
        stats.0, stats.1, stats.2, stats.3, stats.4, stats.5,
    );

    // Sanity: both arms measured the full sample set and lost nothing. A short
    // sample set would make the percentiles below meaningless, and a loss would
    // mean the difference being read is a retransmission, not a dispatch.
    assert_eq!(
        raw.dist.samples, SAMPLES,
        "the bare-socket arm lost samples"
    );
    assert_eq!(
        dispatched.dist.samples, SAMPLES,
        "the dispatch arm lost samples: {} datagram(s) were never echoed",
        dispatched.lost
    );
    assert_eq!(raw.lost, 0, "the bare-socket echo lost a datagram");
    assert_eq!(
        stats.0 as usize,
        SAMPLES + 5,
        "the listener received a different number of datagrams than were sent"
    );
    assert_eq!(
        stats.2, 0,
        "a one-deep window overflowed a dispatcher channel on loopback"
    );

    let added = dispatched
        .dist
        .p50
        .checked_sub(raw.dist.p50)
        .unwrap_or(Duration::ZERO);
    println!(
        "DISPATCH_FLOOR dispatch_p50_minus_bare_p50={:.3}ms (dispatch p50 {:.3}ms, bare p50 \
         {:.3}ms); the field floor this is read against is 190ms",
        ms(added),
        ms(dispatched.dist.p50),
        ms(raw.dist.p50),
    );
    assert!(
        added < Duration::from_millis(2),
        "the dispatch path adds {:.3}ms to a lone datagram's round trip: a fixed per-datagram \
         cost has appeared (dispatch {:.3}ms, bare {:.3}ms)",
        ms(added),
        ms(dispatched.dist.p50),
        ms(raw.dist.p50),
    );
    assert!(
        dispatched.dist.p99 < Duration::from_millis(20),
        "the dispatch path's p99 round trip is {:.3}ms on loopback",
        ms(dispatched.dist.p99)
    );
}

/// The same measurement across pipelining depths, with the achieved rate beside
/// it: the separation of a constant toll from a queue.
///
/// A fixed per-datagram cost holds its median from one datagram in flight to
/// sixty-four; a queue grows it with the depth. This arm prints both and asserts
/// the properties that would indict the path: every depth delivers everything it
/// sent, and no depth produces a round trip in the hundreds of milliseconds,
/// which on loopback can only be a stall rather than propagation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_dispatch_rate_sweep_separates_a_toll_from_a_queue() {
    const WINDOWS: [usize; 4] = [1, 4, 16, 64];
    const PER_WINDOW: usize = 400;
    const STALL: Duration = Duration::from_secs(2);

    let mut listener_tasks = JoinSet::new();
    let (listener, addr) = listener_echo_server(&mut listener_tasks).await;
    let client = warmed_client(addr).await;

    let mut runs = Vec::new();
    for window in WINDOWS {
        let stats_before = snapshot(&listener);
        let run = measure_window(&client, window, PER_WINDOW, STALL).await;
        let stats_after = snapshot(&listener);
        println!("{}", window_line("udp_listener", window, &run));
        assert_eq!(
            run.dist.samples,
            PER_WINDOW,
            "window {window} lost {} datagram(s) (dispatched {}, full-drop {})",
            run.lost,
            stats_after.1 - stats_before.1,
            stats_after.2 - stats_before.2,
        );
        runs.push((window, run));
    }
    let stats = snapshot(&listener);
    println!(
        "DISPATCH_SWEEP_STATS received={} dispatched={} dropped_dispatcher_full={} flows_opened={}",
        stats.0, stats.1, stats.2, stats.5,
    );
    drop(client);
    listener_tasks.shutdown().await;

    // Sanity: the sweep actually covered the declared depths, each achieved a
    // throughput, and the deepest window was faster than the shallowest —
    // otherwise there is no depth axis for the comparison to be read on.
    assert_eq!(runs.len(), WINDOWS.len());
    assert!(
        runs.iter().all(|(_, run)| run.achieved_per_sec > 0.0),
        "an arm achieved no throughput, so it measured nothing"
    );
    for (window, run) in &runs {
        assert!(
            run.dist.p99 < Duration::from_millis(100),
            "window {window} produced a {:.3}ms p99 round trip on loopback: that is a stall, not \
             propagation",
            ms(run.dist.p99)
        );
    }
    let (_, deep) = runs.last().expect("the sweep is non-empty");
    assert!(
        deep.achieved_per_sec > runs[0].1.achieved_per_sec,
        "a 64-deep window was no faster than a 1-deep one ({:.0}/s vs {:.0}/s): the depth axis is \
         not an offered-rate axis",
        deep.achieved_per_sec,
        runs[0].1.achieved_per_sec,
    );
}
