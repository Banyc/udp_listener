//! Liveness soak for the listener accept handover.
//!
//! One *dial* is one datagram carrying an 8-byte token. The dispatch closure
//! turns the token into the flow key, so every dial opens exactly one flow,
//! and the accept side must hand that flow back exactly once, echo the token,
//! and close it. Tokens are unique per `(dialer, iteration)`, so the
//! assertion is a set comparison over identities rather than a count: a token
//! that never comes back, one that comes back twice, or one whose echo
//! belongs to a different dial is a failure. Nothing is retried, nothing is
//! consumed that was not sent, and a dial that is never accepted is reported
//! as lost rather than absorbed by a timeout.
//!
//! Sizes and the seed come from the environment (`SOAK_*`), so `cargo test`
//! runs a short version and `local/soak_accept_churn.py` scales a batch up
//! without rebuilding. Every run prints one `SOAK_RESULT {json}` line so the
//! driver can aggregate dials, failures and hangs into a detection limit
//! instead of a bare "it passed".
//!
//! The modes differ in which cross-task path carries the handover:
//!
//! - `churn`: N dialers, closed loop, K tasks on the combined
//!   `poll_next_conn` (each task both enqueues and dequeues).
//! - `multi_accept`: one dispatcher plus K `accept_next` tasks — the
//!   queue-length fast path read by tasks that never enqueue.
//! - `burst`: barrier-synchronized rounds of N simultaneous dials against a
//!   paced acceptor, so the queue holds many flows across an await.
//! - `cancel`: K acceptors cancelled mid-accept every few milliseconds,
//!   dropping the accept future while a handover may be in flight.
//! - `mixed`: one task owns dispatch and never accepts, racing acceptors that
//!   only drain at their loop head — so a flow enqueued by the dispatcher has
//!   to be noticed by a task already parked in a datagram read.
//! - `capacity`: a blast of more dials than the bounded accept queue holds,
//!   with the accept side not polling until every datagram has been
//!   dispatched. Every refusal must be counted, none may hide a queued flow,
//!   and the fast path must still work afterwards.

use core::net::SocketAddr;
use core::num::NonZeroUsize;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::task::JoinSet;
use udp_listener::{Classified, Classify, DispatchPolicy, Packet, UtpListener};

/// Bytes in one token datagram; the dispatch key is the token itself.
const TOKEN_LEN: usize = 8;
const DISPATCH_BUFFER: usize = 8;

fn token(dialer: u32, iteration: u32) -> u64 {
    ((dialer as u64) << 32) | iteration as u64
}

fn splitmix(seed: u64) -> u64 {
    let x = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let z = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A cancel span drawn around `base`, so repeated cancellations do not all
/// land at the same offset of the accept future.
fn jittered_span(base: Duration, seed: u64, attempt: u64) -> Duration {
    let micros = base.as_micros().max(2) as u64;
    let lo = micros / 2;
    let span = lo + splitmix(seed ^ attempt.wrapping_mul(0x9E37_79B9_7F4A_7C15)) % (micros + 1);
    Duration::from_micros(span)
}

fn env_parse<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: core::fmt::Display,
{
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<T>()
            .unwrap_or_else(|err| panic!("{name}={raw:?} is not parseable: {err}")),
        Err(_) => default,
    }
}

fn env_duration_ms(name: &str, default_ms: u64) -> Duration {
    Duration::from_millis(env_parse(name, default_ms))
}

/// Run shape: how many dials, in which order, against which accept topology.
#[derive(Clone, Copy)]
struct Soak {
    dialers: u32,
    iterations: u32,
    seed: u64,
    acceptors: usize,
    dial_timeout: Duration,
}

impl Soak {
    fn from_env(acceptors_default: usize) -> Self {
        Self {
            dialers: env_parse("SOAK_DIALERS", 16),
            iterations: env_parse("SOAK_ITERATIONS", 24),
            seed: env_parse("SOAK_SEED", 1),
            acceptors: env_parse("SOAK_ACCEPTORS", acceptors_default),
            dial_timeout: env_duration_ms("SOAK_DIAL_TIMEOUT_MS", 30_000),
        }
    }
}

fn cancel_span_from_env() -> Option<Duration> {
    let micros: u64 = env_parse("SOAK_CANCEL_US", 2_000);
    (micros > 0).then(|| Duration::from_micros(micros))
}

fn pace_from_env() -> Duration {
    env_duration_ms("SOAK_ACCEPT_PACE_MS", 2)
}

/// Why a dial did not produce the echo of its own token.
#[derive(Clone, Debug)]
enum DialFailure {
    /// No echo before the deadline: the dial is never accepted, or its echo is
    /// never sent. This is the failure the soak exists to find.
    Unanswered {
        token: u64,
    },
    Io {
        token: u64,
        detail: String,
    },
    WrongSource {
        token: u64,
        got: SocketAddr,
    },
    WrongLength {
        token: u64,
        got: usize,
    },
    WrongToken {
        sent: u64,
        got: u64,
    },
}

impl core::fmt::Display for DialFailure {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            DialFailure::Unanswered { token } => {
                write!(f, "dial {token} was never accepted within the deadline")
            }
            DialFailure::Io { token, detail } => write!(f, "dial {token} failed: {detail}"),
            DialFailure::WrongSource { token, got } => {
                write!(f, "dial {token} got a datagram from {got}")
            }
            DialFailure::WrongLength { token, got } => {
                write!(f, "dial {token} got a {got}-byte datagram")
            }
            DialFailure::WrongToken { sent, got } => {
                write!(f, "dial {sent} got the echo of dial {got}")
            }
        }
    }
}

#[derive(Default)]
struct DialerReport {
    sent: Vec<u64>,
    echoed: Vec<u64>,
    failures: Vec<DialFailure>,
}

/// How a dialer paces itself. `wait_for_echo` off is the blast shape: send
/// every token up front and return, so the accept side sees the whole burst
/// before it polls.
#[derive(Clone, Default)]
struct DialPlan {
    wait_for_echo: bool,
    barrier: Option<Arc<tokio::sync::Barrier>>,
}

struct Server {
    listener: Arc<UtpListener<tokio_udp::UdpSocket, u64, Packet>>,
    listen_addr: SocketAddr,
    /// Every flow the accept side handed back, with the number of times each
    /// token was received: an identity multiset, so a duplicated handover
    /// cannot balance against a missing one.
    accepted: Mutex<BTreeMap<u64, u32>>,
    handled: AtomicUsize,
    /// Shutdown as a watch value, not a bare `Notify`: a receiver that
    /// subscribes after the value is set still observes it, so stopping the
    /// accept side cannot park a task that had not reached the notify yet.
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl Server {
    async fn new() -> Self {
        let udp = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .expect("bind the listener socket");
        let listen_addr = udp.local_addr().expect("listener local addr");
        let dispatch: Classify<SocketAddr, u64, Packet> =
            Arc::new(|_addr: &SocketAddr, pkt: Packet| {
                let bytes: [u8; TOKEN_LEN] = pkt.as_ref().try_into().ok()?;
                Some(Classified {
                    key: u64::from_be_bytes(bytes),
                    value: pkt,
                    policy: DispatchPolicy::Create,
                })
            });
        let listener = Arc::new(UtpListener::new(
            udp,
            NonZeroUsize::new(DISPATCH_BUFFER).unwrap(),
            dispatch,
        ));
        Self {
            listener,
            listen_addr,
            accepted: Mutex::new(BTreeMap::new()),
            handled: AtomicUsize::new(0),
            shutdown: tokio::sync::watch::channel(false).0,
        }
    }

    fn shutdown_rx(&self) -> tokio::sync::watch::Receiver<bool> {
        self.shutdown.subscribe()
    }
}

/// Read the flow's opening datagram, prove it belongs to the key it was
/// dispatched under, record the identity, and echo the token back. Anything
/// else is a routing defect: a flow must carry the datagram that opened it and
/// nothing else.
async fn handle_flow(
    server: &Server,
    mut conn: udp_listener::Conn<tokio_udp::UdpSocket, u64, Packet>,
) {
    let key = *conn.conn_key();
    let pkt = conn
        .read_half()
        .read_half()
        .recv()
        .await
        .expect("a flow was handed back after its opening datagram was dropped");
    assert_eq!(
        pkt.len(),
        TOKEN_LEN,
        "flow {key} carried a {}-byte payload",
        pkt.len()
    );
    let seen = u64::from_be_bytes(pkt.as_ref().try_into().unwrap());
    assert_eq!(
        seen, key,
        "flow {key} carried the payload of dial {seen}: the datagram was dispatched to the wrong flow"
    );
    *server.accepted.lock().unwrap().entry(seen).or_insert(0) += 1;
    server.handled.fetch_add(1, Ordering::Relaxed);
    conn.write()
        .send(pkt.as_ref())
        .await
        .expect("echo the dialled token");
}

/// Combined accept topology: each task both reads datagrams and drains the
/// accept queue.
async fn accept_combined(server: Arc<Server>, cancel: Option<Duration>, seed: u64) {
    let mut shutdown = server.shutdown_rx();
    let mut attempt = 0u64;
    loop {
        let next = match cancel {
            None => tokio::select! {
                biased;
                _ = shutdown.wait_for(|stop| *stop) => break,
                conn = server.listener.poll_next_conn() => conn,
            },
            Some(base) => {
                attempt += 1;
                let span = jittered_span(base, seed, attempt);
                tokio::select! {
                    biased;
                    _ = shutdown.wait_for(|stop| *stop) => break,
                    conn = server.listener.poll_next_conn() => conn,
                    _ = tokio::time::sleep(span) => continue,
                }
            }
        };
        let conn = next.expect("poll_next_conn failed");
        handle_flow(&server, conn).await;
    }
}

/// Split accept topology: this task only ever dequeues, so its
/// `try_accept_next` fast path is the cross-task read of the queue-length
/// counter.
async fn accept_split(server: Arc<Server>, pace: Duration, cancel: Option<Duration>, seed: u64) {
    let mut shutdown = server.shutdown_rx();
    let mut attempt = 0u64;
    loop {
        let next = match cancel {
            None => tokio::select! {
                biased;
                _ = shutdown.wait_for(|stop| *stop) => break,
                conn = server.listener.accept_next() => conn,
            },
            Some(base) => {
                attempt += 1;
                let span = jittered_span(base, seed, attempt);
                tokio::select! {
                    biased;
                    _ = shutdown.wait_for(|stop| *stop) => break,
                    conn = server.listener.accept_next() => conn,
                    _ = tokio::time::sleep(span) => continue,
                }
            }
        };
        let conn = next.expect("accept_next returned None while the listener is alive");
        handle_flow(&server, conn).await;
        if !pace.is_zero() {
            tokio::time::sleep(pace).await;
        }
    }
}

async fn dispatch_loop(server: Arc<Server>) {
    let mut shutdown = server.shutdown_rx();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.wait_for(|stop| *stop) => break,
            result = server.listener.dispatch_next() => {
                result.expect("dispatch_next failed");
            }
        }
    }
}

/// One dialer: its own socket, its own sequence of unique tokens. On the
/// first failure it stops rather than continuing, so a late echo cannot be
/// misread as the answer to the next dial.
async fn dialer(
    soak: Soak,
    id: u32,
    socket: Arc<tokio::net::UdpSocket>,
    listen: SocketAddr,
    plan: DialPlan,
) -> DialerReport {
    let mut report = DialerReport::default();
    for i in 0..soak.iterations {
        if let Some(barrier) = &plan.barrier {
            barrier.wait().await;
        }
        let dialled = token(id, i);
        if let Err(err) = socket.send_to(&dialled.to_be_bytes(), listen).await {
            report.failures.push(DialFailure::Io {
                token: dialled,
                detail: format!("send: {err}"),
            });
            break;
        }
        report.sent.push(dialled);
        if splitmix(soak.seed ^ dialled).is_multiple_of(4) {
            tokio::task::yield_now().await;
        }
        if !plan.wait_for_echo {
            continue;
        }
        match tokio::time::timeout(soak.dial_timeout, echo_of(socket.as_ref(), listen, dialled))
            .await
        {
            Err(_) => {
                report
                    .failures
                    .push(DialFailure::Unanswered { token: dialled });
                break;
            }
            Ok(Err(failure)) => {
                report.failures.push(failure);
                break;
            }
            Ok(Ok(got)) => {
                if got != dialled {
                    report
                        .failures
                        .push(DialFailure::WrongToken { sent: dialled, got });
                    break;
                }
                report.echoed.push(got);
            }
        }
    }
    report
}

/// The token of the next datagram from the listener, with its provenance
/// checked. A datagram from anywhere else is a failure, never skipped.
async fn echo_of(
    socket: &tokio::net::UdpSocket,
    listen: SocketAddr,
    dialled: u64,
) -> Result<u64, DialFailure> {
    let mut buf = [0u8; 64];
    let (n, from) = socket
        .recv_from(&mut buf)
        .await
        .map_err(|err| DialFailure::Io {
            token: dialled,
            detail: format!("recv: {err}"),
        })?;
    if from != listen {
        return Err(DialFailure::WrongSource {
            token: dialled,
            got: from,
        });
    }
    let bytes: [u8; TOKEN_LEN] = buf[..n].try_into().map_err(|_| DialFailure::WrongLength {
        token: dialled,
        got: n,
    })?;
    Ok(u64::from_be_bytes(bytes))
}

#[derive(Clone, Copy, Default)]
struct StatsSnapshot {
    packets_received: u64,
    packets_dispatched: u64,
    packets_dropped_rejected: u64,
    packets_dropped_existing_only: u64,
    packets_dropped_dispatcher_full: u64,
    packets_dropped_pkt_buf_overflow: u64,
    accepts_dropped_queue_full: u64,
    connections_opened: u64,
}

fn snapshot(server: &Server) -> StatsSnapshot {
    let stats = server.listener.stats();
    StatsSnapshot {
        packets_received: stats.packets_received.load(Ordering::Relaxed),
        packets_dispatched: stats.packets_dispatched.load(Ordering::Relaxed),
        packets_dropped_rejected: stats.packets_dropped_rejected.load(Ordering::Relaxed),
        packets_dropped_existing_only: stats.packets_dropped_existing_only.load(Ordering::Relaxed),
        packets_dropped_dispatcher_full: stats
            .packets_dropped_dispatcher_full
            .load(Ordering::Relaxed),
        packets_dropped_pkt_buf_overflow: stats
            .packets_dropped_pkt_buf_overflow
            .load(Ordering::Relaxed),
        accepts_dropped_queue_full: stats.accepts_dropped_queue_full.load(Ordering::Relaxed),
        connections_opened: stats.connections_opened.load(Ordering::Relaxed),
    }
}

/// Everything one batch gathered, with the violations the soak defines.
struct Batch {
    label: &'static str,
    soak: Soak,
    start: Instant,
    sent: Vec<u64>,
    echoed: Vec<u64>,
    dial_failures: Vec<DialFailure>,
    handled_tokens: BTreeSet<u64>,
    duplicated_handovers: u32,
    handled: usize,
    leftover: Vec<u64>,
    stats: StatsSnapshot,
    /// The batch refused dials on purpose (the queue bound was the point), so
    /// an unanswered dial is only a failure if the accounting does not
    /// explain it.
    refusals_expected: bool,
}

impl Batch {
    fn new(label: &'static str, soak: Soak, refusals_expected: bool, server: &Server) -> Self {
        Self {
            label,
            soak,
            start: Instant::now(),
            sent: Vec::new(),
            echoed: Vec::new(),
            dial_failures: Vec::new(),
            handled_tokens: BTreeSet::new(),
            duplicated_handovers: 0,
            handled: 0,
            leftover: Vec::new(),
            stats: snapshot(server),
            refusals_expected,
        }
    }

    /// Read the accept side's identity multiset and drain anything it never
    /// took. Must run after the accept tasks have stopped: a leftover here is
    /// a connection that was queued but never accepted.
    fn collect(&mut self, server: &Server) {
        let accepted = server.accepted.lock().unwrap();
        for (tok, count) in accepted.iter() {
            self.handled_tokens.insert(*tok);
            if *count > 1 {
                self.duplicated_handovers += count - 1;
            }
        }
        drop(accepted);
        self.handled = server.handled.load(Ordering::Relaxed);
        while let Some(conn) = server.listener.try_accept_next() {
            self.leftover.push(*conn.conn_key());
        }
        self.stats = snapshot(server);
    }

    fn violations(&self) -> Vec<String> {
        let mut problems = Vec::new();
        for failure in &self.dial_failures {
            problems.push(failure.to_string());
        }
        let sent: BTreeSet<u64> = self.sent.iter().copied().collect();
        if sent.len() != self.sent.len() {
            problems.push("the batch dialled the same token twice".to_string());
        }
        if self.duplicated_handovers > 0 {
            problems.push(format!(
                "{} token(s) were handed back more than once",
                self.duplicated_handovers
            ));
        }
        if !self.handled_tokens.is_subset(&sent) {
            problems.push("a flow was handed back whose token was never dialled".to_string());
        }
        if self.stats.packets_received != self.sent.len() as u64 {
            problems.push(format!(
                "the listener received {} of {} dials: {} datagram(s) never reached it",
                self.stats.packets_received,
                self.sent.len(),
                self.sent.len() as u64 - self.stats.packets_received
            ));
        }
        // Every received datagram is either dispatched or dropped by exactly
        // one counted path.
        let accounted = self.stats.packets_dispatched
            + self.stats.packets_dropped_rejected
            + self.stats.packets_dropped_existing_only
            + self.stats.packets_dropped_dispatcher_full
            + self.stats.packets_dropped_pkt_buf_overflow;
        if accounted != self.stats.packets_received {
            problems.push(format!(
                "{} datagram(s) were counted into no dispatch/drop path",
                self.stats.packets_received as i64 - accounted as i64
            ));
        }
        for (name, value) in [
            (
                "packets_dropped_rejected",
                self.stats.packets_dropped_rejected,
            ),
            (
                "packets_dropped_existing_only",
                self.stats.packets_dropped_existing_only,
            ),
            (
                "packets_dropped_dispatcher_full",
                self.stats.packets_dropped_dispatcher_full,
            ),
            (
                "packets_dropped_pkt_buf_overflow",
                self.stats.packets_dropped_pkt_buf_overflow,
            ),
        ] {
            if value != 0 {
                problems.push(format!(
                    "{name} is {value}, but the soak dials nothing droppable"
                ));
            }
        }
        // No flow may exist that was neither handed back nor counted refused.
        let unexplained = self.stats.connections_opened as i64
            - self.handled as i64
            - self.leftover.len() as i64
            - self.stats.accepts_dropped_queue_full as i64;
        if unexplained != 0 {
            problems.push(format!(
                "{unexplained} flow(s) were opened but neither accepted nor counted refused"
            ));
        }
        if !self.leftover.is_empty() {
            problems.push(format!(
                "{} connection(s) were queued but never accepted",
                self.leftover.len()
            ));
        }
        if self.refusals_expected {
            if self.stats.accepts_dropped_queue_full == 0 {
                problems.push(
                    "the phase dialled past the accept-queue bound but no dial was refused: resize the phase"
                        .to_string(),
                );
            }
            let unanswered: BTreeSet<u64> =
                sent.difference(&self.handled_tokens).copied().collect();
            if unanswered.len() as u64 != self.stats.accepts_dropped_queue_full {
                problems.push(format!(
                    "{} flows were refused but {} dials were never handed back",
                    self.stats.accepts_dropped_queue_full,
                    unanswered.len()
                ));
            }
        } else {
            if self.stats.accepts_dropped_queue_full != 0 {
                problems.push(format!(
                    "{} dials were refused although the phase stays under the queue bound",
                    self.stats.accepts_dropped_queue_full
                ));
            }
            if self.handled_tokens != sent {
                problems.push(format!(
                    "{} dial(s) were sent but never handed back",
                    sent.difference(&self.handled_tokens).count()
                ));
            }
        }
        problems
    }

    fn finish(&self) {
        let cpus = std::thread::available_parallelism().map_or(0, NonZeroUsize::get);
        let problems = self.violations();
        let json = format!(
            concat!(
                r#"{{"label":"{}","seed":{},"dialers":{},"iterations":{},"acceptors":{},"#,
                r#""sent":{},"echoed":{},"handled":{},"handled_unique":{},"duplicated_handovers":{},"#,
                r#""refused":{},"leftover":{},"dial_failures":{},"elapsed_ms":{},"cpus":{},"#,
                r#""packets_received":{},"packets_dispatched":{},"connections_opened":{},"#,
                r#""problems":{}}}"#
            ),
            self.label,
            self.soak.seed,
            self.soak.dialers,
            self.soak.iterations,
            self.soak.acceptors,
            self.sent.len(),
            self.echoed.len(),
            self.handled,
            self.handled_tokens.len(),
            self.duplicated_handovers,
            self.stats.accepts_dropped_queue_full,
            self.leftover.len(),
            self.dial_failures.len(),
            self.start.elapsed().as_millis(),
            cpus,
            self.stats.packets_received,
            self.stats.packets_dispatched,
            self.stats.connections_opened,
            problems.len(),
        );
        println!("SOAK_RESULT {json}");
        for problem in &problems {
            println!("SOAK_VIOLATION {problem}");
        }
        assert!(
            problems.is_empty(),
            "the accept path lost or misdelivered a dial: {problems:#?}"
        );
    }
}

/// Bind one socket per dialer and keep them alive for the whole batch: a
/// socket that outlives its last dial keeps a late echo from drawing an ICMP
/// error into an unrelated send.
async fn dialer_sockets(dialers: u32) -> Vec<Arc<tokio::net::UdpSocket>> {
    let mut sockets = Vec::with_capacity(dialers as usize);
    for _ in 0..dialers {
        sockets.push(Arc::new(
            tokio::net::UdpSocket::bind("127.0.0.1:0")
                .await
                .expect("bind a dialer socket"),
        ));
    }
    sockets
}

async fn run_dialers(
    soak: Soak,
    sockets: &[Arc<tokio::net::UdpSocket>],
    listen: SocketAddr,
    plan: DialPlan,
) -> (Vec<u64>, Vec<u64>, Vec<DialFailure>) {
    let mut tasks = JoinSet::new();
    for (id, socket) in sockets.iter().enumerate() {
        let socket = Arc::clone(socket);
        let plan = plan.clone();
        tasks.spawn(async move { dialer(soak, id as u32, socket, listen, plan).await });
    }
    let mut sent = Vec::new();
    let mut echoed = Vec::new();
    let mut failures = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let report = result.expect("a dialer task panicked");
        sent.extend(report.sent);
        echoed.extend(report.echoed);
        failures.extend(report.failures);
    }
    // A dialer stops at its first failure, so a short `sent` means a dial was
    // not answered — the failure `Batch::violations` reports with its token,
    // and which must not be pre-empted here by a bare count assertion that
    // reads like the harness lost the dial. The expected count is only a
    // harness self-check, so it is asserted solely when no dial failed.
    if failures.is_empty() {
        assert_eq!(
            sent.len(),
            soak.dialers as usize * soak.iterations as usize,
            "no dial failed, yet the batch did not dial every token"
        );
    }
    (sent, echoed, failures)
}

async fn stop_accept_side(server: &Arc<Server>, tasks: &mut JoinSet<()>) {
    server.shutdown.send_replace(true);
    while let Some(result) = tasks.join_next().await {
        result.expect("an accept-side task panicked");
    }
}

/// Every dial has been read and has reached its one dispatch or drop path.
fn dispatch_settled(server: &Server, sent: usize) -> bool {
    let stats = snapshot(server);
    stats.packets_received == sent as u64
        && stats.packets_dispatched
            + stats.packets_dropped_rejected
            + stats.packets_dropped_existing_only
            + stats.packets_dropped_dispatcher_full
            + stats.packets_dropped_pkt_buf_overflow
            == sent as u64
}

fn spawn_acceptors(tasks: &mut JoinSet<()>, server: &Arc<Server>, soak: Soak, topology: Topology) {
    // A slow heartbeat, so a run that is killed for hanging reports how far it
    // got instead of only the sizes it was started with.
    {
        let server = Arc::clone(server);
        let mut shutdown = server.shutdown_rx();
        tasks.spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.wait_for(|stop| *stop) => return,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {
                        println!(
                            "SOAK_PROGRESS {{\"handled\":{},\"received\":{}}}",
                            server.handled.load(Ordering::Relaxed),
                            snapshot(&server).packets_received,
                        );
                    }
                }
            }
        });
    }
    match topology {
        Topology::Combined { cancel } => {
            for _ in 0..soak.acceptors {
                let server = Arc::clone(server);
                tasks.spawn(accept_combined(server, cancel, soak.seed));
            }
        }
        Topology::Split { pace, cancel } => {
            let dispatcher = Arc::clone(server);
            tasks.spawn(dispatch_loop(dispatcher));
            for _ in 0..soak.acceptors {
                let server = Arc::clone(server);
                tasks.spawn(accept_split(server, pace, cancel, soak.seed));
            }
        }
        // One task owns dispatch and never accepts, while the acceptors only
        // ever drain at their loop head: a flow the dispatcher enqueues has to
        // be noticed by a task that is parked in a datagram read.
        Topology::Mixed { cancel } => {
            let dispatcher = Arc::clone(server);
            tasks.spawn(dispatch_loop(dispatcher));
            for _ in 0..soak.acceptors {
                let server = Arc::clone(server);
                tasks.spawn(accept_combined(server, cancel, soak.seed));
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Topology {
    Combined {
        cancel: Option<Duration>,
    },
    Split {
        pace: Duration,
        cancel: Option<Duration>,
    },
    Mixed {
        cancel: Option<Duration>,
    },
}

/// Closed-loop dialing against an accept side, then the batch's verdict.
/// `rounds` synchronizes the dialers on a barrier so each round lands as a
/// burst instead of a trickle.
async fn churn_batch(label: &'static str, soak: Soak, topology: Topology, rounds: bool) {
    let server = Arc::new(Server::new().await);
    let mut batch = Batch::new(label, soak, false, &server);
    let mut tasks = JoinSet::new();
    spawn_acceptors(&mut tasks, &server, soak, topology);
    let barrier = rounds.then(|| Arc::new(tokio::sync::Barrier::new(soak.dialers as usize)));
    let sockets = dialer_sockets(soak.dialers).await;
    let (sent, echoed, failures) = run_dialers(
        soak,
        &sockets,
        server.listen_addr,
        DialPlan {
            wait_for_echo: true,
            barrier,
        },
    )
    .await;
    stop_accept_side(&server, &mut tasks).await;
    batch.sent = sent;
    batch.echoed = echoed;
    batch.dial_failures = failures;
    batch.collect(&server);
    batch.finish();
}

/// N closed-loop dialers against K combined accept-and-dispatch tasks.
#[tokio::test(flavor = "multi_thread")]
async fn churn_over_the_combined_accept_path_loses_no_dial() {
    churn_batch(
        "churn",
        Soak::from_env(2),
        Topology::Combined {
            cancel: cancel_span_from_env(),
        },
        false,
    )
    .await;
}

/// N closed-loop dialers against one dispatcher and K accept-only tasks: the
/// dequeuing tasks never enqueue, so they reach the queue through the
/// lock-free length check alone.
#[tokio::test(flavor = "multi_thread")]
async fn churn_over_split_accept_tasks_loses_no_dial() {
    churn_batch(
        "multi_accept",
        Soak::from_env(4),
        Topology::Split {
            pace: Duration::ZERO,
            cancel: None,
        },
        false,
    )
    .await;
}

/// One dispatcher that never accepts, racing acceptors that only drain at
/// their loop head: a flow enqueued by the dispatcher must still be handed
/// back without waiting for another datagram.
#[tokio::test(flavor = "multi_thread")]
async fn mixed_dispatcher_and_combined_acceptors_lose_no_dial() {
    churn_batch(
        "mixed",
        Soak::from_env(2),
        Topology::Mixed {
            cancel: cancel_span_from_env(),
        },
        false,
    )
    .await;
}

/// Rounds of N simultaneous dials against an accept side that is deliberately
/// slow to poll, so many flows sit in the accept queue across an await.
#[tokio::test(flavor = "multi_thread")]
async fn bursts_against_a_slow_acceptor_lose_no_dial() {
    churn_batch(
        "burst",
        Soak::from_env(2),
        Topology::Split {
            pace: pace_from_env(),
            cancel: None,
        },
        true,
    )
    .await;
}

/// The accept future is dropped mid-flight and rebuilt, over both the split
/// notify path and the combined path, while dials are in flight.
#[tokio::test(flavor = "multi_thread")]
async fn accept_under_frequent_cancellation_loses_no_dial() {
    churn_batch(
        "cancel",
        Soak::from_env(4),
        Topology::Split {
            pace: Duration::ZERO,
            cancel: cancel_span_from_env(),
        },
        false,
    )
    .await;
}

/// More dials than the bounded accept queue holds, all dispatched before the
/// accept side polls once. Every dial is either handed back or counted as a
/// refusal; a refusal must not hide a queued flow, must not skew the
/// empty-queue check, and must leave the fast path working for the next dial.
#[tokio::test(flavor = "multi_thread")]
async fn accept_queue_at_its_bound_accounts_for_every_flow() {
    let soak = Soak::from_env(2);
    let server = Arc::new(Server::new().await);
    let mut batch = Batch::new("capacity", soak, true, &server);
    let sockets = dialer_sockets(soak.dialers).await;

    // Dispatch only: the accept side is not polling, so the queue fills to
    // its bound and every further flow is refused.
    let mut dispatchers = JoinSet::new();
    let dispatcher = Arc::clone(&server);
    dispatchers.spawn(dispatch_loop(dispatcher));

    let (sent, _echoed, failures) = run_dialers(
        soak,
        &sockets,
        server.listen_addr,
        DialPlan {
            wait_for_echo: false,
            barrier: None,
        },
    )
    .await;

    // Wait until every dial has been read and has reached its dispatch or drop
    // path, so the refusal count is final before it is read.
    tokio::time::timeout(Duration::from_secs(30), async {
        while !dispatch_settled(&server, sent.len()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the dispatch loop did not process every dial");
    let refused_before_accepting = snapshot(&server).accepts_dropped_queue_full;

    // Now let the accept side drain what was queued.
    let mut tasks = JoinSet::new();
    spawn_acceptors(
        &mut tasks,
        &server,
        soak,
        Topology::Split {
            pace: Duration::ZERO,
            cancel: None,
        },
    );
    let expected_handed_back = sent.len() as u64 - refused_before_accepting;
    tokio::time::timeout(Duration::from_secs(30), async {
        while server.handled.load(Ordering::Relaxed) as u64 != expected_handed_back {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the accept side did not hand back every queued flow");
    stop_accept_side(&server, &mut tasks).await;
    server.shutdown.send_replace(true);
    while let Some(result) = dispatchers.join_next().await {
        result.expect("the dispatch loop panicked");
    }

    batch.sent = sent;
    batch.echoed = Vec::new();
    batch.dial_failures = failures;
    batch.collect(&server);
    batch.finish();

    // The refusals must not have poisoned the empty-queue check: a fresh dial
    // still has to be accepted and echoed. Its token id was dialled in the
    // blast, so this also re-opens a key whose flow was handed back (or
    // refused) and closed.
    server.shutdown.send_replace(false);
    let fresh_sockets = dialer_sockets(1).await;
    let fresh_soak = Soak {
        dialers: 1,
        iterations: 1,
        ..soak
    };
    let mut fresh_tasks = JoinSet::new();
    spawn_acceptors(
        &mut fresh_tasks,
        &server,
        fresh_soak,
        Topology::Split {
            pace: Duration::ZERO,
            cancel: None,
        },
    );
    let (fresh_sent, fresh_echoed, fresh_failures) = run_dialers(
        fresh_soak,
        &fresh_sockets,
        server.listen_addr,
        DialPlan {
            wait_for_echo: true,
            barrier: None,
        },
    )
    .await;
    stop_accept_side(&server, &mut fresh_tasks).await;
    assert_eq!(fresh_sent, vec![token(0, 0)]);
    assert!(
        fresh_failures.is_empty() && fresh_echoed == fresh_sent,
        "a dial after the queue had refused flows was not accepted: {fresh_failures:#?}"
    );
    assert!(
        server.listener.try_accept_next().is_none(),
        "the accept queue still held a flow after every dial was handed back"
    );
}
