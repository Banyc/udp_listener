# The udp_listener accept-path gate

`udp_listener` is the dispatch/accept layer beneath `rtp`: a `UtpListener`
routes datagrams to per-key sub-connections and hands each new flow back
exactly once through a bounded accept queue (`src/lib.rs`). Its failure mode is
silent *liveness* loss — a flow queued with no waiter ever woken, a wake
consumed by a waiter that never takes the flow, a teardown that leaves a waiter
parked, a refusal that still counts as a handover — and none of those is
visible to a counter alone, only to a test that dials and requires the flow
back. This file is the authoritative record of what this crate runs, what it
declares, and what it does not cover.

## The surface: one opt-in tier

Measured on the tree this file is committed with, `cargo test --release`:

* `-- --list --ignored` reports **4 ignored tests**: the receive-buffer
  drop-site sweep (`recv_buffer_drop_sites`), the kernel's per-socket refusal
  arm (`kernel_refusal`), and the two channel-capacity arms
  (`channel_capacity_sweep`). Every other test in the crate is required-default.
* There is **no bench target**: no `benches/` directory, no `[[bench]]` in
  `Cargo.toml`, no `criterion` in `Cargo.lock`.
* The whole default tier costs **0.89 s** wall clock (`lib` 0.07 s,
  `accept_churn_soak` 0.70 s, `dispatch_delay` 0.03 s, `dispatcher_overflow`
  0.00 s). The churn target's cost is one cell —
  `bursts_against_a_slow_acceptor_lose_no_dial` at 0.70 s — and the lib tier and
  `dispatcher_overflow` are at the process-start floor (the overflow cell's own
  median is 2.6 ms over ten process runs). The new sweep is `#[ignore]`d and so
  is not part of this cost.

So `gate-manifest` below carries four lines, the opt-in scenarios; the checker
re-derives that set from the compiled binaries, so a test silently re-ignored is
an error. The dual mandate's *time* half still has nothing to shorten in the
always-run tier — it is under a second — and its *coverage* half declares the
delay measurement under "The dispatch path's per-datagram delay", the buffer
split under "The two receive-side buffers", and the channel's capacity under
"The per-flow channel" below.

## The always-run liveness cells

These seven are the crate's liveness gate and are required-default: each is
worth zero if it stops running. One *dial* is one datagram whose 8-byte token becomes the
flow key, so the assertion is an identity set over tokens (a token that never
returns, returns twice, or returns from a different dial is a failure), never a
timeout-absorbed count.

| cell (`tests/accept_churn_soak.rs`) | line | quantity it claims |
| --- | --- | --- |
| `churn_over_the_combined_accept_path_loses_no_dial` | 850 | fraction of dials whose own token never returns, over the combined `poll_next_conn` path |
| `churn_over_split_accept_tasks_loses_no_dial` | 866 | the same quantity, over one dispatcher plus `accept_next` tasks |
| `mixed_dispatcher_and_combined_acceptors_lose_no_dial` | 883 | the same quantity, when the dispatch role and the accept role never share a task |
| `bursts_against_a_slow_acceptor_lose_no_dial` | 898 | the same quantity, with the queue occupied across an await (barrier-synchronized rounds, paced acceptor) |
| `accept_under_frequent_cancellation_loses_no_dial` | 914 | the same quantity, with accept futures dropped mid-handover every few milliseconds |
| `accept_queue_at_its_bound_accounts_for_every_flow` | 932 | refused-versus-handed-back accounting past the 256-entry bound (every dial accounted, none hidden) plus fast-path recovery after the drain |
| `a_failed_dialer_does_not_strand_the_other_round_participants` | 1069 | round completion with a failed dialer present (a stranded barrier would hang, not report) |

Two lib-tier cells carry a wall-clock *wake bound*, which is the closest thing
this crate has to a latency assertion. `SOAK_WAKE_BOUND_MS` (default 5000) is
the bound; an overrun is printed as `SOAK_WAKE_LATE` and is explicitly a
scheduling observation, never a catch, while a drain that never completes is
fatal:

| cell (`src/accept_queue_soak.rs`) | line | quantity it claims |
| --- | --- | --- |
| `a_burst_enqueued_before_any_waiter_is_handed_back_once_and_in_bound` | 770 | accept-handover wake latency for N flows enqueued before any waiter exists, under `SOAK_WAKE_BOUND_MS`; identity multiset is the verdict |
| `a_teardown_drains_every_parked_waiter_and_releases_the_listener` (`src/teardown_soak.rs`) | 526 | the parked-waiter census reaching zero inside `SOAK_WAKE_BOUND_MS`, with the listener released |

## The opt-in surface: `SOAK_*`, in `gate-env-tier`

The crate *is* scalable without rebuilding, but by environment variable rather
than by `#[ignore]`, so the blocks derived from the `#[ignore]` set cannot see
it: those are the scenario directory's ignored set (`ignored_scenarios`,
`netem-tools check-gate`, over the scenario targets) resolved through
`cargo test --list`. `gate-env-tier` is the block for such a
surface, and `netem-tools check-gate` is what enforces it: detection needs a
crate script *and* a Rust source to name the variable (closed
transitively over the crate's own calls), a detected name the
declaration omits is an error, and a declared name must be read
by a source and named by the surface's runner.

* `SOAK_DIALERS`, `SOAK_ITERATIONS`, `SOAK_SEED`, `SOAK_ACCEPTORS`,
  `SOAK_DIAL_TIMEOUT_MS` (`tests/accept_churn_soak.rs:105-109`),
  `SOAK_CANCEL_US` (`:115`), `SOAK_ACCEPT_PACE_MS` (`:120`) size the six
  accept-churn modes; `SOAK_WAKE_ITEMS`, `SOAK_WAKE_WAITERS`,
  `SOAK_WAKE_COMBINED`, `SOAK_WAKE_BOUND_MS` (`src/accept_queue_soak.rs:771-774`,
  `src/teardown_soak.rs:530`) size the two lib-tier handover cells.
* The runner of record is `local/soak_accept_churn.py`: it runs each mode as
  its own process group with a per-batch timeout, kills a hung batch by group
  plus a path-matched sweep, and turns the batches into a **detection limit**
  rather than a bare pass — zero failures in N dials excludes a per-dial loss
  rate above ~3/N at 95 % (`local/soak_accept_churn.py:319`, `:338`).

Measured cost, default driver sizing (32 dialers, 100 iterations, one batch
per mode, **debug** build as the driver builds it): 16 320 dials in 1.97 s wall
clock, detection limit 1.8e-4 per dial, zero failures, zero hangs, zero
strays. The quantity the sweep claims is therefore a **per-dial liveness rate**,
under a load shape (`dialers × iterations`), an accept topology (the six modes
of `local/soak_accept_churn.py:48`), a cancellation span and a pacing — not a
latency or a goodput.

The block carries **one row**, because a row's runner must be a file under the
crate root that names at least one of the row's variables
(enforced by `netem-tools check-gate`) and the four `SOAK_WAKE_*` names have
no script runner: they are read in-process (`src/accept_queue_soak.rs:465-467`)
and sized by whoever invokes `cargo test`. They are therefore declared in the
one row beside the runner that does exist, and that row's `measures` says which
half each variable sizes. The two halves are not interchangeable: the
`SOAK_DIALERS…` half is a per-dial **liveness rate** with a rule-of-three
bound, and `SOAK_WAKE_BOUND_MS` is the **handover wake bound**, whose overrun
prints `SOAK_WAKE_LATE` and is informational (`src/accept_queue_soak.rs:741`,
`src/teardown_soak.rs:505`) while a drain that never completes is fatal.

## The dispatch path's per-datagram delay

A deployed client reports a 190 ms **minimum** round trip where the harness's
clean arm reports tens of milliseconds. `rtp` reaches the network only through
this crate, so `tests/dispatch_delay.rs` measures the toll this dispatch path
adds to a datagram that never queues behind another one — the pooled-buffer
take, the `HashMap`-under-a-mutex lookup, the `mpsc` hop and the task wakeup —
**differentially** against a bare socket serving the same echo. The client is
byte-identical between the two arms, so the difference is the dispatch path and
nothing else.

Measured on the tree this file is committed with (`--release`, best of three):
median 0.039 ms through the dispatch path against 0.036 ms for the bare echo —
**0.002 ms of added cost, 0.001 % of the field's floor** — p99 0.079 ms, with
all 400 datagrams echoed and zero dispatcher-channel drops. The bound is a
tripwire at 2 ms on the median.

The second arm sweeps pipelining depth and reports the achieved rate beside each
window, which is what separates a per-datagram toll from a queue: the median
holds at one datagram in flight (0.032 ms at 25 875/s) and grows with the depth
(0.146 ms at 16, 0.437 ms at 64, at 126 867/s) — the shape of a serialized
echoer, not of a fixed cost.

The one dimension that stays empty by construction is `impairment`: `Cargo.toml`
has no `netem-test` dependency and this crate sits *below* `rtp` in the
dependency graph, so loss, delay, reordering and rate shaping are the composing
scenarios' to measure, not this crate's. It is recorded as a gap below rather
than claimed as a row.

## The dispatcher overflow, attributed to the flow that dropped

`dispatch_next` never blocks: a full per-flow channel drops the datagram rather
than backpressuring the accept loop. Dropping is the right policy for a reliable
transport over UDP — `rtp` repairs it over a round trip — and this crate does not
change it. What the arm asserts is that the drop is **readable**: the listener
totals the overflow in `ListenerStats::packets_dropped_dispatcher_full`, and the
same drop is counted against the flow whose buffer filled in
`ConnStats::packets_dropped_dispatcher_full`, which a flow's `Conn::stats()` (or
its split `ConnRead::stats()`) exposes. An aggregate alone cannot attribute an
overload: at the moment a path is most degraded, the dropped datagrams are
exactly the ones carrying no information about which flow to look at, and the
repair is per flow.

The arm is
`dispatcher_overflow::a_dispatcher_overflow_is_attributed_to_the_flow_that_dropped`
(required-default, asserting, 0.01 s nominal against a measured 2.6 ms median
over ten process runs): a 64-datagram burst is offered into a four-slot channel
whose reader is parked, then drained. One run printed:

```
DISPATCH_OVERFLOW_STATS offered=65 received=65 delivered=4
dropped_dispatcher_full_per_flow=61 dropped_dispatcher_full_aggregate=61
channel_capacity=4 drop_rate=0.938 flows=1
```

so the offer exceeds the drain by a **drop rate of 0.938** while the channel
still delivers its whole capacity — the arm measures an overload, not an outage.
It asserts the per-flow count (61, not merely "non-zero"), the identity
`offered == delivered + dropped`, the flow's count equalling the listener's total
over the one flow, and the split read half agreeing with the connection it came
from. Both counters were probed: with the per-flow increment removed the arm is
red naming `the flow's own overflow count is 0, not 61` (while the aggregate
still printed 61), and with the listener's increment removed the lib cell
`counters_distinguish_dispatcher_buffer_overflows` is red.

What this layer **cannot** show is the drop's consequence one layer up. `rtp`
repairs a lost datagram with a retransmission a round trip later, and that
repaired round trip is `rtp`'s to measure — this crate has no `rtp` dependency.
The count is the deliverable; the repair is recorded as a gap below.

Its one `gate-perf-design` cell varies the offer shape, the reader state and the
channel bound together and is a composite for that reason: a ping-pong against a
bare socket cannot overflow anything.

## The two receive-side buffers: which drop site is attributable

`rtp` reaches the network through two buffers in series. The kernel's receive
queue is sized by `tokio_udp::UdpSocket::set_recv_buffer_size` (exposed by
`tokio_udp` v0.0.6 but, in this workspace, called by no consumer), and this
crate's per-flow dispatcher channel is sized by the `dispatcher_buffer_size`
argument to `UtpListener::new`. A datagram can be refused at either, and the two
refusals differ in consequence: the dispatcher counts its refusal against the
flow whose channel filled
(`ConnStats::packets_dropped_dispatcher_full`, the accessor this crate gained),
while a datagram the kernel refused before `recv` was never seen here, so from
inside `udp_listener` it is indistinguishable from path loss.

`recv_buffer_drop_sites::sizing_the_receive_buffer_moves_a_drop_between_sites_and_the_knee_is_the_channel`
(opt-in, `standard` tier, asserting, measured 1.9 s) offers
`BURST = 32 768` datagrams while nothing reads the socket — a scheduling stall,
the only shape in which the kernel queue accumulates — then starts the dispatch
loop and reads both sites. It sweeps five budgets at two payload sizes against a
channel sized to `rtp`'s own `DISPATCHER_BUF_SIZE` (1024, `rtp/src/udp.rs:71`).
One run, load 6–11 on ten cores, release:

```
RECV_BUFFER_DROP_SITES size=256   label=floor-4KiB     effective=4096    offered=32769 received=15    channel_accepted=15   dispatcher_dropped=0     kernel_dropped=32754 delivered=15   total_lost=32754
RECV_BUFFER_DROP_SITES size=256   label=linux-default  effective=212992  offered=32769 received=740   channel_accepted=740  dispatcher_dropped=0     kernel_dropped=32029 delivered=740  total_lost=32029
RECV_BUFFER_DROP_SITES size=256   label=host-default   effective=786896  offered=32769 received=2733  channel_accepted=1024 dispatcher_dropped=1709  kernel_dropped=30036 delivered=1024 total_lost=31745
RECV_BUFFER_DROP_SITES size=256   label=1MiB           effective=1048576 offered=32769 received=3641  channel_accepted=1024 dispatcher_dropped=2617  kernel_dropped=29128 delivered=1024 total_lost=31745
RECV_BUFFER_DROP_SITES size=256   label=4MiB           effective=4194304 offered=32769 received=14564 channel_accepted=1024 dispatcher_dropped=13540 kernel_dropped=18205 delivered=1024 total_lost=31745
RECV_BUFFER_DROP_SITES size=1200  label=4MiB           effective=4194304 offered=32769 received=3405  channel_accepted=1024 dispatcher_dropped=2381  kernel_dropped=29364 delivered=1024 total_lost=31745
```

The mechanism is an identity, not a trade: `offered = received + kernel_dropped`,
`received = channel_accepted + dispatcher_dropped`, and
`channel_accepted = min(received, 1024)`. Below a buffer holding 1024 datagrams
the dispatcher drops nothing and all loss is kernel-side, so sizing the buffer
there *reduces* total loss (32 754 → 32 029 → 31 745); above that knee the
channel is saturated, total loss is pinned at `offered - 1024 = 31 745` whatever
the buffer holds, and every extra datagram a larger buffer admits becomes a
`dispatcher_dropped`. The dispatcher drop never falls as the buffer grows — the
256 B series is 0, 0, 1709, 2617, 13540 — and the residual loss is the channel's.

What this means is a refusal, not a default: sizing the kernel buffer does not
reduce the per-flow dispatcher drop; it reduces the kernel drop — the one the
operator cannot tell from path loss — only up to the channel's own depth, and
beyond that it merely relocates the drop onto the attributable counter. The
lever for the residual is the channel capacity (and the consumer's drain), not
the socket buffer.

The two sites are separable *in this arm* because the arm is the sender and so
knows `offered`, making `kernel_dropped = offered - received`. In production the
listener knows neither `offered` nor the kernel's refusal count, so it still
cannot tell a kernel drop from path loss; that gap is recorded below.

Both properties this arm rests on were probed. Removing the per-flow
`fetch_add` in `src/lib.rs` (one occurrence before the edit) turns it red naming
`size 256 host-default: the flow's own overflow count is 0 but the listener
totals 1709 over its one flow`. Removing the `set_recv_buffer_size` call in the
arm's own fixture turns it red naming `size 256: the sweep's 5-row budget series
did not produce strictly increasing effective buffer sizes: [("floor-4KiB",
786896), …]` — every row at the host default, which is the fixture measuring
nothing. Both were restored with `touch` and the pristine run is green.

## The per-flow channel: the knee, the burst bound, and what a slot costs

The sibling arm above sizes the *kernel* receive buffer and reads the dispatcher
drop beside it; the residual it cannot remove is the channel's. This arm sweeps
the channel instead. The knob is the `dispatcher_buffer_size` argument to
`UtpListener::new`, which sizes the per-flow `tokio::sync::mpsc::channel` this
crate builds at `src/lib.rs:479` on the dispatch path and `:589` in
`register_conn`. The channel is this crate's and the tests reach its capacity
through the constructor's own seam; the *value* is `rtp`'s, which passes
`DISPATCHER_BUF_SIZE + data_settings.max()` (`rtp/src/udp.rs:274`) and bare
`DISPATCHER_BUF_SIZE` — 1024, `rtp/src/udp.rs:71` — at `keyed_udp.rs:107,215`
and `mpudp.rs:30`. No `rtp` change is needed to sweep it and none was made.

`channel_capacity_sweep::the_channel_capacity_sets_the_burst_a_stalled_consumer_accumulates`
(opt-in, `standard` tier, asserting, measured 0.5 s) offers `BURST = 4096`
256-byte datagrams at six capacities with the **dispatch loop live** and a
**stalled consumer**, so the kernel queue never accumulates and the channel is
the only buffer measured. One run, load 2.8 on ten cores, release:

```
CHANNEL_CAPACITY_SWEEP capacity=64     offered=4097 received=4097 accepted=64     dispatcher_dropped=4033 kernel_dropped=0 delivered=64     bytes=4327968   bytes_per_slot=67624 drain_datagrams_per_s=4876190
CHANNEL_CAPACITY_SWEEP capacity=256    offered=4097 received=4097 accepted=256    dispatcher_dropped=3841 kernel_dropped=0 delivered=256    bytes=16923536  bytes_per_slot=66108 drain_datagrams_per_s=7897091
CHANNEL_CAPACITY_SWEEP capacity=1024   offered=4097 received=4097 accepted=1024   dispatcher_dropped=3073 kernel_dropped=0 delivered=1024   bytes=67304992  bytes_per_slot=65728 drain_datagrams_per_s=13370939
CHANNEL_CAPACITY_SWEEP capacity=2048   offered=4097 received=4097 accepted=2048   dispatcher_dropped=2049 kernel_dropped=0 delivered=2048   bytes=134480352 bytes_per_slot=65664 drain_datagrams_per_s=20168994
CHANNEL_CAPACITY_SWEEP capacity=4096   offered=4097 received=4097 accepted=4096   dispatcher_dropped=1    kernel_dropped=0 delivered=4096   bytes=268699744 bytes_per_slot=65601 drain_datagrams_per_s=30406509
CHANNEL_CAPACITY_SWEEP capacity=8192   offered=4097 received=4097 accepted=4097   dispatcher_dropped=0    kernel_dropped=0 delivered=4097   bytes=268701696 bytes_per_slot=65585 drain_datagrams_per_s=35768851
CHANNEL_CAPACITY_SWEEP_KNEE first_capacity_with_zero_drops=8192 received=4097 delivered=4097 kernel_dropped=0 bytes_per_slot=65585
```

The loss is a **burst bound** and the knee is exact: `dispatcher_dropped =
received - capacity` on every row, and the drop reaches zero at the first
capacity that holds the burst (4097 datagrams here, so the sweep's next grid
point, 8192, is the first row that reads zero while a row at 4096 still drops
the one opener that overflows it). **The channel only has to be as deep as the
burst a stalled consumer accumulates**: above that every extra slot is idle,
below it every extra slot is a datagram delivered. A *rate* mismatch would show
a drop that does not fall with capacity; the live arm and the vacuity probe
below are that branch.

What a slot costs is the second reading, and it is not the datagram. A channel
entry is an `ObjScoped<BytesMut>` whose buffer was allocated with
`PACKET_BUFFER_LENGTH` (65 536 B) and is *not* shrunk after `recv_buf_from`
writes the datagram into it, so `bytes_per_slot` measures 65 585–67 624 B: a
256-byte datagram occupies 64 KiB of resident memory while it waits. At `rtp`'s
own 1024 slots that is **67.3 MB for one full flow**; at the listener's
`max_connections` ceiling of 1024 flows (`rtp/src/udp.rs:81`) that is a **64 GiB**
ceiling if every flow fills. The operator's client reaches this through **one
long-lived mux session**, so the flow count today is one and the cost is 67.3 MB;
but it is capacity-linear at 64 KiB per slot whenever the channel grows.

`channel_capacity_sweep::a_live_consumer_capacity_sweep_shows_the_stall_coverage`
(opt-in, `standard` tier, asserting, measured 0.4 s) fixes the offer at 50 000
datagrams and varies only the capacity while a consumer drains the flow's read
half in a tight loop — the fastest a consumer can take datagrams out of this
channel, and so an upper bound on `rtp`'s own rate. One run, same host:

```
CHANNEL_CAPACITY_LIVE capacity=1      offered=50001 received=50001 accepted=15306 dispatcher_dropped=34695 delivered=15306 arrive_datagrams_per_s=600251 consumer_datagrams_per_s=113521
CHANNEL_CAPACITY_LIVE capacity=64     offered=50001 received=50001 accepted=49412 dispatcher_dropped=589   delivered=49412 arrive_datagrams_per_s=586588 consumer_datagrams_per_s=360928
CHANNEL_CAPACITY_LIVE capacity=1024   offered=50001 received=50001 accepted=50001 dispatcher_dropped=0     delivered=50001 arrive_datagrams_per_s=590647 consumer_datagrams_per_s=367949
CHANNEL_CAPACITY_LIVE_SWEEP smallest_overloading_capacity=1 drops_at_smallest=34695 product_capacity=1024 product_dropped=0 product_consumer_datagrams_per_s=367949 product_arrive_datagrams_per_s=590647
```

A live consumer is not a reprieve: a one-slot channel still loses 69 % of a
saturating offer, because the consumer's scheduling latency exceeds the one-slot
horizon. Capacity buys **stall coverage** of `capacity / arrival_rate` seconds,
so the knee for a live flow is the same formula as for a burst — `capacity >
arrival_rate × consumer_stall`. At this rig's ~590 000 datagrams/s, 64 slots
cover 108 µs (589 datagrams lost to micro-stalls) and 1024 slots cover 1.7 ms
with zero loss. `rtp`'s own 1024 already covers the local micro-stalls measured
here, and a *wire* RTT spike (the field's 190 ms–3.2 s) does not stall the local
drain at all — it delays ACKs, not the consumer.

**What the product should do.** Neither arm shows a rate mismatch, so no capacity
is *unboundedly* required: the product should size the channel by the **local
consumer stall**, not by the wire RTT, and the value it has (1024, chosen by
`rtp`) measures loss-free under a live consumer at this host's arrival rate. The
recommendation is therefore **not** to raise `DISPATCHER_BUF_SIZE`: at 64 KiB per
slot, covering a one-second consumer stall at 10 000 datagrams/s would cost
640 MB per flow and a field-spike-sized 3.2 s would cost 2 GiB, while the
current 1024 already carries zero dispatcher loss. If a deeper channel is ever
needed, the lever to fix first is this crate's own per-slot buffer — the pool
allocates `PACKET_BUFFER_LENGTH` for every datagram whatever its size, so a slot
costs 64 KiB rather than the 256 B–1200 B it carries — which would make the same
coverage affordable by two orders of magnitude. That was recorded as a
recommendation rather than a change while the capacity had no failing cell to
repair and changing its *value* belongs behind a measurement of `rtp`'s own
consumer cadence; the per-slot buffer itself is now a per-listener knob (landed
below) and leaves the capacity untouched.

Three mutations were each shown to turn a check red, and each was restored with
`touch` and its file's `shasum` re-checked before the next. Replacing the
channel's capacity argument with `1` at `src/lib.rs:479` (one of two
`self.dispatcher_buffer_size.get()` occurrences, so the count 2 → 1 was printed
between the edit and the verdict) reddens the sweep naming `capacity 64: the
channel accepted 1 of 4097 received datagrams, not min(received, 64)`. Allocating
the pooled buffer at 1024 B instead of `PACKET_BUFFER_LENGTH` (the pool's one
such occurrence) reddens the cost reading naming `capacity 64: 64 delivered
datagrams retained only 328464 bytes, under one PACKET_BUFFER_LENGTH (65536)
each`. Sleeping 200 µs per drained datagram in the live arm's own consumer
reddens its zero-drop assertion naming `the 1024-slot channel dropped 48840 of
50001 datagrams with a live consumer draining` while printing
`arrive_datagrams_per_s=371816 consumer_datagrams_per_s=676` — the rate-mismatch
signature, where the drop at capacity 64 (49 818) and at 1024 (48 840) barely
differ, which is what "no capacity removes it" looks like.

### The per-datagram receive buffer: sized to the configured bound, and the oversize refusal

The capacity arm above left one cost open: a channel slot retains a pooled
`PACKET_BUFFER_LENGTH` buffer (65 536 B) whatever the datagram's size, so a
256-byte datagram occupies 64 KiB while it waits and a full 1024-slot flow
retains 67.3 MB. That buffer is this crate's, so it is sized here rather than in
`rtp`.

What bounds the wire datagram is `rtp`'s own MSS, not UDP's ceiling: a data
datagram is at most `Mss::max_datagram_size()` = `MAX_MSS - 1` bytes
(`rtp/src/mss.rs`), and `MAX_MSS` is `64 * 1024`. A caller that configures a
large MSS therefore needs a 64 KiB buffer, and the **default stays
`PACKET_BUFFER_LENGTH`**: shrinking it below a datagram the kernel may deliver
would convert that datagram into a drop, and the default is the one length that
refuses none. The deployed product is bounded well below it — the default
`NO_FEC_MSS` is 1424 (`rtp/src/udp.rs:70`) — so a caller can set the slot to its
bound. `rtp` itself still calls `new` (the 64 KiB default) and must be re-pinned
to pass its MSS before the deployed footprint changes; that is a one-line change
in `rtp`, a different crate and pin, and is not made here.

`UtpListener::new_with_packet_buffer` (and
`new_identity_dispatch_with_packet_buffer`) take a `PacketBufferLength`; `new`
and `new_identity_dispatch` keep the 64 KiB default, so existing callers are
unchanged. The pool allocates each buffer at the configured length — the pool
moved in-crate because `primitive`'s `ArcObjPool` takes `fn`-pointer
allocator/reset and so cannot carry a per-listener capacity — and the read's
overflow guard compares `n` against the configured length, not the default. A
datagram that fills (and may therefore have been truncated at) the configured
length is dropped and counted in `packets_dropped_pkt_buf_overflow`: a counted
refusal, never a silent shortening.

`packet_buffer_length::a_slot_sized_to_the_bound_delivers_within_it_and_refuses_over_it`
(default tier, asserting) binds a listener with a 2048-byte bound, sends a
256-byte datagram (delivered whole, in a slot whose `capacity()` is exactly
2048), then a 4096-byte datagram: the kernel copies at most 2048 bytes and
discards the rest, the guard counts one `packets_dropped_pkt_buf_overflow`, and
nothing is delivered — the flow then carries a second 256-byte datagram intact.
`packet_buffer_length_footprint::a_slot_sized_to_the_bound_retains_the_bound_not_the_default`
(default tier, asserting) reads the retained bytes per occupied slot with a
counting allocator, one bound at a time:

```
PACKET_BUFFER_SLOT bound=2048 bytes_per_slot=2146 per_flow_at_1024_slots=2.1 MiB; default_bytes_per_slot=67620 default_per_flow_at_1024_slots=66.0 MiB
```

The bound slot retains 2146 B (2048 plus the handle's overhead) against the
default's 67 620 B — the same range as the sibling arm's 65 585–67 624 B — so a
1024-slot flow retains 2.1 MiB instead of 66.0 MiB when the bound is configured
(32x) and the listener's 1024-flow
ceiling from 64 GiB to ~2 GiB, for a bound that still carries the deployed MSS
with room for the codec, FEC and nonce overheads. The worst case is unchanged:
a caller that needs a 64 KiB datagram passes the 64 KiB default and pays what it
paid before.

Two mutations were each shown to turn a check red, and each was restored with
`touch` and its `src/lib.rs` shasum re-checked (`d19a5d1f…`). Making the overflow
guard compare against `PACKET_BUFFER_LENGTH` instead of
`self.packet_buffer_length.get()` (the realistic "sized the buffer down, left the
guard" bug; the count of `self.packet_buffer_length.get()` reads 1 → 0 between
the edit and the verdict) reddens the functional arm naming `an over-bound
datagram must be counted as a buffer-overflow refusal` with `left: 0, right: 1`
— the refusal was not counted, and the truncated 2048-byte datagram was
dispatched. Passing `PACKET_BUFFER_LENGTH` to `PacketPool::new` instead of the
configured length reddens the footprint arm naming `the 2048-byte slot retained
67621 bytes per occupied slot, over twice the bound`.

### The kernel's own refusal count, and what a sample of it costs

This arm separates the two drop sites only because the arm *is* the sender and
so knows `offered`. In production the listener knows neither `offered` nor the
kernel's refusal count, so a kernel refusal and a datagram the path never
delivered were one number — which is the operator's field problem, where an ISP
penalises a saturated UDP destination for loss that is not the path's. That gap
is closed on Linux by `UtpListener::kernel_refused`, which reads the socket's own
`sk_drops` from the `drops` column of `/proc/net/udp` and `/proc/net/udp6`
(`udp4_format_sock`, `net/ipv4/udp.c:3246`) — the same counter the `SO_RXQ_OVFL`
control message carries. The socket is named by its descriptor
(`UnreliableTransmit::raw_fd`, resolved through `/proc/self/fd` and matched on
the table's `inode` column), which is exact where a local-address match is
ambiguous under `SO_REUSEPORT`.

`kernel_refusal::a_kernel_refusal_is_separable_and_the_receive_side_reconciles`
(opt-in, `standard` tier, asserting, measured 0.5 s) offers the sibling arm's
stalled burst on two receive buffers and reads both self-inflicted sites plus the
kernel's own count. On a remote x86_64 Linux 6.8 host, release musl, load 0.13:

```
KERNEL_REFUSAL platform=linux label=floor-4KiB effective=8192 offered=32769 received=11 dispatched=11 self_refused=0 delivered=11 kernel_refused=32758 sender_derived=32758 source=ProcNetUdp4
KERNEL_REFUSAL platform=linux label=host-default effective=212992 offered=32769 received=239 dispatched=239 self_refused=0 delivered=239 kernel_refused=32530 sender_derived=32530 source=ProcNetUdp4
KERNEL_REFUSAL platform=linux fresh_socket per_socket refused=0 source=ProcNetUdp4
```

`kernel_refused` equals the sender's own `offered - received` exactly, row for
row, and a fresh socket reads zero — so the number is this socket's, counted by
the kernel, and not a quantity the sender recomputed. That host's
`net.core.rmem_max` is 212 992 B, so its receive buffer holds fewer datagrams
than the 1 024-slot channel and `self_refused` stays 0; the two sites carrying a
non-zero count *at once* is shown on macOS instead, where the arm still reads the
dispatcher drop (1 709) beside the kernel refusal (30 036).

macOS keeps no per-socket counter, so the reading there is
`KernelRefused::NotPerSocket` with the reason, and the arm measures what macOS
does keep — one host-wide `dropped due to full socket buffers` total — against a
quiet baseline. Release, load 5.2–9.2 on ten cores:

```
KERNEL_REFUSAL platform=macos quiet_window_ms=200 host_wide_drift=0
KERNEL_REFUSAL platform=macos label=floor-4KiB effective=4096 offered=32769 received=15 dispatched=15 self_refused=0 delivered=15 sender_derived_kernel_dropped=32754 host_wide_delta=32754
KERNEL_REFUSAL platform=macos label=host-default effective=786896 offered=32769 received=2733 dispatched=1024 self_refused=1709 delivered=1024 sender_derived_kernel_dropped=30036 host_wide_delta=30036
```

The host-wide delta equals the per-socket refusals exactly on this quiet host,
and a zero-drift baseline shows it was the burst that moved it. It is an upper
bound in general — every UDP socket on the machine contributes — which is why it
is an instrument inside the arm and not a value the product reports.

Four mutations were each shown to turn a check red, and each was restored with
`touch` before the next: reading the `ref` column as the inode in `parse_row`
(one of two `tail.next()?;` lines removed) reddens three of the four decoder unit
tests, naming `Absent` where `Found(4)` was expected and `Some(Row { inode: 2,
drops: 4 })` where the header must be `None`; a macOS reader returning
`PerSocket { refused: 0, .. }` reddens the platform arm naming `no per-socket
refusal count exists on macos, yet one was reported: 0 from ProcNetUdp4`;
taking the table's first row instead of matching the inode reddens the Linux arm
naming `Unidentified { reason: "two rows … different refusal counts" }`; a
host-wide counter pinned to a constant reddens the macOS arm naming `the
host-wide counter moved by 0 over a burst the sender says was refused 32754
times`.

**Cost.** The reading is polled and nothing on the receive path calls it, so the
per-datagram cost is zero by construction — `kernel_refused` is referenced only
by its own definition, its module and the tests. A sample costs one
`open`+`read`+`close` of the kernel's socket table plus the decode, and both are
linear in the number of UDP sockets the host has. Measured on the same 1-vCPU
Linux host, release musl, padding the table by opening extra UDP sockets:

```
/proc/net/udp rows           32      2032
kernel_refused per call     167-213 us   7.81-7.94 ms
```

The decode alone over a 33-row table is 63.8 µs on that host and 7.0 µs on a
faster one, so roughly half a sample is the decode and half the kernel's own
`/proc` walk and `seq_printf`. Nothing is retained: a `String` for the table, one
for the fd path, and a scratch slice per row. A socket-heavy host therefore pays
milliseconds per sample, which is why the reading is a poll and why that is
recorded as a gap below rather than left implied by the word "polled".

### The tooling gaps this file recorded, and their state

1. **The lib target was outside the manifest — closed.** The manifest set is
   still the scenario directory's `#[ignore]` set (`netem-tools check-gate`)
   resolved through `cargo test --list`, but the reserved
   `lib` target is now derived beside it and is nameable in
   `gate-manifest`, `gate-default-required` and `gate-asserting` as a
   `lib::<module>::<test>` line (`TargetListings`), resolved
   through `cargo test -p <package> --lib` rather than the non-target
   `--test lib`. Probed on this crate: adding
   `lib::accept_queue_soak::a_burst_enqueued_before_any_waiter_is_handed_back_once_and_in_bound`
   to `gate-default-required` resolves it and then reports `ASSERTING scenario
   missing from gate-asserting`, which is the lib target's own test list. The
   two lib-tier wake-bound cells this file claims are therefore nameable; they
   stay named in prose here rather than added to the required set, because a
   lib opt-in no block names is an advisory note and never a failure,
   and no lib test of this crate is `#[ignore]`d.
2. **An env-scaled opt-in tier was invisible to every gate — closed by
   `gate-env-tier`**, the block at the end of this file. It names the
   variables, the runner, the measured quantity and the cells, parses as
   `<name> = <vars> | <runner> | <measures> | <cells>`
   (`netem-tools check-gate`), and detects the surface it declares from a
   script name *and* a Rust read, so it cannot go stale in
   silence. Its one limit is a variable with **no script runner**, which is why
   the four `SOAK_WAKE_*` names share the churn row and are separated by that
   row's `measures` field.

Everything else in the crate is the `--lib` target's correctness work: the 17
`tests` module cells, the 8 `accept_queue_soak` cells and the 3 `teardown_soak`
cells, plus 3 `conn` and 2 `transmit` unit tests. They are outside this
manifest and are not cost-declared here; the two wake-bound cells named above
are the only lib-tier cells this record claims.

## Blocks the checker reads

The `#[ignore]`d scenarios, and the tier each belongs to:

```gate-manifest
recv_buffer_drop_sites::sizing_the_receive_buffer_moves_a_drop_between_sites_and_the_knee_is_the_channel = standard
kernel_refusal::a_kernel_refusal_is_separable_and_the_receive_side_reconciles = standard
channel_capacity_sweep::the_channel_capacity_sets_the_burst_a_stalled_consumer_accumulates = standard
channel_capacity_sweep::a_live_consumer_capacity_sweep_shows_the_stall_coverage = standard
```

The always-run cells the gate exists for are pinned as required-default, so a
test silently re-ignored leaves the crate's liveness property unasserted:

```gate-default-required
accept_churn_soak::churn_over_the_combined_accept_path_loses_no_dial
accept_churn_soak::churn_over_split_accept_tasks_loses_no_dial
accept_churn_soak::mixed_dispatcher_and_combined_acceptors_lose_no_dial
accept_churn_soak::bursts_against_a_slow_acceptor_lose_no_dial
accept_churn_soak::accept_under_frequent_cancellation_loses_no_dial
accept_churn_soak::accept_queue_at_its_bound_accounts_for_every_flow
accept_churn_soak::a_failed_dialer_does_not_strand_the_other_round_participants
dispatch_delay::the_dispatch_path_adds_no_floor_to_a_lone_datagram
dispatch_delay::the_dispatch_rate_sweep_separates_a_toll_from_a_queue
dispatcher_overflow::a_dispatcher_overflow_is_attributed_to_the_flow_that_dropped
kernel_refusal::the_per_socket_answer_is_the_platforms_and_is_never_a_substitute
packet_buffer_length::a_slot_sized_to_the_bound_delivers_within_it_and_refuses_over_it
packet_buffer_length_footprint::a_slot_sized_to_the_bound_retains_the_bound_not_the_default
```

Every one of them asserts, so the asserting set is the required set plus the
four opt-in scenarios:

```gate-asserting
accept_churn_soak::churn_over_the_combined_accept_path_loses_no_dial
accept_churn_soak::churn_over_split_accept_tasks_loses_no_dial
accept_churn_soak::mixed_dispatcher_and_combined_acceptors_lose_no_dial
accept_churn_soak::bursts_against_a_slow_acceptor_lose_no_dial
accept_churn_soak::accept_under_frequent_cancellation_loses_no_dial
accept_churn_soak::accept_queue_at_its_bound_accounts_for_every_flow
accept_churn_soak::a_failed_dialer_does_not_strand_the_other_round_participants
dispatch_delay::the_dispatch_path_adds_no_floor_to_a_lone_datagram
dispatch_delay::the_dispatch_rate_sweep_separates_a_toll_from_a_queue
dispatcher_overflow::a_dispatcher_overflow_is_attributed_to_the_flow_that_dropped
recv_buffer_drop_sites::sizing_the_receive_buffer_moves_a_drop_between_sites_and_the_knee_is_the_channel
kernel_refusal::the_per_socket_answer_is_the_platforms_and_is_never_a_substitute
kernel_refusal::a_kernel_refusal_is_separable_and_the_receive_side_reconciles
channel_capacity_sweep::the_channel_capacity_sets_the_burst_a_stalled_consumer_accumulates
channel_capacity_sweep::a_live_consumer_capacity_sweep_shows_the_stall_coverage
packet_buffer_length::a_slot_sized_to_the_bound_delivers_within_it_and_refuses_over_it
packet_buffer_length_footprint::a_slot_sized_to_the_bound_retains_the_bound_not_the_default
```

No `perf`-tier scenario exists, so no report-only body can reach an asserting
helper and this block is empty:

```gate-perf-guard-helpers
```

The delay measurement against the bare-socket echo is the reference the
dispatch sweep is read against, and the sweep's cells vary the depth and the
achieved rate:

```gate-perf-design
dispatch_delay::the_dispatch_path_adds_no_floor_to_a_lone_datagram = default | 0.03 | baseline | dispatch-floor@path=dispatch+shape=ping-pong+reference=bare-socket
dispatch_delay::the_dispatch_rate_sweep_separates_a_toll_from_a_queue = default | 0.03 | composite(depth,rate) | dispatch-sweep@depth=one-to-sixty-four+rate=achieved
dispatcher_overflow::a_dispatcher_overflow_is_attributed_to_the_flow_that_dropped = default | 0.01 | composite(buffer,reference,shape) | dispatcher-overflow@path=dispatch+shape=burst+reference=parked-reader+buffer=four-slots
recv_buffer_drop_sites::sizing_the_receive_buffer_moves_a_drop_between_sites_and_the_knee_is_the_channel = standard | 1.9 | composite(size,name,load,split) | recv-buffer-drops@size=256-and-1200+name=so_rcvbuf+load=stalled-burst+split=kernel-vs-dispatcher
kernel_refusal::a_kernel_refusal_is_separable_and_the_receive_side_reconciles = standard | 0.5 | composite(source,buffer,split) | kernel-refusal@source=proc-net-udp+buffer=floor-4KiB-and-host-default+split=kernel-vs-dispatcher
channel_capacity_sweep::the_channel_capacity_sets_the_burst_a_stalled_consumer_accumulates = standard | 0.5 | composite(capacity,payload,load) | channel-capacity@capacity=64-to-8192+payload=256+load=stalled-burst
channel_capacity_sweep::a_live_consumer_capacity_sweep_shows_the_stall_coverage = standard | 0.4 | composite(capacity,rate) | channel-capacity-live@capacity=1-and-64-and-1024+rate=achieved
```

```gate-budgets
default = 1
standard = 5
full = 60
perf = 60
baseline = dispatch_delay::the_dispatch_path_adds_no_floor_to_a_lone_datagram
drift = 0.5
drift_floor_s = 2.0
```

```gate-coverage-gaps
impairment@crate=udp_listener = no impairment instrument is reachable: the crate sits below `rtp` in the dependency graph and `Cargo.toml` has no `netem-test` dependency, so the `impairment` dimension is empty by construction
dispatch-floor@host=linux = the floor is measured on the host the suite runs on; the deployed target is linux-musl, and no linux measurement of this path exists here
dispatch-floor@metric=syscall-count = the syscall and copy counts are properties of the composed paths and are measured in `tokio_udp`; this crate's arm differences them by cost rather than counting them
dispatch-sweep@lane=multiplexed = the sweep drives one flow; several flows sharing the dispatcher is `rtp`'s composition, not a cell this crate can attribute.
dispatcher-overflow@layer=repair = the repaired round trip a dropped datagram causes is `rtp`'s to measure; this crate has no `rtp` dependency, so the arm reads the drop and its delivery consequence only.
recv-buffer-drops@host=linux = the buffer depths are this host's (macOS); the deployed Linux default is carried as an explicit 212 992 B request, but Linux's `skb->truesize` accounting is not reproduced, so every Linux depth here is an upper bound.
recv-buffer-drops@transport=rtp = `rtp` was off limits; the channel is sized to rtp's `DISPATCHER_BUF_SIZE` but the composing transport's own drain, repair ladder and congestion response are not exercised, and the per-flow drop's repair cost is not measured here.
recv-buffer-drops@shape=live-dispatch-loop = the burst is offered while the dispatch task is not polled; with the loop live the kernel queue never accumulates, so this arm says nothing about a live-reader regime — `tokio_udp`'s `rcvbuf_cliff` measures that shape.
recv-buffer-drops@metric=kernel-refusal-counter = **closed for Linux, and only there.** The kernel's refused-datagram count is no longer derived by the sender: `UtpListener::kernel_refused` reads it from `/proc/net/udp{,6}`'s `drops` column (`sk_drops`), so `peer_offered = packets_received + kernel_refused` is computable and the residual is the path loss. The platform and mechanism limits that remain are the three gaps below.
kernel-refusal@host=linux-musl = the per-socket reading was type-checked for `x86_64-unknown-linux-musl` on this host and run end-to-end as a static release musl build on a remote x86_64 Linux 6.8 host, where `kernel_refused` equalled the sender's own `offered - received` exactly; it was not run on the deployed hosts' kernels. That host's `net.core.rmem_max` is 212 992 B, so its receive buffer holds fewer datagrams than the 1 024-slot channel and the above-knee regime of the sibling arm is not reachable there.
kernel-refusal@host=macos = this host has no per-socket counter; the arm measures the host-wide `dropped due to full socket buffers` total instead, which every UDP socket on the machine contributes to, so on macOS the reconciliation it runs is that host-wide delta and not a reading the product reports.
kernel-refusal@source=so-rxq-ovfl = the `SO_RXQ_OVFL` control message carries the same `sk_drops` count and is not implemented: it costs a control-message parse per received datagram and an enable step the transport's `recv_buf` path has no place for, where the polled `/proc` read costs one file read per sample. The choice is source-level, not measured: the cmsg path's per-datagram cost was not measured because it was not built.
kernel-refusal@scale=socket-count = a sample costs one `open`+`read`+`close` of `/proc/net/udp` and the decode, both linear in the host's UDP socket count (measured: 167-213 µs at 32 rows, 7.8-7.9 ms at 2 032 rows on a 1-vCPU host). A host with thousands of UDP sockets therefore pays milliseconds per sample, and no cheaper source of the same per-socket count exists in this crate.
channel-capacity@transport=rtp = the sweep sizes the channel through `udp_listener`'s own constructor seam and reads its own counters; `rtp`'s composing consumer — its poll cadence over `UnreliableRead::try_recv` — is not reachable from this crate, so the drain rate measured here is a tight-`try_recv` upper bound and not the product's own rate. Sizing the channel for the field therefore needs `rtp`'s consumer cadence, which is not a cell this crate can fill; the value `rtp` passes (1024) is left untouched.
channel-capacity@host=linux-musl = the knee, the arrival rate and the drain rate are this host's (macOS). `PACKET_BUFFER_LENGTH` is platform-independent, so the 64 KiB per slot carries to the deployed target, but the rates do not.
channel-capacity@shape=dispatch-stall = both arms run the dispatch loop live from the first datagram, so `received` is the wire's delivery and the channel is the only buffer measured; the unpolled-dispatch shape, where the kernel queue itself fills, is the sibling arm's and `tokio_udp`'s, not a cell here.
```

The crate is scaled without rebuilding by `SOAK_*` variables rather than by
`#[ignore]`, so its opt-in surface is declared as one `gate-env-tier` row — the
per-dial liveness rate the runner bounds, and the handover wake bound the
lib-tier variables size:

```gate-env-tier
soak-accept-churn = SOAK_DIALERS,SOAK_ITERATIONS,SOAK_SEED,SOAK_ACCEPTORS,SOAK_DIAL_TIMEOUT_MS,SOAK_CANCEL_US,SOAK_ACCEPT_PACE_MS,SOAK_WAKE_ITEMS,SOAK_WAKE_WAITERS,SOAK_WAKE_COMBINED,SOAK_WAKE_BOUND_MS | local/soak_accept_churn.py | the per-dial liveness rate over the six accept-churn modes, reported as N dials plus a rule-of-three 95% upper bound 3/N on the per-dial loss rate rather than as a bare pass, and the lib-tier handover wake bound that the four SOAK_WAKE_* variables size, whose overrun prints SOAK_WAKE_LATE and is informational while a drain that never completes is fatal | liveness@shape=dial-churn+topology=churn, liveness@shape=dial-churn+topology=multi-accept, liveness@shape=dial-churn+topology=mixed, liveness@shape=dial-churn+topology=burst, liveness@shape=dial-churn+topology=cancel, liveness@shape=dial-churn+topology=capacity, wake-bound@shape=handover+scale=items-before-waiter, wake-bound@shape=teardown+scale=parked-waiters
```

Run it from this crate's root:

```sh
netem-tools check-gate --crate . udp_listener tests GATE.md
```
