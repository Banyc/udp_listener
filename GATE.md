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

## The surface: there is no opt-in tier

Measured on the tree this file is committed with, `cargo test --release`:

* `-- --list --ignored` reports **0 ignored tests** in both targets (33 lib
  tests, 9 integration tests). Nothing in this crate is `#[ignore]`d.
* There is **no bench target**: no `benches/` directory, no `[[bench]]` in
  `Cargo.toml`, no `criterion` in `Cargo.lock`.
* The whole default tier costs **0.91 s** wall clock (`lib` 0.06 s,
  `accept_churn_soak` 0.71 s, `dispatch_delay` 0.06 s). The churn target's cost
  is one cell — `bursts_against_a_slow_acceptor_lose_no_dial` at 0.71 s — and
  the lib tier is at the process-start floor.

So `gate-manifest` below is empty because the crate's ignored set is empty, not
because the manifest is unwritten: the set the checker re-derives from the
compiled binaries *is* the empty set. The dual mandate's *time* half has
nothing to shorten here — the always-run tier is under a second — and its
*coverage* half declares the delay measurement under "The dispatch path's
per-datagram delay" below.

## The always-run liveness cells

These seven are the crate's gate and are required-default: each is worth zero
if it stops running. One *dial* is one datagram whose 8-byte token becomes the
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
`check-gate.py:3412`, over the scenario targets at `:3544`) resolved through
`cargo test --list` (`:3379`). `gate-env-tier` is the block for such a
surface, and `check-gate.py:2769-2875` is what enforces it: detection needs a
crate script *and* a Rust source to name the variable (`:2796-2799`, closed
transitively over the crate's own calls at `:3001`), a detected name the
declaration omits is an error (`:2822-2830`), and a declared name must be read
by a source (`:2842-2848`) and named by the surface's runner (`:2849-2856`).

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
(`check-gate.py:2833-2838`, `:2849-2856`) and the four `SOAK_WAKE_*` names have
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

### The tooling gaps this file recorded, and their state

1. **The lib target was outside the manifest — closed.** The manifest set is
   still the scenario directory's `#[ignore]` set (`check-gate.py:3412`,
   `:3544`) resolved through `cargo test --list` (`:3379`), but the reserved
   `lib` target is now derived beside it (`:3560`) and is nameable in
   `gate-manifest`, `gate-default-required` and `gate-asserting` as a
   `lib::<module>::<test>` line (`TargetListings`, `:3138-3160`), resolved
   through `cargo test -p <package> --lib` rather than the non-target
   `--test lib`. Probed on this crate: adding
   `lib::accept_queue_soak::a_burst_enqueued_before_any_waiter_is_handed_back_once_and_in_bound`
   to `gate-default-required` resolves it and then reports `ASSERTING scenario
   missing from gate-asserting`, which is the lib target's own test list. The
   two lib-tier wake-bound cells this file claims are therefore nameable; they
   stay named in prose here rather than added to the required set, because a
   lib opt-in no block names is an advisory note and never a failure
   (`:3575-3593`), and no lib test of this crate is `#[ignore]`d.
2. **An env-scaled opt-in tier was invisible to every gate — closed by
   `gate-env-tier`**, the block at the end of this file. It names the
   variables, the runner, the measured quantity and the cells, parses as
   `<name> = <vars> | <runner> | <measures> | <cells>`
   (`check-gate.py:2880-2962`), and detects the surface it declares from a
   script name *and* a Rust read (`:2796-2799`), so it cannot go stale in
   silence. Its one limit is a variable with **no script runner**, which is why
   the four `SOAK_WAKE_*` names share the churn row and are separated by that
   row's `measures` field.

Everything else in the crate is the `--lib` target's correctness work: the 17
`tests` module cells, the 8 `accept_queue_soak` cells and the 3 `teardown_soak`
cells, plus 3 `conn` and 2 `transmit` unit tests. They are outside this
manifest and are not cost-declared here; the two wake-bound cells named above
are the only lib-tier cells this record claims.

## Blocks the checker reads

Nothing is `#[ignore]`d, so the manifest is empty:

```gate-manifest
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
```

Every one of them asserts, so the asserting set equals the required set (there
is no `standard`/`full` scenario, because there is no `#[ignore]`d test):

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
```

```gate-budgets
default = 1
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
python3 ../netem_test/tools/check-gate.py --crate . udp_listener tests GATE.md
```
