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
  tests, 7 integration tests). Nothing in this crate is `#[ignore]`d.
* There is **no bench target**: no `benches/` directory, no `[[bench]]` in
  `Cargo.toml`, no `criterion` in `Cargo.lock`.
* The whole default tier costs **0.85 s** wall clock (`lib` 0.06 s,
  `accept_churn_soak` 0.71 s). The integration total is one cell —
  `bursts_against_a_slow_acceptor_lose_no_dial` at 0.71 s — and the lib tier is
  at the process-start floor.

So `gate-manifest` below is empty because the crate's ignored set is empty, not
because the manifest is unwritten: the set the checker re-derives from the
compiled binaries *is* the empty set. The dual mandate's *time* half has
nothing to shorten here — the always-run tier is under a second — and its
*coverage* half has no perf arm to declare, for the reason and with the repair
recorded under "The perf declaration is a negative" below.

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

## The opt-in surface the grammar cannot hold

The crate *is* scalable without rebuilding, but by environment variable rather
than by `#[ignore]`, so the shared checker's manifest cannot see it: it is
derived from the `#[ignore]` set alone (`check-gate.py:2896`, `:2809`,
`:2821`).

* `SOAK_DIALERS`, `SOAK_ITERATIONS`, `SOAK_SEED`, `SOAK_ACCEPTORS`,
  `SOAK_DIAL_TIMEOUT_MS` (`tests/accept_churn_soak.rs:105-109`),
  `SOAK_CANCEL_US` (`:115`), `SOAK_ACCEPT_PACE_MS` (`:120`);
  `SOAK_WAKE_ITEMS`, `SOAK_WAKE_WAITERS`, `SOAK_WAKE_COMBINED`,
  `SOAK_WAKE_BOUND_MS` (`src/accept_queue_soak.rs:771-774`,
  `src/teardown_soak.rs:530`).
* The runner of record is `local/soak_accept_churn.py`: it runs each mode as
  its own process group with a per-batch timeout, kills a hung batch by group
  plus a path-matched sweep, and turns the batches into a **detection limit**
  rather than a bare pass — zero failures in N dials excludes a per-dial loss
  rate above ~3/N at 95 % (`local/soak_accept_churn.py:343`, `:338`).

Measured cost, default driver sizing (32 dialers, 100 iterations, one batch
per mode, **debug** build as the driver builds it): 16 320 dials in 1.97 s wall
clock, detection limit 1.8e-4 per dial, zero failures, zero hangs, zero
strays. The quantity the sweep claims is therefore a **per-dial liveness rate**,
under a load shape (`dialers × iterations`), an accept topology (the six modes
of `local/soak_accept_churn.py:48`), a cancellation span and a pacing — not a
latency or a goodput.

## The perf declaration is a negative

There is **no `gate-perf-design` row here**, and that is a finding rather than
a placeholder:

* No test in this crate asserts a performance bound. The soaks assert
  *liveness* (identity sets and counter equalities) and the one wall-clock
  bound they carry is a 5 s handover bound whose overrun is deliberately
  informational. A `default`-tier row would declare a perf claim the test does
  not make.
* No impairment instrument is reachable. `Cargo.toml` has no `netem-test`
  dependency and the crate sits *below* `rtp` in the dependency graph, so the
  `impairment` dimension of the coverage space is empty here by construction,
  not by omission. The one loopback-shaped dimension that does exist is
  `scale`, and the sweep above buys it with an external driver.
* The grammar cannot express a zero-row declaration. A `gate-budgets` block
  without `baseline = <row>` is an error (`check-gate.py:1968`), a baseline
  that is not a `gate-perf-design` row is an error (`:2732`), and a baseline no
  row states a relation against is an error (`:2269`) — so the minimum
  expressible declaration is *two* rows (a reference plus one relation). An
  empty `gate-perf-design` plus an empty `gate-budgets` cannot be written, and
  a `gate-coverage-gaps` block is refused without them
  (`check-gate.py:2670-2675`). The negative is therefore stated in prose, and
  the two repairs are: **(a)** add an `#[ignore]`d opt-in scenario in `tests/`
  that runs a sized sweep in-process and asserts it (the driver's shapes are
  the natural source, and a second shape one dimension away gives the two-row
  family the grammar wants), or **(b)** extend the shared grammar with a
  zero-row / gap-only declaration form, which is tooling this crate does not
  own.

### Two tooling gaps, precisely

1. **The lib target is outside the manifest.** The checked manifest set is the
   `#[ignore]` set of the `*.rs` files in the scenario directory
   (`check-gate.py:2896`) resolved through `cargo test --test <target>`
   (`:2809`, `:2821`); the reserved `lib` target is honoured only when resolving a
   `gate-perf-design` row (`:2617-2627`). A lib unit test — which is where this
   crate's wake-bound cells live, in `src/accept_queue_soak.rs` and
   `src/teardown_soak.rs`, gated `#[cfg(test)]` at `src/lib.rs:21-24` — can
   therefore appear in no block: in `gate-manifest` it is a STALE entry, and in
   `gate-default-required` it resolves through `cargo test --test lib`, which
   is not a target, so the checker exits on a cargo failure. Repair: accept the
   reserved `lib` target in `gate-manifest` and `gate-default-required`, or
   enumerate the package's lib target alongside the scenario directory.
2. **An env-scaled opt-in tier is invisible to every gate.** `SOAK_*` scaling
   is a real opt-in surface with a real detection limit and a real runner, and
   no shared block can name it, because every block keys on `#[ignore]`. Repair:
   a block that names a non-`test` opt-in runner (its command, its
   cost and its cells), or a convention that an opt-in tier must be `#[ignore]`d
   so the existing blocks keep working.

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
```

No `perf`-tier scenario exists, so no report-only body can reach an asserting
helper and this block is empty:

```gate-perf-guard-helpers
```

Run it from this crate's root:

```sh
python3 ../netem_test/tools/check-gate.py --crate . udp_listener tests GATE.md
```
