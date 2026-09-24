#!/usr/bin/env python3
"""Accept-path liveness soak driver.

Runs the `accept_churn_soak` integration tests as batches, each in its own
process group, and turns their per-batch `SOAK_RESULT` lines into a sweep
result with a detection limit rather than a bare pass/fail.

Why a driver and not just `cargo test`:

- A batch that hangs must be reported as HUNG with its seed and how far it
  got, never scored as a pass and never scored as a genuine catch. It is
  killed by process group (`start_new_session=True` + `killpg`; `setsid` does
  not exist on macOS) and then swept with a path-matched backstop, because a
  leaked test binary holds a UDP socket and corrupts every later batch.
- A sweep of thousands of dials needs varied shapes and seeds, which is a
  loop over environment variables, not a test argument.
- The null result has to carry a bound: zero failures in N dials excludes a
  rate above ~3/N at 95%, so N and the load are part of the output.

Every batch's stdout is kept under the log directory given by --log-dir.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time

CRATE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# test name -> (label, extra per-mode sizing)
def modes_iterations(args):
    """Per-mode iteration count.

    The capacity phase must dial past the accept-queue bound (256) or it tests
    nothing, and must keep the blast small enough that the kernel socket
    buffer can hold every datagram: a kernel-side drop would look like a
    listener defect. The in-crate test refuses to pass if no dial is refused.
    """
    return {
        "accept_queue_at_its_bound_accounts_for_every_flow": (300 + args.dialers - 1)
        // args.dialers
    }


MODES = {
    "churn_over_the_combined_accept_path_loses_no_dial": ("churn", {}),
    "churn_over_split_accept_tasks_loses_no_dial": ("multi_accept", {}),
    "mixed_dispatcher_and_combined_acceptors_lose_no_dial": ("mixed", {}),
    "bursts_against_a_slow_acceptor_lose_no_dial": ("burst", {}),
    "accept_under_frequent_cancellation_loses_no_dial": ("cancel", {}),
    "accept_queue_at_its_bound_accounts_for_every_flow": ("capacity", {}),
}


def build_test_binary(timeout):
    """`cargo test --no-run --message-format=json` -> the test executable."""
    proc = subprocess.run(
        [
            "cargo",
            "test",
            "--test",
            "accept_churn_soak",
            "--no-run",
            "--message-format=json",
        ],
        cwd=CRATE,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout)
        sys.stderr.write(proc.stderr)
        raise SystemExit(f"cargo test --no-run failed with {proc.returncode}")
    executable = None
    for line in proc.stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("reason") != "compiler-artifact":
            continue
        target = message.get("target", {})
        if target.get("name") == "accept_churn_soak" and message.get("executable"):
            executable = message["executable"]
    if not executable:
        raise SystemExit("cargo did not report an accept_churn_soak executable")
    if not os.path.exists(executable):
        raise SystemExit(f"the reported test executable does not exist: {executable}")
    return executable


def processes_matching(path):
    """(pid, pgid) of every live process whose command line names `path`."""
    proc = subprocess.run(
        ["ps", "-Ao", "pid=,pgid=,command="],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if proc.returncode != 0:
        raise SystemExit(f"ps failed with {proc.returncode}: {proc.stderr}")
    found = []
    for line in proc.stdout.splitlines():
        fields = line.split(None, 2)
        if len(fields) != 3:
            continue
        pid, pgid, command = fields
        if path in command and int(pid) != os.getpid():
            found.append((int(pid), int(pgid)))
    return found


def kill_group(pgid, sig=signal.SIGKILL):
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        pass
    except PermissionError:
        pass


def sweep(binary, pgid):
    """Kill the batch's process group, then anything else still running the
    binary. Returns the strays that had to be killed."""
    kill_group(pgid)
    strays = processes_matching(binary)
    for pid, stray_pgid in strays:
        kill_group(stray_pgid)
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 10
    while processes_matching(binary) and time.monotonic() < deadline:
        time.sleep(0.05)
    return strays


def run_batch(binary, test, env, timeout, log_dir, name):
    command = [binary, "--exact", test, "--nocapture", "--test-threads", "1"]
    started = time.monotonic()
    proc = subprocess.Popen(
        command,
        cwd=CRATE,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    hung = False
    try:
        out, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        hung = True
        kill_group(proc.pid)
        out, _ = proc.communicate()
    elapsed = time.monotonic() - started
    strays = sweep(binary, proc.pid)
    with open(os.path.join(log_dir, f"{name}.log"), "w") as handle:
        handle.write(out)
    return {
        "test": test,
        "command": command,
        "returncode": proc.returncode,
        "hung": hung,
        "elapsed_s": elapsed,
        "stdout": out,
        "strays": strays,
    }


def parse_batch(result):
    """Pull the structured lines out of one batch's stdout.

    libtest writes the running test's name without a trailing newline, so a
    marker can share a line with that prefix: search for the marker rather
    than anchoring at the start of the line.
    """
    def after(line, marker):
        at = line.find(marker)
        return None if at < 0 else line[at + len(marker) :]

    record = None
    violations = []
    progress = None
    for line in result["stdout"].splitlines():
        tail = after(line, "SOAK_RESULT ")
        if tail is not None:
            record = json.loads(tail)
            continue
        tail = after(line, "SOAK_VIOLATION ")
        if tail is not None:
            violations.append(tail)
            continue
        tail = after(line, "SOAK_PROGRESS ")
        if tail is not None:
            progress = json.loads(tail)
    return record, violations, progress


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=1, help="batches per mode")
    parser.add_argument("--seed0", type=int, default=1)
    parser.add_argument("--dialers", type=int, default=32)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--acceptors", type=int, default=8)
    parser.add_argument("--cancel-us", type=int, default=2000)
    parser.add_argument("--pace-ms", type=int, default=2)
    parser.add_argument("--dial-timeout-ms", type=int, default=30000)
    parser.add_argument("--timeout", type=float, default=900.0, help="per batch")
    parser.add_argument("--build-timeout", type=float, default=1800.0)
    parser.add_argument(
        "--log-dir", default="/Users/charliesmith/code/tmp/it87-soak-logs"
    )
    parser.add_argument(
        "--modes",
        nargs="*",
        default=sorted(MODES),
        help="test names to run",
    )
    args = parser.parse_args()
    for test in args.modes:
        if test not in MODES:
            parser.error(f"unknown mode {test}; known: {sorted(MODES)}")
    os.makedirs(args.log_dir, exist_ok=True)

    binary = build_test_binary(args.build_timeout)
    load_start = os.getloadavg()
    sizing = modes_iterations(args)

    batches = []
    dials = 0
    dials_in_failed_batches = 0
    failures = []
    hangs = []
    strays = []
    sweep_started = time.monotonic()

    for run in range(args.runs):
        for index, test in enumerate(args.modes):
            label, extra = MODES[test]
            iterations = sizing.get(test, args.iterations)
            seed = args.seed0 + run * len(args.modes) + index
            env = dict(os.environ)
            env.update(
                {
                    "SOAK_SEED": str(seed),
                    "SOAK_DIALERS": str(args.dialers),
                    "SOAK_ITERATIONS": str(iterations),
                    "SOAK_ACCEPTORS": str(args.acceptors),
                    "SOAK_CANCEL_US": str(args.cancel_us),
                    "SOAK_ACCEPT_PACE_MS": str(args.pace_ms),
                    "SOAK_DIAL_TIMEOUT_MS": str(args.dial_timeout_ms),
                }
            )
            env.update(extra)
            name = f"{label}-seed{seed}"
            result = run_batch(binary, test, env, args.timeout, args.log_dir, name)
            record, violations, progress = parse_batch(result)
            batch = {
                "label": label,
                "seed": seed,
                "test": test,
                "hung": result["hung"],
                "returncode": result["returncode"],
                "elapsed_s": round(result["elapsed_s"], 3),
                "violations": violations,
                "last_progress": progress,
                "dialers": args.dialers,
                "iterations": iterations,
                "acceptors": args.acceptors,
            }
            if record:
                batch["result"] = record
            else:
                batch["result"] = None
            if result["strays"]:
                strays.extend(result["strays"])
                batch["strays"] = result["strays"]
            status = "ok"
            if result["hung"]:
                status = "HUNG"
                hangs.append(batch)
            elif (
                record is None
                or result["returncode"] != 0
                or violations
                or record.get("problems")
            ):
                status = "FAIL"
                failures.append(batch)
            if status == "ok" and record:
                dials += record.get("sent", 0)
            elif record:
                dials_in_failed_batches += record.get("sent", 0)
            batches.append(batch)
            tail = ""
            if record:
                tail = (
                    f" sent={record['sent']} handled={record['handled']} "
                    f"refused={record['refused']} leftover={record['leftover']} "
                    f"dial_failures={record['dial_failures']} "
                    f"{record['elapsed_ms']}ms"
                )
            print(f"[{status}] {name}{tail}", flush=True)
            for violation in violations:
                print(f"        {violation}", flush=True)

    sweep_elapsed = time.monotonic() - sweep_started
    load_end = os.getloadavg()
    # Rule of three: with zero observed failures, the 95% upper bound on the
    # per-dial failure rate is 3/N.
    bound = 3.0 / dials if dials else None
    summary = {
        "test": "accept_churn_soak",
        "binary": binary,
        "batches": len(batches),
        "dials": dials,
        "dials_in_failed_batches": dials_in_failed_batches,
        "dialers": args.dialers,
        "iterations": args.iterations,
        "acceptors": args.acceptors,
        "cancel_us": args.cancel_us,
        "dial_timeout_ms": args.dial_timeout_ms,
        "failures": len(failures),
        "hangs": len(hangs),
        "strays": len(strays),
        "sweep_elapsed_s": round(sweep_elapsed, 3),
        "load_start": [round(x, 2) for x in load_start],
        "load_end": [round(x, 2) for x in load_end],
        "cpus": os.cpu_count(),
        "detection_limit_per_dial_95": bound,
        "failure_detail": [b for b in failures],
        "hang_detail": [b for b in hangs],
        "log_dir": args.log_dir,
    }
    print("SOAK_SWEEP_RESULT " + json.dumps(summary))
    return 1 if failures or hangs or strays else 0


if __name__ == "__main__":
    raise SystemExit(main())
