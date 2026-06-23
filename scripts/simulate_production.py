#!/usr/bin/env python3
"""Production workload simulation: 100 concurrent users writing and reading files.

Each user independently performs random operations (write / read / delete) on
files up to 10 MB.  The script prints throughput stats every few seconds and
stops cleanly on SIGINT / SIGTERM.

Usage:
  # inside the Docker client container
  python3 /samples/../scripts/simulate_production.py

  # locally (with NAMING_SERVER pointing at a reachable naming server)
  NAMING_SERVER=localhost:50051 uv run python scripts/simulate_production.py

Environment variables:
  NAMING_SERVER      naming server address (default: naming:50051)
  NUM_USERS          number of concurrent users (default: 100)
  MAX_FILE_MB        maximum file size in MB (default: 10)
  WRITE_RATIO        fraction of operations that are writes (default: 0.5)
  READ_RATIO         fraction that are reads (default: 0.3, rest deletes)
  STATS_INTERVAL     seconds between stats lines (default: 10)
  DURATION_SECONDS   stop after this many seconds (default: 0 = run until ^C)
  CLEANUP_ON_EXIT    delete all test files on exit (default: 1)
"""

from __future__ import annotations

import hashlib
import os
import random
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

# --- try to import the GFS client -------------------------------------------
# The gfs package may be in /app (Docker image WORKDIR), the repo root
# (local dev), or alongside this script.  Probe each in order.
try:
    from gfs.client.client import GFSClient, GFSError
except ModuleNotFoundError:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    for _candidate in (
        os.path.dirname(_script_dir),        # repo root (scripts/../)
        "/app",                               # Docker image WORKDIR
    ):
        if _candidate not in sys.path:
            sys.path.insert(0, _candidate)
    from gfs.client.client import GFSClient, GFSError  # type: ignore[no-redef]

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
NAMING_SERVER = os.environ.get("NAMING_SERVER", "naming:50051")
NUM_USERS = int(os.environ.get("NUM_USERS", "10"))
MAX_FILE_SIZE = int(os.environ.get("MAX_FILE_MB", "1")) * 1_000_000
MIN_FILE_SIZE = 1_024  # 1 KB
CHUNK_SIZE = 1_024      # must match gfs.config.CHUNK_SIZE
WRITE_RATIO = float(os.environ.get("WRITE_RATIO", "0.5"))
READ_RATIO = float(os.environ.get("READ_RATIO", "0.3"))
STATS_INTERVAL = float(os.environ.get("STATS_INTERVAL", "10"))
DURATION_SECONDS = float(os.environ.get("DURATION_SECONDS", "0"))
CLEANUP_ON_EXIT = os.environ.get("CLEANUP_ON_EXIT", "1") not in ("0", "no", "false")

# Derive delete ratio from what remains.
_delete_ratio = max(0.0, 1.0 - WRITE_RATIO - READ_RATIO)
OP_WEIGHTS = (WRITE_RATIO, READ_RATIO, _delete_ratio)
OP_LABELS = ("write", "read", "delete")

USER_NAME_PREFIX = "sim-user-"
SHOULD_STOP: threading.Event = threading.Event()  # set after import for clarity


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------
_MAX_ERROR_SAMPLES = 100  # cap per-op-type error samples to avoid memory bloat


@dataclass
class Stats:
    lock: threading.Lock = field(default_factory=threading.Lock)
    writes: int = 0
    reads: int = 0
    deletes: int = 0
    write_errors: int = 0
    read_errors: int = 0
    delete_errors: int = 0
    bytes_written: int = 0
    bytes_read: int = 0
    start_time: float = field(default_factory=time.monotonic)
    # Sample actual error strings so the user can see *what* is failing.
    _write_error_samples: list[str] = field(default_factory=list)
    _read_error_samples: list[str] = field(default_factory=list)
    _delete_error_samples: list[str] = field(default_factory=list)

    def record_error(self, op: str, message: str) -> None:
        with self.lock:
            if op == "write":
                self.write_errors += 1
                if len(self._write_error_samples) < _MAX_ERROR_SAMPLES:
                    self._write_error_samples.append(message)
            elif op == "read":
                self.read_errors += 1
                if len(self._read_error_samples) < _MAX_ERROR_SAMPLES:
                    self._read_error_samples.append(message)
            else:
                self.delete_errors += 1
                if len(self._delete_error_samples) < _MAX_ERROR_SAMPLES:
                    self._delete_error_samples.append(message)

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "writes": self.writes,
                "reads": self.reads,
                "deletes": self.deletes,
                "write_errors": self.write_errors,
                "read_errors": self.read_errors,
                "delete_errors": self.delete_errors,
                "bytes_written": self.bytes_written,
                "bytes_read": self.bytes_read,
                "elapsed": time.monotonic() - self.start_time,
            }

    def error_summary(self) -> dict:
        """Return counts of distinct error messages grouped by operation."""
        with self.lock:

            def _counts(samples: list[str]) -> list[tuple[str, int]]:
                counts: dict[str, int] = {}
                for msg in samples:
                    counts[msg] = counts.get(msg, 0) + 1
                return sorted(counts.items(), key=lambda x: -x[1])

            return {
                "write": _counts(self._write_error_samples),
                "read": _counts(self._read_error_samples),
                "delete": _counts(self._delete_error_samples),
            }


STATS = Stats()


# ---------------------------------------------------------------------------
# Deterministic data generation (no disk I/O for test payloads)
# ---------------------------------------------------------------------------
def _generate_data(size: int, seed: int) -> bytes:
    """Return `size` bytes of deterministic pseudo-random data (seed-based)."""
    data = bytearray(size)
    state = seed.to_bytes(32, "big")
    written = 0
    while written < size:
        block = hashlib.sha256(state).digest() + hashlib.sha256(
            state + b"\x01"
        ).digest()
        take = min(len(block), size - written)
        data[written : written + take] = block[:take]
        state = hashlib.sha256(state).digest()
        written += take
    return bytes(data)


# ---------------------------------------------------------------------------
# Per-user session
# ---------------------------------------------------------------------------
def _user_session(user_id: int) -> None:
    """Single simulated user: loops writing / reading / deleting until stopped."""
    client = GFSClient(NAMING_SERVER, timeout=30)
    prefix = f"{USER_NAME_PREFIX}{user_id:03d}-"
    rng = random.Random(os.urandom(8))  # per-thread RNG
    file_counter = 0
    my_files: list[str] = []  # local cache of filenames this user created

    while not SHOULD_STOP.is_set():
        op: str = rng.choices(OP_LABELS, weights=OP_WEIGHTS, k=1)[0]  # type: ignore[assignment]

        try:
            if op == "write":
                # Pick a random size, aligned to CHUNK_SIZE.
                raw = rng.randint(MIN_FILE_SIZE, MAX_FILE_SIZE)
                size = (raw // CHUNK_SIZE) * CHUNK_SIZE
                if size == 0:
                    size = CHUNK_SIZE

                filename = f"{prefix}{file_counter:06d}.bin"
                data = _generate_data(size, user_id * 10_000_000 + file_counter)

                client.create(filename, data)
                my_files.append(filename)
                file_counter += 1

                with STATS.lock:
                    STATS.writes += 1
                    STATS.bytes_written += size

            elif op == "read":
                # Prefer reading one of this user's own files; fall back to listing.
                candidates = my_files
                if not candidates:
                    try:
                        all_files = client.list_files()
                        candidates = [
                            f[0] for f in all_files if f[0].startswith(prefix)
                        ]
                        my_files = candidates
                    except GFSError:
                        candidates = []

                if candidates:
                    target = rng.choice(candidates)
                    data = client.read(target)
                    with STATS.lock:
                        STATS.reads += 1
                        STATS.bytes_read += len(data)

            else:  # delete
                # Delete an old file to keep total disk usage bounded.
                if my_files:
                    target = my_files.pop(0)
                    try:
                        client.delete(target)
                        with STATS.lock:
                            STATS.deletes += 1
                    except GFSError as exc:
                        # File may already be gone (e.g. cleaned up by another
                        # user or purged by naming-server stale-pending GC).
                        STATS.record_error("delete", str(exc))
                else:
                    # Nothing to delete — do a read or write instead.
                    pass

        except GFSError as exc:
            STATS.record_error(op, str(exc))
            time.sleep(rng.uniform(0.05, 0.2))

        except Exception as exc:
            STATS.record_error(op, f"{type(exc).__name__}: {exc}")
            # Back off a little on unexpected errors.
            time.sleep(rng.uniform(0.5, 1.0))


# ---------------------------------------------------------------------------
# Background stats reporter
# ---------------------------------------------------------------------------
def _stats_reporter() -> None:
    last = STATS.snapshot()
    header_printed = False

    while not SHOULD_STOP.wait(STATS_INTERVAL):
        now = STATS.snapshot()
        dt = now["elapsed"] - last["elapsed"]
        if dt <= 0:
            last = now
            continue

        if not header_printed:
            print(
                f"{'elapsed':>8s}  {'writes':>8s}  {'reads':>8s}  "
                f"{'dels':>6s}  {'write BW':>9s}  {'read BW':>9s}  "
                f"{'errs':>6s}  {'W-err':>5s}  {'R-err':>5s}  {'D-err':>5s}"
            )
            header_printed = True

        wr_rate = (now["writes"] - last["writes"]) / dt
        rd_rate = (now["reads"] - last["reads"]) / dt
        bw_wr = (now["bytes_written"] - last["bytes_written"]) / dt / 1_000_000
        bw_rd = (now["bytes_read"] - last["bytes_read"]) / dt / 1_000_000
        interval_wr_err = now["write_errors"] - last["write_errors"]
        interval_rd_err = now["read_errors"] - last["read_errors"]
        interval_dl_err = now["delete_errors"] - last["delete_errors"]
        total_interval_err = interval_wr_err + interval_rd_err + interval_dl_err

        print(
            f"{now['elapsed']:7.1f}s  "
            f"{now['writes']:6d} {wr_rate:4.1f}/s  "
            f"{now['reads']:6d} {rd_rate:4.1f}/s  "
            f"{now['deletes']:4d}   "
            f"{bw_wr:7.1f} MB/s  "
            f"{bw_rd:7.1f} MB/s  "
            f"{total_interval_err:4.0f}    "
            f"{interval_wr_err:4.0f}   "
            f"{interval_rd_err:4.0f}   "
            f"{interval_dl_err:4.0f}"
        )
        last = now


# ---------------------------------------------------------------------------
# Signal handling & main
# ---------------------------------------------------------------------------
def _on_stop(signum: int, frame: object) -> None:
    print("\nreceived stop signal, draining…", file=sys.stderr, flush=True)
    SHOULD_STOP.set()


def _cleanup(client: GFSClient, prefix: str) -> None:
    """Best-effort removal of all simulation files."""
    try:
        files = client.list_files()
    except GFSError:
        print("cleanup: could not list files, skipping delete phase")
        return

    sim_files = [f[0] for f in files if f[0].startswith(prefix)]
    if not sim_files:
        return
    print(f"cleanup: deleting {len(sim_files)} simulation files…")

    # Delete in parallel batches.
    def _delete_batch(batch: list[str]) -> int:
        c = GFSClient(NAMING_SERVER, timeout=10)
        deleted = 0
        for fn in batch:
            try:
                c.delete(fn)
                deleted += 1
            except GFSError:
                pass
        return deleted

    workers = min(20, len(sim_files))
    batch_size = max(1, len(sim_files) // workers)
    batches = [
        sim_files[i : i + batch_size] for i in range(0, len(sim_files), batch_size)
    ]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = pool.map(_delete_batch, batches)
    print(f"cleanup: removed {sum(results)} files")


def main() -> None:
    signal.signal(signal.SIGINT, _on_stop)
    signal.signal(signal.SIGTERM, _on_stop)

    print(
        f"Production simulation: {NUM_USERS} users, "
        f"files up to {MAX_FILE_SIZE // 1_000_000} MB"
    )
    print(
        f"Operations — write: {WRITE_RATIO:.0%}  "
        f"read: {READ_RATIO:.0%}  "
        f"delete: {_delete_ratio:.0%}"
    )
    print(f"Naming server: {NAMING_SERVER}")
    if DURATION_SECONDS:
        print(f"Duration: {DURATION_SECONDS:.0f}s")
    print()

    # Start stats reporter thread.
    reporter = threading.Thread(target=_stats_reporter, daemon=True)
    reporter.start()

    # Optional auto-stop timer.
    if DURATION_SECONDS > 0:
        timer = threading.Timer(DURATION_SECONDS, SHOULD_STOP.set)
        timer.start()

    # Launch user sessions.
    with ThreadPoolExecutor(max_workers=NUM_USERS) as pool:
        futures = [pool.submit(_user_session, i) for i in range(NUM_USERS)]
        try:
            for f in as_completed(futures):
                f.result()
        except KeyboardInterrupt:
            SHOULD_STOP.set()

    # ------------------------------------------------------------------
    # Final report
    # ------------------------------------------------------------------
    final = STATS.snapshot()
    elapsed = final["elapsed"]
    total_ops = final["writes"] + final["reads"] + final["deletes"]
    total_err = (
        final["write_errors"] + final["read_errors"] + final["delete_errors"]
    )

    print()
    print("=" * 60)
    print(f"  Final stats ({elapsed:.1f}s)")
    print("=" * 60)
    print(f"  Writes:  {final['writes']:>8d}  "
          f"({final['writes'] / max(elapsed, 1):.1f}/s)")
    print(f"  Reads:   {final['reads']:>8d}  "
          f"({final['reads'] / max(elapsed, 1):.1f}/s)")
    print(f"  Deletes: {final['deletes']:>8d}")
    print(f"  Data written:  {final['bytes_written'] / 1_000_000:.1f} MB")
    print(f"  Data read:     {final['bytes_read'] / 1_000_000:.1f} MB")
    print(f"  Errors:")
    print(f"    Write:  {final['write_errors']:>6d}")
    print(f"    Read:   {final['read_errors']:>6d}")
    print(f"    Delete: {final['delete_errors']:>6d}")
    if total_err:
        print(f"    Total:  {total_err:>6d}  "
              f"({total_err / max(total_ops, 1) * 100:.2f}% of {total_ops} ops)")
    else:
        print(f"    Total:        0")
    print("=" * 60)

    # Print detailed error breakdown if any errors occurred.
    if total_err:
        summary = STATS.error_summary()
        for op_label, op_key in [("WRITE", "write"), ("READ", "read"),
                                  ("DELETE", "delete")]:
            entries = summary[op_key]
            if not entries:
                continue
            print(f"\n  {op_label} errors (top 10):")
            for msg, count in entries[:10]:
                # Truncate long messages for readability.
                display = msg if len(msg) <= 100 else msg[:97] + "..."
                print(f"    [{count:>4d}x]  {display}")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    if CLEANUP_ON_EXIT and (final["writes"] > 0):
        print()
        _cleanup(GFSClient(NAMING_SERVER, timeout=10), USER_NAME_PREFIX)


if __name__ == "__main__":
    main()
