# Failover Testing Plan

Focused plan for verifying that the system **keeps working during a failure** and is
**fully healthy after recovery** — not just that a single operation didn't throw.
Companion to [`plan.md`](./plan.md) and [`requirements.md`](./requirements.md).

---

## 1. Goals & definitions

A failover test passes only if **all three** hold:

1. **Availability** — the in-flight operation (read/write/size) succeeds while the failure is active.
2. **Correctness** — bytes read back are identical to bytes written (`sha256` match), and file
   size is unchanged.
3. **Convergence after recovery** — once the failed component returns, the system returns to its
   target invariants: every chunk has **R live replicas**, metadata matches disk, no orphan or
   missing chunk files.

> "Failover works" is not "the read didn't error." It is: correct data **and** the cluster
> heals back to full replication with no drift.

---

## 2. Health invariants (the oracle)

Every scenario ends by asserting these invariants via a `dfs fsck` helper (test utility that
queries the master + scans storage disks):

| Invariant | Check |
|---|---|
| **I1 Replication** | every chunk_id is present on exactly R **live** storage servers |
| **I2 Distinctness** | the R replicas of a chunk live on R distinct servers |
| **I3 Metadata→disk** | every `chunk_id` in master metadata has a file on each listed server; unreferenced disk chunks are reported as reclaimable orphans |
| **I4 Content** | `sha256(read(file)) == sha256(original)` for every test file |
| **I5 Size** | `dfs size file == len(original)` without transferring chunks |
| **I6 Liveness** | master's live-server set matches actually-running servers within one heartbeat interval |

`fsck` returns a structured report; assertions are made against it rather than by eyeballing logs.

---

## 3. Test fixtures

- **Cluster:** 1 master + 4 storage servers (one more than R=3 so re-replication has somewhere to go).
- **Files:** `empty.txt` (0 B), `small.txt` (<1 KB), `exact.txt` (exactly 3 KB = 3 chunks),
  `large.txt` (≥50 KB, many chunks).
- **Helpers:**
  - `kill_server(id)` / `start_server(id)` — stop/start a storage container (SIGKILL, not graceful).
  - `partition(id)` — block its port (iptables / docker network disconnect) to simulate a hang.
  - `wait_until_dead(id)` — block until master marks it dead (heartbeat timeout elapsed).
  - `fsck()` — returns the invariant report above.
  - `digest(path)` — sha256 of original file for comparison.

---

## 4. Scenarios

Each scenario: **Setup → Inject → Assert during failure → Recover → Assert after recovery.**

### S1 — Read survives a single storage failure
- **Setup:** create `large.txt`; record digest; note which servers hold each chunk.
- **Inject:** `kill_server(X)` where X holds at least one chunk of the file.
- **During:** `dfs read large.txt` succeeds; **I4** content matches; client transparently used a
  surviving replica (assert via client logs that it failed over, didn't just get lucky).
- **Recover:** `start_server(X)`.
- **After:** **I1, I2, I4, I5, I6** hold; re-replication restores the affected chunks to R live
  replicas on the live spare. If X later returns with old chunk files that metadata no longer
  references, those files are reclaimable orphans and must not affect reads.

### S2 — Read survives R−1 simultaneous failures
- **Setup:** create `exact.txt`; identify the 3 servers holding chunk #1.
- **Inject:** kill 2 of those 3 servers.
- **During:** read still succeeds from the last replica; **I4/I5** hold.
- **After (no recovery):** read availability is preserved while one live replica remains, but the
  4-node/R=3 cluster now has only 2 live servers, so it cannot climb back to 3 live replicas yet.
  Assert the system reports the chunk as under-replicated and rejects new writes until at least
  3 storage servers are live again. After one failed server returns, self-healing restores **I1**.

### S3 — Total loss of a chunk's replicas (unrecoverable boundary)
- **Setup:** create `small.txt` (single chunk, 3 replicas).
- **Inject:** kill **all 3** servers holding that chunk.
- **During:** `dfs read small.txt` must fail with a clear, typed error (`ChunkUnavailable`), not a
  hang or a corrupt/partial result.
- **After:** restart the killed servers → the chunk files are still on their disks → read succeeds
  again and **I4** holds. (Confirms this is *unavailability*, recoverable on restart — distinct
  from true data loss if disks were wiped.)

### S4 — Write while a storage server is down
- **Setup:** kill_server(X); `wait_until_dead(X)`.
- **Inject (the op under test):** `dfs create newfile.txt`.
- **During:** master allocates replicas only among the 3 live servers; **I2** still satisfied
  (3 distinct live servers); write succeeds; read-back **I4** holds.
- **Recover:** start_server(X).
- **After:** **I1–I6** hold; newfile's chunks are not placed on X retroactively unless
  re-replication rebalances — either is acceptable, but **I1** (R live replicas) must hold.

### S5 — Slow/partitioned server (hang, not crash)
- **Setup:** create `large.txt`.
- **Inject:** `partition(X)` so RPCs to X time out rather than fail fast.
- **During:** client read applies a per-replica timeout and fails over to another replica within
  the deadline; total read time stays bounded (assert wall-clock < threshold). No deadlock.
- **Recover:** heal the partition.
- **After:** **I6** master re-marks X live; **I1–I5** hold.

### S6 — Master restart (single point of failure + recovery)
- **Setup:** create several files; capture digests and `dfs size` for each.
- **Inject:** kill the master.
- **During:** all client ops fail fast with a clear "master unavailable" error (no hang); storage
  servers keep their chunks intact on disk.
- **Recover:** restart the master (SQLite metadata reloads); storage servers re-register via
  heartbeat.
- **After:** **without re-creating anything**, every file reads back correctly (**I4**), sizes
  match (**I5**), and **I1–I3, I6** hold. This proves metadata survived and the cluster
  reconverged. Documents the master as the critical SPOF whose state is nonetheless recoverable.

### S7 — Failure during an in-flight write (atomicity)
- **Setup:** begin `dfs create large.txt`; kill one target replica **mid-upload** (after some
  chunks committed, before `CommitFile`).
- **During/After:** the file must end up either (a) fully committed with R replicas per chunk, or
  (b) not registered at all — **never** half-registered. Assert no orphan chunk files (**I3**) and
  no metadata entry pointing at missing chunks. If client retries to a fresh replica, final state
  must satisfy **I1–I4**.

### S8 — Idempotent delete under failure
- **Setup:** create `exact.txt`.
- **Inject:** kill_server(X) (holds some of its chunks); `dfs delete exact.txt`.
- **During:** delete removes metadata + chunk files on the **live** servers.
- **Recover:** start_server(X) — it comes back holding now-orphaned chunk files.
- **After:** the file is gone from metadata and re-issuing delete is a no-op, not an error. Chunks
  left on X are reclaimable orphans unless a GC sweep is enabled.

---

## 5. Recovery / convergence checks (explicit)

After **every** recovery step, run `fsck()` and additionally:

- **No-drift:** diff master metadata against a snapshot taken before the failure (filenames,
  chunk lists, sizes unchanged for untouched files).
- **Re-replication bound:** if re-replication is implemented, assert it completes within a time
  bound (e.g. ≤ N heartbeat intervals) and that it created exactly the missing replicas — not
  extra copies (no over-replication beyond R).
- **Disk accounting:** referenced chunk files on disk == `Σ chunks × R` for all committed files
  (within the re-replication settling window); any extra files are reported as reclaimable orphans.

---

## 6. How to run

- **Fast integration suite** (client replica-selection, master dead-server filtering, repair) —
  `uv run pytest`; fast, no containers.
- **Full failover suite** (S1–S8) — future `pytest` tests driving `docker-compose`, using
  `kill_server` / `partition` helpers; tagged `@pytest.mark.failover` so they can run separately
  from fast tests.
- **Manual demo script** — `scripts/demo_failover.sh` walks S1, S3, and S6 with printed
  before/after `fsck` reports for the project presentation.

### Pass criteria
All S1–S8 green **and** every post-recovery `fsck` reports I1–I6 satisfied. A scenario that is
available but leaves the cluster under-replicated, with orphans, or with metadata drift is a
**fail**, even if the read returned correct bytes.

---

## 7. Traceability to requirements

| Requirement (§4 fault-tolerance) | Covered by |
|---|---|
| Storage server down — can clients still read/write? | S1, S4, S5 |
| Replication value — how many simultaneous failures survivable? | S2, S3 |
| Naming server down — critical SPOF | S6 |
| Recoverable vs unrecoverable | S3 (recoverable-on-restart), S6 (metadata recoverable), S7/S8 (no corrupt state) |
