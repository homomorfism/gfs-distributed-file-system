# Architecture & Design

A GFS-inspired distributed file system for text files. This document covers the
system design, the key trade-offs, and a fault-tolerance analysis that
distinguishes critical from non-critical failures.

---

## 1. Components

### Naming server (single — the "master")
The metadata authority. It holds, for every file, the ordered list of chunks
and, for every chunk, the set of storage servers that hold a replica. It also
tracks which storage servers are currently alive.

It exposes (gRPC):
- `RegisterStorage` / `Heartbeat` — cluster membership and liveness.
- `CreateFile` — reserves metadata and returns a **placement plan** (which
  servers to write each chunk to).
- `CommitFile` — marks a file readable once all replicas are written.
- `GetFile` — returns the chunk layout and replica locations for reading.
- `DeleteFile` — orchestrates deletion of all chunk replicas, then drops metadata.
- `GetFileSize` — answers from metadata alone, with **no data transfer**.
- `ListFiles`.

Metadata is persisted in **SQLite** (`files`, `chunks`, `replicas` tables). The
requirement allows a database for metadata; chunk **content never goes into the
database**.

### Storage servers (multiple — the "chunkservers")
Each stores only the chunks assigned to it — never the whole dataset — as plain
files (`<chunk_id>.chunk`) under `DATA_DIR` on its local file system. This
satisfies "chunk content stored in the file system, not in a database." On
startup each registers with the naming server and then heartbeats every few
seconds. It exposes `StoreChunk`, `GetChunk`, `DeleteChunk`.

### Client
A library + CLI that hides distribution from the user. The user deals in whole
text files; the client splits them into 1 KB chunks on write and reassembles
them on read. Following the GFS model, the client gets **metadata from the
master** and exchanges **bulk chunk data directly with the storage servers** —
the naming server is never in the data path.

---

## 2. Data handling

- **File type:** text files.
- **Chunk size:** fixed **1 KB (1024 bytes)** (`gfs/config.py: CHUNK_SIZE`). The
  last chunk of a file may be smaller.
- **Chunk identity:** each chunk gets a globally unique id (`uuid4` hex), used as
  both the metadata key and the on-disk filename. Unique ids mean chunks from
  different files never collide on a storage server.
- **Replication:** every chunk is written to `REPLICATION_FACTOR` distinct
  storage servers (default **2**, required to be > 1). The naming server picks
  replicas round-robin across the live servers to spread load.
- **Storage separation:** metadata in SQLite on the naming server; content in
  files on storage-server disks. The two layers are independent services.

### Write path (create)
1. Client splits the file into 1 KB chunks.
2. `CreateFile` → naming server allocates chunk ids, picks replica locations,
   stores the metadata as **`pending`**, and returns the placement plan.
3. Client uploads each chunk to **all** of its replica locations. If any replica
   write fails, the client aborts and does **not** commit — so a file is never
   half-replicated in the committed set.
4. `CommitFile` flips the file to **`committed`**, making it readable.

A crash between steps 2 and 4 leaves a `pending` row that is invisible to reads
(garbage that can be cleaned up); it never corrupts a readable file.

### Read path
1. `GetFile` → ordered chunk list + replica locations.
2. For each chunk the client tries replicas **in order** and uses the first that
   responds — so a single dead replica is transparent.
3. Client concatenates the chunks and returns the original bytes.

### Delete path
The naming server tells every replica to drop the chunk (idempotent), then
removes the metadata. Replicas on a server that is down at delete time become
**orphaned but unreachable** (the file's metadata is gone), so they waste space
but cannot corrupt anything; a background sweeper could reclaim them.

### Size path
Answered purely from the `files` table — no chunk transfer, as required.

---

## 3. Design decisions & trade-offs

| Decision | Why | Trade-off |
| --- | --- | --- |
| Single naming server | Simple, strongly-consistent metadata; matches GFS. | It is a **single point of failure** (see §4). |
| gRPC | Typed contract, multi-language clients, efficient. | Stubs must be generated (`scripts/gen_proto.py`). |
| Client writes to all replicas synchronously | Guarantees the replication factor before commit. | Slower writes; a single down server blocks new writes to it (the master just avoids picking dead servers). |
| Client reads directly from storage servers | Keeps the master off the data path → scalable. | Client must handle replica fallback (it does). |
| SQLite for metadata | Durable across naming-server restarts; zero-ops. | Single-node; not a distributed store. |
| Fixed tiny 1 KB chunk | Required; makes sharding/replication visible. | High per-chunk overhead vs. real GFS (64 MB); fine for a teaching system. |
| Replication factor 2 (default) | "More than one"; tolerates one failure with 3 servers. | Only survives 1 simultaneous storage failure; raise the factor for more. |

---

## 4. Fault-tolerance analysis

### 4.1 Storage server down — **non-critical (survivable)**
Because every chunk lives on ≥ 2 servers, losing one storage server does not
lose data:
- **Reads:** fully available. The client falls back to another replica
  (verified: stop a storage server, reads still succeed).
- **Writes:** still possible as long as at least `REPLICATION_FACTOR` servers
  remain live — the naming server only places chunks on live servers. With the
  default 3 servers / factor 2, one can be down and writes continue; if only 1
  remains, `CreateFile` is rejected with a clear error rather than silently
  under-replicating.
- **Recovery:** when the server comes back it re-registers and its on-disk
  chunks are usable again.

### 4.2 Naming server down — **CRITICAL (single point of failure)**
The naming server is the **only** place that maps files to chunk locations.
While it is down:
- No operation works — create, read, delete and size all require the master.
- The chunk **data is still safe** on the storage servers; the system is
  *unavailable*, not *lost*.
- **Recovery:** because metadata is persisted in SQLite, restarting the naming
  server restores the full file index; storage servers re-register within a few
  heartbeats and the system resumes. So a naming-server crash is a
  **recoverable availability outage**, not data loss — *unless the SQLite file
  itself is destroyed*, which would be unrecoverable.
- **How a production system removes this SPOF:** replicate the master (e.g.
  shadow masters + an operation log, or a Raft/Paxos-replicated metadata
  group). That is out of scope here and called out explicitly as the system's
  main weakness.

### 4.3 Replication value — how many simultaneous failures can we survive?
With replication factor **R** and replicas spread across distinct servers, a
chunk is lost only if **all R servers holding it fail simultaneously**. So the
system tolerates **R − 1 simultaneous storage-server failures** without data
loss (default R = 2 → tolerates **1**). Increasing `REPLICATION_FACTOR` (with
enough storage servers) directly raises the number of tolerated simultaneous
failures, at the cost of more storage and slower writes.

### 4.4 Recoverable vs. unrecoverable

| Failure | Data loss? | Available during? | Recoverable? |
| --- | --- | --- | --- |
| 1 storage server down (R = 2) | No | Yes (reads + writes) | Yes — comes back and rejoins |
| R or more servers holding the *same* chunk down at once | **Yes, for those chunks** | Partial | Only if a replica disk survives |
| Naming server process crash | No (metadata on disk) | **No** (whole system unavailable) | Yes — restart reloads SQLite |
| Naming server metadata (SQLite) destroyed | **Yes** (file index lost) | No | No — chunks exist but are unmappable |
| Storage server disk lost | Only that server's replicas | Yes | Yes if another replica exists |
| Client crash mid-write | No committed data affected | n/a | Yes — file stays `pending`, just retry |

**Summary:** the storage layer is fault-tolerant through replication and
degrades gracefully; the naming server is the deliberate single point of failure
and the first thing one would replicate to harden the system.
