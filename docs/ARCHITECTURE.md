# Architecture & Design

A GFS-inspired distributed file system for text files. This document covers the
system design, the key trade-offs, and a fault-tolerance analysis that
distinguishes critical from non-critical failures.

---

## 1. Components

### Naming server (single — the "master")
The metadata authority. It holds, for every file, the ordered list of chunks
and, for every chunk, the set of storage servers that hold a replica. It also
tracks which storage servers are currently alive and repairs chunks that fall
below the target number of live replicas.

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
seconds. It exposes `StoreChunk`, `StoreChunks`, `GetChunk`, `GetChunks`,
`DeleteChunk`, and
`ReplicateChunk` for server-to-server repair.

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
  storage servers (default **3**, required to be > 1). The Docker cluster runs
  four storage servers so one failure still leaves a live repair target.
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
2. The naming server returns live replica locations first. The client groups
   chunk IDs by storage server and fetches them with `GetChunks`, up to 1,024
   chunks per RPC.
3. If a batch misses chunks or a storage server is unreachable, the client
   retries only those missing chunks against the next replica.
4. Client concatenates the chunks and returns the original bytes.

### Self-healing path
The naming server periodically scans committed chunks. If a chunk has fewer
than `REPLICATION_FACTOR` live replicas and at least one live source replica,
the naming server asks a live storage server that does not yet hold the chunk to
run `ReplicateChunk`. The target server pulls bytes directly from the surviving
source, stores the chunk on disk, and then the naming server records the new
replica in SQLite. When the repair replaces a dead server, stale metadata is
removed so the committed layout stays at exactly R replicas.

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
| Replication factor 3 (default) | Survives up to 2 simultaneous failures for a chunk while one replica remains; 4 storage servers leave a spare repair target after one failure. | Higher storage cost and slower writes than R=2. |
| Server-to-server self-healing | Restores chunks to R live replicas after a storage failure without client involvement. | Needs at least R live storage servers and one surviving source replica. |

---

## 4. Fault-tolerance analysis

### 4.1 Storage server down — **non-critical (survivable)**
Because every chunk lives on 3 distinct servers by default, losing one storage
server does not lose data:
- **Reads:** fully available. The client falls back to another replica
  (verified: stop a storage server, reads still succeed).
- **Writes:** still possible as long as at least `REPLICATION_FACTOR` servers
  remain live — the naming server only places chunks on live servers. With the
  default 4 servers / factor 3, one can be down and writes continue; if fewer
  than 3 remain, `CreateFile` is rejected with a clear error rather than
  silently under-replicating.
- **Self-healing:** when a dead server held a chunk, the naming server orders a
  surviving replica to copy that chunk to the live spare server, restoring 3
  live replicas. If too few servers remain live, repair waits for recovery.
- **Recovery:** when the server comes back it re-registers. Any chunk files no
  longer referenced by metadata are harmless orphans; a future sweeper can
  reclaim them.

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
loss (default R = 3 → tolerates **2** for a chunk). Immediate re-replication
requires enough live capacity: after one failure in the default 4-server
cluster, there are still 3 live servers, so repair can restore R. After two
failures, reads may still succeed from the last replica, but the cluster cannot
restore R until a failed storage server returns or another storage server is
added.

### 4.4 Recoverable vs. unrecoverable

| Failure | Data loss? | Available during? | Recoverable? |
| --- | --- | --- | --- |
| 1 storage server down (R = 3, 4 servers) | No | Yes (reads + writes) | Yes — self-heals to the live spare |
| R or more servers holding the *same* chunk down at once | **Yes, for those chunks** | Partial | Only if a replica disk survives |
| Naming server process crash | No (metadata on disk) | **No** (whole system unavailable) | Yes — restart reloads SQLite |
| Naming server metadata (SQLite) destroyed | **Yes** (file index lost) | No | No — chunks exist but are unmappable |
| Storage server disk lost | Only that server's replicas | Yes | Yes if another replica exists |
| Client crash mid-write | No committed data affected | n/a | Yes — file stays `pending`, just retry |

**Summary:** the storage layer is fault-tolerant through replication and
degrades gracefully; the naming server is the deliberate single point of failure
and the first thing one would replicate to harden the system.
