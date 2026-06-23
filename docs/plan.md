# Distributed File System — Implementation & Testing Plan

GFS-inspired distributed file system for text files. Python + gRPC.
Companion to [`requirements.md`](./requirements.md).

---

## 1. Architecture

```
                  ┌─────────────────┐
   metadata ops   │  Naming Server  │   registration + heartbeats
  ┌──────gRPC─────▶│   (master)      │◀──────gRPC──────┐
  │               │  metadata only   │                 │
  │               └─────────────────┘                 │
┌─┴────┐                                        ┌──────┴───────┐
│Client│───────chunk upload/download (gRPC)────▶│Storage Server│ × N
└──────┘                                        │ chunks on FS  │
                                                └──────────────┘
```

| Component | Responsibility | Stores |
|---|---|---|
| **Naming server** (single) | Metadata authority. `filename → [chunk_id…]`, `chunk_id → [storage_server…]`, file size. Picks replica placement. Tracks live servers via heartbeats. | Metadata only (SQLite). **No chunk bytes.** |
| **Storage server** (multiple) | Chunk store with single-chunk and bulk `put/get/delete` RPCs. Registers with master, sends heartbeats. | Chunk bytes as files on disk (`/data/<chunk_id>`). |
| **Client** | Library + CLI. Hides distribution. Splits/reassembles, talks to master for placement then directly to storage servers for batched bytes. | Nothing persistent. |

---

## 2. Design decisions (confirmed)

1. **Transport: gRPC everywhere.** Binary chunk transfer and metadata both over gRPC.
   Optional REST `/health` + `/status` on the master for ops/demo visibility only.
2. **Replication topology: client-direct fan-out.** Master assigns R storage servers per
   chunk; client uploads to all R replicas itself, grouped into bulk RPCs per storage server.
   Keeps storage servers simple and avoids primary-forwarding complexity without creating
   thousands of serial RPCs for 1 MiB files.
3. **Replication factor R = 3** (configurable). Survives R−1 simultaneous storage-server failures.
4. **Chunk size: fixed 1 KB** per spec.
5. **Metadata persistence: SQLite** on the master. Chunk *content* stays on the file system.
   Survives master restart.
6. **Placement: round-robin** across live servers; the R replicas of a chunk
   are forced onto distinct servers.
7. **Self-healing (re-replication):** when a storage server dies and a chunk drops below R live
   replicas, the master schedules a **server-to-server copy** from a surviving replica onto
   another live server that lacks the chunk — restoring R replicas with no client involvement and
   no new nodes. Cluster runs **4 storage servers for R=3** so a heal target always exists.

---

## 3. Repository layout

```
gfs-distributed-file-system/
├── proto/gfs.proto                # shared gRPC contract
├── gfs/_generated/                # generated stubs (gitignored except package marker)
├── gfs/naming_server/             # master: metadata, placement, heartbeats, healing
├── gfs/storage_server/            # chunkserver: put/get/delete/replicate on FS
├── gfs/client/                    # library + CLI (create/read/delete/size/list)
├── tests/                         # fast in-process integration tests
├── scripts/load_simulation.py      # 100-user local load profile
├── docker-compose.yml
├── docs/ARCHITECTURE.md           # design + fault-tolerance analysis
└── README.md
```

---

## 4. gRPC contract (sketch)

```proto
service NamingService {
  rpc RegisterStorage(...)      returns (...);
  rpc Heartbeat(...)            returns (...);
  rpc AllocateChunks(FileReq)   returns (ChunkPlacement);  // for write
  rpc CommitFile(FileMeta)      returns (Ack);
  rpc GetFileLocations(Name)    returns (ChunkPlacement);  // for read
  rpc DeleteFile(Name)          returns (Ack);
  rpc GetFileSize(Name)         returns (SizeReply);
}
service StorageService {
  rpc StoreChunk(ChunkData)     returns (Ack);    // bytes
  rpc StoreChunks(ChunkDataBatch) returns (Ack);
  rpc GetChunk(ChunkId)         returns (ChunkData);
  rpc GetChunks(ChunkIdBatch)   returns (ChunkDataBatch);
  rpc DeleteChunk(ChunkId)      returns (Ack);
  rpc DeleteChunks(ChunkIdBatch) returns (Ack);
  rpc ReplicateChunk(CopyOrder) returns (Ack);    // pull chunk_id from a peer server (self-heal)
}
```

---

## 5. Implementation phases

**Phase 0 — Scaffolding.** Repo init, `requirements.txt` (`grpcio`, `grpcio-tools`),
proto definition, codegen.

**Phase 1 — Storage server.** Single-chunk and bulk put/get/delete writing files to disk;
`Register` + `Heartbeat` to master; in-memory chunk index. Unit-testable standalone.

**Phase 2 — Naming server.** Server registry with liveness from heartbeats; metadata store
(SQLite); placement (`AllocateChunks` returns chunk_ids + replica locations);
`CreateFile/GetFileLocations/DeleteFile/GetFileSize`.

**Phase 3 — Client.** Split text file into 1 KB chunks → ask master for placement → batch upload
chunks to R replicas by storage server → commit metadata. Read: get locations → fetch chunk
batches from live replicas with fallback → reassemble. Delete + size. CLI:
`dfs create/read/delete/size`.

**Phase 4 — Fault tolerance + self-healing.** Read failover across replicas; master skips dead
servers in placement; **re-replication** — a background loop on the master scans `chunk → live
replica count`, and for any chunk below R issues a `ReplicateChunk` order so a surviving replica
copies the chunk to a live server that lacks it. Guards against over-replication (never more than
R) and avoids duplicate in-flight copies for the same chunk.

**Phase 5 — Docker + docs.** Dockerfiles, `docker-compose` (1 master + 4 storage + client),
`architecture.md` with the required fault analysis, `README`.

---

## 6. Testing plan

### Fast in-process tests
- **Storage:** put→get round-trips bytes; delete removes file; get-missing errors.
- **Naming:** placement returns R *distinct* live servers; dead servers excluded; metadata CRUD.
- **Client:** chunking splits at exactly 1 KB boundaries; reassembly byte-exact. Cases: empty
  file, file < 1 KB, file = exact multiple of 1 KB, large multi-chunk file.

### Integration (real processes / compose)
- Full create → read → verify identical content.
- Get-size matches without transferring chunks.
- Delete removes metadata **and** all replica files on disk.

### Fault-injection (graded)
| Scenario | Expected | Verdict |
|---|---|---|
| Kill 1 storage server | Read still succeeds via replica failover; master re-replicates chunk to a live server back to R | recoverable + self-heals |
| Kill R−1 servers holding a chunk | Chunk still readable if one replica remains; repair waits until at least R storage servers are live again | demonstrates replication value + capacity limit |
| Kill all R replicas of a chunk | Chunk unavailable | unrecoverable while down (recoverable on restart) |
| Kill master | Reads/writes fail (no placement/lookup) | single point of failure |
| Restart master | Metadata survives (SQLite) | recoverable |
| Write while one server down | Master places on remaining live servers | recoverable |

### Acceptance
The requirements checklist (all 13 items), driven by an end-to-end demo script.

---

## 7. Coverage of required fault-tolerance questions

- **Storage server down** → reads/writes continue via remaining replicas; master self-heals the
  under-replicated chunks back to R on the surviving servers (§6 fault table).
- **Naming server down** → critical single point of failure; no metadata = no lookups/writes.
- **Replication value** → R replicas survive R−1 *simultaneous* storage failures; because the
  cluster re-replicates back to R after each loss, it can then survive R−1 *more* — not just R−1 ever.
- **Recoverable vs unrecoverable** → master restart & ≤R−1 storage losses recoverable (and
  self-healed); loss of all R replicas of a chunk simultaneously is unavailable while down
  (recoverable on restart); master metadata loss is unrecoverable.
