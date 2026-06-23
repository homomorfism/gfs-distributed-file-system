# Communication Flows

Diagrams of client ↔ master (naming server) and client ↔ chunkserver
(storage server) interactions for write and read operations.

---

## 1. Full write path (create)

The client first gets a **placement plan** from the master, then writes chunks
**directly** to the storage servers, and finally commits the file through the master.

```mermaid
sequenceDiagram
    participant C as Client
    participant M as Master<br/>(Naming Server)
    participant S1 as Storage 1
    participant S2 as Storage 2
    participant S3 as Storage 3
    participant DB as SQLite<br/>(metadata)

    Note over C: 1. Splits file into 1 KB chunks

    C->>M: CreateFile(filename, size, num_chunks)
    activate M
    M->>M: Checks that live storage servers ≥ REPLICATION_FACTOR
    M->>M: Generates chunk_id (UUID) for each chunk
    M->>M: Picks R servers for each chunk (round-robin)
    M->>DB: INSERT file (status=pending) + chunks + replicas
    M-->>C: CreateFileResponse{ placements: [{index, chunk_id, locations}] }
    deactivate M

    Note over C: 2. Groups chunks by target storage server

    par Parallel upload to all storage servers
        C->>S1: StoreChunks([{chunk_id, data}, ...])
        activate S1
        S1->>S1: Writes each chunk to DATA_DIR/{chunk_id}.chunk
        S1-->>C: ok, stored=N
        deactivate S1
    and
        C->>S2: StoreChunks([{chunk_id, data}, ...])
        activate S2
        S2->>S2: Writes each chunk to DATA_DIR/{chunk_id}.chunk
        S2-->>C: ok, stored=N
        deactivate S2
    and
        C->>S3: StoreChunks([{chunk_id, data}, ...])
        activate S3
        S3->>S3: Writes each chunk to DATA_DIR/{chunk_id}.chunk
        S3-->>C: ok, stored=N
        deactivate S3
    end

    Note over C: 3. All replicas written — commit

    C->>M: CommitFile(filename)
    activate M
    M->>DB: UPDATE file SET status='committed'
    M-->>C: ok
    deactivate M

    Note over C: File is now readable
```

### client→master details (CreateFile)

| RPC | Direction | Data |
| --- | --- | --- |
| `CreateFile` | Client → Master | `filename`, `size` (bytes), `num_chunks` |
| `CreateFileResponse` | Master → Client | `placements[]` — per chunk: `index`, `chunk_id` (UUID), `locations[]` (storage addresses) |

The master **never stores or transfers** chunk content — only addresses.

### client→chunkserver details (StoreChunks)

| RPC | Direction | Data |
| --- | --- | --- |
| `StoreChunks` | Client → Storage | `chunks[]` — list of `{chunk_id, data}` (batched up to 1024 chunks ≈ 1 MB) |
| `StoreChunksResponse` | Storage → Client | `ok`, `stored` (how many written) |

The client groups all chunks destined for the same storage server into a single
`StoreChunks` RPC and sends to all servers **in parallel**.

---

## 2. Full read path

The client requests chunk locations from the master, then reads each chunk
**directly** from any available storage server.

```mermaid
sequenceDiagram
    participant C as Client
    participant M as Master<br/>(Naming Server)
    participant S1 as Storage 1
    participant S2 as Storage 2
    participant S3 as Storage 3

    C->>M: GetFile(filename)
    activate M
    M->>M: Looks up file in SQLite (committed only)
    M->>M: Sorts locations: live servers first
    M-->>C: GetFileResponse{ size, placements: [{index, chunk_id, locations}] }
    deactivate M

    Note over C: For each chunk, tries replicas in order

    par Parallel chunk reads (up to 16 threads)
        C->>S1: GetChunk(chunk_id=abc)
        activate S1
        S1->>S1: Reads DATA_DIR/abc.chunk
        S1-->>C: data (1 KB)
        deactivate S1
    and
        C->>S2: GetChunk(chunk_id=def)
        activate S2
        S2->>S2: Reads DATA_DIR/def.chunk
        S2-->>C: data (1 KB)
        deactivate S2
    and
        C->>S2: GetChunk(chunk_id=ghi)
        Note over C,S2: If S2 is unreachable — fallback to S3
        C->>S3: GetChunk(chunk_id=ghi)
        activate S3
        S3->>S3: Reads DATA_DIR/ghi.chunk
        S3-->>C: data (1 KB)
        deactivate S3
    end

    Note over C: Reassembles chunks by index → original file
```

### client→master details (GetFile)

| RPC | Direction | Data |
| --- | --- | --- |
| `GetFile` | Client → Master | `filename` |
| `GetFileResponse` | Master → Client | `size`, `placements[]` (sorted by `index`), live replicas listed first |

### client→chunkserver details (GetChunk)

| RPC | Direction | Data |
| --- | --- | --- |
| `GetChunk` | Client → Storage | `chunk_id` |
| `GetChunkResponse` | Storage → Client | `data` (bytes, up to 1 KB) |

For each chunk the client iterates over `locations` in order and uses the first
server that responds. If a server is unreachable — automatic fallback to the next
replica.

---

## 3. Other client→master RPCs

```mermaid
sequenceDiagram
    participant C as Client
    participant M as Master

    rect rgb(240, 248, 255)
        Note over C,M: File size (zero data transfer)
        C->>M: GetFileSize(filename)
        M-->>C: { size, num_chunks }
    end

    rect rgb(255, 248, 240)
        Note over C,M: List files
        C->>M: ListFiles()
        M-->>C: [{ filename, size, num_chunks, status }]
    end

    rect rgb(255, 240, 240)
        Note over C,M: Delete
        C->>M: DeleteFile(filename)
        M->>M: Deletes all chunk replicas (best-effort)
        M->>M: Removes metadata from SQLite
        M-->>C: { ok, message }
    end
```

### All client↔master RPCs summary

| RPC | Client → Master | Master → Client | Purpose |
| --- | --- | --- | --- |
| `CreateFile` | filename, size, num_chunks | placement plan (chunk_id + locations) | Reserves metadata |
| `CommitFile` | filename | ok | Makes the file readable |
| `GetFile` | filename | size + placement plan | Returns chunk locations for reads |
| `GetFileSize` | filename | size, num_chunks | Size from metadata (0 bytes of data) |
| `ListFiles` | — | [filename, size, status, …] | Lists all files |
| `DeleteFile` | filename | ok + message | Deletes replicas and metadata |

### All client↔chunkserver RPCs summary

| RPC | Client → Storage | Storage → Client | Purpose |
| --- | --- | --- | --- |
| `StoreChunks` | [{chunk_id, data}, …] | ok, stored=N | Batch chunk writes |
| `GetChunk` | chunk_id | data | Single chunk read |

---

## 4. Key principle: separation of metadata and data

```
                    ┌──────────────────────┐
                    │       Master         │
                    │   (Naming Server)    │
                    │                      │
                    │  Metadata (SQLite):  │
                    │  • file → [chunks]   │
                    │  • chunk → [servers] │
                    │  • liveness          │
                    └──────┬───────────────┘
                           │
              ┌────────────┼────────────┐
              │ metadata   │ metadata   │ metadata
              │ (placement │ (locations │ (commit,
              │  plan)     │  for read) │  delete)
              │            │            │
           ┌──▼────────────▼────────────▼──┐
           │            Client             │
           │  • splits/reassembles chunks  │
           │  • knows placement from master│
           │  • reads/writes chunks direct │
           └──┬─────────┬─────────┬────────┘
              │ data    │ data    │ data
              │ (gRPC)  │ (gRPC)  │ (gRPC)
        ┌─────▼──┐ ┌───▼────┐ ┌▼───────┐
        │Storage1│ │Storage2│ │Storage3│
        │ .chunk │ │ .chunk │ │ .chunk │
        │ files  │ │ files  │ │ files  │
        └────────┘ └────────┘ └────────┘
```

The master **never participates in data transfer** — the client talks to it only
for metadata (placement/locations). All chunk content is transferred directly
between the client and storage servers. This is the core architectural idea of GFS:
**separation of the metadata path from the data path**.
