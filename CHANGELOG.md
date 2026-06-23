# Changelog

All notable changes to the GFS distributed file system.

## [Unreleased]

### Changed

- **Write throughput: batched chunk writes via `StoreChunks` RPC.**  
  Previously every 1 KB chunk was a separate `StoreChunk` gRPC call — a 10 MB
  file meant ~30,720 RPCs (10,240 chunks × 3 replicas). Each call paid for
  protobuf serialization, HTTP/2 framing, and thread-pool dispatch on a 1 KB
  payload. The new `StoreChunks` RPC packs up to 1,024 chunks (~1 MB) into a
  single message, reducing RPC count by ~1,000× for large files. The client
  groups writes by storage server (already done) and sends one batched RPC per
  server instead of one per chunk.
  - New proto messages: `ChunkData`, `StoreChunksRequest`, `StoreChunksResponse`
  - `StorageServicer.StoreChunks` writes all chunks in a batch, incrementing
    byte counters once per batch instead of once per chunk.
  - Client drops the per-chunk `ThreadPoolExecutor` inside `_upload`; batches
    are sent sequentially per server (one at a time), but uploads to different
    servers still run in parallel.

- **gRPC channel cache in the client.**  
  The client now caches and reuses gRPC channels by address (`_ChannelCache`)
  instead of opening a new TCP connection on every operation. The naming-server
  channel is held for the client's lifetime; storage-server channels are
  created lazily on first use. This eliminates repeated TCP/TLS handshakes
  across multiple `create`, `read`, or `delete` calls made by the same client
  instance.

- **Fix N+1 SQL queries in `MetadataStore.get_file()`.**  
  `get_file()` previously ran the chunk query, then one `SELECT` per chunk to
  find replicas — for a file with 10,240 chunks that was 10,242 queries. It now
  uses a single `LEFT JOIN` across `chunks` and `replicas` (same pattern already
  used in `list_committed_chunks()`).

- **Async overwrite cleanup in `CreateFile`.**  
  When overwriting a file, old chunks are now deleted in a fire-and-forget
  background thread instead of synchronously blocking the `CreateFile` RPC.
  This prevents the RPC from stalling for seconds while deleting thousands of
  chunks from the previous version.

### Added

- **Orphan chunk cleanup at storage server startup.** When a storage server
  starts, it queries the naming server for the list of chunk IDs it *should*
  hold (via the new `ListExpectedChunks` RPC) and deletes any chunks on disk
  that are no longer in metadata. This reclaims disk space wasted by replica
  migrations during self-healing — when a failed server returns, the chunks
  that were moved to other servers are no longer needed locally.
  - New RPC: `NamingServer.ListExpectedChunks(address) → chunk_ids`
  - New metadata query: `MetadataStore.list_chunk_ids_for(address)`

- **Garbage collection for interrupted writes.** Pending files whose write was
  interrupted (client crash, timeout, etc.) are now automatically cleaned up.
  A background thread in the naming server deletes pending files older than 60
  seconds (configurable via `cleanup_max_age`). Orphaned chunk data on storage
  servers is deleted on a best-effort basis before removing the metadata.
  - Added `created_at` timestamp column to the `files` table with automatic
    migration for existing databases.
  - `MetadataStore.list_stale_pending(max_age_seconds)` returns stale pending
    files with full chunk location info.

### Fixed

- **Performance: synchronous metrics collection degraded write throughput.**  
  The `_refresh_cluster_metrics()` method in the naming server performed a full
  scan of all committed chunks (including an N+1 SQL query for replicas) on
  **every** RPC call (CreateFile, CommitFile, Heartbeat, GetFile, etc.). The
  same pattern existed in the storage server where `refresh_metrics()` did
  `os.listdir()` + `os.path.getsize()` for every chunk on every `StoreChunk`
  and `DeleteChunk`. As the number of chunks grew, each individual operation
  became linearly slower.

  - Naming server: `_refresh_cluster_metrics()` now runs on a background thread
    every 10 seconds instead of synchronously on every RPC.
  - Storage server: `refresh_metrics()` removed from `StoreChunk` and
    `DeleteChunk` hot paths; it continues to run periodically in the heartbeat
    loop (every 5 seconds). Byte counters remain incremental.
  - `MetadataStore.list_committed_chunks()`: replaced N+1 per-chunk `SELECT`
    queries with a single `LEFT JOIN replicas` query.

- **`CreateFile` timeout and message size for large files.**  
  - `create_pending()` uses `executemany` with generator expressions instead
    of per-row `execute()` calls (4M rows → single batch per table).
  - SQLite: WAL journal mode and `synchronous=NORMAL` for better concurrent
    read/write throughput during large transactions.
  - gRPC message size limit raised from 4 MB to 256 MB (a 1 GB file produces
    a ~90 MB `CreateFileResponse` with ~977K `ChunkPlacement` entries).
  - CLI: `--timeout` flag (default 10s) to extend gRPC deadline for large files.

- **Write throughput: ~193 KB/s per server → ~2–6 MB/s.**  
  The client opened a new TCP connection (`grpc.insecure_channel`) for every
  single 1 KB chunk write — ~2.93M connections for a 1 GB file with replication
  factor 3. Additionally, replicas were written sequentially (chunk → s1, then
  s2, then s3).

  - Client groups writes by target storage server and opens one persistent gRPC
    channel per server (connection reuse).
  - Replicas are uploaded to all storage servers in parallel via
    `ThreadPoolExecutor`.
  - Each server's writes remain sequential (unary gRPC calls on one channel),
    but the per-server uploads run concurrently.

## [0.1.0] — 2026-06-22

### Added

- GFS-inspired distributed file system with 1 KB fixed-size chunks.
- Naming server (metadata authority) with SQLite-backed metadata persistence.
- Storage server (chunkserver) with atomic chunk writes via temp-file rename.
- Client library with file-level read/write/delete/size/list operations.
- Configurable replication factor (default 3) with round-robin placement.
- Self-healing: periodic scan of committed chunks to repair under-replicated
  replicas after storage server failures.
- Docker Compose cluster (1 naming + 4 storage servers).
- Prometheus metrics and Grafana monitoring dashboards.
