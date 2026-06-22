# Changelog

All notable changes to the GFS distributed file system.

## [Unreleased]

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
