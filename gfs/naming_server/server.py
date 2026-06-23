"""Naming server: the single metadata authority ("master") of the GFS clone.

Responsibilities:
  * Track the live set of storage servers (registration + heartbeats).
  * Hand clients a chunk *placement plan* on create (which servers to write to).
  * Hand clients chunk *locations* on read.
  * Orchestrate deletion of a file's chunks across storage servers.
  * Answer size queries from metadata alone (no data transfer).

It never stores or proxies chunk content.
"""
from __future__ import annotations

import itertools
import logging
import os
import threading
import time
import uuid
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

import grpc

from gfs import config, metrics
from gfs._generated import gfs_pb2, gfs_pb2_grpc
from gfs.naming_server.metadata import ChunkMeta, MetadataStore

logger = logging.getLogger("naming")


class _StorageRegistry:
    """Tracks live storage servers via last-heartbeat timestamps."""

    def __init__(self, monotonic):
        self._monotonic = monotonic
        self._last_seen: dict[str, float] = {}
        self._lock = threading.Lock()
        self._rr = itertools.count()  # round-robin cursor for balancing

    def mark_alive(self, address: str) -> None:
        with self._lock:
            self._last_seen[address] = self._monotonic()

    def mark_dead(self, address: str) -> None:
        with self._lock:
            self._last_seen.pop(address, None)

    def live_servers(self) -> list[str]:
        now = self._monotonic()
        with self._lock:
            return [
                addr for addr, ts in self._last_seen.items()
                if now - ts <= config.HEARTBEAT_TIMEOUT
            ]

    def pick(self, count: int) -> list[str]:
        """Pick up to `count` distinct live servers, round-robin balanced."""
        live = self.live_servers()
        if not live:
            return []
        live.sort()
        start = next(self._rr) % len(live)
        ordered = live[start:] + live[:start]
        return ordered[:count]


class NamingServicer(gfs_pb2_grpc.NamingServerServicer):
    def __init__(self, store: MetadataStore, replication_factor: int,
                 enable_healing: bool = True,
                 heal_interval: float = config.HEAL_INTERVAL,
                 metrics_interval: float = 10.0,
                 cleanup_interval: float = 15.0,
                 cleanup_max_age: float = 60.0):
        self._store = store
        self._replication = replication_factor
        self._registry = _StorageRegistry(time.monotonic)
        self._heal_interval = heal_interval
        self._cleanup_max_age = cleanup_max_age
        self._stop_healing = threading.Event()
        if enable_healing:
            self._healer = threading.Thread(target=self._heal_loop, daemon=True)
            self._healer.start()
        # Background metrics refresh so expensive full-table scans don't block
        # every RPC (see CHANGELOG.md).
        self._metrics_interval = metrics_interval
        self._metrics_thread = threading.Thread(
            target=self._metrics_loop, daemon=True)
        self._metrics_thread.start()
        # Background cleanup of pending files left behind by interrupted writes.
        if cleanup_max_age > 0:
            self._cleanup_thread = threading.Thread(
                target=self._cleanup_loop, args=(cleanup_interval,), daemon=True)
            self._cleanup_thread.start()

    # ---------- membership ----------
    def RegisterStorage(self, request, context):
        def handle():
            self._registry.mark_alive(request.address)
            logger.info("storage server registered: %s", request.address)
            return gfs_pb2.RegisterStorageResponse(ok=True, message="registered")
        return metrics.observe_rpc("naming", "RegisterStorage", handle)

    def Heartbeat(self, request, context):
        def handle():
            self._registry.mark_alive(request.address)
            return gfs_pb2.HeartbeatResponse(ok=True)
        return metrics.observe_rpc("naming", "Heartbeat", handle)

    # ---------- self-healing ----------
    def _heal_loop(self) -> None:
        while not self._stop_healing.wait(self._heal_interval):
            try:
                repaired = self.heal_once()
                if repaired:
                    logger.info("self-healing repaired %d replica(s)",
                                repaired)
            except Exception:  # keep the background repair loop alive
                logger.exception("self-healing pass failed")

    def _metrics_loop(self) -> None:
        """Refresh Prometheus gauges on a timer to avoid scanning all chunks
        on every RPC."""
        while not self._stop_healing.wait(self._metrics_interval):
            try:
                self._refresh_cluster_metrics()
            except Exception:
                logger.exception("metrics refresh failed")

    def _cleanup_loop(self, interval: float) -> None:
        """Periodically garbage-collect pending files whose writes were
        interrupted (client crashed mid-upload).  Metadata is removed first
        so the file disappears immediately; chunk deletion on storage servers
        is best-effort and happens after."""
        while not self._stop_healing.wait(interval):
            try:
                stale = self._store.list_stale_pending(self._cleanup_max_age)
                for fm in stale:
                    self._store.delete_file(fm.filename)
                    logger.info(
                        "cleanup: removed stale pending file '%s' "
                        "(%d chunks, age > %.0fs)",
                        fm.filename, len(fm.chunks), self._cleanup_max_age)
                    # Best-effort chunk cleanup — don't block the loop.
                    for chunk in fm.chunks:
                        for addr in chunk.locations:
                            _delete_chunk_on(addr, chunk.chunk_id)
            except Exception:
                logger.exception("pending-file cleanup failed")

    def heal_once(self) -> int:
        """Repair committed chunks that are below the live replica target."""
        start = time.perf_counter()
        result = "ok"
        live = self._registry.pick(len(self._registry.live_servers()))
        live_set = set(live)
        repaired = 0
        try:
            if len(live_set) < self._replication:
                return 0

            # Steady-state short-circuit: under-replication only arises when a
            # storage server that holds committed chunks goes away.  If every
            # address referenced by committed metadata is currently live, there
            # is nothing to repair — skip the scan entirely.  (New chunks are
            # always placed on R live servers, so a fully-live cluster is
            # always fully replicated.)
            if self._store.known_replica_addresses() <= live_set:
                return 0

            # Otherwise fetch *only* the chunks that are actually below target.
            for chunk in self._store.under_replicated_chunks(
                    live_set, self._replication):
                known = list(dict.fromkeys(chunk.locations))
                live_locations = [addr for addr in known if addr in live_set]
                if len(live_locations) >= self._replication:
                    continue
                if not live_locations:
                    logger.error("chunk %s has no live replica; cannot heal",
                                 chunk.chunk_id)
                    continue

                stale_locations = [addr for addr in known if addr not in live_set]
                candidates = [addr for addr in live if addr not in known]
                while len(live_locations) < self._replication and candidates:
                    target = candidates.pop(0)
                    source = live_locations[0]
                    if _replicate_chunk_on(target, chunk.chunk_id, source):
                        self._store.add_replica(chunk.chunk_id, target)
                        live_locations.append(target)
                        repaired += 1
                        metrics.NAMING_HEAL_REPAIRS.inc()
                        # Keep metadata at exactly R replicas when replacing a
                        # dead server; stale disk chunks become harmless orphans.
                        if stale_locations:
                            self._store.remove_replica(
                                chunk.chunk_id, stale_locations.pop(0))
                    else:
                        logger.warning(
                            "replication repair failed: chunk=%s source=%s target=%s",
                            chunk.chunk_id, source, target)
            return repaired
        except Exception:
            result = "error"
            raise
        finally:
            metrics.NAMING_HEAL_PASSES.labels(result).inc()
            metrics.NAMING_HEAL_DURATION.observe(time.perf_counter() - start)
            self._refresh_cluster_metrics()

    # ---------- create ----------
    def CreateFile(self, request, context):
        def handle():
            live = self._registry.live_servers()
            if len(live) < self._replication:
                return gfs_pb2.CreateFileResponse(
                    ok=False,
                    message=(
                        f"need {self._replication} storage servers for replication, "
                        f"only {len(live)} are live"
                    ),
                )

            # Fire-and-forget old chunk deletion so overwrite doesn't block
            # the CreateFile RPC.  Best-effort — orphaned chunks that survive
            # are harmless and will be cleaned up by the storage server's
            # startup scrub or the next overwrite of this file.
            old = self._store.get_file(request.filename)
            if old is not None:
                old_chunks = [
                    (addr, chunk.chunk_id)
                    for chunk in old.chunks
                    for addr in chunk.locations
                ]
                if old_chunks:
                    threading.Thread(
                        target=_delete_chunks_best_effort,
                        args=(old_chunks, request.filename),
                        daemon=True,
                    ).start()

            placements = []
            chunks: list[ChunkMeta] = []
            for index in range(request.num_chunks):
                locations = self._registry.pick(self._replication)
                if len(locations) < self._replication:
                    return gfs_pb2.CreateFileResponse(
                        ok=False, message="not enough live storage servers")
                chunk_id = uuid.uuid4().hex
                placements.append(gfs_pb2.ChunkPlacement(
                    index=index, chunk_id=chunk_id, locations=locations))
                chunks.append(ChunkMeta(chunk_id=chunk_id, index=index,
                                        locations=list(locations)))

            self._store.create_pending(request.filename, request.size,
                                       request.num_chunks, chunks)
            logger.debug("create reserved: %s (%d chunks)", request.filename,
                         request.num_chunks)
            return gfs_pb2.CreateFileResponse(ok=True, message="reserved",
                                              placements=placements)
        return metrics.observe_rpc("naming", "CreateFile", handle)

    def CommitFile(self, request, context):
        def handle():
            ok = self._store.commit_file(request.filename)
            msg = "committed" if ok else "unknown file"
            logger.debug("commit %s: %s", request.filename, msg)
            return gfs_pb2.CommitFileResponse(ok=ok, message=msg)
        return metrics.observe_rpc("naming", "CommitFile", handle)

    # ---------- read ----------
    def GetFile(self, request, context):
        def handle():
            fm = self._store.get_file(request.filename)
            if fm is None or fm.status != "committed":
                return gfs_pb2.GetFileResponse(ok=False, message="file not found")
            live = set(self._registry.live_servers())
            placements = [
                gfs_pb2.ChunkPlacement(index=c.index, chunk_id=c.chunk_id,
                                       locations=_live_first(c.locations, live))
                for c in fm.chunks
            ]
            return gfs_pb2.GetFileResponse(ok=True, message="ok", size=fm.size,
                                           placements=placements)
        return metrics.observe_rpc("naming", "GetFile", handle)

    # ---------- delete ----------
    def DeleteFile(self, request, context):
        def handle():
            fm = self._store.get_file(request.filename)
            if fm is None:
                return gfs_pb2.DeleteFileResponse(ok=False, message="file not found")

            # Drop metadata first so the file disappears for clients immediately
            # and we don't hold the worker for the whole fan-out.
            self._store.delete_file(request.filename)

            # Best-effort: tell every replica to drop its chunk, in parallel. A
            # dead server simply misses the delete; its chunks are orphaned but
            # unreachable (the file's metadata is gone), so they are harmless.
            deletions = [
                (addr, chunk.chunk_id)
                for chunk in fm.chunks
                for addr in chunk.locations
            ]
            failures = _delete_chunks_parallel(deletions)

            msg = "deleted"
            if failures:
                msg = f"metadata deleted; {failures} replica deletes failed (orphaned)"
            logger.debug("delete %s: %s", request.filename, msg)
            return gfs_pb2.DeleteFileResponse(ok=True, message=msg)
        return metrics.observe_rpc("naming", "DeleteFile", handle)

    # ---------- size ----------
    def GetFileSize(self, request, context):
        def handle():
            fm = self._store.get_file(request.filename)
            if fm is None or fm.status != "committed":
                return gfs_pb2.GetFileSizeResponse(
                    ok=False, message="file not found")
            return gfs_pb2.GetFileSizeResponse(
                ok=True, message="ok", size=fm.size, num_chunks=fm.num_chunks)
        return metrics.observe_rpc("naming", "GetFileSize", handle)

    # ---------- list ----------
    def ListFiles(self, request, context):
        def handle():
            files = [
                gfs_pb2.FileInfo(filename=f.filename, size=f.size,
                                 num_chunks=f.num_chunks, status=f.status)
                for f in self._store.list_files()
            ]
            return gfs_pb2.ListFilesResponse(files=files)
        return metrics.observe_rpc("naming", "ListFiles", handle)

    # ---------- orphan cleanup ----------
    def ListExpectedChunks(self, request, context):
        def handle():
            ids = self._store.list_chunk_ids_for(request.address)
            return gfs_pb2.ListExpectedChunksResponse(ok=True, chunk_ids=ids)
        return metrics.observe_rpc("naming", "ListExpectedChunks", handle)

    def _refresh_cluster_metrics(self) -> None:
        """Publish cluster gauges using cheap SQL aggregates instead of
        materialising every committed chunk in Python (which used to hold the
        DB busy and starve client RPCs / heartbeats as the chunk count grew)."""
        live = set(self._registry.live_servers())
        metrics.NAMING_LIVE_STORAGE.set(len(live))

        files_by_status = self._store.count_files_by_status()
        for status in ("pending", "committed"):
            metrics.NAMING_FILES.labels(status).set(
                files_by_status.get(status, 0))
        metrics.NAMING_FILES_BYTES.set(self._store.committed_bytes())

        metrics.NAMING_COMMITTED_CHUNKS.set(
            self._store.count_committed_chunks())
        metrics.NAMING_UNDER_REPLICATED_CHUNKS.set(
            self._store.count_under_replicated(live, self._replication))


# Maximum concurrent outbound chunk-delete RPCs when removing a file.  A 1 MB
# file fans out to ~1000 chunks × R replicas; doing those sequentially on a
# single worker thread (the old behaviour) blocked the worker for the whole
# fan-out.  Reusing channels + a bounded pool keeps deletes fast and frees the
# worker quickly.
_DELETE_FANOUT = int(os.environ.get("DELETE_FANOUT", "32"))

# Cache of outbound gRPC channels to storage servers, keyed by address.  The
# naming server used to open (and immediately close) a fresh TCP connection for
# every delete/replicate RPC — thousands of handshakes per large file.  A
# persistent channel per storage server amortises that to one connection.
_CHANNEL_OPTS = [
    ("grpc.max_send_message_length", 256 * 1024 * 1024),
    ("grpc.max_receive_message_length", 256 * 1024 * 1024),
]
_channels: dict[str, grpc.Channel] = {}
_channels_lock = threading.Lock()


def _storage_channel(address: str) -> grpc.Channel:
    with _channels_lock:
        ch = _channels.get(address)
        if ch is None:
            ch = grpc.insecure_channel(address, options=_CHANNEL_OPTS)
            _channels[address] = ch
        return ch


def _delete_chunk_on(address: str, chunk_id: str) -> bool:
    try:
        stub = gfs_pb2_grpc.StorageServerStub(_storage_channel(address))
        resp = stub.DeleteChunk(
            gfs_pb2.DeleteChunkRequest(chunk_id=chunk_id), timeout=5)
        return resp.ok
    except grpc.RpcError as exc:  # server unreachable
        logger.warning("delete chunk %s on %s failed: %s", chunk_id, address,
                       exc.code())
        return False


def _delete_chunks_parallel(deletions: list[tuple[str, str]]) -> int:
    """Delete (address, chunk_id) pairs concurrently; return failure count."""
    if not deletions:
        return 0
    workers = min(_DELETE_FANOUT, len(deletions))
    failed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for ok in pool.map(lambda d: _delete_chunk_on(d[0], d[1]), deletions):
            if not ok:
                failed += 1
    return failed


def _delete_chunks_best_effort(deletions: list[tuple[str, str]],
                               filename: str) -> None:
    """Delete a list of (address, chunk_id) pairs in parallel (background)."""
    failed = _delete_chunks_parallel(deletions)
    if failed:
        logger.warning(
            "overwrite cleanup '%s': %d/%d old-chunk deletes failed",
            filename, failed, len(deletions))
    else:
        logger.info("overwrite cleanup '%s': %d old chunks purged",
                    filename, len(deletions))


def _replicate_chunk_on(target_address: str, chunk_id: str,
                        source_address: str) -> bool:
    try:
        stub = gfs_pb2_grpc.StorageServerStub(_storage_channel(target_address))
        resp = stub.ReplicateChunk(
            gfs_pb2.ReplicateChunkRequest(
                chunk_id=chunk_id, source_address=source_address),
            timeout=10,
        )
        return resp.ok
    except grpc.RpcError as exc:
        logger.warning("replicate chunk %s to %s from %s failed: %s",
                       chunk_id, target_address, source_address, exc.code())
        return False


def _live_first(locations: list[str], live: set[str]) -> list[str]:
    return (
        [addr for addr in locations if addr in live] +
        [addr for addr in locations if addr not in live]
    )


def serve() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [naming] %(levelname)s %(message)s",
    )
    port = int(os.environ.get("PORT", "50051"))
    db_path = os.environ.get("METADATA_DB", "/data/metadata.db")
    replication = int(os.environ.get(
        "REPLICATION_FACTOR", config.DEFAULT_REPLICATION_FACTOR))
    heal_interval = float(os.environ.get(
        "HEAL_INTERVAL", config.HEAL_INTERVAL))
    cleanup_max_age = float(os.environ.get(
        "CLEANUP_MAX_AGE", "60"))

    # Plenty of worker threads so a burst of slow metadata RPCs can never
    # starve the cheap membership RPCs (RegisterStorage/Heartbeat) — that
    # starvation is what made live storage servers look "dead" under load.
    max_workers = int(os.environ.get("GRPC_MAX_WORKERS", "64"))

    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    metrics.start_metrics_server_from_env()
    store = MetadataStore(db_path)

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=max_workers),
        options=[
            ("grpc.max_send_message_length", 256 * 1024 * 1024),
            ("grpc.max_receive_message_length", 256 * 1024 * 1024),
        ],
    )
    gfs_pb2_grpc.add_NamingServerServicer_to_server(
        NamingServicer(store, replication, heal_interval=heal_interval,
                       cleanup_max_age=cleanup_max_age), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(
        "naming server listening on :%d (replication=%d, heal_interval=%.1fs, "
        "workers=%d, db=%s)",
        port, replication, heal_interval, max_workers, db_path)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
