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
                 metrics_interval: float = 10.0):
        self._store = store
        self._replication = replication_factor
        self._registry = _StorageRegistry(time.monotonic)
        self._heal_interval = heal_interval
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

            for chunk in self._store.list_committed_chunks():
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

            # Delete old chunks from storage servers before overwriting.
            # Otherwise overwritten chunks become orphaned on disk (unreachable
            # by metadata but never freed).
            old = self._store.get_file(request.filename)
            if old is not None:
                for chunk in old.chunks:
                    for addr in chunk.locations:
                        _delete_chunk_on(addr, chunk.chunk_id)
                logger.info("overwrite cleanup: %s (%d chunks purged)",
                            request.filename, len(old.chunks))

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
            logger.info("create reserved: %s (%d chunks)", request.filename,
                        request.num_chunks)
            return gfs_pb2.CreateFileResponse(ok=True, message="reserved",
                                              placements=placements)
        return metrics.observe_rpc("naming", "CreateFile", handle)

    def CommitFile(self, request, context):
        def handle():
            ok = self._store.commit_file(request.filename)
            msg = "committed" if ok else "unknown file"
            logger.info("commit %s: %s", request.filename, msg)
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

            # Best-effort: tell every replica to drop its chunk. A dead server
            # simply misses the delete; its chunks are orphaned but unreachable
            # (the file's metadata is gone), so they are harmless.
            failures = 0
            for chunk in fm.chunks:
                for addr in chunk.locations:
                    if not _delete_chunk_on(addr, chunk.chunk_id):
                        failures += 1

            self._store.delete_file(request.filename)
            msg = "deleted"
            if failures:
                msg = f"metadata deleted; {failures} replica deletes failed (orphaned)"
            logger.info("delete %s: %s", request.filename, msg)
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

    def _refresh_cluster_metrics(self) -> None:
        live = set(self._registry.live_servers())
        metrics.NAMING_LIVE_STORAGE.set(len(live))

        files_by_status = {"pending": 0, "committed": 0}
        for file_meta in self._store.list_files():
            files_by_status[file_meta.status] = (
                files_by_status.get(file_meta.status, 0) + 1
            )
        for status, count in files_by_status.items():
            metrics.NAMING_FILES.labels(status).set(count)

        committed_chunks = self._store.list_committed_chunks()
        metrics.NAMING_COMMITTED_CHUNKS.set(len(committed_chunks))
        under_replicated = 0
        for chunk in committed_chunks:
            live_replicas = {addr for addr in chunk.locations if addr in live}
            if len(live_replicas) < self._replication:
                under_replicated += 1
        metrics.NAMING_UNDER_REPLICATED_CHUNKS.set(under_replicated)


def _delete_chunk_on(address: str, chunk_id: str) -> bool:
    try:
        with grpc.insecure_channel(address) as channel:
            stub = gfs_pb2_grpc.StorageServerStub(channel)
            resp = stub.DeleteChunk(
                gfs_pb2.DeleteChunkRequest(chunk_id=chunk_id), timeout=5)
            return resp.ok
    except grpc.RpcError as exc:  # server unreachable
        logger.warning("delete chunk %s on %s failed: %s", chunk_id, address,
                       exc.code())
        return False


def _replicate_chunk_on(target_address: str, chunk_id: str,
                        source_address: str) -> bool:
    try:
        with grpc.insecure_channel(target_address) as channel:
            stub = gfs_pb2_grpc.StorageServerStub(channel)
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

    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    metrics.start_metrics_server_from_env()
    store = MetadataStore(db_path)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    gfs_pb2_grpc.add_NamingServerServicer_to_server(
        NamingServicer(store, replication, heal_interval=heal_interval), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(
        "naming server listening on :%d (replication=%d, heal_interval=%.1fs, db=%s)",
        port, replication, heal_interval, db_path)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
