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
from collections import defaultdict
from concurrent import futures

import grpc

from gfs import config
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
                 heal_interval: float = config.HEAL_INTERVAL):
        self._store = store
        self._replication = replication_factor
        self._registry = _StorageRegistry(time.monotonic)
        self._heal_interval = heal_interval
        self._stop_healing = threading.Event()
        if enable_healing:
            self._healer = threading.Thread(target=self._heal_loop, daemon=True)
            self._healer.start()

    # ---------- membership ----------
    def RegisterStorage(self, request, context):
        self._registry.mark_alive(request.address)
        logger.info("storage server registered: %s", request.address)
        return gfs_pb2.RegisterStorageResponse(ok=True, message="registered")

    def Heartbeat(self, request, context):
        self._registry.mark_alive(request.address)
        return gfs_pb2.HeartbeatResponse(ok=True)

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

    def heal_once(self) -> int:
        """Repair committed chunks that are below the live replica target."""
        live = self._registry.pick(len(self._registry.live_servers()))
        live_set = set(live)
        if len(live_set) < self._replication:
            return 0

        repaired = 0
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

    # ---------- create ----------
    def CreateFile(self, request, context):
        live = self._registry.live_servers()
        if len(live) < self._replication:
            return gfs_pb2.CreateFileResponse(
                ok=False,
                message=(
                    f"need {self._replication} storage servers for replication, "
                    f"only {len(live)} are live"
                ),
            )

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

    def CommitFile(self, request, context):
        ok = self._store.commit_file(request.filename)
        msg = "committed" if ok else "unknown file"
        logger.info("commit %s: %s", request.filename, msg)
        return gfs_pb2.CommitFileResponse(ok=ok, message=msg)

    # ---------- read ----------
    def GetFile(self, request, context):
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

    # ---------- delete ----------
    def DeleteFile(self, request, context):
        fm = self._store.get_file(request.filename)
        if fm is None:
            return gfs_pb2.DeleteFileResponse(ok=False, message="file not found")

        # Best-effort: tell every replica to drop its chunk. A dead server
        # simply misses the delete; its chunks are orphaned but unreachable
        # (the file's metadata is gone), so they are harmless.
        by_address = defaultdict(list)
        for chunk in fm.chunks:
            for addr in chunk.locations:
                by_address[addr].append(chunk.chunk_id)

        failures = 0
        workers = min(config.DEFAULT_CLIENT_MAX_WORKERS,
                      max(1, len(by_address)))
        with futures.ThreadPoolExecutor(max_workers=workers) as pool:
            jobs = {
                pool.submit(_delete_chunks_on, addr, chunk_ids): (addr, chunk_ids)
                for addr, chunk_ids in by_address.items()
            }
            for job in futures.as_completed(jobs):
                _addr, chunk_ids = jobs[job]
                if not job.result():
                    failures += len(chunk_ids)

        self._store.delete_file(request.filename)
        msg = "deleted"
        if failures:
            msg = f"metadata deleted; {failures} replica deletes failed (orphaned)"
        logger.info("delete %s: %s", request.filename, msg)
        return gfs_pb2.DeleteFileResponse(ok=True, message=msg)

    # ---------- size ----------
    def GetFileSize(self, request, context):
        fm = self._store.get_file(request.filename)
        if fm is None or fm.status != "committed":
            return gfs_pb2.GetFileSizeResponse(ok=False, message="file not found")
        return gfs_pb2.GetFileSizeResponse(ok=True, message="ok", size=fm.size,
                                           num_chunks=fm.num_chunks)

    # ---------- list ----------
    def ListFiles(self, request, context):
        files = [
            gfs_pb2.FileInfo(filename=f.filename, size=f.size,
                             num_chunks=f.num_chunks, status=f.status)
            for f in self._store.list_files()
        ]
        return gfs_pb2.ListFilesResponse(files=files)


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


def _delete_chunks_on(address: str, chunk_ids: list[str]) -> bool:
    try:
        with grpc.insecure_channel(address) as channel:
            stub = gfs_pb2_grpc.StorageServerStub(channel)
            resp = stub.DeleteChunks(
                gfs_pb2.DeleteChunksRequest(chunk_ids=chunk_ids), timeout=10)
            return resp.ok
    except grpc.RpcError as exc:
        logger.warning("delete chunks on %s failed: %s", address, exc.code())
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
    store = MetadataStore(db_path)

    workers = int(os.environ.get("GRPC_MAX_WORKERS",
                                 str(config.DEFAULT_GRPC_MAX_WORKERS)))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    gfs_pb2_grpc.add_NamingServerServicer_to_server(
        NamingServicer(store, replication, heal_interval=heal_interval), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(
        "naming server listening on :%d (replication=%d, heal_interval=%.1fs, db=%s, workers=%d)",
        port, replication, heal_interval, db_path, workers)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
