"""Storage server ("chunkserver"): stores raw chunk bytes on local disk.

Each storage server holds only the chunks assigned to it by the naming
server's placement decisions — never the whole dataset. Chunk content is
written to the local file system (one file per chunk), satisfying the
"content lives in the file system, not the database" requirement.

On startup it registers with the naming server and then sends periodic
heartbeats so the master knows it is alive.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from concurrent import futures

import grpc

from gfs import config, metrics
from gfs._generated import gfs_pb2, gfs_pb2_grpc

logger = logging.getLogger("storage")


class StorageServicer(gfs_pb2_grpc.StorageServerServicer):
    def __init__(self, data_dir: str, address: str = "in-process"):
        self._data_dir = data_dir
        self._address = address
        os.makedirs(data_dir, exist_ok=True)
        # Incremental counters so refresh_metrics() never scans the whole
        # directory on every heartbeat (O(chunks) → O(1)).
        self._chunk_count = 0
        self._total_bytes = 0
        self._counters_lock = threading.Lock()
        self._sync_counters_from_disk()

    def _path(self, chunk_id: str) -> str:
        # chunk_id is a uuid hex string -> safe as a filename.
        return os.path.join(self._data_dir, f"{chunk_id}.chunk")

    def held_chunk_ids(self) -> list[str]:
        return [f[:-6] for f in os.listdir(self._data_dir)
                if f.endswith(".chunk")]

    def _sync_counters_from_disk(self) -> None:
        """One-shot: scan disk to initialise counters (only at startup)."""
        chunks = self.held_chunk_ids()
        total = 0
        for cid in chunks:
            try:
                total += os.path.getsize(self._path(cid))
            except FileNotFoundError:
                pass
        self._chunk_count = len(chunks)
        self._total_bytes = total
        self._publish_metrics()

    def _publish_metrics(self) -> None:
        metrics.STORAGE_CHUNKS.labels(self._address).set(self._chunk_count)
        metrics.STORAGE_BYTES.labels(self._address).set(self._total_bytes)

    def refresh_metrics(self) -> None:
        """Publish current counters (O(1) — no disk scan)."""
        self._publish_metrics()

    def StoreChunk(self, request, context):
        def handle():
            path = self._path(request.chunk_id)
            size = len(request.data)
            try:
                # Each chunk_id is a unique UUID — writes to different chunks
                # never conflict, so no global lock is needed.  temp-file +
                # atomic rename is safe even when two threads write to the same
                # chunk_id (the last rename wins, which is fine for a
                # fixed-content idempotent write).
                try:
                    old_size = os.path.getsize(path)
                except FileNotFoundError:
                    old_size = -1  # new chunk

                tmp = path + ".tmp"
                with open(tmp, "wb") as fh:
                    fh.write(request.data)
                os.replace(tmp, path)

                with self._counters_lock:
                    if old_size == -1:
                        self._chunk_count += 1
                        self._total_bytes += size
                    else:
                        self._total_bytes += size - old_size
                metrics.STORAGE_CHUNK_BYTES_WRITTEN.labels(
                    self._address).inc(size)
                logger.info("stored chunk %s (%d bytes)", request.chunk_id, size)
                return gfs_pb2.StoreChunkResponse(ok=True, message="stored")
            except OSError as exc:
                logger.error("store chunk %s failed: %s", request.chunk_id, exc)
                return gfs_pb2.StoreChunkResponse(ok=False, message=str(exc))
        return metrics.observe_rpc("storage", "StoreChunk", handle)

    def StoreChunks(self, request, context):
        def handle():
            stored = 0
            total_bytes = 0
            for chunk in request.chunks:
                path = self._path(chunk.chunk_id)
                size = len(chunk.data)
                try:
                    try:
                        old_size = os.path.getsize(path)
                    except FileNotFoundError:
                        old_size = -1

                    tmp = path + ".tmp"
                    with open(tmp, "wb") as fh:
                        fh.write(chunk.data)
                    os.replace(tmp, path)

                    with self._counters_lock:
                        if old_size == -1:
                            self._chunk_count += 1
                        self._total_bytes += size - max(old_size, 0)
                    stored += 1
                    total_bytes += size
                except OSError as exc:
                    logger.error("store chunk %s failed: %s", chunk.chunk_id, exc)
                    return gfs_pb2.StoreChunksResponse(
                        ok=False, message=f"chunk {chunk.chunk_id[:8]}…: {exc}",
                        stored=stored)

            if total_bytes:
                metrics.STORAGE_CHUNK_BYTES_WRITTEN.labels(
                    self._address).inc(total_bytes)
            logger.info("stored %d chunks (%d bytes) via batch", stored, total_bytes)
            return gfs_pb2.StoreChunksResponse(
                ok=True, message=f"stored {stored}", stored=stored)
        return metrics.observe_rpc("storage", "StoreChunks", handle)

    def GetChunk(self, request, context):
        def handle():
            path = self._path(request.chunk_id)
            try:
                with open(path, "rb") as fh:
                    data = fh.read()
                metrics.STORAGE_CHUNK_BYTES_READ.labels(
                    self._address).inc(len(data))
                return gfs_pb2.GetChunkResponse(ok=True, message="ok", data=data)
            except FileNotFoundError:
                return gfs_pb2.GetChunkResponse(
                    ok=False, message="chunk not found")
            except OSError as exc:
                return gfs_pb2.GetChunkResponse(ok=False, message=str(exc))
        return metrics.observe_rpc("storage", "GetChunk", handle)

    def DeleteChunk(self, request, context):
        def handle():
            path = self._path(request.chunk_id)
            try:
                old_size = os.path.getsize(path)
                os.remove(path)
                with self._counters_lock:
                    self._chunk_count -= 1
                    self._total_bytes -= old_size
                logger.info("deleted chunk %s", request.chunk_id)
            except FileNotFoundError:
                pass  # already gone; deletion is idempotent
            return gfs_pb2.DeleteChunkResponse(ok=True, message="deleted")
        return metrics.observe_rpc("storage", "DeleteChunk", handle)

    def ReplicateChunk(self, request, context):
        def handle():
            data = _get_chunk_from(request.source_address, request.chunk_id)
            if data is None:
                return gfs_pb2.ReplicateChunkResponse(
                    ok=False, message="source chunk unavailable")

            stored = self.StoreChunk(
                gfs_pb2.StoreChunkRequest(chunk_id=request.chunk_id, data=data),
                context,
            )
            return gfs_pb2.ReplicateChunkResponse(
                ok=stored.ok, message=stored.message)
        return metrics.observe_rpc("storage", "ReplicateChunk", handle)


def _get_chunk_from(address: str, chunk_id: str) -> bytes | None:
    try:
        with grpc.insecure_channel(address) as channel:
            stub = gfs_pb2_grpc.StorageServerStub(channel)
            resp = stub.GetChunk(
                gfs_pb2.GetChunkRequest(chunk_id=chunk_id), timeout=10)
            return resp.data if resp.ok else None
    except grpc.RpcError as exc:
        logger.warning("fetch chunk %s from %s failed: %s", chunk_id, address,
                       exc.code())
        return None


def _cleanup_orphans_once(naming_addr: str, self_addr: str,
                          servicer: StorageServicer) -> None:
    """One-shot: delete chunks on disk that the naming server does not know
    about (orphans left behind by self-healing replica migration)."""
    time.sleep(2)  # Give the heartbeat loop time to register.
    try:
        with grpc.insecure_channel(
            naming_addr,
            options=[
                ("grpc.max_receive_message_length", 256 * 1024 * 1024),
                ("grpc.max_send_message_length", 256 * 1024 * 1024),
            ],
        ) as channel:
            stub = gfs_pb2_grpc.NamingServerStub(channel)
            resp = stub.ListExpectedChunks(
                gfs_pb2.ListExpectedChunksRequest(address=self_addr),
                timeout=30,
            )
            if not resp.ok:
                return
            expected = set(resp.chunk_ids)
            on_disk = set(servicer.held_chunk_ids())
            orphans = on_disk - expected
            if not orphans:
                return
            for chunk_id in orphans:
                try:
                    os.remove(servicer._path(chunk_id))
                except FileNotFoundError:
                    pass
            logger.info(
                "orphan cleanup: removed %d chunk(s) no longer in metadata",
                len(orphans),
            )
            # Re-sync counters once after cleanup (one-time cost at startup).
            servicer._sync_counters_from_disk()
    except grpc.RpcError as exc:
        logger.warning("orphan cleanup skipped (naming unreachable): %s",
                       exc.code())


def _heartbeat_loop(naming_addr: str, self_addr: str,
                    servicer: StorageServicer) -> None:
    """Register once, then heartbeat forever so the master tracks liveness."""
    while True:
        try:
            with grpc.insecure_channel(naming_addr) as channel:
                stub = gfs_pb2_grpc.NamingServerStub(channel)
                stub.RegisterStorage(
                    gfs_pb2.RegisterStorageRequest(address=self_addr),
                    timeout=5)
                while True:
                    stub.Heartbeat(
                        gfs_pb2.HeartbeatRequest(
                            address=self_addr,
                            # naming server ignores chunk_ids; skip O(n)
                            # listdir on every heartbeat.
                            chunk_ids=[]),
                        timeout=5)
                    servicer.refresh_metrics()
                    metrics.STORAGE_HEARTBEATS.labels(self_addr).inc()
                    time.sleep(config.HEARTBEAT_INTERVAL)
        except grpc.RpcError as exc:
            logger.warning("naming server unreachable (%s); retrying",
                           exc.code())
            time.sleep(config.HEARTBEAT_INTERVAL)


def serve() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [storage] %(levelname)s %(message)s",
    )
    port = int(os.environ.get("PORT", "50061"))
    data_dir = os.environ.get("DATA_DIR", "/data/chunks")
    naming_addr = os.environ.get("NAMING_SERVER", "naming:50051")
    # Address other containers/clients use to reach this server.
    self_addr = os.environ.get("ADVERTISE_ADDR", f"localhost:{port}")

    metrics.start_metrics_server_from_env()
    servicer = StorageServicer(data_dir, self_addr)
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16),
        options=[
            ("grpc.max_send_message_length", 256 * 1024 * 1024),
            ("grpc.max_receive_message_length", 256 * 1024 * 1024),
        ],
    )
    gfs_pb2_grpc.add_StorageServerServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info("storage server listening on :%d (advertise=%s, data=%s)",
                port, self_addr, data_dir)

    hb = threading.Thread(
        target=_heartbeat_loop,
        args=(naming_addr, self_addr, servicer),
        daemon=True,
    )
    hb.start()

    # One-shot orphan cleanup after registration.
    cleanup = threading.Thread(
        target=_cleanup_orphans_once,
        args=(naming_addr, self_addr, servicer),
        daemon=True,
    )
    cleanup.start()

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
