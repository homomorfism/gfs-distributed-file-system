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

from gfs import config
from gfs._generated import gfs_pb2, gfs_pb2_grpc

logger = logging.getLogger("storage")


class StorageServicer(gfs_pb2_grpc.StorageServerServicer):
    def __init__(self, data_dir: str):
        self._data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self._lock = threading.Lock()

    def _path(self, chunk_id: str) -> str:
        # chunk_id is a uuid hex string -> safe as a filename.
        return os.path.join(self._data_dir, f"{chunk_id}.chunk")

    def held_chunk_ids(self) -> list[str]:
        return [f[:-6] for f in os.listdir(self._data_dir)
                if f.endswith(".chunk")]

    def StoreChunk(self, request, context):
        path = self._path(request.chunk_id)
        try:
            with self._lock:
                # Atomic write: temp file then rename.
                tmp = path + ".tmp"
                with open(tmp, "wb") as fh:
                    fh.write(request.data)
                os.replace(tmp, path)
            logger.info("stored chunk %s (%d bytes)", request.chunk_id,
                        len(request.data))
            return gfs_pb2.StoreChunkResponse(ok=True, message="stored")
        except OSError as exc:
            logger.error("store chunk %s failed: %s", request.chunk_id, exc)
            return gfs_pb2.StoreChunkResponse(ok=False, message=str(exc))

    def GetChunk(self, request, context):
        path = self._path(request.chunk_id)
        try:
            with open(path, "rb") as fh:
                data = fh.read()
            return gfs_pb2.GetChunkResponse(ok=True, message="ok", data=data)
        except FileNotFoundError:
            return gfs_pb2.GetChunkResponse(ok=False, message="chunk not found")
        except OSError as exc:
            return gfs_pb2.GetChunkResponse(ok=False, message=str(exc))

    def DeleteChunk(self, request, context):
        path = self._path(request.chunk_id)
        try:
            os.remove(path)
            logger.info("deleted chunk %s", request.chunk_id)
        except FileNotFoundError:
            pass  # already gone; deletion is idempotent
        return gfs_pb2.DeleteChunkResponse(ok=True, message="deleted")

    def ReplicateChunk(self, request, context):
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
                            chunk_ids=servicer.held_chunk_ids()),
                        timeout=5)
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

    servicer = StorageServicer(data_dir)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
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
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
