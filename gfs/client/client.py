"""Client library: hides distribution, sharding and replication from the user.

The user works with whole files; the library handles splitting into 1 KB
chunks, talking to the naming server for placement/locations, and reading or
writing chunks directly to/from the storage servers (GFS-style: metadata via
the master, bulk data straight to the chunkservers).
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc

from gfs import config
from gfs._generated import gfs_pb2, gfs_pb2_grpc

# Number of chunks to pack into a single StoreChunks RPC.  At 1 KB per chunk
# this is ~1 MB of payload per batch — large enough to amortise gRPC overhead
# but well under the 256 MB message limit.
_BATCH_SIZE = 1024


class GFSError(Exception):
    """Raised when an operation cannot be completed."""


class _ChannelCache:
    """Thread-safe cache of gRPC channels keyed by address.

    Reusing channels avoids paying the TCP handshake on every operation and
    lets HTTP/2 connection reuse kick in across calls.
    """

    def __init__(self, options: list[tuple[str, int]]):
        self._options = options
        self._channels: dict[str, grpc.Channel] = {}
        self._lock = threading.Lock()

    def get(self, addr: str) -> grpc.Channel:
        with self._lock:
            ch = self._channels.get(addr)
            if ch is None:
                ch = grpc.insecure_channel(addr, options=self._options)
                self._channels[addr] = ch
            return ch

    def close_all(self) -> None:
        with self._lock:
            for ch in self._channels.values():
                ch.close()
            self._channels.clear()


class GFSClient:
    def __init__(self, naming_addr: str, timeout: float = 10.0):
        self._naming_addr = naming_addr
        self._timeout = timeout
        opts = self._channel_options()
        self._channels = _ChannelCache(opts)
        self._naming_channel = self._channels.get(naming_addr)
        self._naming_stub = gfs_pb2_grpc.NamingServerStub(self._naming_channel)

    # ---- helpers ----
    @staticmethod
    def _channel_options():
        # Default gRPC message limit is 4 MB.  A CreateFileResponse for a 1 GB
        # file contains ~977K ChunkPlacement entries and weighs ~90 MB, so bump
        # the limit to 256 MB.
        return [
            ("grpc.max_send_message_length", 256 * 1024 * 1024),
            ("grpc.max_receive_message_length", 256 * 1024 * 1024),
        ]

    @staticmethod
    def _split_chunks(data: bytes) -> list[bytes]:
        return [data[i:i + config.CHUNK_SIZE]
                for i in range(0, len(data), config.CHUNK_SIZE)]

    # ---- operations ----
    def create(self, filename: str, data: bytes) -> None:
        """Split `data` into 1 KB chunks and store it under `filename`."""
        chunks = self._split_chunks(data)

        resp = self._naming_stub.CreateFile(
            gfs_pb2.CreateFileRequest(
                filename=filename, size=len(data), num_chunks=len(chunks)),
            timeout=self._timeout)
        if not resp.ok:
            raise GFSError(f"create rejected: {resp.message}")

        # Group writes by target storage server so we can send one batched
        # StoreChunks RPC per server instead of one RPC per 1 KB chunk.
        by_addr: dict[str, list[tuple[str, bytes]]] = {}
        for placement in resp.placements:
            payload = chunks[placement.index]
            for addr in placement.locations:
                by_addr.setdefault(addr, []).append(
                    (placement.chunk_id, payload))

        # Upload to all storage servers in parallel.  Each server gets its
        # chunks in one or more batched StoreChunks RPCs, dramatically
        # reducing per-message gRPC overhead.
        def _upload(addr: str, writes: list[tuple[str, bytes]]) -> None:
            ch = self._channels.get(addr)
            stub = gfs_pb2_grpc.StorageServerStub(ch)

            for i in range(0, len(writes), _BATCH_SIZE):
                batch = writes[i:i + _BATCH_SIZE]
                chunk_msgs = [
                    gfs_pb2.ChunkData(chunk_id=cid, data=payload)
                    for cid, payload in batch
                ]
                resp = stub.StoreChunks(
                    gfs_pb2.StoreChunksRequest(chunks=chunk_msgs),
                    timeout=60,
                )
                if not resp.ok:
                    raise GFSError(
                        f"batch write failed on {addr}: {resp.message}")

        with ThreadPoolExecutor(max_workers=len(by_addr)) as pool:
            futs = {
                pool.submit(_upload, addr, writes): addr
                for addr, writes in by_addr.items()
            }
            for fut in as_completed(futs):
                fut.result()  # raises on first failure, cancels the rest

        commit = self._naming_stub.CommitFile(
            gfs_pb2.CommitFileRequest(filename=filename),
            timeout=self._timeout)
        if not commit.ok:
            raise GFSError(f"commit failed: {commit.message}")

    def read(self, filename: str) -> bytes:
        """Fetch and reassemble the full contents of `filename`."""
        resp = self._naming_stub.GetFile(
            gfs_pb2.GetFileRequest(filename=filename),
            timeout=self._timeout)
        if not resp.ok:
            raise GFSError(resp.message)

        placements = sorted(resp.placements, key=lambda p: p.index)

        def _fetch_one(placement):
            data = self._fetch_chunk_any(placement)
            if data is None:
                raise GFSError(
                    f"chunk {placement.index} unavailable: all "
                    f"{len(placement.locations)} replicas unreachable")
            return placement.index, data

        # Pipeline up to 16 concurrent chunk reads (HTTP/2 multiplexing).
        with ThreadPoolExecutor(max_workers=min(16, len(placements))) as pool:
            results = list(pool.map(_fetch_one, placements))

        # Reassemble in chunk-index order.
        results.sort(key=lambda r: r[0])
        return b"".join(data for _, data in results)

    def _fetch_chunk_any(self, placement) -> bytes | None:
        """Try each replica in turn; return the first that responds."""
        for addr in placement.locations:
            data = _get_chunk(
                self._channels.get(addr), placement.chunk_id)
            if data is not None:
                return data
        return None

    def delete(self, filename: str) -> str:
        resp = self._naming_stub.DeleteFile(
            gfs_pb2.DeleteFileRequest(filename=filename),
            timeout=self._timeout)
        if not resp.ok:
            raise GFSError(resp.message)
        return resp.message

    def size(self, filename: str) -> tuple[int, int]:
        """Return (size_bytes, num_chunks) from metadata only."""
        resp = self._naming_stub.GetFileSize(
            gfs_pb2.GetFileSizeRequest(filename=filename),
            timeout=self._timeout)
        if not resp.ok:
            raise GFSError(resp.message)
        return resp.size, resp.num_chunks

    def list_files(self) -> list[tuple[str, int, int, str]]:
        resp = self._naming_stub.ListFiles(gfs_pb2.ListFilesRequest(),
                                           timeout=self._timeout)
        return [(f.filename, f.size, f.num_chunks, f.status)
                for f in resp.files]

    def close(self) -> None:
        self._channels.close_all()


def _get_chunk(channel: grpc.Channel, chunk_id: str) -> bytes | None:
    try:
        stub = gfs_pb2_grpc.StorageServerStub(channel)
        resp = stub.GetChunk(
            gfs_pb2.GetChunkRequest(chunk_id=chunk_id), timeout=10)
        return resp.data if resp.ok else None
    except grpc.RpcError:
        return None
