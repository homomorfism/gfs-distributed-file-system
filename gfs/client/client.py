"""Client library: hides distribution, sharding and replication from the user.

The user works with whole files; the library handles splitting into 1 KB
chunks, talking to the naming server for placement/locations, and reading or
writing chunks directly to/from the storage servers (GFS-style: metadata via
the master, bulk data straight to the chunkservers).
"""
from __future__ import annotations

import grpc

from gfs import config
from gfs._generated import gfs_pb2, gfs_pb2_grpc


class GFSError(Exception):
    """Raised when an operation cannot be completed."""


class GFSClient:
    def __init__(self, naming_addr: str, timeout: float = 10.0):
        self._naming_addr = naming_addr
        self._timeout = timeout

    # ---- helpers ----
    def _naming(self):
        channel = grpc.insecure_channel(self._naming_addr)
        return channel, gfs_pb2_grpc.NamingServerStub(channel)

    @staticmethod
    def _split_chunks(data: bytes) -> list[bytes]:
        return [data[i:i + config.CHUNK_SIZE]
                for i in range(0, len(data), config.CHUNK_SIZE)]

    # ---- operations ----
    def create(self, filename: str, data: bytes) -> None:
        """Split `data` into 1 KB chunks and store it under `filename`."""
        chunks = self._split_chunks(data)
        channel, naming = self._naming()
        try:
            resp = naming.CreateFile(
                gfs_pb2.CreateFileRequest(
                    filename=filename, size=len(data), num_chunks=len(chunks)),
                timeout=self._timeout)
            if not resp.ok:
                raise GFSError(f"create rejected: {resp.message}")

            # Upload every chunk to *all* of its assigned replica locations.
            # All replicas must accept the write, otherwise the configured
            # replication factor would not be met.
            for placement in resp.placements:
                payload = chunks[placement.index]
                for addr in placement.locations:
                    if not _store_chunk(addr, placement.chunk_id, payload):
                        raise GFSError(
                            f"failed to write chunk {placement.index} "
                            f"replica on {addr}; aborting")

            commit = naming.CommitFile(
                gfs_pb2.CommitFileRequest(filename=filename),
                timeout=self._timeout)
            if not commit.ok:
                raise GFSError(f"commit failed: {commit.message}")
        finally:
            channel.close()

    def read(self, filename: str) -> bytes:
        """Fetch and reassemble the full contents of `filename`."""
        channel, naming = self._naming()
        try:
            resp = naming.GetFile(
                gfs_pb2.GetFileRequest(filename=filename),
                timeout=self._timeout)
            if not resp.ok:
                raise GFSError(resp.message)

            parts: list[bytes] = []
            for placement in sorted(resp.placements, key=lambda p: p.index):
                data = self._fetch_chunk_any(placement)
                if data is None:
                    raise GFSError(
                        f"chunk {placement.index} unavailable: all "
                        f"{len(placement.locations)} replicas unreachable")
                parts.append(data)
            return b"".join(parts)
        finally:
            channel.close()

    @staticmethod
    def _fetch_chunk_any(placement) -> bytes | None:
        """Try each replica in turn; return the first that responds."""
        for addr in placement.locations:
            data = _get_chunk(addr, placement.chunk_id)
            if data is not None:
                return data
        return None

    def delete(self, filename: str) -> str:
        channel, naming = self._naming()
        try:
            resp = naming.DeleteFile(
                gfs_pb2.DeleteFileRequest(filename=filename),
                timeout=self._timeout)
            if not resp.ok:
                raise GFSError(resp.message)
            return resp.message
        finally:
            channel.close()

    def size(self, filename: str) -> tuple[int, int]:
        """Return (size_bytes, num_chunks) from metadata only."""
        channel, naming = self._naming()
        try:
            resp = naming.GetFileSize(
                gfs_pb2.GetFileSizeRequest(filename=filename),
                timeout=self._timeout)
            if not resp.ok:
                raise GFSError(resp.message)
            return resp.size, resp.num_chunks
        finally:
            channel.close()

    def list_files(self) -> list[tuple[str, int, int, str]]:
        channel, naming = self._naming()
        try:
            resp = naming.ListFiles(gfs_pb2.ListFilesRequest(),
                                    timeout=self._timeout)
            return [(f.filename, f.size, f.num_chunks, f.status)
                    for f in resp.files]
        finally:
            channel.close()


def _store_chunk(address: str, chunk_id: str, data: bytes) -> bool:
    try:
        with grpc.insecure_channel(address) as channel:
            stub = gfs_pb2_grpc.StorageServerStub(channel)
            resp = stub.StoreChunk(
                gfs_pb2.StoreChunkRequest(chunk_id=chunk_id, data=data),
                timeout=10)
            return resp.ok
    except grpc.RpcError:
        return False


def _get_chunk(address: str, chunk_id: str) -> bytes | None:
    try:
        with grpc.insecure_channel(address) as channel:
            stub = gfs_pb2_grpc.StorageServerStub(channel)
            resp = stub.GetChunk(
                gfs_pb2.GetChunkRequest(chunk_id=chunk_id), timeout=10)
            return resp.data if resp.ok else None
    except grpc.RpcError:
        return None
