"""Client library: hides distribution, sharding and replication from the user.

The user works with whole files; the library handles splitting into 1 KB
chunks, talking to the naming server for placement/locations, and reading or
writing chunks directly to/from the storage servers (GFS-style: metadata via
the master, bulk data straight to the chunkservers).
"""
from __future__ import annotations

from collections import defaultdict
from concurrent import futures
import os

import grpc

from gfs import config
from gfs._generated import gfs_pb2, gfs_pb2_grpc


class GFSError(Exception):
    """Raised when an operation cannot be completed."""


class GFSClient:
    def __init__(self, naming_addr: str, timeout: float = 30.0,
                 max_workers: int | None = None):
        self._naming_addr = naming_addr
        self._timeout = timeout
        self._max_workers = max_workers or int(os.environ.get(
            "CLIENT_MAX_WORKERS", str(config.DEFAULT_CLIENT_MAX_WORKERS)))

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

            # Upload every chunk to all replicas, grouped by storage server.
            # For a 1 MB file this turns thousands of tiny RPCs into one bulk
            # request per storage node, which keeps 100-user simulations from
            # leaving many writes stuck in "pending".
            failures = self._store_all_replicas(resp.placements, chunks)
            if failures:
                self._abort_pending(naming, filename)
                raise GFSError(
                    f"failed to write replicas on {', '.join(failures)}; "
                    "aborted pending file")

            commit = naming.CommitFile(
                gfs_pb2.CommitFileRequest(filename=filename),
                timeout=self._timeout)
            if not commit.ok:
                self._abort_pending(naming, filename)
                raise GFSError(f"commit failed: {commit.message}")
        finally:
            channel.close()

    def _store_all_replicas(self, placements, chunks: list[bytes]) -> list[str]:
        by_address = defaultdict(list)
        for placement in placements:
            payload = chunks[placement.index]
            for addr in placement.locations:
                by_address[addr].append(gfs_pb2.ChunkData(
                    chunk_id=placement.chunk_id, data=payload))

        failures = []
        workers = min(self._max_workers, max(1, len(by_address)))
        with futures.ThreadPoolExecutor(max_workers=workers) as pool:
            jobs = {
                pool.submit(_store_chunks, addr, batch, self._timeout): addr
                for addr, payloads in by_address.items()
                for batch in _chunk_data_batches(payloads)
            }
            for job in futures.as_completed(jobs):
                addr = jobs[job]
                if not job.result():
                    failures.append(addr)
        return sorted(set(failures))

    def _abort_pending(self, naming, filename: str) -> None:
        try:
            naming.DeleteFile(
                gfs_pb2.DeleteFileRequest(filename=filename),
                timeout=self._timeout)
        except grpc.RpcError:
            pass

    def read(self, filename: str) -> bytes:
        """Fetch and reassemble the full contents of `filename`."""
        channel, naming = self._naming()
        try:
            resp = naming.GetFile(
                gfs_pb2.GetFileRequest(filename=filename),
                timeout=self._timeout)
            if not resp.ok:
                raise GFSError(resp.message)

            placements = sorted(resp.placements, key=lambda p: p.index)
            parts = self._fetch_chunks(placements)
            return b"".join(parts)
        finally:
            channel.close()

    def _fetch_chunks(self, placements) -> list[bytes]:
        parts: list[bytes | None] = [None] * len(placements)
        index_by_chunk = {p.chunk_id: i for i, p in enumerate(placements)}
        tried = {p.chunk_id: set() for p in placements}

        while any(part is None for part in parts):
            by_address = defaultdict(list)
            for placement in placements:
                idx = index_by_chunk[placement.chunk_id]
                if parts[idx] is not None:
                    continue
                addr = next(
                    (candidate for candidate in placement.locations
                     if candidate not in tried[placement.chunk_id]),
                    None,
                )
                if addr is None:
                    continue
                tried[placement.chunk_id].add(addr)
                by_address[addr].append(placement.chunk_id)

            if not by_address:
                missing = [
                    str(p.index) for p in placements
                    if parts[index_by_chunk[p.chunk_id]] is None
                ]
                raise GFSError(
                    f"chunks unavailable after trying every replica: "
                    f"{', '.join(missing[:10])}")

            workers = min(self._max_workers, len(by_address))
            with futures.ThreadPoolExecutor(max_workers=workers) as pool:
                jobs = {
                    pool.submit(_get_chunks, addr, batch, self._timeout): addr
                    for addr, chunk_ids in by_address.items()
                    for batch in _chunk_id_batches(chunk_ids)
                }
                for job in futures.as_completed(jobs):
                    for chunk_id, data in job.result().items():
                        parts[index_by_chunk[chunk_id]] = data

        return [part for part in parts if part is not None]

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


def _store_chunks(address: str, chunks, timeout: float) -> bool:
    try:
        with grpc.insecure_channel(address) as channel:
            stub = gfs_pb2_grpc.StorageServerStub(channel)
            resp = stub.StoreChunks(
                gfs_pb2.StoreChunksRequest(chunks=chunks), timeout=timeout)
            return resp.ok
    except grpc.RpcError:
        return False


def _chunk_data_batches(chunks):
    batch = []
    size = 0
    for chunk in chunks:
        chunk_size = len(chunk.chunk_id) + len(chunk.data)
        if batch and size + chunk_size > config.MAX_BULK_RPC_BYTES:
            yield batch
            batch = []
            size = 0
        batch.append(chunk)
        size += chunk_size
    if batch:
        yield batch


def _get_chunk(address: str, chunk_id: str) -> bytes | None:
    try:
        with grpc.insecure_channel(address) as channel:
            stub = gfs_pb2_grpc.StorageServerStub(channel)
            resp = stub.GetChunk(
                gfs_pb2.GetChunkRequest(chunk_id=chunk_id), timeout=10)
            return resp.data if resp.ok else None
    except grpc.RpcError:
        return None


def _chunk_id_batches(chunk_ids: list[str]):
    # Responses contain chunk bytes, so cap by the data we expect back.
    max_ids = max(1, config.MAX_BULK_RPC_BYTES // config.CHUNK_SIZE)
    for start in range(0, len(chunk_ids), max_ids):
        yield chunk_ids[start:start + max_ids]


def _get_chunks(address: str, chunk_ids: list[str],
                timeout: float) -> dict[str, bytes]:
    try:
        with grpc.insecure_channel(address) as channel:
            stub = gfs_pb2_grpc.StorageServerStub(channel)
            resp = stub.GetChunks(
                gfs_pb2.GetChunksRequest(chunk_ids=chunk_ids),
                timeout=timeout)
            return {chunk.chunk_id: chunk.data for chunk in resp.chunks}
    except grpc.RpcError:
        return {}
