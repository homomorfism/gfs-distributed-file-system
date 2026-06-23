"""Shared pytest fixtures for in-process GFS integration tests."""
from __future__ import annotations

import hashlib
import sys
from concurrent import futures
from pathlib import Path

import grpc
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from gfs._generated import gfs_pb2, gfs_pb2_grpc
from gfs.client.client import GFSClient
from gfs.naming_server.metadata import MetadataStore
from gfs.naming_server.server import NamingServicer
from gfs.storage_server.server import StorageServicer


def sha256(data: bytes) -> str:
    """Return a stable digest for end-to-end byte integrity assertions."""
    return hashlib.sha256(data).hexdigest()


def _start(servicer, add_fn):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    add_fn(servicer, server)
    port = server.add_insecure_port("localhost:0")
    server.start()
    return server, f"localhost:{port}"


class Cluster:
    """A naming server plus N storage servers, all in this process."""

    def __init__(self, tmp_path: Path, num_storage: int = 4,
                 replication: int = 3):
        self.tmp_path = tmp_path
        self.replication = replication
        self.db_path = tmp_path / "meta.db"
        self._servers = []
        self.storage = {}
        self.storage_servicers = {}
        self._store = None
        self.naming_servicer = None
        self.naming_server = None
        self.naming_addr = ""

        self._start_naming()
        for index in range(num_storage):
            data_dir = tmp_path / f"storage{index}"
            svc = StorageServicer(str(data_dir))
            srv, addr = _start(
                svc, gfs_pb2_grpc.add_StorageServerServicer_to_server)
            self._servers.append(srv)
            self.storage[addr] = srv
            self.storage_servicers[addr] = svc
            self.register_storage(addr)

    def _start_naming(self) -> None:
        self._store = MetadataStore(str(self.db_path))
        self.naming_servicer = NamingServicer(
            self._store, self.replication, enable_healing=False)
        self.naming_server, self.naming_addr = _start(
            self.naming_servicer,
            gfs_pb2_grpc.add_NamingServerServicer_to_server)
        self._servers.append(self.naming_server)

    def register_storage(self, addr: str) -> None:
        self.naming_servicer.RegisterStorage(
            gfs_pb2.RegisterStorageRequest(address=addr), None)

    def register_all_storage(self) -> None:
        for addr in self.storage:
            self.register_storage(addr)

    def client(self) -> GFSClient:
        return GFSClient(self.naming_addr)

    def metadata(self, filename: str):
        return self._store.get_file(filename)

    def stop_one_storage(self) -> str:
        addr = next(iter(self.storage.keys()))
        return self.stop_storage(addr)

    def stop_storage(self, addr: str) -> str:
        srv = self.storage.pop(addr)
        srv.stop(0).wait()
        self.naming_servicer._registry.mark_dead(addr)
        return addr

    def stop_naming(self) -> None:
        if self.naming_server is not None:
            self.naming_server.stop(0).wait()
            self.naming_server = None
        if self._store is not None:
            self._store.close()
            self._store = None

    def restart_naming(self) -> None:
        self.stop_naming()
        self._start_naming()
        self.register_all_storage()

    def shutdown(self) -> None:
        for server in self._servers:
            server.stop(0).wait()
        if self._store is not None:
            self._store.close()
            self._store = None


@pytest.fixture
def cluster(tmp_path):
    gfs_cluster = Cluster(tmp_path)
    try:
        yield gfs_cluster
    finally:
        gfs_cluster.shutdown()


@pytest.fixture
def cluster_factory(tmp_path):
    clusters = []

    def make_cluster(num_storage: int = 4, replication: int = 3) -> Cluster:
        path = tmp_path / f"cluster{len(clusters)}"
        path.mkdir()
        gfs_cluster = Cluster(path, num_storage=num_storage,
                              replication=replication)
        clusters.append(gfs_cluster)
        return gfs_cluster

    try:
        yield make_cluster
    finally:
        for gfs_cluster in clusters:
            gfs_cluster.shutdown()
