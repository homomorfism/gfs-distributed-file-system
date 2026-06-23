"""End-to-end tests: spin up a naming server + several storage servers
in-process and exercise the client, including a storage-server failure.

Run with:  python tests/test_gfs.py
"""
import os
import sys
import tempfile
import threading
import time
from concurrent import futures

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gfs import config
from gfs._generated import gfs_pb2, gfs_pb2_grpc
from gfs.client.client import GFSClient, GFSError
from gfs.naming_server.metadata import MetadataStore
from gfs.naming_server.server import NamingServicer
from gfs.storage_server.server import StorageServicer


def _start(servicer, add_fn):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    add_fn(servicer, server)
    port = server.add_insecure_port("localhost:0")
    server.start()
    return server, f"localhost:{port}"


class Cluster:
    """A naming server plus N storage servers, all in this process."""

    def __init__(self, tmpdir, num_storage=4, replication=3):
        self.servers = []
        self._heartbeat_threads = []
        self._stop_heartbeats = threading.Event()
        store = MetadataStore(os.path.join(tmpdir, "meta.db"))
        self._store = store
        self.naming_servicer = NamingServicer(
            store, replication, enable_healing=False)
        naming_server, self.naming_addr = _start(
            self.naming_servicer,
            gfs_pb2_grpc.add_NamingServerServicer_to_server)
        self.servers.append(naming_server)

        self.storage = {}  # addr -> grpc server (so we can stop one)
        for i in range(num_storage):
            data_dir = os.path.join(tmpdir, f"storage{i}")
            svc = StorageServicer(data_dir)
            srv, addr = _start(
                svc, gfs_pb2_grpc.add_StorageServerServicer_to_server)
            self.servers.append(srv)
            self.storage[addr] = srv
            # Register with the naming server (marks it alive).
            self.naming_servicer.RegisterStorage(
                gfs_pb2.RegisterStorageRequest(address=addr), None)
            # Keep the storage server alive via periodic heartbeats so the
            # naming server's 15-second liveness timeout never expires during
            # long-running tests or benchmarks.
            t = threading.Thread(
                target=self._heartbeat_loop, args=(addr, svc), daemon=True)
            t.start()
            self._heartbeat_threads.append(t)

    def _heartbeat_loop(self, addr: str, svc: StorageServicer) -> None:
        """Send periodic heartbeats so the naming server keeps this storage server
        marked alive beyond HEARTBEAT_TIMEOUT."""
        while not self._stop_heartbeats.wait(config.HEARTBEAT_INTERVAL):
            self.naming_servicer.Heartbeat(
                gfs_pb2.HeartbeatRequest(
                    address=addr, chunk_ids=svc.held_chunk_ids()),
                None)

    def client(self):
        return GFSClient(self.naming_addr)

    def stop_one_storage(self):
        addr = next(iter(self.storage.keys()))
        return self.stop_storage(addr)

    def stop_storage(self, addr):
        srv = self.storage[addr]
        srv.stop(0)
        del self.storage[addr]
        self.naming_servicer._registry.mark_dead(addr)
        return addr

    def shutdown(self):
        self._stop_heartbeats.set()
        for s in self.servers:
            s.stop(0)
        self._store.close()

    def close(self):
        """Alias for shutdown() so benchmarks that call .close() don't crash."""
        self.shutdown()


def run_test(name, fn):
    with tempfile.TemporaryDirectory() as tmp:
        cluster = Cluster(tmp)
        try:
            fn(cluster)
            print(f"PASS  {name}")
            return True
        except AssertionError as exc:
            print(f"FAIL  {name}: {exc}")
            return False
        finally:
            cluster.shutdown()


# ----------------------------- tests -----------------------------
def test_create_read_roundtrip(cluster):
    client = cluster.client()
    content = ("The quick brown fox. " * 200).encode()  # ~4 KB -> multi-chunk
    client.create("fox.txt", content)
    got = client.read("fox.txt")
    assert got == content, "read content does not match what was written"


def test_size_no_transfer(cluster):
    client = cluster.client()
    content = b"x" * 2500  # 3 chunks (1024 + 1024 + 452)
    client.create("big.txt", content)
    size, num_chunks = client.size("big.txt")
    assert size == 2500, f"expected size 2500, got {size}"
    assert num_chunks == 3, f"expected 3 chunks, got {num_chunks}"


def test_replication_survives_one_failure(cluster):
    client = cluster.client()
    content = ("replicate me " * 300).encode()
    client.create("rep.txt", content)
    dead = cluster.stop_one_storage()
    # With replication factor 3 across 4 servers, one death must not lose data.
    got = client.read("rep.txt")
    assert got == content, f"read failed after {dead} went down"


def test_self_healing_restores_replication(cluster):
    client = cluster.client()
    content = ("heal this chunk " * 400).encode()
    client.create("heal.txt", content)

    before = cluster._store.get_file("heal.txt")
    assert before is not None, "file metadata missing before failure"
    assert all(len(c.locations) == 3 for c in before.chunks), (
        "initial placement should use 3 replicas")

    dead = before.chunks[0].locations[0]
    cluster.stop_storage(dead)
    repaired = cluster.naming_servicer.heal_once()
    assert repaired > 0, "healing pass should repair at least one replica"

    after = cluster._store.get_file("heal.txt")
    assert after is not None, "file metadata missing after healing"
    live = set(cluster.storage.keys())
    for chunk in after.chunks:
        assert len(chunk.locations) == 3, (
            f"chunk {chunk.index} should be back to exactly 3 replicas")
        assert dead not in chunk.locations, (
            f"chunk {chunk.index} still points at dead server {dead}")
        assert set(chunk.locations).issubset(live), (
            f"chunk {chunk.index} has non-live locations {chunk.locations}")

    got = client.read("heal.txt")
    assert got == content, "read after self-healing changed file contents"


def test_delete(cluster):
    client = cluster.client()
    client.create("gone.txt", b"delete me please")
    client.delete("gone.txt")
    try:
        client.read("gone.txt")
        assert False, "reading a deleted file should fail"
    except GFSError:
        pass


def test_create_needs_enough_servers():
    with tempfile.TemporaryDirectory() as tmp:
        cluster = Cluster(tmp, num_storage=1, replication=2)
        try:
            client = cluster.client()
            try:
                client.create("nope.txt", b"data")
                assert False, "create should fail without enough servers"
            except GFSError:
                pass
            print("PASS  test_create_needs_enough_servers")
            return True
        except AssertionError as exc:
            print(f"FAIL  test_create_needs_enough_servers: {exc}")
            return False
        finally:
            cluster.shutdown()


def main():
    results = []
    results.append(run_test("create_read_roundtrip", test_create_read_roundtrip))
    results.append(run_test("size_no_transfer", test_size_no_transfer))
    results.append(run_test("replication_survives_one_failure",
                            test_replication_survives_one_failure))
    results.append(run_test("self_healing_restores_replication",
                            test_self_healing_restores_replication))
    results.append(run_test("delete", test_delete))
    results.append(test_create_needs_enough_servers())
    passed = sum(results)
    print(f"\n{passed}/{len(results)} tests passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
