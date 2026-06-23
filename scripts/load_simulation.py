#!/usr/bin/env python3
"""Run an in-process production-style load simulation.

Default profile:
  100 concurrent operations
  writes 50%, reads 30%, deletes 20%
  write files up to 1 MiB

The script intentionally uses the real gRPC client/server code paths, but runs
all services in this process so it can be used without Docker.
"""
from __future__ import annotations

import argparse
import os
import random
import sys
import tempfile
import time
from concurrent import futures

import grpc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from gfs._generated import gfs_pb2, gfs_pb2_grpc
from gfs.client.client import GFSClient
from gfs.naming_server.metadata import MetadataStore
from gfs.naming_server.server import NamingServicer
from gfs.storage_server.server import StorageServicer


def _start(servicer, add_fn, workers: int):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    add_fn(servicer, server)
    port = server.add_insecure_port("localhost:0")
    server.start()
    return server, f"localhost:{port}"


class Cluster:
    def __init__(self, tmpdir: str, storage_nodes: int, replication: int,
                 server_workers: int):
        self.servers = []
        self.store = MetadataStore(os.path.join(tmpdir, "meta.db"))
        self.naming_servicer = NamingServicer(
            self.store, replication, enable_healing=False)
        naming_server, self.naming_addr = _start(
            self.naming_servicer,
            gfs_pb2_grpc.add_NamingServerServicer_to_server,
            server_workers,
        )
        self.servers.append(naming_server)

        for index in range(storage_nodes):
            svc = StorageServicer(os.path.join(tmpdir, f"storage{index}"))
            server, addr = _start(
                svc,
                gfs_pb2_grpc.add_StorageServerServicer_to_server,
                server_workers,
            )
            self.servers.append(server)
            self.naming_servicer.RegisterStorage(
                gfs_pb2.RegisterStorageRequest(address=addr), None)

    def shutdown(self) -> None:
        for server in self.servers:
            server.stop(0)
        self.store.close()


def _payload(seed: int, size: int) -> bytes:
    pattern = f"load-{seed:08d}-".encode()
    return (pattern * ((size // len(pattern)) + 1))[:size]


def _operation_counts(users: int, write_pct: int, read_pct: int) -> tuple[int, int, int]:
    writes = users * write_pct // 100
    reads = users * read_pct // 100
    deletes = users - writes - reads
    return writes, reads, deletes


def run(args) -> int:
    random.seed(args.seed)
    writes, reads, deletes = _operation_counts(
        args.users, args.write_pct, args.read_pct)

    with tempfile.TemporaryDirectory() as tmp:
        cluster = Cluster(tmp, args.storage_nodes, args.replication,
                          args.server_workers)
        try:
            seed_size = min(args.max_size, 64 * 1024)
            read_payloads = {
                f"seed-read-{i}.txt": _payload(i, seed_size)
                for i in range(reads)
            }
            delete_payloads = {
                f"seed-delete-{i}.txt": _payload(10_000 + i, seed_size)
                for i in range(deletes)
            }

            seed_client = GFSClient(
                cluster.naming_addr, timeout=args.timeout,
                max_workers=args.client_workers)
            for name, payload in {**read_payloads, **delete_payloads}.items():
                seed_client.create(name, payload)

            operations = []
            for i in range(writes):
                size = args.max_size if i == 0 else random.randint(1, args.max_size)
                operations.append(("write", f"load-write-{i}.txt", i, size))
            for name, payload in read_payloads.items():
                operations.append(("read", name, payload, 0))
            for name in delete_payloads:
                operations.append(("delete", name, None, 0))
            random.shuffle(operations)

            def do_operation(operation):
                kind, name, payload_or_seed, size = operation
                client = GFSClient(
                    cluster.naming_addr, timeout=args.timeout,
                    max_workers=args.client_workers)
                if kind == "write":
                    client.create(name, _payload(payload_or_seed, size))
                elif kind == "read":
                    data = client.read(name)
                    if data != payload_or_seed:
                        raise AssertionError(f"read mismatch for {name}")
                elif kind == "delete":
                    client.delete(name)
                else:
                    raise AssertionError(f"unknown operation {kind}")

            start = time.perf_counter()
            with futures.ThreadPoolExecutor(max_workers=args.users) as pool:
                list(pool.map(do_operation, operations))
            elapsed = time.perf_counter() - start

            pending = [
                f.filename for f in cluster.store.list_files()
                if f.status == "pending"
            ]
            if pending:
                raise AssertionError(
                    f"{len(pending)} files left pending: {pending[:5]}")

            print(
                f"PASS load simulation: users={args.users}, writes={writes}, "
                f"reads={reads}, deletes={deletes}, max_size={args.max_size}, "
                f"elapsed={elapsed:.2f}s, pending=0"
            )
            return 0
        finally:
            cluster.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--write-pct", type=int, default=50)
    parser.add_argument("--read-pct", type=int, default=30)
    parser.add_argument("--max-size", type=int, default=1024 * 1024)
    parser.add_argument("--storage-nodes", type=int, default=4)
    parser.add_argument("--replication", type=int, default=3)
    parser.add_argument("--server-workers", type=int, default=64)
    parser.add_argument("--client-workers", type=int, default=16)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--seed", type=int, default=26)
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
