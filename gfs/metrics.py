"""Prometheus metrics helpers shared by naming and storage services."""
from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from typing import TypeVar

from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger("metrics")
T = TypeVar("T")


RPC_REQUESTS = Counter(
    "gfs_rpc_requests_total",
    "Total gRPC requests handled by a GFS service.",
    ["service", "method", "status"],
)
RPC_LATENCY = Histogram(
    "gfs_rpc_latency_seconds",
    "gRPC request latency by service and method.",
    ["service", "method"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)

NAMING_LIVE_STORAGE = Gauge(
    "gfs_naming_live_storage_servers",
    "Number of storage servers currently considered live by the naming server.",
)
NAMING_FILES = Gauge(
    "gfs_naming_files_total",
    "Number of files known by the naming server, grouped by status.",
    ["status"],
)
NAMING_COMMITTED_CHUNKS = Gauge(
    "gfs_naming_committed_chunks_total",
    "Number of chunks belonging to committed files.",
)
NAMING_UNDER_REPLICATED_CHUNKS = Gauge(
    "gfs_naming_under_replicated_chunks",
    "Committed chunks with fewer than the target number of live replicas.",
)
NAMING_HEAL_REPAIRS = Counter(
    "gfs_naming_heal_repairs_total",
    "Replica repairs completed by the naming server self-healing loop.",
)
NAMING_HEAL_PASSES = Counter(
    "gfs_naming_heal_passes_total",
    "Self-healing passes run by result.",
    ["result"],
)
NAMING_HEAL_DURATION = Histogram(
    "gfs_naming_heal_duration_seconds",
    "Duration of one self-healing pass.",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)

STORAGE_CHUNKS = Gauge(
    "gfs_storage_chunks",
    "Number of chunk files currently stored on this storage server.",
    ["address"],
)
STORAGE_BYTES = Gauge(
    "gfs_storage_bytes",
    "Total bytes currently stored on this storage server.",
    ["address"],
)
STORAGE_CHUNK_BYTES_WRITTEN = Counter(
    "gfs_storage_chunk_bytes_written_total",
    "Chunk bytes written by StoreChunk or ReplicateChunk.",
    ["address"],
)
STORAGE_CHUNK_BYTES_READ = Counter(
    "gfs_storage_chunk_bytes_read_total",
    "Chunk bytes read by GetChunk.",
    ["address"],
)
STORAGE_HEARTBEATS = Counter(
    "gfs_storage_heartbeats_total",
    "Heartbeats successfully sent by a storage server.",
    ["address"],
)


def start_metrics_server_from_env(default_port: int | None = None) -> None:
    raw = os.environ.get("METRICS_PORT")
    if raw is None and default_port is None:
        return
    port = int(raw if raw is not None else default_port)
    start_http_server(port)
    logger.info("prometheus metrics listening on :%d", port)


def observe_rpc(service: str, method: str, fn: Callable[[], T]) -> T:
    start = time.perf_counter()
    status = "ok"
    try:
        return fn()
    except Exception:
        status = "error"
        raise
    finally:
        RPC_REQUESTS.labels(service, method, status).inc()
        RPC_LATENCY.labels(service, method).observe(time.perf_counter() - start)
