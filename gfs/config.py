"""Shared constants and small helpers for the GFS clone."""
import math

# Fixed chunk size mandated by the requirements: 1 KB.
CHUNK_SIZE = 1024

# Default replication factor (each chunk lives on this many storage servers).
# Must be > 1 per the requirements. Overridable via env on the naming server.
DEFAULT_REPLICATION_FACTOR = 3

# Seconds without a heartbeat before a storage server is considered dead.
HEARTBEAT_TIMEOUT = 15
# How often storage servers send heartbeats.
HEARTBEAT_INTERVAL = 5
# How often the naming server scans committed metadata for under-replicated
# chunks and asks storage servers to repair them.
HEAL_INTERVAL = 5

# Production simulation settings: many clients generate thousands of small
# chunk operations, so keep server pools and client fan-out bounded but larger
# than the tiny demo defaults.
DEFAULT_GRPC_MAX_WORKERS = 64
DEFAULT_CLIENT_MAX_WORKERS = 16
MAX_BULK_RPC_BYTES = 2 * 1024 * 1024


def num_chunks_for_size(size: int) -> int:
    """Number of fixed-size chunks needed to hold `size` bytes."""
    if size <= 0:
        return 0
    return math.ceil(size / CHUNK_SIZE)
