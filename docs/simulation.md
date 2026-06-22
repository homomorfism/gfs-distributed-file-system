# Simulation Guide

This guide shows how to manually simulate load, large files, fault tolerance,
and network problems against the Docker Compose cluster. Commands use fish
shell syntax.

The default cluster is:

- 1 naming server
- 4 storage servers
- replication factor 3
- 1 helper `client` container

With replication factor 3, the system should survive one storage-server failure
for existing replicated files. Some reads may survive two storage-server
failures if at least one replica remains for every chunk, but the cluster cannot
restore full replication until at least three storage servers are live.

---

## 1. Start from a clean cluster

```fish
docker compose down -v
docker compose up --build -d
docker compose ps
```

Wait for all storage servers to register with the naming server
(before running any client commands that create files):

```fish
for i in (seq 1 10)
    set live (docker compose logs naming 2>&1 | grep -c "registered")
    test "$live" -ge 4 && break
    echo "waiting for storage servers to register... ($live/4)"
    sleep 2
end
docker compose logs naming | grep "registered"
```

Optionally follow the naming server logs in a separate terminal:

```fish
docker compose logs -f naming
```

Basic smoke test:

```fish
docker compose exec client python -m gfs.client create /samples/hello.txt hello.txt
docker compose exec client python -m gfs.client read hello.txt
docker compose exec client python -m gfs.client size hello.txt
docker compose exec client python -m gfs.client ls
```

---

## 2. Simulate multiple clients

Use `docker compose run` with a single Python script that fans out writes
via `ThreadPoolExecutor`. This avoids `docker compose exec` lock contention
when running many commands in parallel.

Create many files concurrently:

```fish
docker compose run --rm -T client python3 -c '
from concurrent.futures import ThreadPoolExecutor
from gfs.client.client import GFSClient, GFSError
import os, time

c = GFSClient(os.environ["NAMING_SERVER"])
with open("/samples/hello.txt", "rb") as f:
    data = f.read()

# Wait until storage servers are registered (handles fresh naming-server
# restarts where heartbeats have not arrived yet).
for _ in range(15):
    try:
        c.create(".__warmup__", data[:1024])
        c.delete(".__warmup__")
        break
    except GFSError as exc:
        print(f"waiting for storage servers... ({exc})")
        time.sleep(1)

def upload(i):
    c.create(f"hello-{i}.txt", data)
    return i

with ThreadPoolExecutor(max_workers=10) as pool:
    for i in pool.map(upload, range(1, 21)):
        print(f"created hello-{i}.txt")
'
```

Read them concurrently:

```fish
docker compose run --rm -T client python3 -c '
from concurrent.futures import ThreadPoolExecutor
from gfs.client.client import GFSClient
import os

c = GFSClient(os.environ["NAMING_SERVER"])

def download(i):
    return i, c.read(f"hello-{i}.txt")

with ThreadPoolExecutor(max_workers=10) as pool:
    for i, content in pool.map(download, range(1, 21)):
        print(f"read hello-{i}.txt ({len(content)} bytes)")
'
```

Check that the files exist in metadata:

```fish
docker compose exec client python -m gfs.client ls
```

Expected result: all creates and reads complete successfully.

---

## 3. Simulate large files

Files are split into 1 KB chunks. A 10 MB text file creates about 10,240 chunks,
so it is enough to exercise chunk placement, metadata growth, and replica reads.

Create a large text sample on the host:

```fish
uv run python -c '
from pathlib import Path

target = Path("samples/large.txt")
line = "hello distributed file system simulation\n"
target.write_text(line * 300_000, encoding="utf-8")
print(target, target.stat().st_size, "bytes")
'
```

Upload it:

```fish
docker compose exec client python -m gfs.client create /samples/large.txt large.txt
docker compose exec client python -m gfs.client size large.txt
```

Read it back and compare hashes:

```fish
docker compose exec client python -m gfs.client read large.txt /tmp/large-out.txt
docker compose cp client:/tmp/large-out.txt /tmp/gfs-sim-large-out.txt

shasum -a 256 samples/large.txt /tmp/gfs-sim-large-out.txt
```

Expected result: both hashes are identical.

---

## 3b. 1 GB write stress test

A 1 GB file creates roughly 1,024,000 chunks (each 1 KB). With replication factor 3,
the cluster writes about 3 million chunk replicas across 4 storage servers. This
exercises chunk placement at scale, metadata growth, and sustained write throughput.

**Expected duration:** 5–20 minutes depending on hardware. The dominant cost is
gRPC round-trips per chunk replica. Plan accordingly.

### Generate a 1 GB file on the host

Use Python with a reproducible pseudo-random seed so the same "random" data can
be generated again for verification without storing a 1 GB reference file:

```fish
uv run python -c '
import os, hashlib, struct, time

target = "samples/gigabyte.bin"
size = 1_000_000_000  # 1 GB (decimal)
seed = 42

t0 = time.monotonic()
with open(target, "wb") as f:
    # Write 64-byte blocks — fast enough, and deterministic per position.
    state = seed.to_bytes(32, "big")
    written = 0
    while written < size:
        chunk = hashlib.sha256(state).digest() + hashlib.sha256(state + b"\x01").digest()
        f.write(chunk)
        state = hashlib.sha256(state).digest()
        written += len(chunk)
        if written % (100 * 1024 * 1024) == 0:
            print(f"  {written / 1_000_000:.0f} MB…")

elapsed = time.monotonic() - t0
actual_size = os.path.getsize(target)
print(f"Generated {actual_size:,} bytes in {elapsed:.1f}s")

# Print host-side hash for later comparison.
h = hashlib.sha256()
with open(target, "rb") as f:
    for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
        h.update(chunk)
print(f"host sha256: {h.hexdigest()}")
'
```

A faster alternative with `dd` + `/dev/urandom` (macOS-compatible, non-reproducible):

```fish
dd if=/dev/urandom of=samples/gigabyte.bin bs=1m count=1000 2>/dev/null
shasum -a 256 samples/gigabyte.bin
```

### Upload to GFS

The `CreateFile` RPC must persist ~977K chunk placements into SQLite before
returning. Use `--timeout 300` (5 minutes) to give the naming server enough
headroom:

```fish
time docker compose exec client python -m gfs.client --timeout 300 create /samples/gigabyte.bin gigabyte.bin
docker compose exec client python -m gfs.client size gigabyte.bin
```

### Read back and verify hashes

```fish
docker compose exec client python -m gfs.client read gigabyte.bin /tmp/gigabyte-out.bin
docker compose cp client:/tmp/gigabyte-out.bin /tmp/gfs-gigabyte-out.bin

shasum -a 256 samples/gigabyte.bin /tmp/gfs-gigabyte-out.bin
```

**Expected result:** both SHA-256 hashes are identical. If they differ, the
simulation found a data-corruption bug.

### Check cluster state after the write

```fish
docker compose exec client python -m gfs.client ls
docker compose logs naming | tail -20
docker compose stats --no-stream
```

After the write, the naming server metadata database grows noticeably (each chunk
row ≈ 100 bytes → ~100 MB for 1 million chunks). The four storage servers together
hold ~3 GB of chunk data (1 GB × replication factor 3).

### Clean up the 1 GB file

The test file is too large to keep around casually. Delete it from GFS and the
host when done:

```fish
docker compose exec client python -m gfs.client delete gigabyte.bin
rm samples/gigabyte.bin /tmp/gfs-gigabyte-out.bin
```

---

## 4. Simulate storage-server failure

Create a file first:

```fish
docker compose exec client python -m gfs.client create /samples/hello.txt failover.txt
```

Stop one storage server:

```fish
docker compose stop storage1
```

Read while the server is down:

```fish
docker compose exec client python -m gfs.client read failover.txt
docker compose exec client python -m gfs.client read large.txt /tmp/large-after-storage1-down.txt
```

Inspect naming-server logs:

```fish
docker compose logs naming
```

Restart the server:

```fish
docker compose start storage1
docker compose logs naming
```

Expected result:

- reads still work while one storage server is down
- naming server marks the server unavailable after missed heartbeats
- after restart, storage re-registers and the cluster heals back toward the
  target replication factor

---

## 5. Simulate the survivability boundary

Stop two storage servers:

```fish
docker compose stop storage1 storage2
```

Try reading existing files:

```fish
docker compose exec client python -m gfs.client read failover.txt
docker compose exec client python -m gfs.client read large.txt /tmp/large-after-two-down.txt
```

Try creating a new file:

```fish
docker compose exec client python -m gfs.client create /samples/hello.txt created-with-two-down.txt
```

Restart the servers:

```fish
docker compose start storage1 storage2
```

Expected result:

- some reads may still work if every chunk has at least one live replica
- reads fail if all replicas for any required chunk are unavailable
- new creates should fail while fewer than 3 storage servers are live, because
  the naming server cannot place 3 distinct replicas

---

## 6. Simulate naming-server failure

Stop the naming server:

```fish
docker compose stop naming
```

Try client operations:

```fish
docker compose exec client python -m gfs.client ls
docker compose exec client python -m gfs.client read failover.txt
docker compose exec client python -m gfs.client create /samples/hello.txt while-naming-down.txt
```

Restart the naming server:

```fish
docker compose start naming
docker compose logs naming
```

After storage servers re-register, verify data is still readable:

```fish
docker compose exec client python -m gfs.client ls
docker compose exec client python -m gfs.client read failover.txt
```

Expected result:

- client operations fail while the naming server is down
- storage chunk data remains on disk
- metadata survives because the naming server uses the `naming-data` volume
- reads work again after restart and re-registration

The naming server is a single point of failure for availability.

---

## 7. Simulate network problems without Toxiproxy

Docker can simulate complete network loss or a frozen process. These are useful
for demos and do not require extra services.

### Pause a storage server

```fish
docker compose pause storage1
docker compose exec client python -m gfs.client read failover.txt
docker compose unpause storage1
```

Expected result: the server stops responding while paused, then resumes.

### Disconnect a storage server from the Compose network

Find the network and container names:

```fish
docker network ls
docker compose ps
```

The default network is usually named after the directory, for example:

```text
distributed-file-system_default
```

Disconnect `storage1`:

```fish
docker network disconnect distributed-file-system_default distributed-file-system-storage1-1
```

Run a read:

```fish
docker compose exec client python -m gfs.client read failover.txt
```

Reconnect it:

```fish
docker network connect distributed-file-system_default distributed-file-system-storage1-1
```

If your Compose project name is different, replace the network and container
names with the values shown by `docker network ls` and `docker compose ps`.

Expected result: the naming server eventually treats the disconnected storage
server as unavailable, and reads should use other replicas.

---

## 8. Optional: simulate latency and timeouts with Toxiproxy

Toxiproxy is useful when you need partial network degradation instead of total
failure, for example:

- high latency
- connection timeout
- limited bandwidth
- temporary network cuts

This project does not route traffic through Toxiproxy by default. To use it,
each advertised storage address must point to a Toxiproxy endpoint, and that
proxy must forward traffic to the real storage server.

Example shape for `storage1`:

```text
client/naming -> toxiproxy-storage1:15061 -> storage1:50061
```

That means `storage1` would advertise:

```yaml
ADVERTISE_ADDR: "toxiproxy-storage1:15061"
```

Then the Toxiproxy service would expose a proxy named `storage1` listening on
`15061` and forwarding to `storage1:50061`.

Typical toxic commands:

```fish
toxiproxy-cli toxic add storage1 -t latency -a latency=1000
toxiproxy-cli toxic add storage1 -t bandwidth -a rate=10
toxiproxy-cli toxic add storage1 -t timeout
toxiproxy-cli toxic remove storage1 -n latency_downstream
```

Use Toxiproxy when the question is "what happens when RPCs are slow or flaky?"
Use `docker compose stop`, `pause`, or `docker network disconnect` when the
question is "what happens when a server disappears?"

---

## 9. Cleanup

Stop containers but keep volumes:

```fish
docker compose down
```

Stop containers and delete all stored data:

```fish
docker compose down -v
```

Remove temporary host outputs:

```fish
rm -rf /tmp/gfs-sim /tmp/gfs-sim-large-out.txt
```

---

## 10. What to record during a simulation

For a report or demo, record:

- command used to inject the failure
- whether create/read/size/delete succeeded
- `docker compose ps`
- relevant `docker compose logs naming` output
- file hash before and after read-back for large files
- how long recovery took after `docker compose start`

The most important correctness check is byte equality:

```fish
shasum -a 256 samples/large.txt /tmp/gfs-sim-large-out.txt
```

If the hashes differ, the simulation found a correctness bug even if the client
command returned successfully.
