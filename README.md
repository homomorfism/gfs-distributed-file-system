# Distributed File System (GFS clone)

A small, runnable distributed file system for **text files**, inspired by the
**Google File System**. Files are split into fixed **1 KB chunks**, every chunk
is **replicated** across more than one storage server, and a single **naming
server** keeps the metadata that maps files → chunks → replica locations.

The three ideas from class:

- **Partitioning (sharding):** each file is cut into 1 KB chunks spread across servers.
- **Replication:** each chunk is stored on more than one storage server.
- **Separation of concerns:** the metadata/naming layer is independent from the storage layer.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the design and the full
fault-tolerance analysis, and [`docs/simulation.md`](docs/simulation.md) for
manual load, large-file, failure, and network simulations.

---

## Architecture at a glance

```
                 ┌─────────────────────┐
                 │    Naming server    │   metadata authority (the "master")
                 │  files → chunks →   │   • placement on create
                 │  replica locations  │   • locations on read
                 │   (SQLite metadata) │   • orchestrates delete
                 └──────────┬──────────┘
            register/        │ metadata RPCs
            heartbeat        │
        ┌─────────┬──────────┼───────────┬─────────┐
        │         │          │           │         │
   ┌────▼───┐ ┌───▼────┐ ┌───▼────┐ ┌────▼───┐  chunk bytes (direct)
   │storage1│ │storage2│ │storage3│ │storage4│
   │ chunks │ │ chunks │ │ chunks │ │ chunks │  each holds only a SUBSET,
   │ on disk│ │ on disk│ │ on disk│ │ on disk│  content stored as files
   └────────┘ └────────┘ └────────┘ └────────┘  (not in a DB)
        ▲                                   ▲
        └───────────  Client  ──────────────┘
        splits/reassembles files, talks to the master for metadata
        and to storage servers directly for chunk data
```

- **Naming server** — single source of truth for metadata. Picks which storage
  servers each chunk is replicated to, answers read/size queries, and
  orchestrates deletes and background replica repair. Never stores chunk
  content.
- **Storage servers** — each stores only the chunks assigned to it, as plain
  files under `DATA_DIR`. Register with the naming server and send heartbeats.
- **Client** — a library (`gfs.client.GFSClient`) plus a CLI
  (`python -m gfs.client`) that hides chunking, placement and replication.

Communication is **gRPC** (see [`proto/gfs.proto`](proto/gfs.proto)).

---

## Requirements / prerequisites

- **Docker + Docker Compose** (recommended way to run everything), **or**
- **Python 3.12+** and **uv** to run locally / run the tests.

---

## Run with Docker (recommended)

The default cluster is **1 naming server + 4 storage servers**, replication
factor **3**. A single storage failure leaves two live replicas and one spare
target, so the naming server can repair back to three live replicas.

```bash
docker compose up --build -d        # build images & start the cluster
docker compose ps                   # see the running services
docker compose logs -f naming       # watch the naming server
```

A `client` helper container is included for running commands inside the
cluster network. Put text files you want to upload in `./samples/`
(mounted at `/samples` in the client container).

> **Windows / Git Bash note:** prefix client commands with `MSYS_NO_PATHCONV=1`
> so paths like `/samples/...` aren't rewritten, e.g.
> `MSYS_NO_PATHCONV=1 docker compose exec client python -m gfs.client size hello.txt`.

### Usage examples

```bash
# Create (store) a text file
docker compose exec client python -m gfs.client create /samples/hello.txt hello.txt

# List stored files
docker compose exec client python -m gfs.client ls

# Get size from metadata (no data transfer)
docker compose exec client python -m gfs.client size hello.txt

# Read a file back (to stdout, or to a path)
docker compose exec client python -m gfs.client read hello.txt
docker compose exec client python -m gfs.client read hello.txt /tmp/out.txt

# Delete a file (removes metadata + all chunk replicas)
docker compose exec client python -m gfs.client delete hello.txt
```

### Try the fault tolerance

```bash
docker compose exec client python -m gfs.client create /samples/hello.txt hello.txt
docker compose stop storage1                                   # kill a storage server
docker compose exec client python -m gfs.client read hello.txt # still works (replica)
docker compose logs naming                                     # shows self-healing
docker compose start storage1
```

### Shut down

```bash
docker compose down       # stop containers
docker compose down -v    # stop and wipe all data volumes
```

---

## Ports

| Service   | Container port | Host port | Purpose                         |
| --------- | -------------- | --------- | ------------------------------- |
| naming    | 50051          | 50051     | metadata gRPC (client + storage)|
| storage1-4| 50061          | —         | chunk I/O gRPC (internal)       |

Only the naming server is published to the host; storage servers talk to it on
the internal compose network. A client running on the host can connect with
`--naming localhost:50051`.

## Environment variables

**Naming server**

| Var                  | Default              | Meaning                                  |
| -------------------- | -------------------- | ---------------------------------------- |
| `PORT`               | `50051`              | gRPC listen port                         |
| `METADATA_DB`        | `/data/metadata.db`  | SQLite metadata file (metadata only)     |
| `REPLICATION_FACTOR` | `3`                  | replicas per chunk (must be > 1)         |
| `HEAL_INTERVAL`      | `5`                  | seconds between self-healing scans       |

**Storage server**

| Var              | Default          | Meaning                                       |
| ---------------- | ---------------- | --------------------------------------------- |
| `PORT`           | `50061`          | gRPC listen port                              |
| `DATA_DIR`       | `/data/chunks`   | where chunk **files** are stored              |
| `NAMING_SERVER`  | `naming:50051`   | naming server address                         |
| `ADVERTISE_ADDR` | `localhost:PORT` | address other peers use to reach this server  |

**Client** — `NAMING_SERVER` (default `localhost:50051`) or `--naming host:port`.

---

## Run locally without Docker

```bash
uv sync
uv run python scripts/gen_proto.py     # generate gRPC stubs (gitignored)

# in separate terminals:
PORT=50051 METADATA_DB=./data/meta.db uv run python -m gfs.naming_server
PORT=50061 DATA_DIR=./data/s1 NAMING_SERVER=localhost:50051 ADVERTISE_ADDR=localhost:50061 uv run python -m gfs.storage_server
PORT=50062 DATA_DIR=./data/s2 NAMING_SERVER=localhost:50051 ADVERTISE_ADDR=localhost:50062 uv run python -m gfs.storage_server
PORT=50063 DATA_DIR=./data/s3 NAMING_SERVER=localhost:50051 ADVERTISE_ADDR=localhost:50063 uv run python -m gfs.storage_server
PORT=50064 DATA_DIR=./data/s4 NAMING_SERVER=localhost:50051 ADVERTISE_ADDR=localhost:50064 uv run python -m gfs.storage_server

# then use the client:
NAMING_SERVER=localhost:50051 uv run python -m gfs.client create samples/hello.txt hello.txt
NAMING_SERVER=localhost:50051 uv run python -m gfs.client read hello.txt
```

(On Windows PowerShell, set env vars with `$env:PORT="50051"` before each command.)

---

## Tests

End-to-end tests spin up a naming server and several storage servers in one
process and exercise create / read / size / delete, storage-server failure,
and self-healing back to the target replication factor:

```bash
uv run python scripts/gen_proto.py
uv run pytest
```

Expected: all tests pass.

---

## Repository layout

```
proto/gfs.proto            gRPC service + message definitions
gfs/config.py              chunk size, replication factor, timeouts
gfs/naming_server/         master: metadata store (SQLite) + gRPC service
gfs/storage_server/        chunkserver: stores chunk files + heartbeats
gfs/client/                client library + CLI
scripts/gen_proto.py       generates the gRPC stubs (into gfs/_generated/)
tests/                     pytest integration, fault-tolerance, and persistence tests
docker-compose.yml         1 naming + 4 storage + client helper
Dockerfile                 single image for all roles
docs/ARCHITECTURE.md       design + fault-tolerance analysis
docs/simulation.md         manual simulation commands and scenarios
docs/requirements.md       the assignment
```
