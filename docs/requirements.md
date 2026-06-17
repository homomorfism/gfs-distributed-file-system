# Distributed File System — Requirements

**Course:** Cloud Computing / Distributed Systems
**Deliverable:** Group Project — Build Your Own Distributed File System

---

## 1. Overview

Design and implement a distributed file system for **text files**, inspired by the
architecture of the **Google File System (GFS)**.

The system applies three core ideas from class:

- **Partitioning (sharding)** — split files into fixed-size chunks spread across servers.
- **Replication** — store each chunk on more than one server for fault tolerance.
- **Separation of concerns** — keep the metadata/naming layer independent from the storage layer.

The result must be a working, runnable system that exposes a small set of operations through a
client, and a clear analysis of which failures the design can and cannot survive.

---

## 2. System Requirements

### 2.1 Architecture

| Component | Role |
| --- | --- |
| **Naming server** (single) | Indexes all files and knows where every chunk is stored. Acts as the metadata authority. |
| **Storage servers** (multiple) | Each stores only a *fraction* of the files' chunks — never the whole dataset. |
| **Client** | A tool/library that makes the file system easy to use and hides distribution from the end user. |

### 2.2 Data Handling

- **File type:** text files only.
- **Sharding:** files are split into chunks with a **fixed chunk size of 1 KB**.
- **Replication:** every chunk must be replicated across **more than one** storage server.
- **Storage separation:** a database *may* be used for metadata, but the **actual chunk content
  must be stored in the file system**, not inside the database.

### 2.3 Supported Operations

The client must support the following operations:

| Operation | Expected behaviour |
| --- | --- |
| **Create file** | Accept a text file, split it into 1 KB chunks, distribute and replicate them across storage servers, and register the file in the naming server. |
| **Read file** | Look up chunk locations via the naming server, fetch all chunks (from any replica), reassemble, and return the original content. |
| **Delete file** | Remove the file's metadata and all of its chunk replicas from the storage servers. |
| **Get size of file** | Return the size of a stored file (e.g. from metadata) without transferring its full content. |

---

## 3. Deliverables

All deliverables must live in a **single Git repository** with a clear `README`.

1. **Architecture document** — system design, which failures are critical vs. non-critical, and
   other important design decisions and trade-offs.
2. **Dockerized applications** — naming server, storage servers, and client packaged as containers.
3. **Run instructions** — how to run the file system (e.g. `docker-compose`, environment, ports).
4. **Usage instructions** — how to use the client operations, with examples.

---

## 4. Fault-Tolerance Analysis

A core part of the grade is reasoning about failure. The architecture document must explicitly
address:

- **Storage server down** — What happens when one storage server goes down? Can clients still
  read and write?
- **Naming server down** — What happens when the naming server goes down? Identify it as a
  **critical / single point of failure** if it is one.
- **Replication value** — How does replication protect data, and how many *simultaneous* failures
  can the system survive?
- **Recoverable vs. unrecoverable** — Which failures are recoverable versus which lead to data
  loss or unavailability.

---

## 5. Requirements Checklist

- [ ] Single naming server acting as metadata authority
- [ ] Multiple storage servers, each holding only a subset of chunks
- [ ] Client tool/library hiding distribution from the user
- [ ] Text-file support
- [ ] Fixed 1 KB chunk sharding
- [ ] Each chunk replicated on more than one storage server
- [ ] Chunk content stored in the file system (not in a database)
- [ ] Create / Read / Delete / Get-size operations implemented
- [ ] Architecture document with fault-tolerance analysis
- [ ] Dockerized naming server, storage servers, and client
- [ ] Run instructions (docker-compose, env, ports)
- [ ] Usage instructions with examples
- [ ] Single Git repository with a clear README
