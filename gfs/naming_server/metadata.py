"""Durable metadata store for the naming server, backed by SQLite.

Only *metadata* lives here (file -> chunks -> replica locations). Chunk
*content* is never stored in the database; it lives on storage-server disks.
Persisting metadata to SQLite lets the naming server survive a restart with
its file index intact.

Concurrency model
-----------------
The gRPC server runs many worker threads.  SQLite in WAL mode allows *one*
writer and *many* concurrent readers, so:

  * Writes go through a single dedicated connection guarded by ``_write_lock``
    (SQLite only permits one writer at a time anyway).
  * Reads use a per-thread connection (``threading.local``) and take **no**
    lock, so read RPCs (GetFile, ListFiles, GetFileSize) and the background
    heal/metrics scans run concurrently with each other *and* with an
    in-flight write.  This is the key to not serialising every RPC behind one
    lock.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass, field


@dataclass
class ChunkMeta:
    chunk_id: str
    index: int
    locations: list[str] = field(default_factory=list)


@dataclass
class FileMeta:
    filename: str
    size: int
    num_chunks: int
    status: str  # "pending" until committed, then "committed"
    chunks: list[ChunkMeta] = field(default_factory=list)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    filename   TEXT PRIMARY KEY,
    size       INTEGER NOT NULL,
    num_chunks INTEGER NOT NULL,
    status     TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS chunks (
    chunk_id  TEXT PRIMARY KEY,
    filename  TEXT NOT NULL,
    idx       INTEGER NOT NULL,
    FOREIGN KEY (filename) REFERENCES files(filename) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS replicas (
    chunk_id TEXT NOT NULL,
    address  TEXT NOT NULL,
    PRIMARY KEY (chunk_id, address),
    FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id) ON DELETE CASCADE
);
-- Indexes that keep the heal/metrics scans and per-file lookups off full
-- table scans as the chunk count grows into the hundreds of thousands.
CREATE INDEX IF NOT EXISTS idx_chunks_filename ON chunks(filename);
CREATE INDEX IF NOT EXISTS idx_replicas_chunk ON replicas(chunk_id);
CREATE INDEX IF NOT EXISTS idx_replicas_address ON replicas(address);
CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
"""


class MetadataStore:
    def __init__(self, db_path: str):
        self._db_path = db_path
        self._local = threading.local()      # per-thread read connections
        self._write_lock = threading.Lock()  # serialises writers
        # Dedicated writer connection (used only while holding _write_lock).
        self._write_conn = self._connect()
        with self._write_lock:
            self._write_conn.executescript(_SCHEMA)
            # Migrate databases created before the created_at column was added.
            cols = [r[1] for r in self._write_conn.execute(
                "PRAGMA table_info(files)").fetchall()]
            if "created_at" not in cols:
                self._write_conn.execute(
                    "ALTER TABLE files ADD COLUMN created_at REAL "
                    "NOT NULL DEFAULT 0")
            self._write_conn.commit()

    def _connect(self) -> sqlite3.Connection:
        # check_same_thread=False: the writer connection is used by whichever
        # worker thread holds the write lock; read connections are per-thread.
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute("PRAGMA foreign_keys = ON")
        # WAL: one writer + many concurrent readers without blocking.
        conn.execute("PRAGMA journal_mode = WAL")
        # synchronous = NORMAL is safe in WAL mode and much faster for bulk
        # inserts (the OS-level fsync is enough).
        conn.execute("PRAGMA synchronous = NORMAL")
        # Wait (don't error) if the DB is momentarily locked during a checkpoint.
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def _reader(self) -> sqlite3.Connection:
        """Return this thread's private read connection (lock-free)."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect()
            self._local.conn = conn
        return conn

    def close(self) -> None:
        with self._write_lock:
            self._write_conn.close()

    # ---- writes ----
    def create_pending(self, filename: str, size: int, num_chunks: int,
                       chunks: list[ChunkMeta]) -> None:
        with self._write_lock:
            cur = self._write_conn.cursor()
            cur.execute("DELETE FROM files WHERE filename = ?", (filename,))
            cur.execute(
                "INSERT INTO files(filename, size, num_chunks, status, created_at) "
                "VALUES (?, ?, ?, 'pending', ?)",
                (filename, size, num_chunks, time.time()),
            )
            cur.executemany(
                "INSERT INTO chunks(chunk_id, filename, idx) VALUES (?, ?, ?)",
                ((ch.chunk_id, filename, ch.index) for ch in chunks),
            )
            cur.executemany(
                "INSERT INTO replicas(chunk_id, address) VALUES (?, ?)",
                ((ch.chunk_id, addr)
                 for ch in chunks for addr in ch.locations),
            )
            self._write_conn.commit()

    def commit_file(self, filename: str) -> bool:
        with self._write_lock:
            cur = self._write_conn.cursor()
            cur.execute(
                "UPDATE files SET status = 'committed' WHERE filename = ?",
                (filename,),
            )
            self._write_conn.commit()
            return cur.rowcount > 0

    def delete_file(self, filename: str) -> bool:
        with self._write_lock:
            cur = self._write_conn.cursor()
            cur.execute("DELETE FROM files WHERE filename = ?", (filename,))
            self._write_conn.commit()
            return cur.rowcount > 0

    def add_replica(self, chunk_id: str, address: str) -> bool:
        """Record that `address` now stores `chunk_id`."""
        with self._write_lock:
            cur = self._write_conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO replicas(chunk_id, address) "
                "VALUES (?, ?)",
                (chunk_id, address),
            )
            self._write_conn.commit()
            return cur.rowcount > 0

    def remove_replica(self, chunk_id: str, address: str) -> bool:
        """Remove a stale replica location from metadata."""
        with self._write_lock:
            cur = self._write_conn.cursor()
            cur.execute(
                "DELETE FROM replicas WHERE chunk_id = ? AND address = ?",
                (chunk_id, address),
            )
            self._write_conn.commit()
            return cur.rowcount > 0

    # ---- reads ----
    def get_file(self, filename: str) -> FileMeta | None:
        cur = self._reader().cursor()
        row = cur.execute(
            "SELECT filename, size, num_chunks, status FROM files "
            "WHERE filename = ?",
            (filename,),
        ).fetchone()
        if row is None:
            return None
        fm = FileMeta(filename=row[0], size=row[1], num_chunks=row[2],
                      status=row[3])
        # Single LEFT JOIN instead of N+1 per-chunk queries for replicas.
        rows = cur.execute(
            "SELECT c.chunk_id, c.idx, r.address "
            "FROM chunks c "
            "LEFT JOIN replicas r ON r.chunk_id = c.chunk_id "
            "WHERE c.filename = ? "
            "ORDER BY c.idx",
            (filename,),
        ).fetchall()
        chunk_map: dict[str, ChunkMeta] = {}
        for chunk_id, idx, address in rows:
            if chunk_id not in chunk_map:
                cm = ChunkMeta(chunk_id=chunk_id, index=idx, locations=[])
                chunk_map[chunk_id] = cm
                fm.chunks.append(cm)
            if address is not None:
                chunk_map[chunk_id].locations.append(address)
        return fm

    def list_files(self) -> list[FileMeta]:
        cur = self._reader().cursor()
        rows = cur.execute(
            "SELECT filename, size, num_chunks, status FROM files "
            "ORDER BY filename"
        ).fetchall()
        return [FileMeta(filename=r[0], size=r[1], num_chunks=r[2],
                         status=r[3]) for r in rows]

    def list_committed_chunks(self) -> list[ChunkMeta]:
        """Return chunks for committed files, with their known replicas.

        Kept for completeness/tests; the hot paths use the cheaper aggregate
        and under-replicated queries below instead of materialising every
        chunk into Python.
        """
        cur = self._reader().cursor()
        rows = cur.execute(
            "SELECT c.chunk_id, c.idx, r.address FROM chunks c "
            "JOIN files f ON f.filename = c.filename "
            "LEFT JOIN replicas r ON r.chunk_id = c.chunk_id "
            "WHERE f.status = 'committed' "
            "ORDER BY c.filename, c.idx"
        ).fetchall()
        chunks_map: dict[str, ChunkMeta] = {}
        ordered: list[str] = []
        for chunk_id, idx, address in rows:
            if chunk_id not in chunks_map:
                cm = ChunkMeta(chunk_id=chunk_id, index=idx, locations=[])
                chunks_map[chunk_id] = cm
                ordered.append(chunk_id)
            if address is not None:
                chunks_map[chunk_id].locations.append(address)
        return [chunks_map[cid] for cid in ordered]

    # ---- aggregate reads (cheap; for metrics + healing) ----
    def count_files_by_status(self) -> dict[str, int]:
        cur = self._reader().cursor()
        rows = cur.execute(
            "SELECT status, COUNT(*) FROM files GROUP BY status"
        ).fetchall()
        return {status: count for status, count in rows}

    def committed_bytes(self) -> int:
        cur = self._reader().cursor()
        row = cur.execute(
            "SELECT COALESCE(SUM(size), 0) FROM files WHERE status = 'committed'"
        ).fetchone()
        return int(row[0])

    def count_committed_chunks(self) -> int:
        cur = self._reader().cursor()
        row = cur.execute(
            "SELECT COUNT(*) FROM chunks c "
            "JOIN files f ON f.filename = c.filename "
            "WHERE f.status = 'committed'"
        ).fetchone()
        return int(row[0])

    def _under_replicated_having(self, live: set[str]) -> tuple[str, list]:
        """Build the GROUP BY ... HAVING fragment selecting committed chunks
        with fewer than R *live* replicas. Returns (sql_having, params)."""
        live_list = list(live)
        if live_list:
            placeholders = ",".join("?" * len(live_list))
            live_count = (
                f"SUM(CASE WHEN r.address IN ({placeholders}) THEN 1 ELSE 0 END)"
            )
        else:
            live_count = "0"
            live_list = []
        return live_count, live_list

    def count_under_replicated(self, live: set[str], replication: int) -> int:
        live_count, params = self._under_replicated_having(live)
        sql = (
            "SELECT COUNT(*) FROM ("
            "  SELECT c.chunk_id FROM chunks c "
            "  JOIN files f ON f.filename = c.filename "
            "  LEFT JOIN replicas r ON r.chunk_id = c.chunk_id "
            "  WHERE f.status = 'committed' "
            "  GROUP BY c.chunk_id "
            f"  HAVING {live_count} < ?"
            ")"
        )
        cur = self._reader().cursor()
        row = cur.execute(sql, (*params, replication)).fetchone()
        return int(row[0])

    def under_replicated_chunks(
        self, live: set[str], replication: int
    ) -> list[ChunkMeta]:
        """Return only the committed chunks that have fewer than `replication`
        live replicas, each with *all* its known replica addresses.  This is
        what the heal loop actually needs — in steady state it returns nothing,
        so the loop does no Python-side work at all."""
        live_count, params = self._under_replicated_having(live)
        sql = (
            "SELECT c.chunk_id, c.idx, GROUP_CONCAT(r.address) "
            "FROM chunks c "
            "JOIN files f ON f.filename = c.filename "
            "LEFT JOIN replicas r ON r.chunk_id = c.chunk_id "
            "WHERE f.status = 'committed' "
            "GROUP BY c.chunk_id, c.idx "
            f"HAVING {live_count} < ?"
        )
        cur = self._reader().cursor()
        rows = cur.execute(sql, (*params, replication)).fetchall()
        result: list[ChunkMeta] = []
        for chunk_id, idx, addrs in rows:
            locations = addrs.split(",") if addrs else []
            result.append(ChunkMeta(chunk_id=chunk_id, index=idx,
                                    locations=locations))
        return result

    def known_replica_addresses(self) -> set[str]:
        """All distinct storage addresses referenced by committed chunks.
        Used to short-circuit healing when every known server is live."""
        cur = self._reader().cursor()
        rows = cur.execute(
            "SELECT DISTINCT r.address FROM replicas r "
            "JOIN chunks c ON c.chunk_id = r.chunk_id "
            "JOIN files f ON f.filename = c.filename "
            "WHERE f.status = 'committed'"
        ).fetchall()
        return {r[0] for r in rows}

    def all_chunk_locations(self, filename: str) -> list[tuple[str, list[str]]]:
        """Return [(chunk_id, [addresses])] for a file (used by delete)."""
        fm = self.get_file(filename)
        if fm is None:
            return []
        return [(c.chunk_id, c.locations) for c in fm.chunks]

    def list_chunk_ids_for(self, address: str) -> list[str]:
        """Return all chunk IDs that metadata says should be stored at
        `address`.  Used by storage servers at startup to find orphans."""
        rows = self._reader().execute(
            "SELECT chunk_id FROM replicas WHERE address = ?",
            (address,),
        ).fetchall()
        return [r[0] for r in rows]

    def list_stale_pending(self, max_age_seconds: float) -> list[FileMeta]:
        """Return pending files older than `max_age_seconds` with full chunk
        location info so the caller can clean up storage-server chunks before
        deleting the metadata."""
        cutoff = time.time() - max_age_seconds
        rows = self._reader().execute(
            "SELECT filename FROM files "
            "WHERE status = 'pending' AND created_at < ?",
            (cutoff,),
        ).fetchall()
        result: list[FileMeta] = []
        for (filename,) in rows:
            fm = self.get_file(filename)
            if fm is not None:
                result.append(fm)
        return result
