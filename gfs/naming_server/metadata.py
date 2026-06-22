"""Durable metadata store for the naming server, backed by SQLite.

Only *metadata* lives here (file -> chunks -> replica locations). Chunk
*content* is never stored in the database; it lives on storage-server disks.
Persisting metadata to SQLite lets the naming server survive a restart with
its file index intact.
"""
from __future__ import annotations

import sqlite3
import threading
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
    status     TEXT NOT NULL
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
"""


class MetadataStore:
    def __init__(self, db_path: str):
        # check_same_thread=False because the gRPC server uses a thread pool;
        # a single lock serializes all access for correctness.
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- writes ----
    def create_pending(self, filename: str, size: int, num_chunks: int,
                       chunks: list[ChunkMeta]) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("DELETE FROM files WHERE filename = ?", (filename,))
            cur.execute(
                "INSERT INTO files(filename, size, num_chunks, status) "
                "VALUES (?, ?, ?, 'pending')",
                (filename, size, num_chunks),
            )
            for ch in chunks:
                cur.execute(
                    "INSERT INTO chunks(chunk_id, filename, idx) VALUES (?, ?, ?)",
                    (ch.chunk_id, filename, ch.index),
                )
                for addr in ch.locations:
                    cur.execute(
                        "INSERT INTO replicas(chunk_id, address) VALUES (?, ?)",
                        (ch.chunk_id, addr),
                    )
            self._conn.commit()

    def commit_file(self, filename: str) -> bool:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "UPDATE files SET status = 'committed' WHERE filename = ?",
                (filename,),
            )
            self._conn.commit()
            return cur.rowcount > 0

    def delete_file(self, filename: str) -> bool:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("DELETE FROM files WHERE filename = ?", (filename,))
            self._conn.commit()
            return cur.rowcount > 0

    def add_replica(self, chunk_id: str, address: str) -> bool:
        """Record that `address` now stores `chunk_id`."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO replicas(chunk_id, address) "
                "VALUES (?, ?)",
                (chunk_id, address),
            )
            self._conn.commit()
            return cur.rowcount > 0

    def remove_replica(self, chunk_id: str, address: str) -> bool:
        """Remove a stale replica location from metadata."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "DELETE FROM replicas WHERE chunk_id = ? AND address = ?",
                (chunk_id, address),
            )
            self._conn.commit()
            return cur.rowcount > 0

    # ---- reads ----
    def get_file(self, filename: str) -> FileMeta | None:
        with self._lock:
            cur = self._conn.cursor()
            row = cur.execute(
                "SELECT filename, size, num_chunks, status FROM files "
                "WHERE filename = ?",
                (filename,),
            ).fetchone()
            if row is None:
                return None
            fm = FileMeta(filename=row[0], size=row[1], num_chunks=row[2],
                          status=row[3])
            chunk_rows = cur.execute(
                "SELECT chunk_id, idx FROM chunks WHERE filename = ? ORDER BY idx",
                (filename,),
            ).fetchall()
            for chunk_id, idx in chunk_rows:
                locs = [r[0] for r in cur.execute(
                    "SELECT address FROM replicas WHERE chunk_id = ?",
                    (chunk_id,),
                ).fetchall()]
                fm.chunks.append(ChunkMeta(chunk_id=chunk_id, index=idx,
                                           locations=locs))
            return fm

    def list_files(self) -> list[FileMeta]:
        with self._lock:
            cur = self._conn.cursor()
            rows = cur.execute(
                "SELECT filename, size, num_chunks, status FROM files "
                "ORDER BY filename"
            ).fetchall()
            return [FileMeta(filename=r[0], size=r[1], num_chunks=r[2],
                             status=r[3]) for r in rows]

    def list_committed_chunks(self) -> list[ChunkMeta]:
        """Return chunks for committed files, with their known replicas."""
        with self._lock:
            cur = self._conn.cursor()
            chunk_rows = cur.execute(
                "SELECT c.chunk_id, c.idx FROM chunks c "
                "JOIN files f ON f.filename = c.filename "
                "WHERE f.status = 'committed' "
                "ORDER BY c.filename, c.idx"
            ).fetchall()
            chunks: list[ChunkMeta] = []
            for chunk_id, idx in chunk_rows:
                locs = [r[0] for r in cur.execute(
                    "SELECT address FROM replicas WHERE chunk_id = ?",
                    (chunk_id,),
                ).fetchall()]
                chunks.append(ChunkMeta(chunk_id=chunk_id, index=idx,
                                        locations=locs))
            return chunks

    def all_chunk_locations(self, filename: str) -> list[tuple[str, list[str]]]:
        """Return [(chunk_id, [addresses])] for a file (used by delete)."""
        fm = self.get_file(filename)
        if fm is None:
            return []
        return [(c.chunk_id, c.locations) for c in fm.chunks]
