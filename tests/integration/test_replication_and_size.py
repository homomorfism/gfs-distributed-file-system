"""Integration tests for metadata-visible replication, delete, and size."""

import pytest

from gfs.client.client import GFSError


def test_every_chunk_has_configured_replication_factor(cluster):
    client = cluster.client()
    content = b"replication metadata " * 300

    client.create("replicated.txt", content)

    metadata = cluster.metadata("replicated.txt")
    assert metadata is not None
    assert metadata.status == "committed"
    assert metadata.chunks
    for chunk in metadata.chunks:
        assert len(chunk.locations) == cluster.replication
        assert len(set(chunk.locations)) == cluster.replication


def test_delete_removes_metadata_and_future_reads_fail(cluster):
    client = cluster.client()
    client.create("gone.txt", b"delete me please")

    message = client.delete("gone.txt")

    assert "deleted" in message
    assert cluster.metadata("gone.txt") is None
    with pytest.raises(GFSError, match="file not found"):
        client.read("gone.txt")
    with pytest.raises(GFSError, match="file not found"):
        client.size("gone.txt")


@pytest.mark.parametrize("size", [0, 1, 1024, 1025, 2500, 100 * 1024])
def test_size_returns_metadata_size_for_multiple_file_sizes(cluster, size):
    client = cluster.client()
    filename = f"size-{size}.txt"
    content = b"s" * size

    client.create(filename, content)

    actual_size, _ = client.size(filename)
    assert actual_size == size
