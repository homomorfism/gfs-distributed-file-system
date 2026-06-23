"""Boundary tests for the required fixed 1 KB sharding behavior."""

import pytest


@pytest.mark.parametrize(
    ("size", "expected_chunks"),
    [
        (0, 0),
        (1, 1),
        (1024, 1),
        (1025, 2),
        (2500, 3),
    ],
)
def test_chunk_boundaries_create_expected_metadata(cluster, size,
                                                   expected_chunks):
    client = cluster.client()
    filename = f"boundary-{size}.txt"
    content = b"x" * size

    client.create(filename, content)

    assert client.read(filename) == content
    actual_size, actual_chunks = client.size(filename)
    assert actual_size == size
    assert actual_chunks == expected_chunks

    metadata = cluster.metadata(filename)
    assert metadata is not None
    assert metadata.num_chunks == expected_chunks
    assert len(metadata.chunks) == expected_chunks
