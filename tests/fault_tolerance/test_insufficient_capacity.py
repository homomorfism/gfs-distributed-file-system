"""Tests for clean failure when replication cannot be satisfied."""

import pytest

from gfs.client.client import GFSError


def test_create_fails_cleanly_without_enough_live_storage(cluster_factory):
    cluster = cluster_factory(num_storage=1, replication=2)
    client = cluster.client()

    with pytest.raises(GFSError, match="need 2 storage servers"):
        client.create("nope.txt", b"data")

    assert cluster.metadata("nope.txt") is None
    assert cluster.client().list_files() == []
