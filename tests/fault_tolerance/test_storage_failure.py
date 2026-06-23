"""Fault-tolerance tests for client reads during storage failures."""

from tests.conftest import sha256


def test_read_survives_single_storage_server_failure(cluster):
    client = cluster.client()
    content = ("replicate me " * 300).encode()
    client.create("rep.txt", content)

    metadata = cluster.metadata("rep.txt")
    assert metadata is not None
    dead = metadata.chunks[0].locations[0]

    cluster.stop_storage(dead)
    got = client.read("rep.txt")

    assert got == content
    assert sha256(got) == sha256(content)
