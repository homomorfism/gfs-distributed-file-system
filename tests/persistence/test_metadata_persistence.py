"""Persistence tests for naming-server metadata durability."""

from tests.conftest import sha256


def test_naming_restart_preserves_metadata_and_file_access(cluster):
    client = cluster.client()
    content = ("metadata survives restart " * 250).encode()
    client.create("persistent.txt", content)

    before = cluster.metadata("persistent.txt")
    assert before is not None
    before_chunks = [(chunk.chunk_id, list(chunk.locations))
                     for chunk in before.chunks]

    cluster.restart_naming()
    recovered_client = cluster.client()

    after = cluster.metadata("persistent.txt")
    assert after is not None
    assert [(chunk.chunk_id, list(chunk.locations))
            for chunk in after.chunks] == before_chunks
    assert after.status == "committed"

    got = recovered_client.read("persistent.txt")
    assert got == content
    assert sha256(got) == sha256(content)
    assert recovered_client.size("persistent.txt") == (
        len(content), after.num_chunks)
