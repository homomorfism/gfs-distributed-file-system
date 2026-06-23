"""Deterministic self-healing tests for under-replicated chunks."""

from tests.conftest import sha256


def test_self_healing_restores_replication_and_preserves_readability(cluster):
    client = cluster.client()
    content = ("heal this chunk " * 400).encode()
    client.create("heal.txt", content)

    before = cluster.metadata("heal.txt")
    assert before is not None
    assert all(len(chunk.locations) == cluster.replication
               for chunk in before.chunks)

    dead = before.chunks[0].locations[0]
    cluster.stop_storage(dead)

    repaired = cluster.naming_servicer.heal_once()

    assert repaired > 0
    after = cluster.metadata("heal.txt")
    assert after is not None
    live = set(cluster.storage.keys())
    for chunk in after.chunks:
        assert len(chunk.locations) == cluster.replication
        assert len(set(chunk.locations)) == cluster.replication
        assert set(chunk.locations).issubset(live)
        assert dead not in chunk.locations

    got = client.read("heal.txt")
    assert got == content
    assert sha256(got) == sha256(content)
