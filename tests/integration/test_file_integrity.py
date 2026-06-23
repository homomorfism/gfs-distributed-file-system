"""End-to-end integrity tests for client-visible file operations."""

from tests.conftest import sha256


def test_create_read_roundtrip_preserves_exact_content(cluster):
    client = cluster.client()
    content = ("The quick brown fox. " * 200).encode()

    client.create("fox.txt", content)
    got = client.read("fox.txt")

    assert got == content
    assert sha256(got) == sha256(content)


def test_large_multi_chunk_file_preserves_order_and_content(cluster):
    client = cluster.client()
    block = bytes(range(256))
    content = (block * 400)[:100 * 1024]

    client.create("large.bin", content)
    got = client.read("large.bin")

    assert got == content
    assert sha256(got) == sha256(content)
