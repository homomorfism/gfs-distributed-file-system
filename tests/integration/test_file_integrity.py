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


def test_one_megabyte_roundtrip(cluster):
    client = cluster.client()
    content = b"0123456789abcdef" * (1024 * 1024 // 16)

    client.create("one-mib.txt", content)
    size, num_chunks = client.size("one-mib.txt")

    assert size == 1024 * 1024, f"expected 1 MiB, got {size}"
    assert num_chunks == 1024, f"expected 1024 chunks, got {num_chunks}"
    assert client.read("one-mib.txt") == content
