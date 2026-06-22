"""Command-line front end for the GFS client library.

Usage:
  python -m gfs.client create <local_path> [remote_name]
  python -m gfs.client read   <remote_name> [output_path]
  python -m gfs.client delete <remote_name>
  python -m gfs.client size   <remote_name>
  python -m gfs.client ls

The naming server address comes from --naming or the NAMING_SERVER env var
(default: localhost:50051).
"""
from __future__ import annotations

import argparse
import os
import sys

from gfs.client.client import GFSClient, GFSError


def _client(args) -> GFSClient:
    addr = args.naming or os.environ.get("NAMING_SERVER", "localhost:50051")
    timeout = getattr(args, "timeout", 10.0)
    return GFSClient(addr, timeout=timeout)


def cmd_create(args) -> int:
    with open(args.local_path, "rb") as fh:
        data = fh.read()
    remote = args.remote_name or os.path.basename(args.local_path)
    _client(args).create(remote, data)
    print(f"created '{remote}' ({len(data)} bytes)")
    return 0


def cmd_read(args) -> int:
    data = _client(args).read(args.remote_name)
    if args.output_path:
        with open(args.output_path, "wb") as fh:
            fh.write(data)
        print(f"wrote {len(data)} bytes to {args.output_path}")
    else:
        sys.stdout.write(data.decode("utf-8", errors="replace"))
    return 0


def cmd_delete(args) -> int:
    msg = _client(args).delete(args.remote_name)
    print(f"deleted '{args.remote_name}': {msg}")
    return 0


def cmd_size(args) -> int:
    size, num_chunks = _client(args).size(args.remote_name)
    print(f"{args.remote_name}: {size} bytes ({num_chunks} chunks)")
    return 0


def cmd_ls(args) -> int:
    files = _client(args).list_files()
    if not files:
        print("(no files)")
        return 0
    print(f"{'NAME':<30} {'SIZE':>10} {'CHUNKS':>7}  STATUS")
    for name, size, chunks, status in files:
        print(f"{name:<30} {size:>10} {chunks:>7}  {status}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gfs.client",
                                     description="GFS distributed FS client")
    parser.add_argument("--naming", help="naming server address host:port")
    parser.add_argument("--timeout", type=float, default=10.0,
                        help="gRPC timeout in seconds (default: 10; use higher "
                             "values like 300 for multi-GB files)")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("create", help="store a text file")
    p.add_argument("local_path")
    p.add_argument("remote_name", nargs="?")
    p.set_defaults(func=cmd_create)

    p = sub.add_parser("read", help="read a stored file")
    p.add_argument("remote_name")
    p.add_argument("output_path", nargs="?")
    p.set_defaults(func=cmd_read)

    p = sub.add_parser("delete", help="delete a stored file")
    p.add_argument("remote_name")
    p.set_defaults(func=cmd_delete)

    p = sub.add_parser("size", help="get size of a stored file")
    p.add_argument("remote_name")
    p.set_defaults(func=cmd_size)

    p = sub.add_parser("ls", help="list stored files")
    p.set_defaults(func=cmd_ls)

    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except GFSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
