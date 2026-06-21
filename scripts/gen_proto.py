#!/usr/bin/env python3
"""Generate gRPC Python stubs from proto/gfs.proto into gfs/_generated/.

The generated *_pb2.py / *_pb2_grpc.py files are intentionally gitignored;
run this script (or `make proto`) after cloning and inside Docker builds.
"""
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT / "proto"
OUT_DIR = ROOT / "gfs" / "_generated"


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "__init__.py").touch()

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={OUT_DIR}",
        f"--grpc_python_out={OUT_DIR}",
        str(PROTO_DIR / "gfs.proto"),
    ]
    print("Running:", " ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        return result.returncode

    # grpc_tools emits `import gfs_pb2` (top-level); rewrite to a package-
    # relative import so the stubs work when imported as gfs._generated.*
    grpc_file = OUT_DIR / "gfs_pb2_grpc.py"
    text = grpc_file.read_text()
    text = text.replace("import gfs_pb2 as", "from . import gfs_pb2 as")
    grpc_file.write_text(text)
    print("Generated stubs in", OUT_DIR)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
