"""Check artifact size, platform tags, bundled provenance and executable hash."""
import hashlib
import json
from pathlib import Path
import sys
import zipfile


def check(directory):
    directory = Path(directory)
    wheels = list(directory.glob("*.whl"))
    if not wheels:
        raise ValueError("No wheels found")
    checksums = []
    for wheel in sorted(wheels):
        # Keep explicit headroom for wrapper and metadata growth.
        if wheel.stat().st_size > 95_000_000:
            raise ValueError(f"Wheel exceeds the 95 MB release budget: {wheel.name}")
        with zipfile.ZipFile(wheel) as archive:
            names = archive.namelist()
            metadata_path = next(n for n in names if n.endswith(".dist-info/WHEEL"))
            metadata = archive.read(metadata_path).decode()
            if "Root-Is-Purelib: false" not in metadata or "Tag: py3-none-" not in metadata:
                raise ValueError("Expected a platform wheel with no Python ABI requirement")
            if "Tag: py3-none-any" in metadata:
                raise ValueError("Native executable cannot use the any platform tag")
            build_path = next(n for n in names if n.endswith("tcia_data_retriever/_build.json"))
            build = json.loads(archive.read(build_path))
            executable = [n for n in names if n.endswith(("tcia_data_retriever/bin/tdr", "tcia_data_retriever/bin/tdr.exe"))]
            if len(executable) != 1:
                raise ValueError("Expected one bundled executable")
            if hashlib.sha256(archive.read(executable[0])).hexdigest() != build["sha256"]:
                raise ValueError("Executable SHA256 mismatch")
            if not build["git_commit"] or build["git_commit"] == "unknown":
                raise ValueError("Source commit provenance is required for review artifacts")
        checksums.append(f"{hashlib.sha256(wheel.read_bytes()).hexdigest()}  {wheel.name}\n")
        print(f"Validated {wheel.name}: {wheel.stat().st_size:,} bytes")
    (directory / "SHA256SUMS").write_text("".join(checksums), encoding="utf-8")


if __name__ == "__main__":
    check(sys.argv[1])
