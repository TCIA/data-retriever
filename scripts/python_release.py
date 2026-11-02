"""Validate release identity before builds or uploads; never contact an index."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import zipfile
from email.parser import BytesParser

from packaging.utils import parse_wheel_filename
from packaging.version import Version

PLATFORMS = {
    "manylinux_2_17_x86_64", "win_amd64", "macosx_11_0_x86_64", "macosx_11_0_arm64",
}


def tag_version(tag):
    if not re.fullmatch(r"python-v[0-9][0-9A-Za-z.]*", tag):
        raise ValueError("Use a python-v tag such as python-v1.0.0 or python-v1.0.0rc1")
    raw = tag.removeprefix("python-v")
    version = Version(raw)
    if str(version) != raw or version.is_devrelease or version.local:
        raise ValueError("Use a canonical PEP 440 version without dev/local suffixes")
    return str(version)


def resolve_tag(tag, repo=None):
    version = tag_version(tag)
    commit = subprocess.check_output(
        ["git", "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}"], text=True, cwd=repo
    ).strip()
    subprocess.run(["git", "merge-base", "--is-ancestor", commit, "origin/main"], check=True, cwd=repo)
    # An old packaging-only commit cannot be released using this new contract.
    subprocess.run(["git", "cat-file", "-e", f"{commit}:scripts/python_release.py"], check=True, cwd=repo)
    return version, commit


def check_wheels(directory, version, commit):
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("Expected an immutable source commit SHA")
    wheels = list(Path(directory).glob("*.whl"))
    if len(wheels) != len(PLATFORMS):
        raise ValueError("Expected exactly four platform wheels")
    found = set()
    for wheel in wheels:
        name, parsed_version, _, tags = parse_wheel_filename(wheel.name)
        if name != "tcia-data-retriever" or str(parsed_version) != version:
            raise ValueError("Unexpected project name or release version")
        if len(tags) != 1:
            raise ValueError("Expected one explicit platform tag per wheel")
        tag = next(iter(tags))
        if tag.interpreter != "py3" or tag.abi != "none" or tag.platform not in PLATFORMS or tag.platform in found:
            raise ValueError("Unexpected or duplicate platform wheel")
        found.add(tag.platform)
        with zipfile.ZipFile(wheel) as archive:
            build = json.loads(archive.read("tcia_data_retriever/_build.json"))
            metadata_paths = [n for n in archive.namelist() if n.endswith(".dist-info/METADATA")]
            if len(metadata_paths) != 1:
                raise ValueError("Expected exactly one distribution metadata record")
            metadata = BytesParser().parsebytes(archive.read(metadata_paths[0]))
            if metadata["Name"] != "tcia-data-retriever" or metadata["Version"] != version:
                raise ValueError("Distribution metadata does not match release identity")
            if build["version"] != version or build["git_commit"] != commit:
                raise ValueError("Bundled executable provenance does not match release identity")
    if found != PLATFORMS:
        raise ValueError("Incomplete platform matrix")


if __name__ == "__main__":
    if sys.argv[1] == "resolve-tag":
        version, commit = resolve_tag(os.environ["RELEASE_TAG"])
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            output.write(f"version={version}\ncommit={commit}\n")
        print(f"Release {version} from {commit}")
    elif sys.argv[1] == "check-wheels":
        check_wheels(sys.argv[2], os.environ["RELEASE_VERSION"], os.environ["RELEASE_COMMIT"])
    else:
        raise ValueError("Unknown release validation command")
