"""Run the bundled TCIA Data Retriever without a desktop runtime or PATH lookup."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from typing import Any

__all__ = ["DownloadError", "download", "executable", "build_info", "main"]


class DownloadError(RuntimeError):
    """The CLI failed or did not produce a valid completion report."""

    def __init__(self, message: str, *, returncode: int | None = None,
                 report: dict[str, Any] | None = None):
        super().__init__(message)
        self.returncode = returncode
        self.report = report


def build_info() -> dict[str, str]:
    """Return bundled version, source revision, and executable checksum."""
    try:
        data = json.loads(Path(__file__).with_name("_build.json").read_text(encoding="utf-8"))
        if not isinstance(data, dict) or not all(
            isinstance(data.get(k), str) and data[k] for k in ("version", "git_commit", "sha256")
        ):
            raise ValueError()
        return data
    except (OSError, ValueError, TypeError):
        raise DownloadError("TDR package has no valid build metadata; install a platform wheel") from None


def executable() -> Path:
    """Resolve and verify the packaged executable. No network or PATH search."""
    path = Path(__file__).parent / "bin" / ("tdr.exe" if os.name == "nt" else "tdr")
    if not path.is_file():
        raise DownloadError("TDR executable is missing; install a supported platform wheel")
    expected = build_info()["sha256"]
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    if digest.hexdigest() != expected:
        raise DownloadError("TDR executable checksum does not match its package metadata")
    return path


def _completion(path: Path, returncode: int) -> dict[str, Any]:
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(report, dict) or report.get("schema_version") != 1:
            raise ValueError()
        if report.get("status") not in {"success", "failed", "cancelled"}:
            raise ValueError()
        counts = ("total", "downloaded", "synced", "skipped", "failed")
        if any(type(report.get(k)) is not int or report[k] < 0 for k in counts):
            raise ValueError()
        if returncode == 0 and (report["status"] != "success" or report["failed"]):
            raise ValueError()
        return report
    except (OSError, ValueError, TypeError):
        raise DownloadError("TDR did not produce a valid completion report", returncode=returncode) from None


def _cancel(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            process.send_signal(signal.SIGINT)
    except OSError:
        # Windows notebooks/pythonw may have no console for CTRL_BREAK_EVENT.
        # A process may also exit between poll() and send_signal().
        if process.poll() is None:
            try:
                process.terminate()
            except OSError:
                if process.poll() is None:
                    raise
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            process.kill()
        except OSError:
            if process.poll() is None:
                raise
        process.wait()


def download(manifest: str | os.PathLike, output_directory: str | os.PathLike, *,
             accept_data_policy: bool = False, credential_file: str | os.PathLike | None = None,
             processes: int = 8, max_retries: int = 3, skip_existing: bool = False,
             directory_mode: str = "descriptive") -> dict[str, Any]:
    """Download a local manifest and return its versioned summary.

    Explicit policy acceptance is required. Supply a credential file path,
    never its contents. Progress is inherited by the notebook/terminal. Ctrl-C
    requests CLI cancellation before re-raising KeyboardInterrupt.
    """
    if accept_data_policy is not True:
        raise ValueError("Set accept_data_policy=True after reviewing TCIA's data usage policy")
    for name, value, minimum in (("processes", processes, 1), ("max_retries", max_retries, 0)):
        if type(value) is not int or value < minimum:
            raise ValueError(f"{name} must be an integer >= {minimum}")
    if directory_mode not in {"classic", "descriptive"}:
        raise ValueError("directory_mode must be classic or descriptive")
    source = Path(manifest).expanduser().resolve(strict=True)
    if not source.is_file():
        raise ValueError("manifest must be a local file")
    destination = Path(output_directory).expanduser().resolve()
    args = [str(executable()), "--input", str(source), "--output", str(destination),
            "--accept-data-policy", "--processes", str(processes),
            "--max-retries", str(max_retries), "--directory-mode", directory_mode]
    if credential_file is not None:
        credential = Path(credential_file).expanduser().resolve(strict=True)
        if not credential.is_file():
            raise ValueError("credential_file must be a local JSON key file")
        args.extend(["--auth", str(credential)])
    if skip_existing:
        args.append("--skip-existing")
    with tempfile.TemporaryDirectory(prefix="tdr-report-") as temporary:
        report_path = Path(temporary) / "report.json"
        args.extend(["--report-json", str(report_path)])
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        try:
            process = subprocess.Popen(args, stdin=subprocess.DEVNULL, creationflags=flags)
        except OSError:
            raise DownloadError("Could not launch the packaged TDR executable") from None
        try:
            returncode = process.wait()
        except KeyboardInterrupt:
            _cancel(process)
            raise
        report = _completion(report_path, returncode)
        if returncode:
            raise DownloadError("TDR download did not complete successfully", returncode=returncode, report=report)
        return report


def main() -> int:
    """Forward console arguments and exit status to the packaged headless CLI."""
    try:
        process = subprocess.Popen([str(executable()), *sys.argv[1:]], stdin=subprocess.DEVNULL,
                                   creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0)
        try:
            return process.wait()
        except KeyboardInterrupt:
            _cancel(process)
            return 130
    except (DownloadError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 1
