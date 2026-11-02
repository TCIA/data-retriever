import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch, Mock

import tcia_data_retriever as tdr


def summary(**changes):
    result = dict(schema_version=1, status="success", total=1, downloaded=1,
                  synced=0, skipped=0, failed=0)
    result.update(changes)
    return result


class WrapperTests(unittest.TestCase):
    def test_policy_rejected_before_any_launch(self):
        with patch.object(subprocess, "Popen") as launch:
            with self.assertRaisesRegex(ValueError, "accept_data_policy"):
                tdr.download("missing.csv", "out")
            launch.assert_not_called()

    def test_bad_reports_and_inconsistent_success_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            for value in [None, [], {}, summary(schema_version=2), summary(failed=1),
                          summary(total=True), summary(status="cancelled")]:
                if value is not None:
                    path.write_text(json.dumps(value))
                with self.assertRaises(tdr.DownloadError):
                    tdr._completion(path, 0)

    def test_launch_uses_packaged_path_no_shell_and_preserves_failure(self):
        with tempfile.TemporaryDirectory(prefix="tdr space ") as directory:
            manifest = Path(directory) / "input.csv"
            manifest.write_text("imageUrl\nhttps://example.invalid/file\n")
            credential = Path(directory) / "key.json"
            credential.write_text('{"api_key":"TEST-SECRET"}')
            process = Mock()
            process.wait.return_value = 1

            def launch(args, **kwargs):
                self.assertEqual(args[0], str(Path("/packaged/tdr")))
                self.assertEqual(args[args.index("--auth") + 1], str(credential.resolve()))
                self.assertNotIn("TEST-SECRET", " ".join(args))
                self.assertNotIn("shell", kwargs)
                self.assertEqual(kwargs["stdin"], subprocess.DEVNULL)
                report = Path(args[args.index("--report-json") + 1])
                report.write_text(json.dumps(summary(status="failed", downloaded=0, failed=1)))
                return process

            with patch.object(tdr, "executable", return_value=Path("/packaged/tdr")), \
                 patch.object(subprocess, "Popen", side_effect=launch):
                with self.assertRaises(tdr.DownloadError) as error:
                    tdr.download(manifest, Path(directory) / "output", accept_data_policy=True,
                                 credential_file=credential)
                self.assertEqual(error.exception.returncode, 1)
                self.assertEqual(error.exception.report["failed"], 1)

    def test_keyboard_interrupt_stops_child(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "input.csv"
            manifest.touch()
            process = Mock()
            process.wait.side_effect = KeyboardInterrupt
            with patch.object(tdr, "executable", return_value=Path("/packaged/tdr")), \
                 patch.object(subprocess, "Popen", return_value=process), \
                 patch.object(tdr, "_cancel") as cancel:
                with self.assertRaises(KeyboardInterrupt):
                    tdr.download(manifest, directory, accept_data_policy=True)
                cancel.assert_called_once_with(process)

    def test_checksum_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory)
            (package / "bin").mkdir()
            (package / "bin" / ("tdr.exe" if tdr.os.name == "nt" else "tdr")).write_bytes(b"wrong")
            with patch.object(tdr, "__file__", str(package / "__init__.py")), \
                 patch.object(tdr, "build_info", return_value={"sha256": "0" * 64}):
                with self.assertRaisesRegex(tdr.DownloadError, "checksum"):
                    tdr.executable()

    def test_cancel_falls_back_when_console_signal_fails(self):
        process = Mock()
        process.poll.return_value = None
        process.send_signal.side_effect = OSError("no console")
        tdr._cancel(process)
        process.terminate.assert_called_once()
        process.wait.assert_called_once_with(timeout=10)

    def test_cancel_kills_unresponsive_child(self):
        process = Mock()
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired("tdr", 10), 1]
        tdr._cancel(process)
        process.kill.assert_called_once()


if __name__ == "__main__":
    unittest.main()
