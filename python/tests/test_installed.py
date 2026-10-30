"""Exercise the installed wheel against local HTTP fixtures, never production data."""
import csv
import json
import http.server
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest

import tcia_data_retriever as tdr

PAYLOAD = b"TCIA wheel smoke fixture\n"
STARTED = threading.Event()
RELEASE = threading.Event()


class Fixture(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/slow.bin":
            self.send_response(200)
            self.send_header("Content-Length", "1000000")
            self.end_headers()
            self.wfile.write(b"start")
            self.wfile.flush()
            STARTED.set()
            RELEASE.wait(timeout=30)
            return
        if self.path != "/fixture.bin":
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(PAYLOAD)))
        self.end_headers()
        self.wfile.write(PAYLOAD)

    def log_message(self, *_):
        pass


@unittest.skipUnless(os.environ.get("TDR_INSTALLED_SMOKE") == "1", "requires installed wheel")
class InstalledWheelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def test_help_and_version_work_without_display_or_policy(self):
        for flag in ("--help", "--version"):
            result = subprocess.run([str(tdr.executable()), flag], stdin=subprocess.DEVNULL,
                                    capture_output=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertTrue(result.stdout or result.stderr)
            if flag == "--version":
                self.assertEqual(result.stdout.decode().strip(), tdr.build_info()["version"])

    def test_download_and_http_failure(self):
        with tempfile.TemporaryDirectory(prefix="tdr wheel space ") as directory:
            root = Path(directory)
            manifest = root / "manifest.csv"
            for resource, success in (("fixture.bin", True), ("missing.bin", False)):
                with manifest.open("w", newline="") as stream:
                    writer = csv.writer(stream)
                    writer.writerow(["imageUrl", "File Name", "Collection", "Patient ID"])
                    writer.writerow([f"http://127.0.0.1:{self.server.server_port}/{resource}",
                                     resource, "Synthetic", "Test"])
                output = root / resource
                if success:
                    report = tdr.download(manifest, output, accept_data_policy=True,
                                          max_retries=0, processes=1)
                    self.assertEqual(report["status"], "success")
                    self.assertEqual(report["downloaded"], 1)
                    files = list(output.rglob("fixture.bin"))
                    self.assertEqual(len(files), 1)
                    self.assertEqual(files[0].read_bytes(), PAYLOAD)
                else:
                    with self.assertRaises(tdr.DownloadError) as error:
                        tdr.download(manifest, output, accept_data_policy=True,
                                     max_retries=0, processes=1)
                    self.assertNotEqual(error.exception.returncode, 0)
                    self.assertEqual(error.exception.report["status"], "failed")
                    self.assertEqual(error.exception.report["failed"], 1)

    def test_invalid_manifest_is_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.csv"
            path.write_text("unknown\ninvalid\n")
            with self.assertRaises(tdr.DownloadError):
                tdr.download(path, Path(directory) / "output", accept_data_policy=True, max_retries=0)

    def test_cancelling_installed_executable_leaves_no_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, report = root / "slow.csv", root / "report.json"
            manifest.write_text(f"imageUrl,name\nhttp://127.0.0.1:{self.server.server_port}/slow.bin,slow.bin\n")
            STARTED.clear()
            RELEASE.clear()
            flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
            process = subprocess.Popen(
                [str(tdr.executable()), "--input", str(manifest), "--output", str(root / "out"),
                 "--accept-data-policy", "--max-retries", "0", "--report-json", str(report)],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                creationflags=flags,
            )
            try:
                self.assertTrue(STARTED.wait(timeout=20), "CLI never started fixture transfer")
                tdr._cancel(process)
                self.assertNotEqual(process.returncode, 0)
                self.assertNotEqual(json.loads(report.read_text())["status"], "success")
                self.assertFalse(list((root / "out").rglob("slow.bin")))
            finally:
                RELEASE.set()
                if process.poll() is None:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    unittest.main()
