import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import shutil
import unittest
from unittest.mock import patch
import zipfile

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "python_release.py"
spec = importlib.util.spec_from_file_location("python_release", SCRIPT)
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


class ReleaseTests(unittest.TestCase):
    def test_version_policy_and_shell_input_rejection(self):
        for tag in ("python-v1.0.0", "python-v2.3.0rc1", "python-v1.0.0.post1"):
            self.assertEqual(release.tag_version(tag), tag[len("python-v"):])
        for tag in ("v1.0.0", "python-v01.0.0", "python-v1.0.0.dev1", "python-v1.0.0+local",
                    "python-v1.0.0;echo secret", "python-v$(id)", "python-v1.0.0\ncommit=x"):
            with self.assertRaises(ValueError):
                release.tag_version(tag)

    def test_unmerged_or_missing_tag_is_rejected(self):
        with patch.object(subprocess, "check_output", return_value="a" * 40 + "\n"), \
             patch.object(subprocess, "run", side_effect=subprocess.CalledProcessError(1, "git")):
            with self.assertRaises(subprocess.CalledProcessError):
                release.resolve_tag("python-v1.0.0")

    def wheels(self, directory, *, commit="a" * 40):
        for platform in release.PLATFORMS:
            path = Path(directory) / f"tcia_data_retriever-1.0.0-py3-none-{platform}.whl"
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("tcia_data_retriever/_build.json", json.dumps({"version": "1.0.0", "git_commit": commit}))
                archive.writestr("tcia_data_retriever-1.0.0.dist-info/METADATA", "Name: tcia-data-retriever\nVersion: 1.0.0\n")

    def test_complete_matrix_is_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            self.wheels(directory)
            release.check_wheels(directory, "1.0.0", "a" * 40)

    def test_missing_platform_wrong_version_and_wrong_commit_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            self.wheels(directory)
            for version, commit in (("2.0.0", "a" * 40), ("1.0.0", "b" * 40)):
                with self.assertRaises(ValueError):
                    release.check_wheels(directory, version, commit)
            next(Path(directory).glob("*.whl")).unlink()
            with self.assertRaises(ValueError):
                release.check_wheels(directory, "1.0.0", "a" * 40)

    def test_wrong_project_and_duplicate_platform_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            self.wheels(directory)
            original = Path(directory) / "tcia_data_retriever-1.0.0-py3-none-win_amd64.whl"
            wrong = original.with_name(original.name.replace("tcia_data_retriever", "other_project"))
            original.rename(wrong)
            with self.assertRaises(ValueError):
                release.check_wheels(directory, "1.0.0", "a" * 40)
            wrong.rename(original)
            next(Path(directory).glob("*manylinux*.whl")).unlink()
            duplicate = original.with_name(original.name.replace("-py3-", "-1-py3-"))
            shutil.copyfile(original, duplicate)
            with self.assertRaisesRegex(ValueError, "duplicate"):
                release.check_wheels(directory, "1.0.0", "a" * 40)

    def test_resolve_real_git_tag_requires_main_ancestry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(["git", "-C", directory, *args], text=True, stderr=subprocess.DEVNULL).strip()
            git("init", "--initial-branch=main")
            (root / "scripts").mkdir()
            (root / "scripts" / "python_release.py").write_text("# release contract\n")
            git("add", ".")
            git("-c", "user.name=ReleaseTest", "-c", "user.email=release@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "fixture")
            commit = git("rev-parse", "HEAD")
            git("update-ref", "refs/remotes/origin/main", commit)
            git("tag", "python-v1.0.0")
            self.assertEqual(release.resolve_tag("python-v1.0.0", root), ("1.0.0", commit))
            (root / "change.txt").write_text("not merged\n")
            git("add", ".")
            git("-c", "user.name=ReleaseTest", "-c", "user.email=release@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "unmerged")
            git("tag", "python-v2.0.0")
            with self.assertRaises(subprocess.CalledProcessError):
                release.resolve_tag("python-v2.0.0", root)


if __name__ == "__main__":
    unittest.main()
