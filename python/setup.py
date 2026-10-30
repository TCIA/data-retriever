"""Bundle an explicitly supplied CLI; never fetch one during installation."""
import hashlib
import json
import os
from pathlib import Path
import shutil

from packaging.version import Version
from setuptools import Distribution, setup
from setuptools.command.build_py import build_py
from wheel.bdist_wheel import bdist_wheel


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


class BundleCLI(build_py):
    def run(self):
        binary = Path(os.environ.get("TDR_BINARY", "")).resolve()
        if not binary.is_file():
            raise RuntimeError("Set TDR_BINARY to the prebuilt ./cmd/tdr executable")
        super().run()
        package = Path(self.build_lib) / "tcia_data_retriever"
        destination = package / "bin" / ("tdr.exe" if binary.suffix == ".exe" else "tdr")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(binary, destination)
        destination.chmod(0o755)
        metadata = {
            "version": version,
            "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "git_commit": os.environ.get("TDR_GIT_COMMIT", "unknown"),
        }
        (package / "_build.json").write_text(json.dumps(metadata) + "\n", encoding="utf-8")


class PlatformWheel(bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        if os.environ.get("TDR_WHEEL_PLATFORM"):
            self.plat_name = os.environ["TDR_WHEEL_PLATFORM"]
            self.plat_name_supplied = True

    def get_tag(self):
        _, _, platform = super().get_tag()
        return "py3", "none", platform


version = str(Version(os.environ.get("TDR_VERSION", "0.0.0.dev0")))
setup(version=version, distclass=BinaryDistribution,
      cmdclass={"build_py": BundleCLI, "bdist_wheel": PlatformWheel})
