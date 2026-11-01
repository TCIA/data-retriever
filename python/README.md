# TCIA Data Retriever for Python

This package bundles the headless TCIA Data Retriever executable with a small
Python interface. It uses the same Go transfer core as the desktop application.
It has no Python runtime dependencies and does not require a desktop, Go, Node,
or a separately installed executable. Installation does not grant controlled
data access.

The proposed distribution name is `tcia-data-retriever`; publication and name
availability must be confirmed by maintainers before announcing installation.
After publication, install a supported platform wheel with:

```bash
python -m pip install --only-binary=:all: tcia-data-retriever
tcia-data-retriever --help
```

Read the [TCIA data usage policy](https://www.cancerimagingarchive.net/data-usage-policies-and-restrictions/)
before explicitly accepting it:

```python
from tcia_data_retriever import download

report = download("manifest.csv", "./images", accept_data_policy=True)
print(report["downloaded"], report["failed"])
```

For restricted data, obtain your own approved JSON key and pass only its path:

```python
report = download(
    "controlled_manifest.csv", "./images",
    accept_data_policy=True, credential_file="/path/to/credentials.json",
)
```

Python and CLI downloads read the supplied key for that invocation only. They
do not copy it into TDR's saved-auth location, reuse a saved GUI key, or open the
GUI credential prompt. Supply `credential_file=` each time controlled access is
needed. The desktop GUI retains its existing remembered-credential behavior.
Existing GUI credentials are neither modified nor removed by CLI runs. Neither
credentials nor temporary storage URLs are included in completion reports.

The published `0.1.0rc1` predates these changes and may save a copy of a supplied
key. These improvements require a later build of the package.

`download()` accepts a local manifest, output directory, `processes` (default 8),
`max_retries` (default 3), `skip_existing` (default false), and `directory_mode`
(`descriptive` or `classic`). It relays live progress to Python's own output
streams so notebooks can display it, closes child stdin,
and returns a schema-versioned summary. Failed or partial runs raise
`DownloadError` with `returncode` and, when available, `report`. Missing or
malformed reports are failures even if the process returned zero. Ctrl-C
requests cancellation, then re-raises `KeyboardInterrupt`.

Progress remains enabled. The `progress="auto"` default uses an updating line
in a terminal and plain-text snapshots in a notebook or redirected log. Use
`progress="plain"` to force readable newline output or `progress="terminal"`
to force the live terminal display. Overall counts describe completed work
items; the active-file bytes/percentage show progress within a large transfer.
Failures and cancellation remain visible and retain their existing reports and
exceptions. Progress output is streamed, not accumulated as an in-memory log.

For all existing CLI options use the console command or
`python -m tcia_data_retriever`. `--help` and `--version` need no policy acceptance.
Transfers require `--accept-data-policy`; interactive password prompts are
unsupported. `build_info()` returns package version, source commit and binary
SHA256. `executable()` resolves the packaged executable without searching PATH.
No executable is fetched or updated at runtime.

This package is independent of `tcia_utils`. A future optional `tcia_utils`
extra can depend on it after the TDR interface is merged and compatible wheels
are published. Existing Python transfer routes are not automatically replaced.

See [CLI progress and credential lifetime](../docs/cli-progress-and-credentials.md)
for the GUI/CLI distinction and progress rendering details.
