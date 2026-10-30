# Headless CLI and Python wheel review

The Python distribution is a separate package, `tcia-data-retriever`, containing
the headless Go executable and a small Python subprocess wrapper. It has no
Python runtime dependencies. It is not embedded in `tcia_utils`: a future
optional extra there can install a compatible TDR version as a dependency.
The initial platform matrix is Linux x86-64, Windows x86-64, and macOS Intel
and Apple Silicon. Other platforms need their own validated wheel.

## Build and test

The headless entry point imports the transfer core without Wails, frontend
assets, GTK, WebKit, or a desktop session. Build it independently:

```bash
CGO_ENABLED=0 go build -trimpath -ldflags '-s -w -X main.version=0.0.0.dev0' -o /tmp/tdr ./cmd/tdr
go test -race ./core/app ./internal/cli ./cmd/tdr
/tmp/tdr --help
```

Create a build environment with Python 3.10+ and `python -m pip install build`.
On the native target platform, bundle the binary using explicit version and
source provenance:

```bash
export TDR_BINARY=/tmp/tdr
export TDR_VERSION=0.0.0.dev0
export TDR_GIT_COMMIT="$(git rev-parse HEAD)"
# For a CGO-disabled Linux x86-64 binary:
# export TDR_WHEEL_PLATFORM=manylinux_2_17_x86_64
python -m build --wheel --outdir wheelhouse python
python scripts/check_python_wheel.py wheelhouse
python -m pip install --only-binary=:all: wheelhouse/*.whl
TDR_INSTALLED_SMOKE=1 python -m unittest discover -s python/tests -p test_installed.py -v
```

Use the matching version in both Go ldflags and `TDR_VERSION`. The wheel records
that version, source commit and executable SHA256. Its Python tag is `py3`, its
ABI tag is `none`, and its platform tag describes the actual native target.
Do not label a native executable `any`, relabel a dynamically linked Linux
binary as manylinux, or claim support for untested OS/architecture combinations.
The Linux matrix uses `CGO_ENABLED=0` so no glibc or GUI shared libraries are
needed. OS CA roots are still required for HTTPS.

The `Headless CLI and Python wheels` workflow builds four development wheels,
tests the transfer core, and installs each wheel on Python 3.10 and 3.13. Smoke
tests transfer only synthetic local HTTP fixtures and cover successful payloads,
HTTP failures, invalid manifests and cancellation. Wrapper tests exercise policy
acceptance, report validation, missing/modified binary detection and cancellation
fallback. The workflow uploads wheels and SHA256SUMS as review artifacts. It
has read-only repository permissions and no PyPI publishing step or token.

The existing embedded IDC parquet files contribute roughly 76 MiB to each
executable. The artifact checker enforces a 95,000,000-byte compressed wheel
budget. If growth exceeds it, review metadata packaging explicitly; do not
silently replace bundled data with runtime downloads.

## Noninteractive contract

`tdr --input manifest.csv --output images --accept-data-policy --report-json result.json`
uses the shared manifest/download options. Stdin is never read for prompts.
`--prompt` is rejected; controlled downloads use `--auth /path/to/key.json`.
`--help`, `--version`, and `--croissant-list` perform no transfer. The headless
entry point does not check for application updates or refresh parquet metadata;
it uses the bundled metadata fallback. Install a newer wheel to update it.

Exit codes: 0 success/help/version; 1 transfer or report failure; 2 invalid
arguments or missing policy acceptance; 130 cancellation. Partial item failures
return nonzero. JSON completion reports use schema version 1:

```json
{
  "schema_version": 1,
  "status": "success",
  "version": "0.0.0.dev0",
  "output_directory": "/absolute/path/images",
  "total": 1,
  "downloaded": 1,
  "synced": 0,
  "skipped": 0,
  "failed": 0,
  "elapsed_seconds": 1.2
}
```

Statuses are `success`, `failed`, `cancelled`. Failures may include an
`error_code`: `missing_input`, `invalid_input`, `invalid_manifest`,
`policy_not_accepted`, `invalid_arguments`, `transfer_failed`, `empty_manifest`,
`items_failed`, `cancelled`, or `incomplete_run`. Counts are files or series,
matching the manifest route; they are not necessarily individual DICOM instances.
Reports exclude raw errors, file identifiers, credentials and temporary URLs.

Reports are replaced atomically with private permissions. A provisional
`failed/incomplete_run` report is written before starting a transfer, so existing
core fatal exits or unexpected termination cannot leave a stale success. Parser
syntax errors and help/version may produce no report. The Python wrapper treats
missing, malformed or inconsistent reports as failure. A forced termination may
leave provisional counts; they are not a complete inventory of transferred files.
Legacy core fatal exits and the desktop event-log setup are not refactored by
this change. Ordinary core logs still appear on the terminal.

## Release and integration sequence

1. Review and merge the headless/package PR and controlled HTTP hardening PR.
   The latter restores TLS verification and confines credentials to the
   recognized CRDC host. Review it before enabling controlled downloads broadly.
2. Inspect successful native-platform CI artifacts and perform a bounded Colab
   check plus an authorized controlled-access check. Local fixture tests do not
   establish real service authorization or production transfer performance.
3. Confirm TCIA ownership/name availability on PyPI, choose the first version,
   build from the reviewed commit, and configure maintainer-controlled trusted
   publishing separately. Publish wheels only initially; unsupported platforms
   should fail installation rather than build or fetch an executable implicitly.
4. In the existing `tcia_utils` development chat, add an optional dependency extra
   with a compatible version range. Use the wrapper/report interface and verify
   parity before replacing an existing route. Merge alone is insufficient: the
   wheel must be published for the extra to resolve.

Approved controlled-data credentials remain required. The current core may save
a private copy of a supplied key in the platform application configuration
directory; document and account for this in notebook/HPC environments. Downloads
currently restart after interruption rather than resuming byte ranges. Aspera
package support is not introduced by this distribution change.
