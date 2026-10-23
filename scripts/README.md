# Download benchmarking

`bench_download.sh` times how fast the CLI downloads a manifest, preserves the
complete log, validates the final download counts, and appends each run to a
provenance-rich CSV. It can build the current checkout or test a supplied
release/candidate binary.

## Quick start

```bash
# Benchmark every manifest in ./benchmarkManifests, one run each
scripts/bench_download.sh
```

Drop any manifest file the CLI accepts into `benchmarkManifests/`
(`.tcia`, `.s5cmd`, `.csv`, `.tsv`, `.xlsx`, `.json`, `.jsonld`) and it's
picked up automatically. That directory is tracked in git as an empty
placeholder (`.gitkeep`) — manifests you drop directly into it are gitignored,
so they stay local.

Curated, version-controlled examples covering the major TCIA download routes
live in `benchmarkManifests/examples/`. Run one example at a time to keep disk
requirements predictable:

```bash
scripts/bench_download.sh benchmarkManifests/examples/idc-rider-lung-pet-ct-pt.csv \
  --output-dir /path/to/local-scratch/bench-results \
  --runs 3 --label candidate
```

The examples directory is deliberately not scanned by the no-argument command.
Passing the whole directory explicitly runs every example and requires enough
space for all payloads.

Curated Croissant JSON-LD inputs are in
`benchmarkManifests/examples/croissant/`. Pass one file explicitly; each can
expand into a large dataset download, and controlled routes require the
tester's own authorization and credential file.

## Usage

```
scripts/bench_download.sh [manifest-or-dir] [--output-dir DIR]
                           [--http-versions 1.1[,2,...]] [--runs N]
                           [--binary FILE] [--label TEXT]
                           [-- extra-cli-args...]
```

| Argument | Default | Meaning |
|---|---|---|
| `manifest-or-dir` | `./benchmarkManifests` | A single manifest file to benchmark just that one, or a directory to benchmark every manifest file directly inside it. |
| `--output-dir DIR` | `./bench-results` | Where per-run output and the results CSV go. Also gitignored aside from `.gitkeep`. |
| `--http-versions v1[,v2,...]` | `1.1` | Comma-separated HTTP versions to run each manifest under. Default is one plain run per manifest; pass e.g. `1.1,2` to run each manifest once per version and compare. |
| `--runs N` | `1` | Number of repetitions for every manifest/protocol arm. Use at least three for reported comparisons. |
| `--binary FILE` | build checkout | Test an existing release or candidate executable instead of compiling the current source. |
| `--label TEXT` | empty | Label stored in each CSV row, such as `release-1.20.0` or `candidate`. |
| `-- extra-cli-args...` | — | Anything after `--` is passed straight through to the CLI, e.g. `--max-connections 4`. |

## Examples

```bash
# Speed-test everything in benchmarkManifests/
scripts/bench_download.sh

# Speed-test just one manifest
scripts/bench_download.sh ./benchmarkManifests/pathdb-test.csv

# Compare HTTP/1.1 vs HTTP/2 for every manifest in the folder
scripts/bench_download.sh --http-versions 1.1,2

# Custom output dir, plus a passthrough CLI flag
scripts/bench_download.sh --output-dir /tmp/bench -- --max-connections 4

# Three runs using a prebuilt production binary
scripts/bench_download.sh benchmarkManifests/examples/nbia-a091105-annotations.csv \
  --output-dir /tmp/bench --runs 3 \
  --binary /path/to/TCIA_Data_Retriever --label release-1.20.0
```

## Output

For each manifest, HTTP-version, and repetition:

- Downloaded files land under
  `<output-dir>/payloads/<manifest-slug>/http-<version>/<trial-id>/`.
- The complete CLI log is retained under
  `<output-dir>/logs/<manifest-slug>/http-<version>/`.
- A row is appended to `<output-dir>/bench_results_v2.csv`, including binary
  and manifest SHA-256 values, git commit, requested and negotiated protocol,
  repetition, elapsed time, disk footprint/rate, final item counts, process
  exit code, benchmark status, and log path.


`benchmark_status=success` requires a zero process exit code, a complete CLI
summary, zero failed items, and internally consistent final counts. The count
checks also keep results from older or release binaries trustworthy if they
return zero after reporting failed downloads.

  `http_protocol_actual` is what `net/http` actually negotiated (e.g.
  `HTTP/1.1=42` or `HTTP/2.0=42`), since requesting HTTP/2 only asks for it
  via ALPN — a server that doesn't support it still answers over HTTP/1.1,
  and this is the only reliable way to tell which one a run actually used.

`disk_mb_per_second` is an end-to-end disk-footprint rate, not a pure network
throughput measurement. Decompression, metadata, hashing, and filesystem work
are included. Use identical CLI flags for every comparison; for NBIA/DRS
transfer-only tests, `--no-md5 --no-decompress` reduces post-download work.

When no `--binary` is supplied, the script builds the current checkout. A
temporary embedded-frontend stub is used for this CLI-only build when
`frontend/dist` is absent, and is removed on exit.

Run the harness self-test without downloading data:

```bash
scripts/bench_download_test.sh
```
