# Download benchmarking

`bench_download.sh` times how fast the CLI downloads a manifest — elapsed
time, bytes written, throughput, file count, and exit code — and appends
each run as a row to a CSV so results from different manifests, machines,
or days can be compared.

## Quick start

```bash
# Benchmark every manifest in ./benchmarkManifests, one run each
scripts/bench_download.sh
```

Drop any manifest file the CLI accepts into `benchmarkManifests/`
(`.tcia`, `.s5cmd`, `.csv`, `.tsv`, `.xlsx`, `.json`, `.jsonld`) and it's
picked up automatically. That directory is tracked in git as an empty
placeholder (`.gitkeep`) — its contents are gitignored, so manifests you
drop in stay local.

## Usage

```
scripts/bench_download.sh [manifest-or-dir] [--output-dir DIR]
                           [--http-versions v1[,v2,...]] [-- extra-cli-args...]
```

| Argument | Default | Meaning |
|---|---|---|
| `manifest-or-dir` | `./benchmarkManifests` | A single manifest file to benchmark just that one, or a directory to benchmark every manifest file directly inside it. |
| `--output-dir DIR` | `./bench-results` | Where per-run output and the results CSV go. Also gitignored aside from `.gitkeep`. |
| `--http-versions v1[,v2,...]` | `1.1` | Comma-separated HTTP versions to run each manifest under. Default is one plain run per manifest; pass e.g. `1.1,2` to run each manifest once per version and compare. |
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
```

## Output

For each manifest x HTTP-version combination:

- Downloaded files land in `<output-dir>/<manifest-slug>/http-<version>/`
  (that directory is wiped and recreated on every run).
- A row is appended to `<output-dir>/bench_results.csv`:

  ```
  timestamp,manifest,http_version_requested,http_protocol_actual,elapsed_seconds,total_mb,throughput_mbps,files,exit_code
  ```

  `http_protocol_actual` is what `net/http` actually negotiated (e.g.
  `HTTP/1.1=42` or `HTTP/2.0=42`), since requesting HTTP/2 only asks for it
  via ALPN — a server that doesn't support it still answers over HTTP/1.1,
  and this is the only reliable way to tell which one a run actually used.

A summary table also prints to the terminal after each manifest finishes.
