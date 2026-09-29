#!/usr/bin/env bash
# Benchmarks download speed for one manifest, or every manifest in a
# directory: runs it through the CLI, timing the run and measuring bytes
# actually written to disk. Results (elapsed time, MB, throughput, files,
# exit code) accumulate as rows in a CSV so runs on different manifests,
# machines, or days stay comparable.
#
# By default each manifest is run once, under the CLI's default HTTP
# version (1.1) — a plain speed test. Pass --http-versions to run each
# manifest once per listed version instead, to compare protocol performance
# against the same server (e.g. HTTP/1.1 vs HTTP/2).
#
# Usage:
#   scripts/bench_download.sh [manifest-or-dir] [--output-dir DIR]
#                              [--http-versions v1[,v2,...]] [-- extra-cli-args...]
#
# manifest-or-dir defaults to ./benchmarkManifests. Point it at a single
# manifest file to benchmark just that one, or at a directory to benchmark
# every manifest file directly inside it (.tcia, .s5cmd, .csv, .tsv, .xlsx,
# .json, .jsonld).
#
# Examples:
#   scripts/bench_download.sh
#   scripts/bench_download.sh ./benchmarkManifests/pathdb-test.csv
#   scripts/bench_download.sh --http-versions 1.1,2
#   scripts/bench_download.sh --output-dir /tmp/bench -- --max-connections 4

set -euo pipefail

MANIFEST_PATH="./benchmarkManifests"
OUTPUT_BASE="./bench-results"
HTTP_VERSIONS_RAW="1.1"
manifest_path_set=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      [[ $# -ge 2 ]] || { echo "--output-dir requires a value" >&2; exit 1; }
      OUTPUT_BASE="$2"
      shift 2
      ;;
    --http-versions)
      [[ $# -ge 2 ]] || { echo "--http-versions requires a value" >&2; exit 1; }
      HTTP_VERSIONS_RAW="$2"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
    *)
      if [[ "$manifest_path_set" == true ]]; then
        echo "Unexpected extra argument: $1 (put CLI passthrough args after --)" >&2
        exit 1
      fi
      MANIFEST_PATH="$1"
      manifest_path_set=true
      shift
      ;;
  esac
done
EXTRA_ARGS=("$@")

IFS=',' read -r -a VERSIONS <<< "$HTTP_VERSIONS_RAW"

if [[ ! -e "$MANIFEST_PATH" ]]; then
  echo "Manifest path not found: $MANIFEST_PATH" >&2
  exit 1
fi

MANIFESTS=()
if [[ -f "$MANIFEST_PATH" ]]; then
  MANIFESTS=("$MANIFEST_PATH")
else
  while IFS= read -r -d '' f; do
    MANIFESTS+=("$f")
  done < <(find "$MANIFEST_PATH" -maxdepth 1 -type f \
    \( -iname "*.tcia" -o -iname "*.s5cmd" -o -iname "*.csv" -o -iname "*.tsv" \
       -o -iname "*.xlsx" -o -iname "*.json" -o -iname "*.jsonld" \) \
    -print0 | sort -z)

  if [[ ${#MANIFESTS[@]} -eq 0 ]]; then
    echo "No manifest files found in $MANIFEST_PATH" >&2
    echo "Drop in .tcia, .s5cmd, .csv/.tsv/.xlsx, or .json/.jsonld manifests, or point at one directly, and re-run." >&2
    exit 1
  fi
fi

echo "Found ${#MANIFESTS[@]} manifest(s):"
printf '  %s\n' "${MANIFESTS[@]}"
echo "HTTP version(s) under test: ${VERSIONS[*]}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$(mktemp -d)/tcia_bench"
CSV="$OUTPUT_BASE/bench_results.csv"

mkdir -p "$OUTPUT_BASE"
if [[ ! -f "$CSV" ]]; then
  echo "timestamp,manifest,http_version_requested,http_protocol_actual,elapsed_seconds,total_mb,throughput_mbps,files,exit_code" > "$CSV"
fi

echo
echo "Building CLI binary..."
( cd "$REPO_ROOT" && go build -o "$BIN" . )

for manifest in "${MANIFESTS[@]}"; do
  manifest_name="$(basename "$manifest")"
  manifest_slug="${manifest_name%.*}"
  manifest_out="$OUTPUT_BASE/$manifest_slug"

  ELAPSED=()
  TOTAL_MB=()
  THROUGHPUT=()
  FILES=()
  EXIT_CODE=()
  ACTUAL_PROTOCOL=()

  echo
  echo "########################################"
  echo "# Manifest: $manifest_name"
  echo "########################################"

  for version in "${VERSIONS[@]}"; do
    run_dir="$manifest_out/http-${version}"
    rm -rf "$run_dir"
    mkdir -p "$run_dir"
    run_log="$run_dir.log"

    echo
    echo "=== Running under HTTP/$version -> $run_dir ==="

    start=$(date +%s)
    set +e
    "$BIN" --cli --accept-data-policy \
      --input "$manifest" \
      --output "$run_dir" \
      --http-version "$version" \
      ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} 2>&1 | tee "$run_log"
    code=${PIPESTATUS[0]}
    set -e
    end=$(date +%s)

    elapsed=$(( end - start ))
    [[ $elapsed -lt 1 ]] && elapsed=1

    # -k gives kilobytes on both macOS and Linux; convert to MB.
    total_kb=$(du -sk "$run_dir" 2>/dev/null | awk '{print $1}')
    total_mb=$(awk -v kb="$total_kb" 'BEGIN { printf "%.2f", kb / 1024 }')
    throughput=$(awk -v mb="$total_mb" -v s="$elapsed" 'BEGIN { printf "%.2f", mb / s }')
    file_count=$(find "$run_dir" -type f | wc -l | tr -d ' ')

    # The binary logs the protocol net/http actually negotiated per request
    # (requesting HTTP/2 only *asks* for it via ALPN; a server without
    # HTTP/2 support still gets answered over HTTP/1.1), e.g.:
    #   HTTP-PROTOCOL-STATS: HTTP/1.1=0 HTTP/2.0=42
    actual_protocol=$(grep -o 'HTTP-PROTOCOL-STATS: .*' "$run_log" | tail -1 | sed 's/^HTTP-PROTOCOL-STATS: //')
    [[ -z "$actual_protocol" ]] && actual_protocol="unknown"

    ELAPSED+=("$elapsed")
    TOTAL_MB+=("$total_mb")
    THROUGHPUT+=("$throughput")
    FILES+=("$file_count")
    EXIT_CODE+=("$code")
    ACTUAL_PROTOCOL+=("$actual_protocol")

    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),$manifest_name,$version,$actual_protocol,$elapsed,$total_mb,$throughput,$file_count,$code" >> "$CSV"
    rm -f "$run_log"

    if [[ $code -ne 0 ]]; then
      echo "WARNING: run under HTTP/$version exited with code $code" >&2
    fi
  done

  echo
  echo "=== Summary: $manifest_name ==="
  printf "%-10s %10s %10s %14s %8s %6s   %s\n" "Requested" "Elapsed(s)" "Total(MB)" "Throughput(MB/s)" "Files" "Exit" "Actual protocol used"
  for i in "${!VERSIONS[@]}"; do
    printf "%-10s %10s %10s %14s %8s %6s   %s\n" \
      "HTTP/${VERSIONS[$i]}" "${ELAPSED[$i]}" "${TOTAL_MB[$i]}" "${THROUGHPUT[$i]}" "${FILES[$i]}" "${EXIT_CODE[$i]}" "${ACTUAL_PROTOCOL[$i]}"
  done
done

rm -rf "$(dirname "$BIN")"

echo
echo "Full history: $CSV"
