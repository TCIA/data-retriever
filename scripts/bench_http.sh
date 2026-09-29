#!/usr/bin/env bash
# Runs every manifest found in a folder through the CLI once under HTTP/1.1
# and once under HTTP/2, timing each run and measuring bytes actually
# written to disk, so the two protocols can be compared against identical
# NBIA/TCIA/IDC servers.
#
# Usage:
#   scripts/bench_http.sh [manifests-dir] [output-base-dir] [-- extra-cli-args...]
#
# manifests-dir defaults to ./benchmarkManifests. Drop any manifest files
# the CLI accepts in there (.tcia, .s5cmd, .csv, .tsv, .xlsx, .json,
# .jsonld) and every one of them is benchmarked in a single run.
#
# Example:
#   scripts/bench_http.sh
#   scripts/bench_http.sh ./benchmarkManifests /tmp/http-bench
#   scripts/bench_http.sh ./benchmarkManifests /tmp/http-bench -- --max-connections 4
#
# Results for every manifest x protocol combination are appended as a row
# to bench_http_results.csv in the output base dir, so results from
# multiple manifests/machines/runs accumulate in one place.

set -euo pipefail

MANIFESTS_DIR="./benchmarkManifests"
if [[ $# -gt 0 && "$1" != "--" ]]; then
  MANIFESTS_DIR="$1"
  shift
fi

OUTPUT_BASE="./bench-http-results"
if [[ $# -gt 0 && "$1" != "--" ]]; then
  OUTPUT_BASE="$1"
  shift
fi

if [[ $# -gt 0 && "$1" == "--" ]]; then
  shift
fi
EXTRA_ARGS=("$@")

if [[ ! -d "$MANIFESTS_DIR" ]]; then
  echo "Manifests directory not found: $MANIFESTS_DIR" >&2
  exit 1
fi

MANIFESTS=()
while IFS= read -r -d '' f; do
  MANIFESTS+=("$f")
done < <(find "$MANIFESTS_DIR" -maxdepth 1 -type f \
  \( -iname "*.tcia" -o -iname "*.s5cmd" -o -iname "*.csv" -o -iname "*.tsv" \
     -o -iname "*.xlsx" -o -iname "*.json" -o -iname "*.jsonld" \) \
  -print0 | sort -z)

if [[ ${#MANIFESTS[@]} -eq 0 ]]; then
  echo "No manifest files found in $MANIFESTS_DIR" >&2
  echo "Drop in .tcia, .s5cmd, .csv/.tsv/.xlsx, or .json/.jsonld manifests and re-run." >&2
  exit 1
fi

echo "Found ${#MANIFESTS[@]} manifest(s) in $MANIFESTS_DIR:"
printf '  %s\n' "${MANIFESTS[@]}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$(mktemp -d)/tcia_bench"
CSV="$OUTPUT_BASE/bench_http_results.csv"

mkdir -p "$OUTPUT_BASE"
if [[ ! -f "$CSV" ]]; then
  echo "timestamp,manifest,http_version_requested,http_protocol_actual,elapsed_seconds,total_mb,throughput_mbps,files,exit_code" > "$CSV"
fi

echo
echo "Building CLI binary..."
( cd "$REPO_ROOT" && go build -o "$BIN" . )

VERSIONS=("1.1" "2")

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
    echo "=== Running with HTTP/$version -> $run_dir ==="

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
    # (HTTPVersion2 only *asks* for HTTP/2 via ALPN; a server without HTTP/2
    # support still gets answered over HTTP/1.1), e.g.:
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
