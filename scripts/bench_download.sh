#!/usr/bin/env bash
# Benchmark TCIA Data Retriever against one manifest or every manifest directly
# inside a directory. The harness preserves per-trial logs, validates the CLI's
# final item counts, and records enough provenance to compare repeated runs.

set -euo pipefail

MANIFEST_PATH="./benchmarkManifests"
OUTPUT_BASE="./bench-results"
HTTP_VERSIONS_RAW="1.1"
RUNS=1
BINARY=""
RUN_LABEL=""
manifest_path_set=false

usage() {
  cat <<'EOF'
Usage:
  scripts/bench_download.sh [manifest-or-dir] [options] [-- extra-cli-args...]

Options:
  --output-dir DIR          Result and payload root (default: ./bench-results)
  --http-versions LIST      Comma-separated 1.1 and/or 2 (default: 1.1)
  --runs N                  Repetitions per manifest/protocol (default: 1)
  --binary FILE             Use an existing Data Retriever binary
  --label TEXT              Label recorded with every result row
  -h, --help                Show this help
EOF
}

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
    --runs)
      [[ $# -ge 2 ]] || { echo "--runs requires a value" >&2; exit 1; }
      RUNS="$2"
      shift 2
      ;;
    --binary)
      [[ $# -ge 2 ]] || { echo "--binary requires a value" >&2; exit 1; }
      BINARY="$2"
      shift 2
      ;;
    --label)
      [[ $# -ge 2 ]] || { echo "--label requires a value" >&2; exit 1; }
      RUN_LABEL="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "Unknown option: $1" >&2
      usage >&2
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

if [[ ! "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
  echo "--runs must be a positive integer, got: $RUNS" >&2
  exit 1
fi

IFS=',' read -r -a VERSIONS <<< "$HTTP_VERSIONS_RAW"
for version in "${VERSIONS[@]}"; do
  if [[ "$version" != "1.1" && "$version" != "2" ]]; then
    echo "Unsupported HTTP version '$version'; use 1.1 and/or 2" >&2
    exit 1
  fi
done

if [[ ! -e "$MANIFEST_PATH" ]]; then
  echo "Manifest path not found: $MANIFEST_PATH" >&2
  exit 1
fi

MANIFESTS=()
if [[ -f "$MANIFEST_PATH" ]]; then
  MANIFESTS=("$MANIFEST_PATH")
else
  while IFS= read -r f; do
    MANIFESTS+=("$f")
  done < <(find "$MANIFEST_PATH" -maxdepth 1 -type f \
    \( -iname "*.tcia" -o -iname "*.s5cmd" -o -iname "*.csv" -o -iname "*.tsv" \
       -o -iname "*.xlsx" -o -iname "*.json" -o -iname "*.jsonld" \) \
    -print | LC_ALL=C sort)

  if [[ ${#MANIFESTS[@]} -eq 0 ]]; then
    echo "No supported manifest files found in $MANIFEST_PATH" >&2
    exit 1
  fi
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_TMP="$(mktemp -d "${TMPDIR:-/tmp}/tcia-bench.XXXXXX")"
OWNED_FRONTEND_STUB=""

cleanup() {
  rm -rf "$SCRIPT_TMP"
  if [[ -n "$OWNED_FRONTEND_STUB" ]]; then
    rm -f "$OWNED_FRONTEND_STUB"
    rmdir "$(dirname "$OWNED_FRONTEND_STUB")" 2>/dev/null || true
  fi
}

on_interrupt() {
  trap - EXIT INT TERM
  cleanup
  exit 130
}

trap cleanup EXIT
trap on_interrupt INT TERM

hash_file() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  elif command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    echo "unavailable"
  fi
}

csv_field() {
  local value="$1"
  value=${value//\"/\"\"}
  printf '"%s"' "$value"
}

summary_value() {
  local label="$1"
  local log_file="$2"
  tr '\r' '\n' < "$log_file" | awk -F': ' -v key="$label" '$1 == key {value=$2} END {print value}'
}

if [[ -n "$BINARY" ]]; then
  if [[ ! -x "$BINARY" ]]; then
    echo "Benchmark binary is not executable: $BINARY" >&2
    exit 1
  fi
  BIN="$(cd "$(dirname "$BINARY")" && pwd)/$(basename "$BINARY")"
else
  BIN="$SCRIPT_TMP/tcia_bench"
  # main.go embeds frontend/dist even though --cli never serves GUI assets.
  # A clean checkout intentionally ignores dist, so provide a temporary stub
  # solely for the CLI build rather than requiring an Angular build first.
  if [[ ! -d "$REPO_ROOT/frontend/dist" ]] || \
     [[ -z "$(find "$REPO_ROOT/frontend/dist" -type f ! -name '.*' -print -quit 2>/dev/null)" ]]; then
    mkdir -p "$REPO_ROOT/frontend/dist"
    OWNED_FRONTEND_STUB="$REPO_ROOT/frontend/dist/index.html"
    printf '<!doctype html><title>CLI benchmark build</title>\n' > "$OWNED_FRONTEND_STUB"
  fi
  echo "Building CLI benchmark binary from $(git -C "$REPO_ROOT" rev-parse --short HEAD)..."
  (cd "$REPO_ROOT" && go build -o "$BIN" .)
fi

BINARY_SHA256="$(hash_file "$BIN")"
GIT_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
CSV="$OUTPUT_BASE/bench_results_v2.csv"
LOG_BASE="$OUTPUT_BASE/logs"
EXPECTED_HEADER='timestamp_utc,run_label,manifest,manifest_sha256,binary_sha256,git_commit,http_version_requested,http_protocol_actual,run_index,elapsed_seconds,disk_mb,disk_mb_per_second,files,total_items,downloaded,synced,skipped,failed,exit_code,benchmark_status,log_path'

mkdir -p "$OUTPUT_BASE" "$LOG_BASE"
if [[ -f "$CSV" ]]; then
  existing_header="$(sed -n '1p' "$CSV")"
  if [[ "$existing_header" != "$EXPECTED_HEADER" ]]; then
    echo "Unexpected results schema in $CSV; move it aside before running" >&2
    exit 1
  fi
else
  echo "$EXPECTED_HEADER" > "$CSV"
fi

echo "Found ${#MANIFESTS[@]} manifest(s); protocols: ${VERSIONS[*]}; runs per arm: $RUNS"
echo "Binary SHA-256: $BINARY_SHA256"

OVERALL_FAILURE=0
for manifest in "${MANIFESTS[@]}"; do
  manifest_name="$(basename "$manifest")"
  manifest_slug="${manifest_name%.*}"
  manifest_sha256="$(hash_file "$manifest")"

  # Interleave protocol arms within each repetition to reduce time-of-day and
  # cache drift (1.1 run 1, 2 run 1, 1.1 run 2, 2 run 2, ...).
  for ((run_index=1; run_index<=RUNS; run_index++)); do
    for version in "${VERSIONS[@]}"; do
      trial_id="$(date -u +%Y%m%dT%H%M%SZ)-$$-$run_index"
      run_dir="$OUTPUT_BASE/payloads/$manifest_slug/http-${version}/$trial_id"
      log_dir="$LOG_BASE/$manifest_slug/http-${version}"
      run_log="$log_dir/$trial_id.log"
      mkdir -p "$run_dir" "$log_dir"

      echo
      echo "=== $manifest_name | HTTP/$version | run $run_index/$RUNS ==="
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

      elapsed=$((end - start))
      [[ $elapsed -lt 1 ]] && elapsed=1
      total_kb=$(du -sk "$run_dir" 2>/dev/null | awk '{print $1}')
      disk_mb=$(awk -v kb="$total_kb" 'BEGIN { printf "%.2f", kb / 1024 }')
      disk_rate=$(awk -v mb="$disk_mb" -v s="$elapsed" 'BEGIN { printf "%.2f", mb / s }')
      file_count=$(find "$run_dir" -type f | wc -l | tr -d ' ')

      actual_protocol=$(grep -o 'HTTP-PROTOCOL-STATS: .*' "$run_log" 2>/dev/null | tail -1 | sed 's/^HTTP-PROTOCOL-STATS: //' || true)
      [[ -z "$actual_protocol" ]] && actual_protocol="unknown"

      total_items="$(summary_value "Total items" "$run_log")"
      downloaded="$(summary_value "Downloaded" "$run_log")"
      synced="$(summary_value "Synced" "$run_log")"
      skipped="$(summary_value "Skipped" "$run_log")"
      failed="$(summary_value "Failed" "$run_log")"
      total_items=${total_items:-unknown}
      downloaded=${downloaded:-unknown}
      synced=${synced:-unknown}
      skipped=${skipped:-unknown}
      failed=${failed:-unknown}

      status="success"
      if [[ $code -ne 0 ]]; then
        status="process_error"
      elif [[ "$total_items" == "unknown" || "$downloaded" == "unknown" || \
              "$synced" == "unknown" || "$skipped" == "unknown" || "$failed" == "unknown" ]]; then
        status="incomplete_summary"
      elif [[ "$failed" -gt 0 ]]; then
        status="download_failures"
      elif [[ $((downloaded + synced + skipped + failed)) -ne "$total_items" ]]; then
        status="inconsistent_summary"
      fi

      if [[ "$status" != "success" ]]; then
        OVERALL_FAILURE=1
        echo "WARNING: benchmark status is $status; inspect $run_log" >&2
      fi

      timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      {
        csv_field "$timestamp"; printf ','
        csv_field "$RUN_LABEL"; printf ','
        csv_field "$manifest_name"; printf ','
        csv_field "$manifest_sha256"; printf ','
        csv_field "$BINARY_SHA256"; printf ','
        csv_field "$GIT_COMMIT"; printf ','
        csv_field "$version"; printf ','
        csv_field "$actual_protocol"; printf ','
        csv_field "$run_index"; printf ','
        csv_field "$elapsed"; printf ','
        csv_field "$disk_mb"; printf ','
        csv_field "$disk_rate"; printf ','
        csv_field "$file_count"; printf ','
        csv_field "$total_items"; printf ','
        csv_field "$downloaded"; printf ','
        csv_field "$synced"; printf ','
        csv_field "$skipped"; printf ','
        csv_field "$failed"; printf ','
        csv_field "$code"; printf ','
        csv_field "$status"; printf ','
        csv_field "$run_log"; printf '\n'
      } >> "$CSV"

      printf 'Result: status=%s elapsed=%ss disk=%sMB disk-rate=%sMB/s files=%s protocol=%s\n' \
        "$status" "$elapsed" "$disk_mb" "$disk_rate" "$file_count" "$actual_protocol"
    done
  done
done

echo
echo "Results: $CSV"
echo "Logs: $LOG_BASE"
exit "$OVERALL_FAILURE"
