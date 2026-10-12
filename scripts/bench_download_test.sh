#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/tcia-bench-test.XXXXXX")"
trap 'rm -rf "$TEST_ROOT"' EXIT INT TERM

FAKE_BINARY="$TEST_ROOT/fake-retriever"
cat > "$FAKE_BINARY" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
output=""
input=""
version=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output="$2"; shift 2 ;;
    --input) input="$2"; shift 2 ;;
    --http-version) version="$2"; shift 2 ;;
    *) shift ;;
  esac
done
mkdir -p "$output/data"
printf 'payload\n' > "$output/data/file.bin"
echo "=== Download Summary ==="
echo "Total items: 2"
if [[ "$(basename "$input")" == fail.csv ]]; then
  echo "Downloaded: 1"
  echo "Synced: 0"
  echo "Skipped: 0"
  echo "Failed: 1"
else
  echo "Downloaded: 2"
  echo "Synced: 0"
  echo "Skipped: 0"
  echo "Failed: 0"
  if [[ "$version" == "2" ]]; then
    echo "HTTP-PROTOCOL-STATS: HTTP/2.0=2"
  else
    echo "HTTP-PROTOCOL-STATS: HTTP/1.1=2"
  fi
fi
EOF
chmod +x "$FAKE_BINARY"

printf 'imageUrl\nhttps://example.invalid/a\n' > "$TEST_ROOT/success.csv"
printf 'imageUrl\nhttps://example.invalid/b\n' > "$TEST_ROOT/fail.csv"

"$REPO_ROOT/scripts/bench_download.sh" "$TEST_ROOT/success.csv" \
  --output-dir "$TEST_ROOT/success-output" \
  --binary "$FAKE_BINARY" \
  --http-versions 1.1,2 \
  --runs 2 \
  --label smoke >/dev/null

SUCCESS_CSV="$TEST_ROOT/success-output/bench_results_v2.csv"
[[ "$(wc -l < "$SUCCESS_CSV" | tr -d ' ')" == 5 ]]
[[ "$(grep -c '"success"' "$SUCCESS_CSV")" == 4 ]]
[[ "$(find "$TEST_ROOT/success-output/logs" -name '*.log' | wc -l | tr -d ' ')" == 4 ]]

if "$REPO_ROOT/scripts/bench_download.sh" "$TEST_ROOT/fail.csv" \
  --output-dir "$TEST_ROOT/fail-output" \
  --binary "$FAKE_BINARY" >/dev/null 2>&1; then
  echo "expected failed download summary to fail the benchmark" >&2
  exit 1
fi

FAIL_CSV="$TEST_ROOT/fail-output/bench_results_v2.csv"
grep -q '"download_failures"' "$FAIL_CSV"
grep -q '"unknown"' "$FAIL_CSV"

echo "bench_download.sh tests passed"
