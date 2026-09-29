#!/usr/bin/env bash
# Start a server binary against a fresh empty database, drive it with the
# loadgen, then stop it.
#
#   benches/run.sh <server-binary> [loadgen key=value ...]
#
# Server env can be passed through the environment (MEM_CACHE_BYTES=...).
# PORT picks the listen port (default 18080). KEEP_DB=1 keeps the temp dir.
set -euo pipefail

BIN="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
shift
HERE="$(cd "$(dirname "$0")" && pwd)"
LOADGEN="${LOADGEN:-$HERE/loadgen/target/release/loadgen}"
PORT="${PORT:-18080}"
DIR="$(mktemp -d "${TMPDIR:-/tmp}/hcs-bench.XXXXXX")"

(cd "$DIR" && CACHE_PORT="$PORT" MEILI_DISABLE=1 RUST_LOG="${RUST_LOG:-warn}" exec "$BIN" >"$DIR/server.log" 2>&1) &
PID=$!
cleanup() {
  kill "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  if [[ "${KEEP_DB:-0}" != "1" ]]; then rm -rf "$DIR"; else echo "kept $DIR"; fi
}
trap cleanup EXIT

for _ in $(seq 1 100); do
  curl -fsS "http://127.0.0.1:$PORT/cache/size" >/dev/null 2>&1 && break
  sleep 0.1
done
echo "[run] $BIN pid=$PID port=$PORT db=$DIR"
"$LOADGEN" url="http://127.0.0.1:$PORT" pid="$PID" "$@"
if curl -fsS "http://127.0.0.1:$PORT/metrics" -o "$DIR/metrics.txt" 2>/dev/null; then
  echo "[run] server memory gauges at the end:"
  grep -E '^(jemalloc_|mem_cache_bytes|mem_cache_entries|inflight_budget_available)' "$DIR/metrics.txt" || true
fi
echo "[run] server log tail:"
tail -n 5 "$DIR/server.log" || true
