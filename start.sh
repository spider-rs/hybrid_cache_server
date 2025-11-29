
#!/usr/bin/env bash
set -euo pipefail

# -------- config (override via env) --------
export MEILI_HOST="${MEILI_HOST:-http://127.0.0.1:7700}"
export MEILI_MASTER_KEY="${MEILI_MASTER_KEY:-masterKey}"
export MEILI_INDEX="${MEILI_INDEX:-hybrid_cache}"
export CACHE_PORT="${CACHE_PORT:-80}"

MEILI_HTTP_ADDR="${MEILI_HTTP_ADDR:-127.0.0.1:7700}"
MEILI_DB_PATH="${MEILI_DB_PATH:-./meili_data}"
MEILI_DUMP_DIR="${MEILI_DUMP_DIR:-./meili_dumps}"
MEILI_LOG_LEVEL="${MEILI_LOG_LEVEL:-INFO}"

# -------- helpers --------
cleanup() {
  # kill child process group on exit
  trap - EXIT INT TERM
  kill 0 >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# -------- start meilisearch --------
mkdir -p "$MEILI_DB_PATH" "$MEILI_DUMP_DIR"

echo "[boot] starting meilisearch on $MEILI_HTTP_ADDR"
./meilisearch \
  --http-addr "$MEILI_HTTP_ADDR" \
  --db-path "$MEILI_DB_PATH" \
  --dump-dir "$MEILI_DUMP_DIR" \
  --log-level "$MEILI_LOG_LEVEL" \
  --master-key "$MEILI_MASTER_KEY" &
MEILI_PID=$!

# wait until meili is up
echo "[boot] waiting for meilisearch..."
for _ in $(seq 1 120); do
  if curl -fsS "http://${MEILI_HTTP_ADDR}/health" >/dev/null 2>&1; then
    echo "[boot] meilisearch is healthy (pid=$MEILI_PID)"
    break
  fi
  sleep 0.25
done

if ! curl -fsS "http://${MEILI_HTTP_ADDR}/health" >/dev/null 2>&1; then
  echo "[boot] meilisearch failed to become healthy; exiting" >&2
  exit 1
fi

# -------- start cache server --------
echo "[boot] starting hybrid_cache_server on port $CACHE_PORT (MEILI_HOST=$MEILI_HOST, MEILI_INDEX=$MEILI_INDEX)"
exec hybrid_cache_server
