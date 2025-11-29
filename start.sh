#!/usr/bin/env bash
set -euo pipefail

# -------- config (override via env) --------
export MEILI_HOST="${MEILI_HOST:-http://127.0.0.1:7700}"
export MEILI_MASTER_KEY="${MEILI_MASTER_KEY:-masterKey}"
export MEILI_INDEX="${MEILI_INDEX:-hybrid_cache}"
export CACHE_PORT="${CACHE_PORT:-8080}"

MEILI_HTTP_ADDR="${MEILI_HTTP_ADDR:-127.0.0.1:7700}"
MEILI_DB_PATH="${MEILI_DB_PATH:-./meili_data}"
MEILI_DUMP_DIR="${MEILI_DUMP_DIR:-./meili_dumps}"
MEILI_LOG_LEVEL="${MEILI_LOG_LEVEL:-INFO}"

# -------- helpers --------
MEILI_STARTED_BY_SCRIPT=0

health_url="http://${MEILI_HTTP_ADDR}/health"

kill_port_listeners() {
  # kills any process listening on the meili port (127.0.0.1:7700 -> port 7700)
  local port="${MEILI_HTTP_ADDR##*:}"
  if command -v lsof >/dev/null 2>&1; then
    sudo lsof -tiTCP:"$port" -sTCP:LISTEN | xargs -r sudo kill -9 || true
  else
    # fuser is usually present on AL; fallback
    sudo fuser -k -n tcp "$port" >/dev/null 2>&1 || true
  fi
}

cleanup() {
  trap - EXIT INT TERM
  # Only kill Meili if we started it in this script.
  if [[ "$MEILI_STARTED_BY_SCRIPT" -eq 1 ]]; then
    kill "${MEILI_PID:-}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

wait_for_meili() {
  for _ in $(seq 1 120); do
    if curl -fsS "$health_url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

# -------- start/reuse meilisearch --------
mkdir -p "$MEILI_DB_PATH" "$MEILI_DUMP_DIR"

if curl -fsS "$health_url" >/dev/null 2>&1; then
  echo "[boot] meilisearch already healthy at $MEILI_HTTP_ADDR; reusing"
else
  echo "[boot] meilisearch not healthy; restarting anything on port ${MEILI_HTTP_ADDR##*:}"
  kill_port_listeners

  echo "[boot] starting meilisearch on $MEILI_HTTP_ADDR"
  ./meilisearch \
    --http-addr "$MEILI_HTTP_ADDR" \
    --db-path "$MEILI_DB_PATH" \
    --dump-dir "$MEILI_DUMP_DIR" \
    --log-level "$MEILI_LOG_LEVEL" \
    --master-key "$MEILI_MASTER_KEY" &
  MEILI_PID=$!
  MEILI_STARTED_BY_SCRIPT=1

  echo "[boot] waiting for meilisearch..."
  if ! wait_for_meili; then
    echo "[boot] meilisearch failed to become healthy; exiting" >&2
    exit 1
  fi
  echo "[boot] meilisearch is healthy (pid=$MEILI_PID)"
fi

# -------- start cache server --------
echo "[boot] starting hybrid_cache_server on port $CACHE_PORT (MEILI_HOST=$MEILI_HOST, MEILI_INDEX=$MEILI_INDEX)"

# cargo install hybrid_cache_server

exec hybrid_cache_server
