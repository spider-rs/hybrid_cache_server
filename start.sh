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

# -------- start/reuse meilisearch (only when MEILI_ENABLE=1) --------
# The server does not read the Meilisearch index back, so since v0.3 it is
# off by default. MEILI_ENABLE=1 turns indexing on in the server and makes
# this script start meilisearch. Every other variable in the environment
# (CACHE_PORT, ROCKSDB_PATH, MEM_CACHE_BYTES, ...) passes through the exec
# below unchanged.
meili_enabled() {
  case "${MEILI_ENABLE:-0}" in 1|true|TRUE|yes|YES) ;; *) return 1 ;; esac
  case "${MEILI_DISABLE:-0}" in 1|true|TRUE|yes|YES) return 1 ;; esac
  return 0
}

if ! meili_enabled; then
  echo "[boot] MEILI_ENABLE is not 1; not starting meilisearch"
elif curl -fsS "$health_url" >/dev/null 2>&1; then
  echo "[boot] meilisearch already healthy at $MEILI_HTTP_ADDR; reusing"
else
  mkdir -p "$MEILI_DB_PATH" "$MEILI_DUMP_DIR"
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
echo "[boot] starting hybrid_cache_server on port $CACHE_PORT (MEILI_ENABLE=${MEILI_ENABLE:-0}, ROCKSDB_PATH=${ROCKSDB_PATH:-cache_db})"

# cargo install hybrid_cache_server

exec hybrid_cache_server
