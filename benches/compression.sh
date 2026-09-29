#!/usr/bin/env bash
# Compare response compression settings on /cache/site responses.
# For each setting: start the server, seed it, fetch the largest sites N
# times, and report bytes on the wire, wall time and server CPU seconds.
#
#   benches/compression.sh <server-binary>
set -euo pipefail
BIN="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
HERE="$(cd "$(dirname "$0")" && pwd)"
LOADGEN="${LOADGEN:-$HERE/loadgen/target/release/loadgen}"
PORT="${PORT:-18081}"
N="${N:-5}"

cpu_secs() { # ps TIME as m:ss.xx -> seconds
  ps -o time= -p "$1" | awk -F: '{ if (NF==3) print $1*3600+$2*60+$3; else print $1*60+$2 }'
}

printf "%-10s %-6s %12s %12s %8s %10s\n" setting level wire_MiB json_MiB ratio cpu_s
for setting in off:0 gzip:1 gzip:6 br:1 br:4 zstd:1 zstd:3; do
  alg="${setting%%:*}"; lvl="${setting##*:}"
  DIR="$(mktemp -d "${TMPDIR:-/tmp}/hcs-comp.XXXXXX")"
  (cd "$DIR" && CACHE_PORT="$PORT" COMPRESSION="$alg" COMPRESSION_LEVEL="$lvl" MAX_SITE_RESPONSE_BYTES=268435456 RUST_LOG=warn exec "$BIN" >"$DIR/log" 2>&1) &
  PID=$!
  for _ in $(seq 1 100); do curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && break; sleep 0.1; done
  "$LOADGEN" url="http://127.0.0.1:$PORT" scenario=seed sites=20 >/dev/null
  enc="$alg"; [[ "$alg" == off ]] && enc=identity
  c0=$(cpu_secs "$PID")
  wire=0; json=0
  for i in $(seq 1 "$N"); do
    for site in site-0 site-1 site-2; do
      w=$(curl -sS -H "Accept-Encoding: $enc" -o "$DIR/out" -w '%{size_download}' "http://127.0.0.1:$PORT/cache/site/$site")
      j=$(curl -sS -o /dev/null -w '%{size_download}' -H 'Accept-Encoding: identity' "http://127.0.0.1:$PORT/cache/site/$site")
      wire=$((wire + w)); json=$((json + j))
    done
  done
  c1=$(cpu_secs "$PID")
  kill "$PID"; wait "$PID" 2>/dev/null || true; rm -rf "$DIR"
  awk -v s="$alg" -v l="$lvl" -v w="$wire" -v j="$json" -v c0="$c0" -v c1="$c1" \
    'BEGIN { printf "%-10s %-6s %12.1f %12.1f %8.3f %10.2f\n", s, l, w/1048576, j/1048576, w/j, c1-c0 }'
done
echo "cpu_s includes serving the identity copies used to count json_MiB; subtract the off row."
