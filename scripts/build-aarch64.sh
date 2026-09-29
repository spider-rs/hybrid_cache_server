#!/usr/bin/env bash
# Cross-build the Graviton3 (c7g) Linux binary from macOS with cargo-zigbuild.
#
#   scripts/build-aarch64.sh
#
# Needs: zig, cargo-zigbuild, rustup target aarch64-unknown-linux-gnu.
# Output: target-aarch64/aarch64-unknown-linux-gnu/release/hybrid_cache_server
# The binary needs glibc 2.25 or newer and links libc++ statically, so the
# host needs no libstdc++.
set -euo pipefail
cd "$(dirname "$0")/.."

# jemalloc's configure calls plain `ar`. On macOS that is Apple's ar, which
# silently writes an empty archive for ELF objects, and the link then fails
# with undefined _rjem_malloc. Put zig's ar and ranlib first on PATH.
shim="$(mktemp -d)"
trap 'rm -r "$shim"' EXIT
printf '#!/bin/sh\nexec zig ar "$@"\n' >"$shim/ar"
printf '#!/bin/sh\nexec zig ranlib "$@"\n' >"$shim/ranlib"
chmod +x "$shim/ar" "$shim/ranlib"

PATH="$shim:$PATH" \
  RUSTFLAGS="-C target-cpu=neoverse-v1" \
  CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target-aarch64}" \
  cargo zigbuild --release --target aarch64-unknown-linux-gnu.2.26

out="${CARGO_TARGET_DIR:-target-aarch64}/aarch64-unknown-linux-gnu/release/hybrid_cache_server"
file "$out"
shasum -a 256 "$out"
