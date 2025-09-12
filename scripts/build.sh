#!/usr/bin/env bash
set -euo pipefail

# Build the root package (default) in release or debug
# Usage: scripts/build.sh [--release] [--package <name>]

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required" >&2
  exit 1
fi

MODE=""
PKG="${PACKAGE:-einsteindb-prod}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --release)
      MODE="--release"; shift ;;
    --package)
      PKG="$2"; shift 2 ;;
    *)
      echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

export RUSTFLAGS="${RUSTFLAGS:-} -C debuginfo=1"

cargo build -p "$PKG" ${MODE}
