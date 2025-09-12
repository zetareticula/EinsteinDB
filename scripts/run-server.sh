#!/usr/bin/env bash
set -euo pipefail

# Run the EinsteinDB server binary if it builds
# Usage: scripts/run-server.sh [--release] [-- args...]

MODE=""
if [[ ${1:-} == "--release" ]]; then
  MODE="--release"
  shift || true
fi

# Prefer building via direct manifest to avoid resolving the whole workspace
if cargo run --manifest-path einsteindb-server/Cargo.toml ${MODE} -- "$@"; then
  exit 0
fi

echo "einsteindb_server failed to build or run. Falling back to relativistic demo."
"$(dirname "$0")/run-demo.sh" ${MODE} -- "$@"
