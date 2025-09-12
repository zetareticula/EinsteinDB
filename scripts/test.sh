#!/usr/bin/env bash
set -euo pipefail

# Run workspace tests
# Usage: scripts/test.sh [--release]

MODE=""
if [[ ${1:-} == "--release" ]]; then
  MODE="--release"
fi

cargo test --workspace ${MODE}
