#!/usr/bin/env bash
set -euo pipefail

# Run the relativistic/causal consistency demo
# Usage: scripts/run-demo.sh [--release] [-- args...]

MODE=""
if [[ ${1:-} == "--release" ]]; then
  MODE="--release"
  shift || true
fi

# Prefer the example target if available
if grep -q "\[\[bin\]\]" Cargo.toml && rg -n "consistency_demo" --glob '!target' >/dev/null 2>&1; then
  : # Not a dedicated bin; fall back to examples
fi

if cargo run ${MODE} --example consistency_demo -- "$@"; then
  exit 0
fi

# Fallback to standalone demo if example target is not configured
if [[ -f "standalone_consistency_demo.rs" ]]; then
  echo "Compiling standalone_consistency_demo.rs with rustc..."
  rustc standalone_consistency_demo.rs -O -o target/standalone_consistency_demo
  ./target/standalone_consistency_demo "$@"
  exit 0
fi

echo "No demo target found. Ensure examples/consistency_demo.rs exists or configure a bin." >&2
exit 1
