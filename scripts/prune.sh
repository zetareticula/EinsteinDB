#!/usr/bin/env bash
set -euo pipefail

# Prune build artifacts and detect unused dependencies/types
# Usage: scripts/prune.sh [--aggressive]

AGGRESSIVE=false
if [[ ${1:-} == "--aggressive" ]]; then
  AGGRESSIVE=true
fi

# Clean incremental caches older than 30 days
if command -v cargo-sweep >/dev/null 2>&1; then
  echo "Sweeping old build artifacts (>=30 days)"
  cargo sweep -t 30 || true
fi

# Full clean if aggressive
if [[ "$AGGRESSIVE" == true ]]; then
  echo "Performing cargo clean (aggressive)"
  cargo clean || true
fi

# Check for unused dependencies
if command -v cargo-udeps >/dev/null 2>&1; then
  echo "Running cargo-udeps to detect unused dependencies"
  cargo +nightly udeps --workspace || true
else
  echo "cargo-udeps not installed; run scripts/bootstrap.sh to install it."
fi

echo "Prune completed. Review udeps output above to prune unused crates/types."
