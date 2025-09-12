#!/usr/bin/env bash
set -euo pipefail

# Format and lint the workspace
# Usage: scripts/format-lint.sh [--fix]

FIX=false
if [[ ${1:-} == "--fix" ]]; then
  FIX=true
fi

if [[ "$FIX" == true ]]; then
  cargo fmt --all
else
  cargo fmt --all -- --check
fi

# Prefer clippy on nightly (matches rust-toolchain)
CHANNEL=${CHANNEL:-nightly}

if [[ "$FIX" == true ]]; then
  cargo +"$CHANNEL" clippy --workspace --all-targets -- -D warnings || true
else
  cargo +"$CHANNEL" clippy --workspace --all-targets -- -D warnings
fi
