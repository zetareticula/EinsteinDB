#!/usr/bin/env bash
set -euo pipefail

# Bootstrap Rust nightly and common components for EinsteinDB
# Usage: scripts/bootstrap.sh

if ! command -v rustup >/dev/null 2>&1; then
  echo "rustup not found. Please install Rustup from https://rustup.rs first." >&2
  exit 1
fi

CHANNEL=${CHANNEL:-nightly}

echo "Installing toolchain: ${CHANNEL}"
rustup toolchain install "${CHANNEL}" --profile minimal --component rustfmt --component clippy || true

# Set override from rust-toolchain if present, else use CHANNEL
if [ -f "rust-toolchain" ]; then
  echo "Using rust-toolchain file override: $(cat rust-toolchain)"
else
  rustup override set "${CHANNEL}"
fi

# Useful cargo utilities
if ! cargo install cargo-binutils >/dev/null 2>&1; then
  cargo install cargo-binutils || true
fi
rustup component add llvm-tools-preview || true

if ! cargo install cargo-sweep >/dev/null 2>&1; then
  cargo install cargo-sweep || true
fi

if ! cargo install cargo-udeps >/dev/null 2>&1; then
  cargo install cargo-udeps || true
fi

echo "Bootstrap complete."
