#!/usr/bin/env bash
set -euo pipefail

# Scan workspace crates and binaries
# Usage: scripts/scan.sh

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required" >&2
  exit 1
fi

echo "== Cargo Metadata =="
cargo metadata --no-deps --format-version 1 | jq '.packages[] | {name, version, id, manifest_path, targets: [.targets[] | {name, kind: .kind} ] }' || cargo metadata --no-deps --format-version 1

echo
echo "== Workspace Members (Cargo.toml [workspace].members) =="
awk '/\[workspace\]/{f=1;next} /\[/{f=0} f' Cargo.toml | sed -n 's/^[[:space:]]*"\(.*\)".*/\1/p'

# Grep for causal/relativistic keywords to aid configuration
printf "\n== Causal/Relativistic references ==\n"
rg -n --no-heading -S "(relativistic|Lamport|causal|lineariz)" --glob '!target' || true
