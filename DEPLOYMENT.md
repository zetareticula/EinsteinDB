# EinsteinDB Deployment Guide

This guide shows how to deploy and run a relativistic/causal instance of EinsteinDB using Cargo. It also documents the helper scripts added under `scripts/`.

## Prerequisites

- Rust toolchain (rustup) installed. The repo targets nightly as specified in `rust-toolchain`.
- Cargo available in your PATH.

Optional developer tools installed by the bootstrap script:
- `cargo-binutils`, `llvm-tools-preview`
- `cargo-sweep` (prune old build artifacts)
- `cargo-udeps` (detect unused dependencies)

## One-time setup

1) Bootstrap the toolchain and utilities

```
./scripts/bootstrap.sh
```

2) Make scripts executable (first time only)

```
chmod +x scripts/*.sh
```

## Repository layout (high level)

- Workspace root manifest: `Cargo.toml` (lists members like `einsteindb-server/`, `causet/`, `berolinasql/`, etc.)
- Server binary crate: `einsteindb-server/` (package `einsteindb_server`)
- Relativistic/Causal demos: `examples/consistency_demo.rs` and `standalone_consistency_demo.rs`
- Causet-based architecture core: `causet/src/causet.rs`

## Common tasks

- Scan workspace and find causal/relativistic references

```
./scripts/scan.sh
```

- Build all crates

```
./scripts/build.sh
# or
./scripts/build.sh --release
```

- Run tests across the workspace

```
./scripts/test.sh
# or
./scripts/test.sh --release
```

- Format and lint (clippy)

```
./scripts/format-lint.sh --fix
```

- Run the relativistic/causal consistency demo

```
./scripts/run-demo.sh
# or optimized
./scripts/run-demo.sh --release
```

- Run the EinsteinDB server binary

```
./scripts/run-server.sh
# or optimized
./scripts/run-server.sh --release
```

If the server crate is not yet fully wired, the script falls back to the demo to provide a working instance showcasing relativistic causal behavior.

- Prune build artifacts and check for unused dependencies

```
./scripts/prune.sh
# or aggressively clean and prune
./scripts/prune.sh --aggressive
```

## Notes on relativistic/causal configuration

- The causal set machinery and mentions of relativistic linearizability are centered in `causet/src/causet.rs`.
- Lamport/causal semantics appear in `causetq/`, core components and the demo files under `examples/` or `standalone_consistency_demo.rs`.
- The server entrypoint is `einsteindb-server/src/main.rs` (package `einsteindb_server`). Depending on the current state of that crate, use the demo as a reference entrypoint.

## Troubleshooting

- Toolchain issues: re-run `./scripts/bootstrap.sh` to ensure nightly and components are present.
- Build failures in optional crates: try building in release or disable optional features in the root `Cargo.toml` features section.
- If `cargo-udeps` is missing: run the bootstrap script to install it.
