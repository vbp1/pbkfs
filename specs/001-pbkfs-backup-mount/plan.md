# Implementation Plan: PBKFS copy-on-write mount of pg_probackup backups

**Branch**: `001-pbkfs-backup-mount` | **Date**: 2025-11-21 | **Spec**: `/home/vbponomarev/pbkfs/specs/001-pbkfs-backup-mount/spec.md`
**Input**: Feature specification from `/specs/001-pbkfs-backup-mount/spec.md`

**Note**: This plan is filled in by the `/speckit.plan` workflow (see `.specify/scripts/bash/setup-plan.sh`).

## Summary

Mount pg_probackup-managed PostgreSQL backups as a live PostgreSQL data directory using a Rust FUSE filesystem with copy-on-write diff storage. `pbkfs mount` exposes a chosen backup chain (FULL + incrementals, compressed or uncompressed) read-only from `pbk_store` while routing all writes to a writable `pbk_diff` overlay bound to a specific instance+backup id. Only one active mount is allowed per (`pbk_target`, `pbk_diff`) binding, runtime guards enforce supported PostgreSQL (14–17) and pg_probackup (2.5+) versions, and `pbkfs cleanup` safely clears bindings and diff data for reuse without ever mutating backup artifacts in `pbk_store`.

All tests and tasks SHOULD reference the relevant FR/PR/SC identifiers (e.g., `FR-002`, `PR-001`) in names or comments to support a requirements traceability matrix maintained in `specs/001-pbkfs-backup-mount/checklists/requirements.md`.

## Technical Context

<!--
  ACTION REQUIRED: Replace the content in this section with the technical details
  for the project. The structure here is presented in advisory capacity to guide
  the iteration process.
-->

**Language/Version**: Rust stable 1.80 pinned via `rust-toolchain.toml` and `rust-version = "1.80"` in `Cargo.toml`.  
**Primary Dependencies**: `fuser` for FUSE filesystem integration, Tokio runtime for async orchestration, `tracing` + `tracing-subscriber` for structured logging, `thiserror`/`anyhow` for error handling, and pg_probackup/PostgreSQL metadata parsing that treats `pg_probackup` JSON CLI output (`show`, `show-config` with `--format=json`) as the single source of truth rather than direct parsing of internal catalog files.  
**Storage**: Local filesystem paths for `pbk_store`, `pbk_target`, and `pbk_diff`; PostgreSQL runs directly on the mounted `pbk_target` directory; for STREAM-mode `pg_probackup` backups the `pg_wal` contents embedded in the backup tree are exposed as part of the mounted data directory, while for ARCHIVE-mode backups pbkfs does not manage WAL sourcing and the operator remains responsible for supplying WAL (e.g., via archive/`restore_command`); no additional database layer beyond PostgreSQL itself.  
**Testing**: `cargo test` with unit tests for FUSE/overlay logic and chain resolution, integration tests that boot PostgreSQL against a mounted backup tree under `postgres/`, and perf/benchmark smoke tests for mount latency and sequential read throughput.  
**Target Platform**: Linux servers with FUSE (libfuse3 or compatible) on x86_64; other platforms (macOS, Windows, ARM) explicitly out of scope for this feature.  
**Project Type**: Single Rust CLI/daemon project (`pbkfs`) with library crate for core filesystem logic and binaries for `pbkfs mount` / `pbkfs unmount` / `pbkfs cleanup`.  
**Performance Goals**: Mount initialization ≤ 60 seconds for backup chains up to 200 GB on local SSD-class storage; sequential read throughput from `pbk_target` ≥ 75% of reading the same data directly from `pbk_store` for a 10 GB dataset; mount-time CPU usage ≤ 2 core equivalents and RAM ≤ 2 GB on reference hardware; regression budget ≤ 10% degradation vs. previous pbkfs release on the same dataset.  
**Constraints**: Backups in `pbk_store` are strictly immutable; all writes are redirected to `pbk_diff` via copy-on-write semantics; single active mount per (`pbk_target`, `pbk_diff`) binding; mount/cleanup must fail closed (no partial mounts) on errors; version guards enforce PostgreSQL 14–17 and pg_probackup 2.5+; no additional authorization layer beyond OS permissions.  
**Scale/Scope**: Single-node PostgreSQL instances with backup chains up to ~200 GB per spec; design and test target of up to 16 concurrent mounts per host; repository currently contains only specs and local PostgreSQL/pg_probackup fixtures, so initial implementation scope is the core FUSE mount, binding metadata, and CLI commands for mount/cleanup.

### Compression Handling (Chosen Strategy)

- Source of truth: pg_probackup JSON (`show --format=json`, `show-config`) supplies `compressed` flag and algorithm; metadata will store `{compressed: bool, algorithm: CompressionAlg, level: Option<u8>}`.  
- Supported algorithms (MVP): zlib (flate2), lz4 (lz4_flex), zstd (zstd). Unsupported/unknown (incl. pglz) → fail fast with clear error (SC-005/FR-003).  
- Read path: on first access to a compressed file, stream-decompress from `pbk_store` into `pbk_diff/data/<path>` (8–16 MiB blocks), then serve all reads/writes from the decompressed copy; `pbk_store` remains immutable.  
- Concurrency: guard copy-up with a diff-side per-file lock so only one decompression runs; waiters reuse the result.  
- Errors: propagate ENOSPC/EACCES from diff FS; log algorithm/size when decompression starts.  
- WAL: unchanged — if WAL files are compressed, the same decompress-on-first-read applies; ARCHIVE-mode WAL still provided externally.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **Rust + FUSE core**: Satisfied. Core filesystem and CLI are implemented in Rust only, targeting a FUSE filesystem that exposes pg_probackup backups as a PostgreSQL-ready data directory; no non-Rust languages are planned for production mount paths.  
- **TDD plan**: Satisfied. For each feature slice, failing unit tests (FUSE/overlay behavior, chain resolution, binding/locking) and integration tests (boot PostgreSQL against a mounted backup under `postgres/` fixtures) will be written first, then implemented until green; perf smoke tests will validate mount latency and read throughput against small reference datasets.  
- **Performance budgets**: Satisfied. Budgets are inherited from the spec: mount ≤ 60s for 200 GB chains, ≥ 75% sequential read throughput vs. `pbk_store`, CPU ≤ 2 cores, RAM ≤ 2 GB, and ≤ 10% regression vs. prior releases; perf tests/benchmarks will be wired into CI to enforce these budgets.  
- **Quality gates**: Satisfied. CI and local workflows will enforce `rustfmt`, `clippy` (no warnings), `cargo test` (unit + integration), perf/benchmark smoke tests for hot paths, and `cargo audit` (or equivalent SAST/dependency checks) as mandatory gates.  
- **context7 research**: Satisfied with plan. All new dependencies (FUSE crate, async runtime, logging, error handling, metrics, CLI parser) will have their current documentation consulted via context7 during Phase 0; links and key decisions will be recorded in `/home/vbponomarev/pbkfs/specs/001-pbkfs-backup-mount/research.md`.  
- **Workflow alignment**: Satisfied. Implementation will follow one-task-per-commit with branch `001-pbkfs-backup-mount`, explicit task/spec references in commit messages, mandatory reviews, and an explicit rollback strategy (unmount/cleanup and feature-flag or binary rollback) captured in this plan.

## Project Structure

### Documentation (this feature)

```text
specs/001-pbkfs-backup-mount/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)
<!--
  ACTION REQUIRED: Replace the placeholder tree below with the concrete layout
  for this feature. Delete unused options and expand the chosen structure with
  real paths (e.g., apps/admin, packages/something). The delivered plan must
  not include Option labels.
-->

```text
src/
├── main.rs              # entry point for pbkfs CLI
├── lib.rs               # core library exposing filesystem + CLI helpers
├── cli/
│   ├── mod.rs
│   ├── mount.rs         # `pbkfs mount` command implementation
│   ├── unmount.rs       # `pbkfs unmount` command implementation
│   └── cleanup.rs       # `pbkfs cleanup` command implementation
├── fs/
│   ├── mod.rs
│   ├── fuse.rs          # FUSE adapter and mount loop
│   └── overlay.rs       # copy-on-write diff handling for pbk_diff
├── backup/
│   ├── mod.rs
│   ├── chain.rs         # pg_probackup backup chain resolution
│   └── metadata.rs      # parsing pg_probackup metadata and layout
├── binding/
│   ├── mod.rs
│   └── lock.rs          # instance+backup binding metadata and single-use locks
└── logging/
    └── mod.rs           # structured logging + journald/syslog integration

tests/
├── unit/
│   ├── fs_overlay_tests.rs
│   ├── chain_resolution_tests.rs
│   └── binding_tests.rs
├── integration/
│   ├── mount_postgres_tests.rs  # boot PostgreSQL against mounted backups
│   └── cleanup_tests.rs
└── contract/
    └── cli_contract_tests.rs    # CLI argument/exit-code contract tests

postgres/
├── docker-compose.yml           # helper for local PostgreSQL/pg_probackup fixtures
└── fixtures/                    # sample backups and data sets for tests
```

**Structure Decision**: Single Rust project with one core library crate (`src/lib.rs`) and a CLI binary (`src/main.rs`) implementing `pbkfs` commands. Feature-specific logic (FUSE filesystem, pg_probackup chain resolution, binding/locking, logging) is organized into dedicated modules under `src/`, with tests mirrored under `tests/` for unit, integration, and contract coverage; `postgres/` holds local fixtures and helper tooling for PostgreSQL/pg_probackup-based integration tests.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| _None_ | Constitution check satisfied post-design. | N/A |
