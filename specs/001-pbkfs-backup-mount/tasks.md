---

description: "Task list template for feature implementation"
---

# Tasks: PBKFS copy-on-write mount of pg_probackup backups

**Input**: Design documents from `/specs/001-pbkfs-backup-mount/`  
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests are MANDATORY (TDD). Write them before implementation and ensure they fail first.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description` (one completed task = one commit)

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- Single Rust project with CLI + library crate at repository root
- Source code under `src/`, tests under `tests/`, PostgreSQL/pg_probackup harness under `postgres/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure for pbkfs CLI + library

- [X] T001 Initialize `pbkfs` Rust crate metadata with binary target and library crate in `Cargo.toml`
- [X] T002 Pin Rust toolchain to stable 1.80 using `rust-toolchain.toml` at repository root
- [X] T003 [P] Create CLI entrypoint skeleton that parses args and calls `pbkfs::run()` in `src/main.rs`
- [X] T004 [P] Create library entrypoint exposing empty `cli`, `fs`, `backup`, `binding`, and `logging` modules in `src/lib.rs`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T005 Add core dependencies (`fuser`, `tokio`, `tracing`, `tracing-subscriber`, `thiserror`, `anyhow`, `serde`, `serde_json`, `uuid`) and `rust-version = "1.80"` in `Cargo.toml`
- [X] T006 [P] Create CLI module skeleton with `mount`, `unmount`, and `cleanup` subcommand stubs and shared argument types in `src/cli/mod.rs`
- [X] T007 [P] Create filesystem modules (`mod.rs`, `fuse.rs`, `overlay.rs`) with placeholder types for FUSE adapter and copy-on-write overlay in `src/fs/mod.rs`
- [X] T008 Define backup metadata and chain modules with domain types matching `data-model.md` in `src/backup/mod.rs`
- [X] T009 [P] Create binding and lock management module with on-disk `.pbkfs-binding.json` and `.pbkfs-lock` constants in `src/binding/mod.rs`
- [X] T010 Configure logging initialization using `tracing`/`tracing-subscriber` with human and JSON formats in `src/logging/mod.rs`
- [X] T011 Create unit test module with failing tests for copy-on-write overlay basic read/write and diff semantics in `tests/unit/fs_overlay_tests.rs`
- [X] T012 [P] Create unit test module with failing tests for FULL + incremental backup chain resolution and error cases in `tests/unit/chain_resolution_tests.rs`
- [X] T013 [P] Create unit test module with failing tests for binding lifecycle, stale lock detection, and binding violations in `tests/unit/binding_tests.rs`
- [X] T014 Create CLI contract test module scaffold for exit codes and validation (no implementation yet) in `tests/contract/cli_contract_tests.rs`
- [X] T015 [P] Add integration test harness file placeholder for mounting PostgreSQL against a pbkfs-backed data directory in `tests/integration/mount_postgres_tests.rs`
- [X] T016 [P] Add integration test harness file placeholder for cleanup behavior against populated diff directories in `tests/integration/cleanup_tests.rs`
- [X] T017 Create docker-compose configuration for PostgreSQL and pg_probackup services used by integration tests in `postgres/docker-compose.yml`
- [X] T018 [P] Add initial fixture layout (directories and README) for pg_probackup backup chains used in tests in `postgres/fixtures/README.md`
- [X] T019 Wire logging initialization into CLI entrypoint by calling `pbkfs::init_logging()` from `main` in `src/main.rs`
- [X] T020 [P] Implement core error type hierarchy using `thiserror` and a `Result<T>` alias using `anyhow::Error` in `src/lib.rs`
- [X] T021 Implement `BackupStore`, `BackupMetadata`, and related helpers for loading pg_probackup JSON metadata and validating versions in `src/backup/metadata.rs`
- [X] T022 [P] Implement `BackupChain` construction and integrity validation from target backup id back to FULL, including mixed compressed/uncompressed chains per FR-003, in `src/backup/chain.rs`
- [X] T023 [P] Implement `DiffDir` and `BindingRecord` structs with JSON (de)serialization and basic validation helpers in `src/binding/lock.rs`
- [X] T024 Implement `MountTarget` and `MountSession` structs with validations for existing, empty, non-mounted directories in `src/fs/mod.rs`
- [X] T025 Implement copy-on-write overlay core logic (without FUSE wiring) to satisfy `fs_overlay_tests` in `src/fs/overlay.rs`

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Mount chosen backup and run PostgreSQL (Priority: P1) 🎯 MVP

**Goal**: A DBA can mount a specific pg_probackup backup (FULL or incremental) into `pbk_target`, start PostgreSQL on it, perform writes that go only to `pbk_diff`, and unmount while keeping backups immutable.

**Independent Test**: With a known backup chain and empty `pbk_target`/`pbk_diff`, mount, start PostgreSQL, perform a write (e.g., create table and insert rows), stop PostgreSQL, unmount, then remount and verify data is present while `pbk_store` checksums remain unchanged.

### Tests for User Story 1 (write first) ⚠️

 - [X] T026 [P] [US1] Add CLI contract tests for `pbkfs mount` and `pbkfs unmount` covering successful operations and validation errors (invalid paths, non-empty targets, missing flags, non-mounted targets) in `tests/contract/cli_contract_tests.rs`
 - [X] T027 [P] [US1] Add integration tests that mount a pg_probackup backup, start PostgreSQL, perform writes, unmount, and verify diff/store invariants in `tests/integration/mount_postgres_tests.rs`
 - [X] T028 [P] [US1] Add perf smoke tests measuring mount latency and sequential read throughput against small fixtures in `tests/integration/mount_perf_tests.rs`

### Implementation for User Story 1

- [X] T029 [US1] Implement `mount` and `unmount` subcommand argument parsing and validation (including required flags and path checks) in `src/cli/mount.rs` and `src/cli/unmount.rs`
- [X] T030 [US1] Implement `BackupStore::load_from_pg_probackup` using JSON CLI output (`show`, `show-config`) and version guards (PostgreSQL 14–17, pg_probackup ≥ 2.5) in `src/backup/metadata.rs`, enforcing FR-010 and SC-005
- [X] T031 [US1] Implement `BackupChain::from_target_backup` resolution with FULL + incremental chains, including mixed compressed/uncompressed segments per FR-003, and map integrity failures to clear errors in `src/backup/chain.rs`
- [X] T032 [US1] Implement diff directory initialization that creates `pbk_diff` structure, writes initial `BindingRecord`, and checks writability in `src/binding/lock.rs`
- [X] T033 [US1] Implement FUSE filesystem adapter that projects `pbk_store` as read-only base and routes writes to copy-on-write overlay in `src/fs/fuse.rs`
- [X] T033a [US1] Implement FUSE create/mknod/mkdir/unlink/rename/setattr/utimens/fsync/flush to allow PostgreSQL to create and manage files/directories in `pbk_diff` in `src/fs/fuse.rs`
- [X] T033b [US1] Implement directory operations (rmdir, readdir_plus if available) and ensure copy-on-write semantics for new paths in `src/fs/fuse.rs`
- [X] T033c [US1] Add writeback/size-truncate handling (truncate, fallocate stub ok) to keep Postgres file sizes consistent in `src/fs/fuse.rs`
- [X] T033d [US1] Keep mount process alive after successful mount (hold `MountHandle` until signal), graceful shutdown on SIGINT/SIGTERM with proper lock cleanup in `src/cli/mount.rs`
- [X] T033e [US1] Enhance `pbkfs unmount` to perform actual OS unmount (fusermount/umount fallback) and terminate background mount while cleaning diff/target lock markers in `src/cli/unmount.rs`
- [ ] T033f [US1] Extend backup metadata to capture compression algorithm/level from pg_probackup JSON and surface in domain types in `src/backup/metadata.rs`
- [ ] T033g [US1] Implement decompress-on-first-read copy-up for compressed backup files (zlib/lz4/zstd) with per-file lock to prevent duplicate work in `src/fs/overlay.rs`
- [ ] T033h [US1] Add unit/integration tests for compressed file handling (copy-up + immutability) and fail-fast on unsupported algorithms in `tests/unit/fs_overlay_tests.rs` and fixtures
- [X] T034 [US1] Implement mount orchestration that validates `pbk_target`, starts and stops the FUSE session, and transitions `MountSession` state for `pbkfs mount`/`pbkfs unmount` in `src/fs/mod.rs`
- [X] T035 [US1] Wire CLI `mount` and `unmount` commands through library entrypoint, combining backup store load, chain resolution, diff/binding setup, and FUSE mount/unmount with proper exit codes in `src/lib.rs`
- [X] T036 [US1] Add structured logging (spans and events) around mount lifecycle (validation, chain resolution, FUSE start/stop) in `src/logging/mod.rs`

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently (core MVP).

---

## Phase 4: User Story 2 - Resume work using existing diff (Priority: P2)

**Goal**: A DBA can remount using an existing `pbk_diff` without re-specifying instance or backup id, reusing binding metadata while preventing misuse with other backups.

**Independent Test**: After completing Story 1, unmount, then mount with only `pbk_store`, `pbk_target`, and `pbk_diff`; pbkfs must reuse the stored instance/backup, expose previous changes, and reject attempts to reuse `pbk_diff` with a different instance/backup.

### Tests for User Story 2 (write first) ⚠️

- [ ] T037 [P] [US2] Extend CLI contract tests to cover `pbkfs mount` with only `--diff-dir` and binding violation errors in `tests/contract/cli_contract_tests.rs`
- [ ] T038 [P] [US2] Extend integration tests to remount using existing `pbk_diff`, verify prior changes, and assert failures on binding mismatches in `tests/integration/mount_postgres_tests.rs`

### Implementation for User Story 2

- [ ] T039 [US2] Implement diff-only `mount` path that reads `.pbkfs-binding.json` when instance/backup flags are omitted in `src/cli/mount.rs`
- [ ] T040 [US2] Implement binding reuse logic that reconstructs `BackupChain` from `BindingRecord` and validates `pbk_store` path in `src/binding/lock.rs`
- [ ] T041 [US2] Implement binding mismatch checks that reject mounts when CLI instance/backup differ from existing binding with clear error codes in `src/backup/chain.rs`
- [ ] T042 [US2] Implement stale lock detection and recovery based on `owner_pid` and `.pbkfs-lock` file semantics in `src/binding/lock.rs`
- [ ] T043 [US2] Extend mount orchestration to support reuse of a single `DiffDir` across multiple `MountSession`s while preserving binding invariants in `src/fs/mod.rs`
- [ ] T044 [US2] Add logging and metrics around binding reuse, stale lock recovery, and violation events in `src/logging/mod.rs`

**Checkpoint**: At this point, User Stories 1 and 2 should both work independently and together.

---

## Phase 5: User Story 3 - Cleanup and reuse diff directory (Priority: P3)

**Goal**: An operator can safely clean up a `pbk_diff` directory for reuse, clearing binding metadata and diff data only when no active mount is using it (or explicitly forced).

**Independent Test**: After Stories 1 or 2, run `pbkfs cleanup --diff-dir=pbk_diff` (and with `--force` on a populated directory) and verify that binding metadata and data are removed, directory becomes reusable, and operations are blocked when a mount is active.

### Tests for User Story 3 (write first) ⚠️

- [ ] T045 [P] [US3] Add CLI contract tests for `pbkfs cleanup` covering safe cleanup, forced cleanup, and active-mount rejection in `tests/contract/cli_contract_tests.rs`
- [ ] T046 [P] [US3] Add integration tests that exercise cleanup of diff directories with and without active mounts using fixtures in `tests/integration/cleanup_tests.rs`

### Implementation for User Story 3

- [ ] T047 [US3] Implement `cleanup` subcommand argument parsing and dispatch for `--diff-dir` and `--force` flags in `src/cli/cleanup.rs`
- [ ] T048 [US3] Implement diff directory inspection, active mount detection, and safe removal of binding metadata in `src/binding/lock.rs`
- [ ] T049 [US3] Implement removal of diff data contents respecting `--force` and propagating filesystem errors (ENOSPC, EACCES, I/O) in `src/fs/overlay.rs`
- [ ] T050 [US3] Ensure cleanup transitions `BindingRecord` state appropriately (Active → Released / Stale → Released) and maintains `.pbkfs-lock` semantics in `src/binding/mod.rs`
- [ ] T051 [US3] Emit structured logs and appropriate exit codes for cleanup outcomes and failures (including guidance to unmount first) in `src/logging/mod.rs`

**Checkpoint**: All three user stories should now be independently functional and testable.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Improvements, validation, and documentation that affect multiple user stories

- [ ] T052 [P] Update quickstart examples to match final CLI flags, exit codes, and behaviors validated in tests in `specs/001-pbkfs-backup-mount/quickstart.md`
- [ ] T053 [P] Add feature-focused documentation for pbkfs mount/cleanup commands and binding semantics in `README.md`
- [ ] T054 Run `cargo fmt`, `cargo clippy --all-targets --all-features`, `cargo test`, and `cargo audit` locally and address any issues in `Cargo.toml`
- [ ] T055 [P] Add additional targeted unit tests for edge cases (ENOSPC, EACCES, concurrent mount conflicts, mixed compression chains) in `tests/unit/fs_overlay_tests.rs`
- [ ] T056 [P] Refine UX copy for CLI messages and confirm against acceptance criteria in `specs/001-pbkfs-backup-mount/checklists/ux.md`
- [ ] T057 [P] Validate performance budgets using perf smoke tests and document results in `specs/001-pbkfs-backup-mount/checklists/performance.md`
- [ ] T058 [P] Maintain a requirements traceability matrix in `specs/001-pbkfs-backup-mount/checklists/requirements.md` mapping each FR/PR/SC ID to:
      - one or more test cases (by file + test name) and
      - one or more tasks (Txxx) that implement it.
      Ensure test names or doc comments include the FR/PR/SC IDs (e.g., `test_fr_002_mixed_compression_chain`) so the matrix can be kept in sync.
- [ ] T059 [P] Document any deviations or follow-up items from research decisions (e.g., library choices, platform constraints) in `specs/001-pbkfs-backup-mount/research.md`
- [ ] T060 [P] Run the full Quickstart scenario end-to-end using test fixtures and record any gotchas or clarifications in `specs/001-pbkfs-backup-mount/quickstart.md`
- [ ] T061 [P] Add CI workflow configuration to run `cargo fmt`, `cargo clippy --all-targets --all-features`, `cargo test` (unit + integration + perf smoke tests), and `cargo audit` on pushes and PRs in `.github/workflows/ci.yml`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies – can start immediately with T001–T004
- **Foundational (Phase 2)**: Depends on Setup completion – T005–T025 BLOCK all user stories
- **User Story 1 (Phase 3, P1)**: Depends on Foundational – T026–T036 implement the MVP mount flow
- **User Story 2 (Phase 4, P2)**: Depends on Foundational and User Story 1 – T037–T044 build on the core mount implementation
- **User Story 3 (Phase 5, P3)**: Depends on Foundational and at least one mount story (US1/US2) – T045–T051 provide cleanup and reuse
- **Polish (Phase 6)**: Depends on all desired user stories being complete – T052–T061 refine docs, performance, UX, and CI

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2); no dependency on other stories and forms the MVP (`pbkfs mount`)
- **User Story 2 (P2)**: Starts after User Story 1; depends on binding and mount infrastructure but remains independently testable
- **User Story 3 (P3)**: Starts after mount infrastructure (US1/US2); cleanup logic is independently testable via `pbkfs cleanup` flows

### Within Each User Story

- Tests MUST be written and FAIL before implementation tasks in the same story
- For US1: contract tests (T026) and integration/perf tests (T027–T028) precede mount implementation (T029–T036)
- For US2: contract and integration tests (T037–T038) precede binding reuse and stale-lock implementation (T039–T044)
- For US3: contract and integration tests (T045–T046) precede cleanup implementation (T047–T051)
- Models and core types are established in Phase 2 before higher-level CLI and FUSE orchestration

### Parallel Opportunities

- Setup tasks T003–T004 can run in parallel once T001–T002 have completed
- Foundational tasks marked [P] (T006–T007, T012–T013, T015–T018, T020, T022–T023) can run in parallel because they touch different files
- Once Foundational is complete, user stories can proceed in parallel (e.g., one developer on US1 while another begins US2 test work), but recommended order is P1 → P2 → P3
- Within each user story, tasks marked [P] (tests and some implementation tasks in separate files) can run in parallel where they do not conflict on the same path
- Polish tasks marked [P] (T052–T053, T055–T061) can be distributed across the team after core stories are stable

---

## Parallel Example: User Story 1

```bash
# Parallel test work for User Story 1:
# - T026 [P] [US1] CLI contract tests for pbkfs mount
# - T027 [P] [US1] Integration tests mounting backup and running PostgreSQL
# - T028 [P] [US1] Perf smoke tests for mount latency and read throughput

# Parallel implementation work for User Story 1 (after tests exist):
# - T033 [US1] FUSE filesystem adapter in src/fs/fuse.rs
# - T032 [US1] Mount orchestration in src/fs/mod.rs
# - T030 [US1] BackupStore loading and version guards in src/backup/metadata.rs
# - T031 [US1] BackupChain resolution in src/backup/chain.rs
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001–T004)
2. Complete Phase 2: Foundational (T005–T025) – CRITICAL blocker for all stories
3. Complete Phase 3: User Story 1 (T026–T036) – implements `pbkfs mount` and core FUSE overlay
4. **STOP and VALIDATE**: Run US1 tests and Quickstart’s first scenario to confirm mount + PostgreSQL workflow
5. Optionally ship an internal MVP focusing on read/write correctness and immutability of `pbk_store`

### Incremental Delivery

1. Deliver MVP with User Story 1 (P1) after Setup + Foundational
2. Add User Story 2 (P2) to enable binding-based remounts without reconfiguration; validate independently via its tests
3. Add User Story 3 (P3) to safely recycle diff directories; validate independently via cleanup tests
4. Use Phase 6 tasks to polish performance, UX, documentation, and CI while keeping each story independently testable

### Suggested MVP Scope

- MVP scope is **User Story 1 only** (Phase 3), built on Setup + Foundational:
  - `pbkfs mount` CLI command
  - Backup store and chain resolution
  - FUSE filesystem with copy-on-write overlay
  - Basic logging, error handling, and perf smoke tests
- User Stories 2 and 3 can be implemented and shipped in subsequent increments without breaking the MVP.
