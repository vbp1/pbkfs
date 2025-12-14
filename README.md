# pbkfs

pbkfs is a Rust FUSE filesystem that mounts pg_probackup backups as a PostgreSQL-ready data directory with copy-on-write diffs. Backups in `pbk_store` stay immutable; all writes flow to a writable `pbk_diff`, enabling fast analysis or recovery drills without restoring full copies.

## Current Status
- CLI commands implemented: `mount`, `unmount`, `cleanup`, `stat`.
- Supports FULL and incremental backup chains (including mixed compression) with binding metadata stored in `pbk_diff` to enforce single-use semantics.
- Target platform: Linux with libfuse3-compatible tooling.

Specs and plans live in `specs/001-pbkfs-backup-mount/` (symlinked as `specs/004-phase-6/`).

## Prerequisites
- Rust 1.80 (pinned via `rust-toolchain.toml`).
- libfuse3 / FUSE userspace tools installed.
- pg_probackup ≥ 2.5 with a populated `pbk_store`.
- PostgreSQL 14–17 available for validation.

## Build & Validate
```bash
cargo fmt
cargo clippy --all-targets --all-features
cargo test
cargo audit
```

## Usage
pbkfs supports two mount modes:
- **Background (default):** `pbkfs mount` forks a worker and returns after it starts. Use `--wait`/`--timeout` to wait for mount completion.
- **Console:** add `--console` to keep the mount in the foreground (useful for interactive debugging).

**Initial mount (requires instance and backup id when `pbk_diff` has no binding yet):**
```bash
pbkfs mount \
  -B /pgbackups \
  --mnt-path /mnt/pbkfs-main \
  --instance main \
  -i SBOL94 \
  --diff-dir /var/lib/pbkfs/diff-main \
  --console
```
- Validates the FULL→incremental chain, writes binding metadata to `pbk_diff`, and mounts FUSE at `mnt-path`.
- Use `--force` only to mount a chain marked corrupt, or to override an existing `.pbkfs-dirty` marker from a previous `--perf-unsafe` run.

**Background mount (wait for completion):**
```bash
pbkfs mount \
  -B /pgbackups \
  --mnt-path /mnt/pbkfs-main \
  --instance main \
  -i SBOL94 \
  --diff-dir /var/lib/pbkfs/diff-main \
  --wait
```

**Remount using existing diff binding (instance/backup flags are optional but must match if provided):**
```bash
pbkfs mount \
  -B /pgbackups \
  --mnt-path /mnt/pbkfs-main \
  --diff-dir /var/lib/pbkfs/diff-main
```

**Unmount:**
```bash
pbkfs unmount --mnt-path /mnt/pbkfs-main
```

**Dump mount statistics:**
```bash
pbkfs stat --mnt-path /mnt/pbkfs-main
pbkfs stat --mnt-path /mnt/pbkfs-main --format json
pbkfs stat --mnt-path /mnt/pbkfs-main --counters-reset
```

**Cleanup diff directory:**
```bash
pbkfs cleanup --diff-dir /var/lib/pbkfs/diff-main          # safe cleanup
pbkfs cleanup --diff-dir /var/lib/pbkfs/diff-main --force  # force wipe even if the diff looks in-use (dangerous)
```

## CLI Notes (selected options)
- `mount`:
  - Required in all cases: `-B/--pbk-store`, `-D/--mnt-path`, `-d/--diff-dir`.
  - Required for a fresh diff dir: `-I/--instance`, `-i/--backup-id`.
  - Mode: `--console` (foreground) vs default background; in background you can add `--wait` or `--timeout <sec>`.
  - Durability/perf: `--perf-unsafe` (skips fsync; leaves `.pbkfs-dirty` until unmount), `--no-wal` (WAL writes are virtual; diff dir becomes non-reusable).
  - Logging (background): `--log-file`, `--log-format text|json`, `--log-sink file|journald`, `--debug`.
  - Files created under the mount root (stored in diff layer and visible via FUSE): `.pbkfs/worker.pid`, `.pbkfs/stat` (and `.pbkfs/pbkfs.log` by default in background mode with `--log-sink file`).
- `unmount`: `-D/--mnt-path` is required; `-d/--diff-dir` is a hint for broken mount endpoints; `--force` does SIGKILL + lazy detach if needed.
- `cleanup`: wipes diff contents and clears lock/binding metadata; refuses to clean an active diff unless `--force` is set.
- `stat`: signals the worker (SIGUSR1/SIGUSR2) and prints the refreshed `.pbkfs/stat`; `--format text|json`, `--counters-reset`.

## Logging & Exit Codes
- Console mode logs to stderr via `tracing`; set `RUST_LOG=debug` to increase verbosity (or use `pbkfs mount --debug`).
- Background mode logs to a file by default (`.pbkfs/pbkfs.log` under the mount root) unless `--log-sink journald` is selected.
- Exit codes: `0` on success; non-zero with `pbkfs error: ...` on stderr for failures.

## References
- Architecture: `ARCHITECTURE.md`
