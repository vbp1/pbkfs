# pbkfs

pbkfs is a Rust FUSE filesystem that mounts pg_probackup backups as a PostgreSQL-ready data directory with copy-on-write diffs. Backups in `pbk_store` stay immutable; all writes flow to a writable `pbk_diff`, enabling fast analysis or recovery drills without restoring full copies.

## Current Status
- CLI commands implemented: `mount`, `unmount`, `cleanup`.
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
**Initial mount (requires instance and backup id):**
```bash
pbkfs mount \
  -B /pgbackups \
  --mnt-path /mnt/pbkfs-main \
  --instance main \
  -i SBOL94 \
  --diff-dir /var/lib/pbkfs/diff-main
```
- Validates the FULL→incremental chain, writes binding metadata to `pbk_diff`, and mounts FUSE at `mnt-path`.
- Use `--force` only to mount a chain marked corrupt.

**Remount using existing diff binding (no instance/backup flags):**
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

**Cleanup diff directory:**
```bash
pbkfs cleanup --diff-dir /var/lib/pbkfs/diff-main          # safe cleanup
pbkfs cleanup --diff-dir /var/lib/pbkfs/diff-main --force  # discard contents if no active owner
```

## Logging & Exit Codes
- Default logging: human-readable stderr via `tracing`; set `RUST_LOG=debug` to increase verbosity.
- Exit codes: `0` on success; non-zero with `pbkfs error: ...` on stderr for failures.

## References
- Quickstart guide: `specs/004-phase-6/quickstart.md`
- Requirements & traceability: `specs/004-phase-6/checklists/requirements.md`
