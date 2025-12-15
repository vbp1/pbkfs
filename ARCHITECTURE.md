# pbkfs architecture

pbkfs is a Rust FUSE filesystem that exposes a selected `pg_probackup` backup chain as a PostgreSQL-ready data directory. The original backup store stays immutable; all modifications are redirected into a writable *diff* directory via a copy-on-write overlay.

## Goals
- Mount `pg_probackup` FULL + incremental chains as a readable PostgreSQL data directory.
- Keep the backup store (`pbk_store`) immutable; write all changes into `pbk_diff`.
- Provide deterministic ownership/binding so one `pbk_diff` is used by at most one live mount.
- Support background daemon mode for “mount-and-return” workflows with stable control endpoints.

## Non-goals (for now)
- Cross-platform support beyond Linux FUSE (libfuse3 tooling).
- Acting as a general purpose overlay filesystem (pbkfs is tuned to PostgreSQL + pg_probackup layouts).

## Tech stack
- Language/runtime: Rust (stable, `rust-version = 1.80`).
- CLI: `clap` v4 (derive).
- FUSE: `fuser` v0.15 (`abi-7-22`).
- Logging/metrics: `tracing` + `tracing-subscriber` (text/json).
- Data formats: `serde` + `serde_json` (binding, pid/stat payloads).
- IDs: `uuid` v4.
- Concurrency/locking: `parking_lot`, `dashmap`, `crossbeam-channel`.
- Compression: `flate2`, `lz4_flex`, `zstd`.
- Errors: `thiserror` (library), `anyhow` (boundaries/CLI).

## High-level components
- `src/main.rs`: thin CLI wrapper (prints `pbkfs error: ...` and exits non-zero on failure).
- `src/lib.rs`: library entrypoint (`pbkfs::run`) and shared error types.
- `src/cli/`: clap subcommands and daemon helpers:
  - `mount`: mount/daemonize/handshake + worker lifecycle.
  - `unmount`: graceful stop via PID file, fallback to system unmount for broken endpoints.
  - `cleanup`: safe/forced diff cleanup.
  - `stat`: signals the worker and prints a refreshed stat snapshot.
  - `pid`, `daemon`: PID record + launcher↔worker handshake primitives.
- `src/backup/`: reads `pg_probackup` metadata, resolves FULL→incremental chains, validates integrity.
- `src/fs/`: FUSE layer plus overlay/delta logic.
- `src/binding/`: diff directory binding + single-owner semantics (`.pbkfs-binding.json`, `.pbkfs-lock`).
- `src/logging/`: `tracing` configuration (console/file/journald, text/json).

## Data directories and on-disk metadata
pbkfs operates with three paths:
- `pbk_store` (read-only): `pg_probackup` backup store.
- `pbk_target` (mountpoint): empty directory where the FUSE filesystem is mounted.
- `pbk_diff` (read-write): writable directory that stores all changes and pbkfs metadata.

Key metadata files:
- `pbk_diff/.pbkfs-binding.json`: binding record that “pins” a diff directory to `(pbk_store, instance, backup_id, pbk_target)` and tracks the last owner PID/host/version.
- `pbk_diff/.pbkfs-lock`: lock marker used to prevent concurrent mounts and to recover stale mounts.
- `pbk_diff/.pbkfs-dirty`: created when mounting with `--perf-unsafe`; indicates deferred fsync state.
- `<pbk_target>/.pbkfs/worker.pid`: PID record of the background worker (stored in diff layer and visible through the mounted FS).
- `<pbk_target>/.pbkfs/stat`: worker-produced stat snapshot (stored in diff layer and visible through the mounted FS).
- `<pbk_target>/.pbkfs/pbkfs.log`: default background log file (when `--log-sink file`).

## Mount lifecycle (console vs background)
### Console mode (`pbkfs mount --console`)
1. CLI validates arguments and prepares `pbk_target` (must be empty) and `pbk_diff` (must be writable).
2. Binding logic:
   - If `pbk_diff` already has a binding, it is validated against the requested store and optional `(instance, backup_id)`.
   - If there is no binding, `--instance` and `--backup-id` are required to create the initial binding.
3. Backup chain is resolved and validated (FULL + incrementals; chain integrity checked).
4. Overlay layers are constructed from the chain and mounted via FUSE.
5. The process stays in the foreground until unmounted or interrupted.

### Background mode (default `pbkfs mount`)
1. A launcher process validates inputs, then `fork()`s a worker.
2. The worker daemonizes (new session, stdio redirected to `/dev/null`) and initializes background logging:
   - file sink by default (under `<pbk_target>/.pbkfs/pbkfs.log` unless `--log-file` is set),
   - or journald with `--log-sink journald`.
3. Launcher↔worker uses a pipe handshake:
   - phase 1: “worker started”,
   - phase 2: “mount ok” / “mount error (message)”.
4. The launcher returns immediately by default; `--wait` / `--timeout <sec>` waits for phase 2.

## Overlay / copy-on-write design
- Reads traverse a stack of immutable layers from `pbk_store` (FULL + incrementals), resolving compression per backup.
- Writes go to `pbk_diff` only.
- The overlay exposes a PostgreSQL-shaped directory tree at the mountpoint.

## Read path (how reads work)
pbkfs treats files differently depending on their role in a PostgreSQL data directory.

### File classes
- **PostgreSQL relation datafiles** (heap/index main fork, forks like `_vm/_fsm/_init`, segments like `.1`): served *page-by-page* (8 KiB blocks).
  - Primary detection is via `backup_content.control` of the newest (target) backup; path heuristics are a fallback.
- **WAL files (`pg_wal/*`)**:
  - Normal mode: treated like regular files (copy-up on write, reads may be served from base/diff handles).
  - `--no-wal`: WAL is “virtual” for writes; reads are served without creating a diff copy (see below).
- **All other files** (configs, global control files, etc.): generally served as whole files (with optional block materialization for `read_range`).

### What happens on read()
At the FUSE level, `pbkfs` routes reads to `Overlay::read_range()` for:
- PostgreSQL datafiles (always), to correctly handle sparse holes, logical lengths, and delta storage.
- `pg_wal/*` when `--no-wal` is enabled (to avoid creating persistent WAL in the diff).

Other files may be served through an already-open host file descriptor for performance (diff or base), but may fall back to `Overlay::read_range()` when needed.

### `Overlay::read_range()` in two modes
`Overlay::read_range(rel, offset, size)` chooses one of two strategies:

1) **Non-materializing mode** (used for datafiles, and for `pg_wal/*` under `--no-wal`)  
   - Does *not* create/extend a diff copy just because of reads.
   - For each touched 8 KiB block, it calls `read_block_data()`:
     - If a datafile delta exists in `pbk_diff` (`.patch`/`.full`), it is applied first.
     - Otherwise it locates a base block in the backup layers and reconstructs it.
     - Missing blocks for datafiles are treated as **zero pages** (important for WAL replay extending relations).

2) **Materializing mode** (used for most non-data files)  
   - Materializes only the blocks intersecting the requested range into the diff file as a sparse file (holes stay holes).
   - Each block is “copied up” from the best available source layer into `pbk_diff/data/<rel>`.
   - After materialization, the response is read from the diff file.

### How base blocks are found (layer traversal)
For block-level reads, the overlay walks layers from newest → oldest:
- If a layer is **incremental** and has a `.pagemap`, the pagemap acts as an existence filter:
  - if the bit is not set, the block is skipped in that layer.
- Datafile page streams are stored in a pg_probackup format (`BackupPageHeader {block, compressed_size} + page_bytes`):
  - pbkfs builds a per-file index (block→offset) once and reuses it for reads and logical length.
  - Per-page compression is handled via the layer’s compression algorithm.
- For “format mismatch” cases, pbkfs falls back to a plain block read from the underlying file.

### Logical length rules
pbkfs reports a **logical size** (what PostgreSQL expects), not raw on-disk sizes in pg_probackup files:
- For datafiles, length is derived from backup metadata (`n_blocks`/`full_size`) and from diff/delta artifacts.
- For PostgreSQL “status” files (`pg_xact`, `pg_multixact`, `pg_subtrans`, `pg_commit_ts`), pbkfs does **not** combine sizes across older backups to avoid extending them beyond what exists in the target backup.

## Write path (how writes work)
### Worker model and ordering
FUSE operations are executed by a small internal worker pool. For each inode, write/fsync tasks are ordered (FIFO) so that:
- reads wait for preceding writes when necessary,
- fsync observes all preceding writes.

### Regular files (non-datafiles)
- For any write intent, pbkfs ensures a writable copy exists under `pbk_diff/data/<rel>` (“copy-up”):
  - directories are created as needed,
  - symlinks are recreated as symlinks,
  - compressed files are decompressed into the diff on first copy-up,
  - certain incremental entries that are stored as whole files are copied as whole files.
- Writes are applied directly to the diff file at the requested offset (`pwrite` semantics), and the file is extended if needed.
- Cache entries are invalidated so future reads/`getattr` see the updated state.

### PostgreSQL datafiles: delta storage (`.patch` + `.full`)
Datafiles are optimized to avoid whole-file copy-up and to keep diffs compact.

When a write is **8 KiB aligned** (offset and length are multiples of PostgreSQL `BLCKSZ`), pbkfs stores it as per-page delta:
- It computes the “base page” by reading from backup layers only (ignoring any diff copy) to ensure deltas are relative to immutable backup data.
- It applies the write chunk to the base page and computes a delta:
  - **Empty** (no changes): slot is cleared.
  - **Patch** (v2 byte-stream encoding, fits in 512-byte slot budget): written into `<rel>.patch`.
  - **Full** (too big for a patch slot): full 8 KiB page written into `<rel>.full`, and `<rel>.patch` stores a “full-ref” slot.

On disk:
- `<rel>.patch`: sparse file with a fixed 512-byte slot per block number (plus a header).
  - Contains `SlotHeader { kind, flags, payload_len }` and optional payload.
- `<rel>.full`: sparse file with a 4096-byte header and 8 KiB pages at fixed offsets.
- An in-memory cached bitmap (loaded from `.patch`) tracks whether a block is Empty/Patch/FullRef for fast reads.

#### Patch file format (`*.patch`)
The patch file is a sparse, fixed-slot index keyed by PostgreSQL block number (`blkno`, 0-based).

**Header (offset 0, size 512 bytes)**
- `magic` (8 bytes): `PBKPATCH`
- `version` (u16 LE): `2`
- `flags` (u16 LE): currently `0`
- `page_size` (u32 LE): `8192` (PostgreSQL `BLCKSZ`)
- `slot_size` (u32 LE): `512`
- remaining bytes: reserved/zero

**Slot addressing**
- Slot `blkno` starts at offset: `512 + blkno * 512`.
- A missing slot (sparse hole beyond EOF) is treated as `EMPTY`.

**Slot layout (size 512 bytes)**
- `kind` (u8):
  - `0` = `EMPTY` (no override; read falls back to base)
  - `1` = `PATCH` (apply patch payload to base page)
  - `2` = `FULL_REF` (page is stored in the companion `*.full` file)
- `flags` (u8):
  - bit `0x01` (`SLOT_FLAG_V2`) is required for `PATCH` slots (v2 encoding)
- `payload_len` (u16 LE): number of payload bytes (must be `> 0` for `PATCH`, must fit in one slot)
- `_reserved` (4 bytes): zero
- `payload` (0..504 bytes): starts at offset 8 within the slot
- remaining bytes in the slot: zero

**PATCH payload v2 encoding**
The v2 payload encodes byte-level modifications relative to the 8 KiB base page as a stream of `(delta, value)` entries:
- The decoder tracks the last modified byte position `pos` (starts at `-1`).
- Each entry advances by `delta + 1` bytes to the next modified position, then writes `value` there.
- `delta` is variable-length:
  - if `< 255`: stored as a single byte
  - if `== 255`: followed by `u16 LE` delta value (allows sparse jumps)
- The payload is never empty for `PATCH` slots; “no changes” is represented by an `EMPTY` slot.

If the computed patch does not fit into `504` bytes (`512 - 8`), pbkfs stores the full 8 KiB page in `*.full` and writes a `FULL_REF` slot in `*.patch`.

If a datafile write is **not** 8 KiB aligned, pbkfs falls back to copy-up and writes into the diff file directly.

### Truncate and unlink for datafiles
- Truncate of datafiles is implemented as a combination of:
  - setting the diff file length,
  - clearing delta slots beyond the new end and punching holes in `.full` where applicable,
  - special handling for size `0` (drop delta artifacts and shadow the base relation as empty).
- Unlink removes the diff copy plus `.patch`/`.full`, clears cached bitmaps and per-block locks.

### fsync and `--perf-unsafe`
- Normal mode: `fsync` on a datafile syncs any touched `.patch`/`.full`/diff pieces; on regular files it syncs the diff copy (or the base file if untouched).
- `--perf-unsafe`: `fsync` becomes a no-op that only records “dirty paths”. Dirty paths are flushed and `.pbkfs-dirty` is removed on clean unmount.

### WAL writes under `--no-wal`
When `--no-wal` is enabled:
- Writes to `pg_wal/*` are not persisted to `pbk_diff`; pbkfs tracks only logical size/metadata in memory for the duration of the mount.
- This makes the diff directory intentionally **non-resumable** (cannot be safely reused across mounts).

## Durability and performance switches
- `--perf-unsafe`: skips fsync for datafiles for performance; creates `.pbkfs-dirty` in `pbk_diff` and expects a clean unmount to flush/clear state. A subsequent mount refuses to proceed unless `--force` is used to acknowledge potential data loss.
- `--no-wal`: WAL writes (`pg_wal`) become virtual and are not persisted, making the diff directory non-resumable by design (pbkfs refuses to reuse a diff that has ever been mounted with `--no-wal`).

## Ownership, locking, and safety
- Binding enforces single-owner semantics: a diff directory is bound to one backup selection and one mount target, and tracks the owner PID to prevent concurrent mounts.
- On mount reuse, stale lock recovery is supported (if the recorded owner PID is not alive).
- `pbkfs cleanup` removes diff data and clears lock/binding metadata; it rejects cleanup if an active owner is detected unless `--force` is used.
- `pbkfs unmount` tries a graceful shutdown via the recorded worker PID, then falls back to OS unmount (`fusermount -u` / `umount`). It is designed to handle broken FUSE mountpoints that return “Transport endpoint is not connected”.

## Observability and control plane
- Logging is implemented via `tracing`:
  - console mode: stderr,
  - background mode: file (default) or journald; text/json formats.
- `pbkfs stat` signals the worker and prints a refreshed snapshot from `<pbk_target>/.pbkfs/stat`:
  - `SIGUSR1` dumps counters,
  - `SIGUSR2` dumps and resets counters (`--counters-reset`).

## Tests
- Unit tests: pure logic (chain resolution, overlay/delta, binding).
- Contract tests: CLI help/usage expectations.
- Integration tests: mount/diff flows; should skip gracefully when FUSE permissions aren’t available.
