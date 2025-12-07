# pbkfs utility scripts (`utils/`)

This directory contains small helper tools used during development and
debugging of pbkfs. They are not part of the public CLI surface, but can
be handy when investigating issues with mounted backups or delta storage.

## `compare_trees.sh`

Compares two directory trees for structural and content differences.
Primarily used to validate that a pbkfs-mounted backup matches a
`pg_probackup restore` of the same backup.

- **What it does**
  - Lists all paths under each directory (files and subdirectories).
  - Reports entries that exist only in the first or only in the second tree.
  - For files present in both trees:
    - compares file types (regular file, directory, symlink, …),
    - for regular files, compares contents byte-by-byte via `cmp`.

- **Usage**

  ```bash
  utils/compare_trees.sh DIR1 DIR2
  ```

  Example to compare a pbkfs mount with a `pg_probackup restore`:

  ```bash
  utils/compare_trees.sh postgres/pbk_target postgres/pbk_restore
  ```

  The script exits with a non-zero status if arguments are invalid; it
  prints differences but does not change any data.

## `analyze_pg_delta.py`

Analyzes per-page differences between a base PostgreSQL relation file and
its pbkfs delta representation (`.patch` / `.full`) in the diff directory.
This is useful when investigating why a particular relation produces a
large `.full` file or many FULLREF slots.

- **What it does**
  - Reads every 8 KiB page from the base relation file.
  - Reconstructs the current logical page by applying:
    - `PATCH` slots from the `.patch` file, or
    - `FULLREF` slots referencing pages in the `.full` file.
  - For each page, computes:
    - `diff_bytes`: how many bytes differ from the base page,
    - `patch_len`: the size of the patch payload that would be needed
      (Σ(4 + segment_len) over contiguous diff segments),
    - `segments`: number of contiguous diff segments,
    - `kind`: 0 = empty, 1 = PATCH, 2 = FULLREF.
  - Validates the pbkfs invariant:
    - pages stored as FULLREF must have `patch_len` greater than the
      maximum payload size (`PATCH_SLOT_SIZE - 8`, i.e. 504 bytes).
  - Prints percentile statistics (70/80/90/95) for `diff_bytes` and
    `patch_len`, and separate stats for FULLREF pages only.

- **Usage**

  ```bash
  utils/analyze_pg_delta.py \
      --base  postgres/pbk_restore/base/16389/16402 \
      --patch postgres/pbk_diff/data/base/16389/16402.patch \
      --full  postgres/pbk_diff/data/base/16389/16402.full
  ```

  You can point it at any relation, as long as:

  - `--base` points to the restored relation file (e.g. from
    `pg_probackup restore`),
  - `--patch` / `--full` point to the corresponding delta files in the
    pbkfs diff directory (usually under `.../pbk_diff/data/...`).

  The script does **not** modify any files; it only reads them and prints
  summary statistics and potential invariant violations.

