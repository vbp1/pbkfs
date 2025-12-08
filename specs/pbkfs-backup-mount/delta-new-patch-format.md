# New compact patch format for PostgreSQL datafile deltas

## 1. Motivation and goals

**Problem observed on real workload**

- For relations like `base/16389/16402`, pbkfs currently generates `.patch` payloads where:
  - `diff_bytes` (number of bytes that actually changed in the page) is around 200–240 at p95.
  - `segments` (contiguous diff runs) is ~190 at p95.
  - `patch_len` (bytes needed to encode the patch in the current format) is ~1 000 at p95.
- Because each segment costs `4 + len` bytes (`u16 offset`, `u16 len`, then `len` bytes of data), the overhead of many 1‑byte segments dominates:
  - `patch_len = Σ(4 + len_i) = 4 * segments + diff_bytes`.
  - With `segments ≈ diff_bytes`, we effectively pay ~5 bytes per changed byte.
- With a fixed per‑slot payload budget of 504 bytes (`PATCH_PAYLOAD_MAX`), many pages where Postgres only flips hint bits or visibility flags are forced into `FULL` mode, producing large `.full` files.

**High‑level goal**

Reduce patch payload size for highly fragmented per‑page changes (hint bits, visibility flags, HOT chains, etc.) so that:

- Many pages that currently require `FULL` (8 KiB) can be represented as `PATCH` (≤504 bytes).
- Disk space in `pbk_diff` becomes closer to the true byte volume of modifications rather than “number of touched pages”.

**Non‑goals**

- Do not try to interpret PostgreSQL page format; still treat pages as opaque 8 KiB byte arrays.
- Do not change the high‑level `PATCH` / `FULL` model, `.patch` / `.full` filenames, or bitmap semantics.

**Constraints**

- Backward compatibility with existing `.patch` / `.full` files is **not** required:
  - After introducing the new format, existing diff directories may need to be
    cleaned up and recreated (e.g. via `pbkfs cleanup` followed by remount).
- Maintain the same crash‑safety and correctness properties:
  - Each block has at most one live delta.
  - Partial writes and corrupted payloads are detected and reported.
- Keep CPU overhead per page small; decoding a patch must remain cheap compared to PostgreSQL I/O.

---

## 2. Current format (v1) recap

The current implementation (`src/fs/delta.rs`) follows the spec in
`specs/pbkfs-backup-mount/delta-datafiles.md`:

- `.patch` is a sparse file of fixed‑size 512‑byte slots.
- Each slot header:

  ```text
  kind: u8     # 0 = EMPTY, 1 = PATCH, 2 = FULL_REF
  flags: u8    # reserved (unused today)
  len: u16     # payload length in bytes (0..504)
  reserved: 4 bytes
  payload: up to 504 bytes
  ```

- The payload for a `PATCH` slot is a concatenation of segments:

  ```text
  segment := offset:u16, len:u16, data[len]
  ```

- In Rust (`DeltaDiff::Patch`), this is represented as `Vec<PatchSegment>` with
  `serialized_len() = 4 + data.len()`.
- `compute_delta(base, new)` walks the page, identifies contiguous diff
  segments, sums `serialized_len()` over segments, and:
  - returns `PATCH` if the sum ≤ `PATCH_PAYLOAD_MAX` (504),
  - otherwise falls back to `FULL`.

This format is simple and robust but inefficient when segments are mostly
1‑byte runs, which is exactly what happens with hint‑bit updates.

---

## 3. Design goals for the new format (v2)

We introduce a new **payload encoding** for `PATCH` slots while keeping:

- `.patch` file header layout unchanged;
- slot size and header layout (kind/flags/len) unchanged;
- `.full` layout unchanged.

Design targets:

- Typical hint‑bit pages (≈200–250 changed bytes scattered across the page)
  should fit into a 504‑byte payload as `PATCH`, not `FULL`.
- Encoding and decoding should be single‑pass, streaming, and require minimal
  temporary allocations.
- The on‑disk representation must be self‑contained and robust against
  corruption (bad deltas, out‑of‑range positions, truncated payloads).

Key idea: **encode each changed byte as a relative offset from the previous
changed byte, plus the new byte value**. For typical pages, this yields
≈2 bytes per changed byte (vs ≈5 today).

---

## 4. New byte‑stream patch payload (v2)

### 4.1. Overview

- The **file‑level** `.patch` header (`PatchFileHeader`) is bumped to
  `version = 2`. pbkfs does not attempt to read older v1 patch files; callers
  are expected to clean/recreate diff directories on upgrade.
- We introduce a **slot‑level encoding flag** in `SlotHeader.flags`:
  - `flags & 0x01 == 1` → byte‑stream encoding (v2).
  - other values for bit 0 are reserved for future formats.
- All slots written by the new implementation use the v2 encoding; there is no
  support for writing or reading legacy v1 segment payloads.

The payload for v2 is a byte stream of **delta/value operations**, interpreted
relative to a moving cursor `pos` within the 8 KiB page.

### 4.2. Encoding semantics

State variable:

- `pos`: index of the last modified byte in the page, initialised to `-1`.

The payload consists of a sequence of operations of the form:

```text
op := delta_code, value
```

where:

- `delta_code`: 1 or 3 bytes (see below),
- `value`: 1 byte (the new value to write at the target position).

For each `op`:

1. Decode `delta` (non‑negative integer) from `delta_code`.
2. Compute the next modified position:
   - `pos = pos + 1 + delta`.
3. Check `0 <= pos < PAGE_SIZE`; if not, the patch is corrupted.
4. Write the new byte:
   - `page[pos] = value`.

Decoding continues until the end of the payload is reached. It is an error if
the payload ends in the middle of `delta_code` or without a `value` byte.

### 4.3. Delta encoding (`delta_code`)

We use a compact variable‑length representation for `delta`:

- **Short delta (0..254)**:
  - Encoded as a single byte: `delta_code = [delta]`, where `delta != 255`.
- **Long delta (255..=65535)**:
  - Encoded as 3 bytes: `delta_code = [255, lo, hi]`,
    where `u16::from_le_bytes([lo, hi])` is the actual `delta`.

Notes:

- The maximum possible `delta` on an 8 KiB page is 8191, so a 16‑bit value is
  sufficient.
- Most real deltas are expected to be <255; long deltas are rare and add 2
  bytes of overhead when they occur.
- Edge‑case examples:
  - `delta = 254` → short: `delta_code = [254]` (1 byte).
  - `delta = 255` → long:  `delta_code = [255, 255, 0]` (3 bytes, `0x00FF` LE).
  - `delta = 256` → long:  `delta_code = [255, 0, 1]`   (3 bytes, `0x0100` LE).

### 4.4. Worked example

Suppose base and new page differ at offsets `{10, 20, 23}` with byte values
`0xAA, 0xBB, 0xCC` respectively.

- First op:
  - `pos = -1`, next changed byte at `10` → `delta = 10 - (-1) - 1 = 10`.
  - `delta < 255`, so `delta_code = [10]`, `value = [0xAA]`.
- Second op:
  - `pos = 10`, next changed byte at `20` → `delta = 20 - 10 - 1 = 9`.
  - `delta_code = [9]`, `value = [0xBB]`.
- Third op:
  - `pos = 20`, next changed byte at `23` → `delta = 23 - 20 - 1 = 2`.
  - `delta_code = [2]`, `value = [0xCC]`.

Payload: `0A AA 09 BB 02 CC` (6 bytes total).

### 4.5. Expected size characteristics

Let `K` be the number of changed bytes on a page. Ignoring rare long‑delta
cases:

- Each changed byte contributes 1 delta byte + 1 value byte → 2 bytes.
- Payload size is thus ≈ `2 * K` in the common case.

For the observed workload:

- At p95, `diff_bytes ≈ 236` → expected payload `≈ 472` bytes.
- Even with a handful of long deltas (which cost +2 bytes each), we should
  stay well under the 504‑byte slot limit for most pages that currently end up
  as FULL.

Pages with very many modified bytes (e.g. heavy VACUUM, bulk updates) will
still exceed the 504‑byte budget and naturally fall back to FULL.

### 4.6. Versioning and upgrade behaviour

- `PatchFileHeader.version` is set to `2` for the new format.
- When opening a `.patch` file:
  - if `version != 2`, pbkfs returns `Error::InvalidPatchFile` (or equivalent),
    and callers may instruct the user to drop/recreate the diff directory.
- All v2 `.patch` files use the byte‑stream encoding described above; there is
  no in‑place upgrade path from legacy v1 files, only recreate‑on‑upgrade.

---

## 5. Behavioural changes and invariants

### 5.1. `compute_delta` decision logic

Today:

- `compute_delta` builds contiguous segments and computes
  `serialized_len = Σ(4 + len)`.
- It chooses `PATCH` if `serialized_len <= PATCH_PAYLOAD_MAX`, otherwise `FULL`.

With v2:

1. `compute_delta` will still scan the page and identify changed positions.
2. It will:
   - compute `diff_bytes` and (optionally) the old “segment” stats for
     diagnostics, and
   - compute the **v2 encoded length** `encoded_len` using the byte‑stream
     rules above (including long deltas).
3. Decision:
   - if `encoded_len == 0` → `DeltaDiff::Empty`,
   - if `encoded_len <= PATCH_PAYLOAD_MAX` → `DeltaDiff::PatchV2 { payload }`,
   - else → `DeltaDiff::Full { page }`.

The existing public API `DeltaDiff::serialized_len()` will be redefined to
return the **actual encoded length** for whichever encoding is used (v1 or v2).

### 5.2. Decoding invariants

For v2 payloads, decoding must enforce:

- No out‑of‑bounds writes:
  - `pos` must always be in `0..PAGE_SIZE`.
- No malformed payloads:
  - payload must not end while reading `delta_code` or before a `value` byte.
  - `delta_code == [255]` or `[255, lo]` at the end of payload is invalid.
- Any such condition must produce an `Error::CorruptedPatch { reason }`.

These checks mirror the existing robustness guarantees for v1 segments
(`segment overruns payload`, `segment overruns page boundary`, etc.).

### 5.3. Bitmap and FULL semantics

- Bitmap semantics are unchanged:
  - `0b00` (0) → no delta,
  - `0b01` (1) → FULL (page in `.full`),
  - `0b10` (2) → PATCH (slot in `.patch`).
- The `.full` file layout and `FULL_REF` slot semantics are unchanged.
- The invariant “FULL implies `encoded_len > PATCH_PAYLOAD_MAX`” becomes:
  - For v2:
    - FULL slots must have `encoded_len_v2 > PATCH_PAYLOAD_MAX`.

### 5.4. Concurrency model

The v2 encoding does **not** change the concurrency or I/O model:

- The same per‑file `DeltaIndex` / `BlockBitmap` structures are used.
- Writers continue to perform in‑place updates of individual slots/pages under
  existing locks; readers see a consistent view via the bitmap + `.patch`/`.full`.
- No additional global locks or ordering guarantees are introduced; only the
  on‑disk payload inside a PATCH slot changes format.

---

## 6. TDD‑first implementation plan and task list

This section lists concrete tasks in the recommended order. The key rule:
**tests first, then code**.

### 6.1. Unit tests for v2 encoding/decoding

Target: new tests in `tests/unit/delta_tests.rs` (or a sibling module) that
exercise the byte‑stream format directly.

Tests to add before implementing v2:

- [X] **Round‑trip basics**
   - Single‑byte change at offset 0, 100, 300, 8191.
   - Several scattered 1‑byte changes with small deltas.
   - Several scattered changes including at least one `delta >= 255` (forces
     long‑delta encoding).
   - Negative cases:
     - payload that tries to modify byte at offset `8192` (or any offset ≥ `PAGE_SIZE`)
       must be rejected with `Error::CorruptedPatch`.
     - payload where decoded `pos` becomes `>= PAGE_SIZE` due to `delta`
       arithmetic must also be rejected.
- [X] **Interaction with `DeltaDiff`**
   - `compute_delta(base, base)` → `DeltaDiff::Empty`.
   - Synthetic page where we know `diff_bytes` and verify that
     `compute_delta`:
       - returns `PATCH` and that `serialized_len()` equals the v2‑encoded size
         computed independently in the test.
- [X] **Boundary behaviour**
   - Construct a pattern where `encoded_len == PATCH_PAYLOAD_MAX` and verify
     that we still choose `PATCH`.
   - Construct a pattern where `encoded_len == PATCH_PAYLOAD_MAX + 1` and
     verify that we choose `FULL`.
   - Construct a pattern with `K` changed bytes and several long deltas
     (`delta >= 255`) and verify that:
     - `encoded_len_v2 = 2 * K + 2 * count_long_deltas`
       (each long delta adds 2 extra bytes over the ideal `2 * K` baseline).
- [X] **Corrupted payloads**
   - Truncated `delta_code` (only `[255]` or `[255, lo]` at the end).
   - Payload that ends right after a valid `delta_code` but before `value`.
   - Payload where decoded `pos` would exceed `PAGE_SIZE - 1`.
   - Ensure `apply_patch_v2` returns `Error::CorruptedPatch` with a descriptive
     reason.

Only after these tests are in place (and failing) should the v2 encoder/decoder
be implemented in `src/fs/delta.rs`.

### 6.2. Overlay / datafile integration tests

Extend `tests/integration/delta_recovery_tests.rs` with scenarios that exercise
v2 end‑to‑end:

- [X] **Synthetic pg datafile with scattered small changes**
   - Create a base page filled with a pattern.
   - Apply writes via `Overlay::write_at` at many scattered byte positions
     within a PostgreSQL datafile.
   - Assert that:
     - the corresponding `.patch` slot is present and uses v2 encoding
       (flags bit set),
     - no plain copy‑up of the relation exists in `pbk_diff`,
     - the payload length is substantially smaller than for the v1 encoding on
       the same pattern (can be compared using an internal helper).
- [X] **Boundary case near slot limit**
   - Construct a pattern of changes that //just// fits under 504 bytes encoded
     with v2; ensure it is stored as PATCH.
   - Add one more change and ensure that it flips the decision to FULL (bitmap
     shows FULL, `.full` has a non‑zero page).
- [X] **Remount recovery**
   - Perform writes that generate v2 PATCH slots.
   - Unmount the filesystem cleanly.
   - Remount the same backup/diff directory and verify that all pages are
     reconstructed correctly from `.patch` / `.full`.
- [X] **FULL → PATCH → FULL transition**
   - Start with a block stored as FULL in `.full`.
   - Apply small changes so that it becomes PATCH (v2) and confirm that
     `punch_full_hole` is called and the FULL page becomes a hole.
   - Apply further changes so that the page again exceeds the PATCH budget,
     becomes FULL, and bitmap / `.full` / `.patch` stay consistent.
Again, tests are written first; v2 wiring in `Overlay::write_datafile_delta`
and FUSE must be implemented afterwards.

---

## 7. Changes required in Rust code (high level)

This section is intentionally high‑level; actual changes must be driven by the
tests described above.

- [X] **Extend slot header handling**
   - Define constants for slot flags (e.g. `const SLOT_FLAG_V2: u8 = 0x01;`).
   - Extend `SlotHeader` handling in `read_patch_slot` and
     `write_patch_slot` to:
     - read and return the `flags` byte (API change),
     - set `flags |= SLOT_FLAG_V2` when writing a v2 patch.
- [X] **Introduce v2 delta types**
   - Add a new internal representation for v2 patches, e.g.:
     - `DeltaDiff::PatchV2 { payload: Vec<u8> }`, or
     - keep a single `Patch` variant but track encoding via an enum.
   - Implement:
     - `encode_delta_v2(&[u8; PAGE_SIZE], &[u8; PAGE_SIZE]) -> Vec<u8>`,
     - `apply_patch_v2(&[u8; PAGE_SIZE], payload: &[u8]) -> Result<[u8; PAGE_SIZE]>`.
- [X] **Wire v2 into `compute_delta`**
   - Compute `encoded_len_v2` and decide PATCH vs FULL using the v2 budget.
   - Preserve the ability to compute v1 stats for diagnostics only (e.g. for
     logging or `analyze_pg_delta.py` parity).
- [X] **Integrate into Overlay / FUSE**
   - Update `write_datafile_delta` to use v2 encoding when writing new patches.
   - Ensure `read_base_block` + patch application choose the correct decoder
     based on slot flags.
- [X] **Metrics and logging**
   - Optionally extend internal metrics (if any) to track:
     - distribution of `encoded_len_v2`,
     - number of pages stored as PATCH vs FULL after the change.
   - Add a histogram/trace metric (e.g. `delta_v2_encoded_len`) so that we can
     observe real‑world distributions of patch sizes in production.
- [X] **Update delta-datafiles.md**
   - Update the original datafile delta spec to reference this document and
     clearly mark the old v1 format as superseded by the v2 byte‑stream
     encoding.

---

## 8. `analyze_pg_delta.py` updates

The analysis script must be extended to understand the v2 encoding and to
report statistics that help validate the effectiveness of v2.

### 8.1. Header / version handling

Today, `load_slot` reads:

- `kind = hdr[0]`,
- `payload_len = struct.unpack_from("<H", hdr, 2)[0]`.

With the new format:

- [X] Update header/version handling in the script
  - The script should also read the `.patch` header and require
    `PatchFileHeader.version == 2`; otherwise it should abort with a clear error
    (“unsupported patch version”).
  - Add a regression test that feeds a `.patch` file with a different version
    (or obviously mixed/garbled header) and asserts that the script exits with
    a non‑zero status and a clear “unsupported/corrupted patch” message.
  - Within v2, we can either ignore `flags` completely (all slots use the same
    encoding) or still read it defensively and treat any non‑zero bit 0 as v2.

### 8.2. Reconstructing pages

- [X] Implement v2 byte‑stream decoding in `reconstruct_page`:
  - maintain `pos = -1`,
  - parse `delta_code` (1 or 3 bytes),
  - read `value`,
  - compute `pos = pos + 1 + delta`, validate range, and apply `value`.

Any decoding error in the script should be reported clearly (but does not have
to mirror Rust error types exactly).

### 8.3. Statistics and invariants

`diff_stats(base, new_page)` currently computes:

- `diff_bytes`,
- `patch_len = Σ(4 + segment_len)` using the v1 model,
- `segments`.

With v2 in place, the script should additionally:

- [X] Keep the current v1‑style `patch_len_v1` for **comparability**.
- [X] Compute `encoded_len_v2` for v2 slots by actually running the v2 encoder on
  `base` vs `new_page` (or by re‑implementing the size calculation directly in
  Python, following the rules from §4).

Recommended output changes:

- Print both:
  - conceptual `patch_len_v1` (for “how bad would v1 be?”), and
  - actual encoded size `encoded_len_v2` when applicable.
- Maintain the invariant check separately:
  - For FULL slots:
    - if the underlying patch would fit in v2 (`encoded_len_v2 <= 504`),
      highlight this as a “potential optimisation opportunity”.

This will allow us to:

- Verify that the majority of pages that are currently FULL purely due to
  v1 overhead would become PATCH under v2.
- Track percentile distributions for:
  - `diff_bytes`,
  - `patch_len_v1`,
  - `encoded_len_v2` (for v2 pages).

---

## 9. Summary

- We keep the existing `.patch` / `.full` architecture but introduce a new
  **byte‑stream patch encoding (v2)** at the slot level that:
  - encodes each changed byte as a relative offset and value,
  - typically uses ≈2 bytes per changed byte,
  - allows many hint‑bit‑only pages to fit comfortably into 504‑byte PATCH
    slots instead of forcing FULL storage.
- Backward compatibility with pre‑existing `.patch` / `.full` files is not a
  requirement; upgrading to the new format may require cleaning up old diff
  directories.
- Implementation is driven by TDD:
  - first unit tests for encoding/decoding and size behaviour,
  - then `Overlay`/FUSE integration tests,
  - then code changes in `src/fs/delta.rs` and friends.
- `utils/analyze_pg_delta.py` must be extended to understand v2, compute both
  conceptual and encoded patch size metrics, and provide visibility into how
  effective v2 is on real workloads.
