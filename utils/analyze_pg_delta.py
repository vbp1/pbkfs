#!/usr/bin/env python3
"""
Analyze per-page deltas between a base PostgreSQL relation file and its
pbkfs delta representation (.patch/.full).

The script reproduces the same diff accounting as pbkfs:
  - diff_bytes: number of bytes that differ between base and current page
  - patch_len:  Σ(4 + segment_len) per contiguous diff segment
  - segments:   number of diff segments

It also validates the invariant used by pbkfs:
  - pages stored as FULLREF (in .full) must have patch_len > PATCH_PAYLOAD_MAX

Usage:
    utils/analyze_pg_delta.py \
        --base  postgres/pbk_restore/base/16389/16402 \
        --patch postgres/pbk_diff/data/base/16389/16402.patch \
        --full  postgres/pbk_diff/data/base/16389/16402.full
"""

import argparse
import os
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

PAGE_SIZE = 8192
PATCH_SLOT_SIZE = 512
PATCH_HEADER_SIZE = 512
FULL_HEADER_SIZE = 4096
PATCH_PAYLOAD_MAX = PATCH_SLOT_SIZE - 8  # matches delta.rs


@dataclass
class PageDelta:
    block: int
    diff_bytes: int
    patch_len: int
    segments: int
    kind: int  # 0 = empty, 1 = PATCH, 2 = FULLREF


def read_page(f, block: int) -> bytes:
    f.seek(block * PAGE_SIZE)
    data = f.read(PAGE_SIZE)
    if len(data) < PAGE_SIZE:
        data = data + b"\x00" * (PAGE_SIZE - len(data))
    return data


def reconstruct_page(
    base: bytes,
    kind: int,
    payload: bytes,
    full_f,
    block: int,
    full_size: int,
) -> bytes:
    page = bytearray(base)

    if kind == 1:  # PATCH slot
        cursor = 0
        while cursor + 4 <= len(payload):
            off = struct.unpack_from("<H", payload, cursor)[0]
            length = struct.unpack_from("<H", payload, cursor + 2)[0]
            cursor += 4
            data_end = cursor + length
            if data_end > len(payload):
                break
            start = int(off)
            end = start + int(length)
            if end > PAGE_SIZE:
                break
            page[start:end] = payload[cursor:data_end]
            cursor = data_end

    elif kind == 2 and full_f is not None:  # FULLREF
        off = FULL_HEADER_SIZE + block * PAGE_SIZE
        if off < full_size:
            full_f.seek(off)
            data = full_f.read(PAGE_SIZE)
            if data:
                if len(data) < PAGE_SIZE:
                    data = data + b"\x00" * (PAGE_SIZE - len(data))
                page[:] = data

    return bytes(page)


def diff_stats(base: bytes, new_page: bytes) -> Tuple[int, int, int]:
    diff_bytes = sum(1 for i in range(PAGE_SIZE) if base[i] != new_page[i])

    patch_len = 0
    segments = 0
    i = 0
    while i < PAGE_SIZE:
        if base[i] == new_page[i]:
            i += 1
            continue
        start = i
        while i < PAGE_SIZE and base[i] != new_page[i]:
            i += 1
        seg_len = i - start
        segments += 1
        patch_len += 4 + seg_len

    return diff_bytes, patch_len, segments


def load_slot(patch_f, patch_size: int, block: int) -> Tuple[int, bytes]:
    off = PATCH_HEADER_SIZE + block * PATCH_SLOT_SIZE
    if off >= patch_size:
        return 0, b""
    patch_f.seek(off)
    hdr = patch_f.read(8)
    if not hdr:
        return 0, b""
    kind = hdr[0]
    payload_len = struct.unpack_from("<H", hdr, 2)[0]
    if kind in (1, 2) and payload_len > 0:
        payload_off = off + 8
        patch_f.seek(payload_off)
        payload = patch_f.read(payload_len)
        return kind, payload
    return kind, b""


def percentile(values: List[int], p: int) -> int:
    if not values:
        return 0
    vals = sorted(values)
    k = int(len(vals) * p / 100)
    if k >= len(vals):
        k = len(vals) - 1
    return vals[k]


def analyze(base_path: Path, patch_path: Path, full_path: Path) -> None:
    base_size = base_path.stat().st_size
    n_blocks = base_size // PAGE_SIZE

    with base_path.open("rb") as base_f:
        patch_f = patch_path.open("rb") if patch_path.exists() else None
        full_f = full_path.open("rb") if full_path.exists() else None
        patch_size = patch_path.stat().st_size if patch_f else 0
        full_size = full_path.stat().st_size if full_f else 0

        deltas: List[PageDelta] = []

        for block in range(n_blocks):
            base_page = read_page(base_f, block)

            kind, payload = (0, b"")
            if patch_f:
                kind, payload = load_slot(patch_f, patch_size, block)

            new_page = reconstruct_page(base_page, kind, payload, full_f, block, full_size)
            diff_bytes, patch_len, segments = diff_stats(base_page, new_page)

            if diff_bytes > 0:
                deltas.append(PageDelta(block, diff_bytes, patch_len, segments, kind))

        # Invariant check: FULL slots must have patch_len > PATCH_PAYLOAD_MAX.
        violations = [
            d for d in deltas if d.kind == 2 and d.patch_len <= PATCH_PAYLOAD_MAX
        ]

        print(f"Base file:   {base_path} ({base_size} bytes, {n_blocks} blocks)")
        print(f"Patch file:  {patch_path} ({patch_size} bytes)")
        print(f"Full file:   {full_path} ({full_size} bytes)")
        print(f"Blocks with diff: {len(deltas)}")
        print(f"FULL slots with patch_len <= {PATCH_PAYLOAD_MAX}: {len(violations)}")
        if violations:
            print("First violations:")
            for v in violations[:10]:
                print(
                    f"  block={v.block} diff_bytes={v.diff_bytes} "
                    f"patch_len={v.patch_len} segments={v.segments}"
                )

        if deltas:
            diff_vals = [d.diff_bytes for d in deltas]
            patch_vals = [d.patch_len for d in deltas]

            print("\nPercentiles for diff_bytes:")
            for p in (70, 80, 90, 95):
                print(f"  p{p}: {percentile(diff_vals, p)}")

            print("\nPercentiles for patch_len:")
            for p in (70, 80, 90, 95):
                print(f"  p{p}: {percentile(patch_vals, p)}")

            full_only = [d for d in deltas if d.kind == 2]
            if full_only:
                full_diff = [d.diff_bytes for d in full_only]
                full_patch = [d.patch_len for d in full_only]
                full_segs = [d.segments for d in full_only]
                print("\nFULL-only pages:")
                for p in (70, 80, 90, 95):
                    print(f"  diff_bytes p{p}: {percentile(full_diff, p)}")
                for p in (70, 80, 90, 95):
                    print(f"  patch_len p{p}: {percentile(full_patch, p)}")
                for p in (70, 80, 90, 95):
                    print(f"  segments p{p}: {percentile(full_segs, p)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze pbkfs delta (.patch/.full) for a PostgreSQL relation file."
    )
    parser.add_argument("--base", type=Path, required=True, help="Path to base relation file")
    parser.add_argument("--patch", type=Path, required=True, help="Path to .patch file")
    parser.add_argument("--full", type=Path, required=True, help="Path to .full file")
    args = parser.parse_args()

    for p in (args.base, args.patch, args.full):
        if not p.exists():
            print(f"error: path does not exist: {p}")
            return

    analyze(args.base, args.patch, args.full)


if __name__ == "__main__":
    main()

