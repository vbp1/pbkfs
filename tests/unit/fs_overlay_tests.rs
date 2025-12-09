#[cfg(target_family = "unix")]
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
use std::{
    fs,
    io::Read,
    io::Write,
    path::{Path, PathBuf},
};

use flate2::{write::ZlibEncoder, Compression};
use parking_lot::ReentrantMutexGuard;
use pbkfs::backup::metadata::{BackupMode, CompressionAlgorithm};
use pbkfs::fs::delta::{
    create_full, create_patch, open_full, open_patch, write_full_page, write_full_ref_slot,
    write_patch_slot,
};
use pbkfs::fs::overlay::{Layer, Overlay};
use tempfile::tempdir;

const BLCKSZ: usize = 8192;

/// Simple RAII helper to set and restore env vars inside tests.
struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
    // Hold exclusive env_lock while the override is in effect to prevent
    // cross-test races (tests run in parallel).
    _lock: ReentrantMutexGuard<'static, ()>,
}

impl EnvGuard {
    fn new(key: &'static str, value: Option<&str>) -> Self {
        let lock = pbkfs::env_lock().lock();
        let prev = std::env::var(key).ok();
        match value {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
        Self {
            key,
            prev,
            _lock: lock,
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

#[test]
fn overlay_reads_base_and_writes_to_diff() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;

    let base_file = base.path().join("data.txt");
    fs::write(&base_file, b"base")?;

    let overlay = Overlay::new(base.path(), diff.path())?;

    let initial = overlay
        .read(Path::new("data.txt"))?
        .expect("base file should exist");
    assert_eq!(b"base", initial.as_slice());

    overlay.write(Path::new("data.txt"), b"diff")?;

    let reread = overlay.read(Path::new("data.txt"))?.expect("diff value");
    assert_eq!(b"diff", reread.as_slice());

    // Base file stays unchanged
    let base_after = fs::read(&base_file)?;
    assert_eq!(b"base", base_after.as_slice());

    Ok(())
}

#[test]
fn overlay_creates_new_files_in_diff_only() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let overlay = Overlay::new(base.path(), diff.path())?;

    let rel = Path::new("subdir/new.txt");
    overlay.write(rel, b"hello")?;

    // File should be readable through overlay but absent from base
    let contents = overlay.read(rel)?.expect("overlay read");
    assert_eq!(b"hello", contents.as_slice());

    let base_path = base.path().join(rel);
    assert!(!base_path.exists());

    // Diff path should exist under diff/data
    let diff_path = diff.path().join("data").join(rel);
    assert!(diff_path.exists());

    Ok(())
}

#[test]
fn copy_up_decompresses_compressed_file_on_first_read() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;

    let original = b"compressed-hello-world";
    let compressed = zstd::stream::encode_all(&original[..], 3)?;
    let base_file = base.path().join("data.bin");
    fs::write(&base_file, compressed)?;

    let overlay =
        Overlay::new_with_compression(base.path(), diff.path(), Some(CompressionAlgorithm::Zstd))?;

    let roundtrip = overlay
        .read(Path::new("data.bin"))?
        .expect("data should be readable after decompress");
    assert_eq!(original.as_slice(), roundtrip.as_slice());

    // Base file stays compressed
    let base_bytes = fs::read(&base_file)?;
    assert_ne!(original.as_slice(), base_bytes.as_slice());

    // Diff contains decompressed copy
    let diff_path = diff.path().join("data").join("data.bin");
    assert!(diff_path.exists());
    let diff_bytes = fs::read(&diff_path)?;
    assert_eq!(original.as_slice(), diff_bytes.as_slice());

    Ok(())
}

#[test]
fn is_pg_datafile_recognizes_segments_and_forks() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let overlay = Overlay::new(base.path(), diff.path())?;

    let positive = [
        "base/16389/16402",
        "base/16389/16402.1",
        "base/16389/16402_vm",
        "base/16389/16402_vm.2",
        "global/1262",
        "pg_tblspc/12345/PG_14_202107181/16384/16402_fsm",
    ];
    for p in positive {
        assert!(
            overlay.is_pg_datafile(Path::new(p)),
            "{p} should be datafile"
        );
    }

    let negative = [
        "base/16389/016402",
        "base/16389/abc",
        "base/16389/16402_ptrack",
        "pg_tblspc/12345/notpgver/16384/16402",
        "other/16389/16402",
    ];
    for p in negative {
        assert!(
            !overlay.is_pg_datafile(Path::new(p)),
            "{p} should NOT be datafile"
        );
    }

    Ok(())
}

#[test]
fn delta_read_empty_returns_base_page() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/1/16402");

    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, vec![7u8; BLCKSZ])?;

    let overlay = Overlay::new(base.path(), diff.path())?;
    let page = overlay
        .read_range(rel, 0, BLCKSZ)?
        .expect("page should be readable");
    assert_eq!(page.len(), BLCKSZ);
    assert!(page.iter().all(|b| *b == 7));
    Ok(())
}

#[test]
fn delta_read_applies_patch() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/1/16402");

    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, vec![0u8; BLCKSZ])?;

    let overlay = Overlay::new(base.path(), diff.path())?;
    let (patch_path, _) = overlay.delta_paths(rel);
    fs::create_dir_all(patch_path.parent().unwrap())?;
    create_patch(&patch_path)?;
    let patch_file = pbkfs::fs::delta::open_patch(&patch_path)?;
    // v2 payload: delta=0, value=5
    write_patch_slot(&patch_file, 0, &[0, 5u8])?;
    let (kind, flags, payload) = pbkfs::fs::delta::read_patch_slot(&patch_file, 0)?;
    assert!(matches!(kind, pbkfs::fs::delta::SlotKind::Patch));
    assert_ne!(flags & pbkfs::fs::delta::SLOT_FLAG_V2, 0);
    assert_eq!(payload, vec![0, 5u8]);

    let page = overlay
        .read_range(rel, 0, BLCKSZ)?
        .expect("page should be readable");
    assert_eq!(page[0], 5);
    Ok(())
}

#[test]
fn delta_read_full_ref_overrides_base() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/1/16402");

    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, vec![0u8; BLCKSZ])?;

    let overlay = Overlay::new(base.path(), diff.path())?;
    let (patch_path, full_path) = overlay.delta_paths(rel);
    fs::create_dir_all(patch_path.parent().unwrap())?;
    create_patch(&patch_path)?;
    create_full(&full_path)?;

    let patch_file = pbkfs::fs::delta::open_patch(&patch_path)?;
    let full_file = pbkfs::fs::delta::open_full(&full_path)?;
    write_full_ref_slot(&patch_file, 0)?;
    let page = [9u8; BLCKSZ];
    write_full_page(&full_file, 0, &page)?;
    let bm = pbkfs::fs::delta::load_bitmap_from_patch(&patch_path)?;
    assert_eq!(bm.get(0), 0b01);

    let page = overlay
        .read_range(rel, 0, BLCKSZ)?
        .expect("page should be readable");
    assert_eq!(page[0], 9);
    Ok(())
}

#[test]
fn delta_invalid_patch_magic_errors() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/1/16402");

    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, vec![0u8; BLCKSZ])?;

    let overlay = Overlay::new(base.path(), diff.path())?;
    let (patch_path, _) = overlay.delta_paths(rel);
    fs::create_dir_all(patch_path.parent().unwrap())?;
    fs::write(&patch_path, b"not-a-patch")?;

    let err = overlay
        .read_range(rel, 0, BLCKSZ)
        .expect_err("invalid patch should error");
    let is_patch_err = err
        .downcast_ref::<pbkfs::Error>()
        .map(|e| matches!(e, pbkfs::Error::InvalidPatchFile { .. }))
        .unwrap_or(false);
    assert!(is_patch_err);
    Ok(())
}

#[test]
fn delta_sparse_slot_defaults_to_base() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/1/16402");

    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, vec![3u8; BLCKSZ * 6])?;

    let overlay = Overlay::new(base.path(), diff.path())?;
    let (patch_path, _) = overlay.delta_paths(rel);
    fs::create_dir_all(patch_path.parent().unwrap())?;
    create_patch(&patch_path)?;
    // no slot written -> EMPTY

    let page = overlay
        .read_range(rel, BLCKSZ as u64 * 5, BLCKSZ)?
        .expect("page should be readable");
    assert!(page.iter().all(|b| *b == 3));
    Ok(())
}

#[test]
fn delta_segments_are_isolated() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel0 = Path::new("base/1/16402");
    let rel1 = Path::new("base/1/16402.1");

    // Create base for both segments.
    fs::create_dir_all(base.path().join("base/1"))?;
    fs::write(base.path().join(rel0), vec![0u8; BLCKSZ])?;
    fs::write(base.path().join(rel1), vec![0u8; BLCKSZ])?;

    let overlay = Overlay::new(base.path(), diff.path())?;

    // Write patch only to segment .1
    let (patch_seg, _) = overlay.delta_paths(rel1);
    fs::create_dir_all(patch_seg.parent().unwrap())?;
    create_patch(&patch_seg)?;
    let f = pbkfs::fs::delta::open_patch(&patch_seg)?;
    // v2 payload: delta=0, value=0xAA
    write_patch_slot(&f, 0, &[0, 0xAA])?;
    let bm = pbkfs::fs::delta::load_bitmap_from_patch(&patch_seg)?;
    assert_eq!(bm.get(0), 0b10);
    let (k, flags, payload) = pbkfs::fs::delta::read_patch_slot(&f, 0)?;
    assert!(matches!(k, pbkfs::fs::delta::SlotKind::Patch));
    assert_ne!(flags & pbkfs::fs::delta::SLOT_FLAG_V2, 0);
    assert_eq!(payload, vec![0, 0xAA]);

    // Reading base segment should not see changes.
    let page0 = overlay
        .read_range(rel0, 0, BLCKSZ)?
        .expect("page should be readable");
    assert_eq!(page0[0], 0);

    // Segment .1 still readable.
    let page1 = overlay
        .read_range(rel1, 0, BLCKSZ)?
        .expect("page should be readable");
    assert_eq!(page1[0], 0xAA);
    Ok(())
}

#[test]
fn delta_truncate_to_zero_clears_patch_and_full() {
    let base = tempdir().unwrap();
    let diff = tempdir().unwrap();
    let rel = Path::new("base/1/16402");

    // Create base and delta artifacts.
    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap()).unwrap();
    fs::write(&base_path, vec![1u8; BLCKSZ]).unwrap();

    let overlay = Overlay::new(base.path(), diff.path()).unwrap();
    let (patch_path, full_path) = overlay.delta_paths(rel);
    fs::create_dir_all(patch_path.parent().unwrap()).unwrap();
    create_patch(&patch_path).unwrap();
    create_full(&full_path).unwrap();
    let patch_file = pbkfs::fs::delta::open_patch(&patch_path).unwrap();
    write_patch_slot(&patch_file, 0, &[0, 0, 1, 0, 9]).unwrap();

    overlay.truncate_pg_datafile(rel, 0).unwrap();

    assert!(!patch_path.exists());
    assert!(!full_path.exists());
    assert!(overlay
        .read_range(rel, 0, BLCKSZ)
        .unwrap()
        .expect("page")
        .iter()
        .all(|b| *b == 0));
}

#[test]
fn delta_extend_without_write_returns_zeroes() {
    let base = tempdir().unwrap();
    let diff = tempdir().unwrap();
    let rel = Path::new("base/1/16402");

    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap()).unwrap();
    fs::write(&base_path, vec![0u8; BLCKSZ]).unwrap();

    let overlay = Overlay::new(base.path(), diff.path()).unwrap();
    overlay
        .truncate_pg_datafile(rel, (BLCKSZ as u64) * 2)
        .unwrap();

    // Second block should read as zeros, with no delta files created.
    let page = overlay
        .read_range(rel, BLCKSZ as u64, BLCKSZ)
        .unwrap()
        .expect("page");
    assert!(page.iter().all(|b| *b == 0));
    let (patch_path, full_path) = overlay.delta_paths(rel);
    assert!(!patch_path.exists());
    assert!(!full_path.exists());
}

#[test]
fn delta_write_stores_patch_for_small_change() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/1/16402");

    // base datafile with zero page
    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, vec![0u8; BLCKSZ])?;

    let overlay = Overlay::new(base.path(), diff.path())?;

    // Write a tiny change (still block aligned).
    let mut page = vec![0u8; BLCKSZ];
    page[10] = 7;
    overlay.write_at(rel, 0, &page)?;

    // .patch should exist with PATCH kind, .full absent or empty hole.
    let (patch_path, full_path) = overlay.delta_paths(rel);
    assert!(patch_path.exists(), "patch file should be created");
    let patch_meta = fs::metadata(&patch_path)?;
    assert!(patch_meta.len() >= 512);
    let full_meta = fs::metadata(&full_path).ok();
    if let Some(meta) = full_meta {
        // full may exist but should not have a real page allocated
        assert!(meta.len() <= 4096 + 8192);
    }

    // Read returns modified page without needing diff copy.
    let read = overlay
        .read_range(rel, 0, BLCKSZ)?
        .expect("read should succeed");
    assert_eq!(read.len(), BLCKSZ);
    assert_eq!(read[10], 7);
    Ok(())
}

#[test]
fn delta_write_stores_full_for_large_change() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/1/16402");

    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, vec![0u8; BLCKSZ])?;

    let overlay = Overlay::new(base.path(), diff.path())?;

    // Write a very different page (all 1s) to trigger FULL.
    let page = vec![1u8; BLCKSZ];
    overlay.write_at(rel, 0, &page)?;

    let (patch_path, full_path) = overlay.delta_paths(rel);
    assert!(patch_path.exists());
    assert!(full_path.exists());

    // Patch slot should be FULL_REF and full file should have data at block 0.
    let full_data = fs::read(&full_path)?;
    assert!(
        full_data.len() >= 4096 + BLCKSZ,
        "full file should contain at least one page"
    );

    let read = overlay
        .read_range(rel, 0, BLCKSZ)?
        .expect("read should succeed");
    assert_eq!(read, page);
    Ok(())
}

#[test]
fn delta_read_prefers_patch_without_materializing_full_page() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/1/16402");

    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, vec![0u8; BLCKSZ])?;

    let overlay = Overlay::new(base.path(), diff.path())?;

    // Write small patch then remove any diff datafile if created (should not be).
    let mut page = vec![0u8; BLCKSZ];
    page[20] = 5;
    overlay.write_at(rel, 0, &page)?;

    let diff_data = diff.path().join("data").join(rel);
    assert!(!diff_data.exists(), "diff datafile should remain absent");

    let read = overlay
        .read_range(rel, 0, BLCKSZ)?
        .expect("read should succeed");
    assert_eq!(read[20], 5);
    assert!(!diff_data.exists(), "read should not create diff copy");
    Ok(())
}

#[test]
fn unsupported_compression_algorithm_fails_fast() {
    let result = CompressionAlgorithm::from_pg_probackup("pglz");
    assert!(result.is_err());
}

#[test]
fn sparse_incremental_materializes_from_base() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let inc = tempdir()?;
    let diff = tempdir()?;

    // Base full file with content
    let base_file = base.path().join("FULL1").join("database");
    let base_data = vec![b'A'; BLCKSZ * 2];
    fs::create_dir_all(&base_file)?;
    fs::write(base_file.join("tbl"), &base_data)?;

    // Incremental file writes only second block using pagemap-aware layout.
    let inc_db = inc.path().join("INC1").join("database");
    fs::create_dir_all(&inc_db)?;
    let inc_path = inc_db.join("tbl");
    let mut inc_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&inc_path)?;
    write_incremental_entry(&mut inc_file, 1, &vec![b'B'; BLCKSZ])?;
    fs::write(inc_path.with_extension("pagemap"), [0b00000010])?;

    // Build overlay with layers: inc then base
    let layers = vec![
        Layer {
            root: inc.path().join("INC1").join("database"),
            compression: None,
            incremental: true,
            backup_mode: BackupMode::Delta,
        },
        Layer {
            root: base.path().join("FULL1").join("database"),
            compression: None,
            incremental: false,
            backup_mode: BackupMode::Full,
        },
    ];
    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;

    let materialized = overlay.read(Path::new("tbl"))?.expect("materialized file");
    assert_eq!(8192 * 2, materialized.len());
    assert_eq!(&base_data[..8192], &materialized[..8192]);
    assert_eq!(&vec![b'B'; 8192][..], &materialized[8192..]);

    Ok(())
}

#[test]
fn incremental_page_backup_reconstructs_full_file() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let inc = tempdir()?;
    let diff = tempdir()?;

    let rel = Path::new("base/1/rel");

    // Base FULL content: three pages.
    let base_path = base.path().join("FULL").join("database").join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    let mut base_bytes = Vec::new();
    base_bytes.extend(vec![b'A'; BLCKSZ]);
    base_bytes.extend(vec![b'B'; BLCKSZ]);
    base_bytes.extend(vec![b'C'; BLCKSZ]);
    fs::write(&base_path, &base_bytes)?;

    // Incremental PAGE backup with changes to blocks 0 and 2.
    let inc_path = inc.path().join("INC").join("database").join(rel);
    fs::create_dir_all(inc_path.parent().unwrap())?;
    let mut inc_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&inc_path)?;
    let block0 = vec![b'X'; BLCKSZ];
    write_incremental_entry(&mut inc_file, 0, &block0)?;
    let block2 = vec![b'Y'; BLCKSZ];
    write_incremental_entry(&mut inc_file, 2, &block2)?;

    // Sidecar pagemap indicating blocks 0 and 2 changed.
    fs::write(inc_path.with_extension("pagemap"), [0b00000101])?;

    let layers = vec![
        Layer {
            root: inc.path().join("INC").join("database"),
            compression: None,
            incremental: true,
            backup_mode: BackupMode::Page,
        },
        Layer {
            root: base.path().join("FULL").join("database"),
            compression: None,
            incremental: false,
            backup_mode: BackupMode::Full,
        },
    ];

    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;
    let result = overlay.read(rel)?.expect("materialized file");

    assert_eq!(&result[..BLCKSZ], &block0[..]);
    assert_eq!(&result[BLCKSZ..BLCKSZ * 2], &vec![b'B'; BLCKSZ][..]);
    assert_eq!(&result[BLCKSZ * 2..BLCKSZ * 3], &block2[..]);

    // Diff should contain reconstructed file.
    let diff_path = diff.path().join("data").join(rel);
    let diff_bytes = fs::read(diff_path)?;
    assert_eq!(result, diff_bytes);

    Ok(())
}

#[test]
fn page_mode_incremental_without_pagemap_falls_back() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let inc = tempdir()?;
    let diff = tempdir()?;

    let rel = Path::new("base/16389/1259");

    // Base FULL content: two pages in pg_probackup page-stream format.
    let base_path = base.path().join("FULL").join("database").join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    let mut base_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&base_path)?;
    write_incremental_entry(&mut base_file, 0, &vec![b'A'; BLCKSZ])?;
    write_incremental_entry(&mut base_file, 1, &vec![b'B'; BLCKSZ])?;

    // Incremental PAGE-mode file without pagemap (pg_probackup skips pagemap
    // for main-fork relations). Contains only block 0.
    let inc_path = inc.path().join("INC").join("database").join(rel);
    fs::create_dir_all(inc_path.parent().unwrap())?;
    let mut inc_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&inc_path)?;
    let block0 = vec![b'X'; BLCKSZ];
    write_incremental_entry(&mut inc_file, 0, &block0)?;

    let layers = vec![
        Layer {
            root: inc.path().join("INC").join("database"),
            compression: None,
            incremental: true,
            backup_mode: BackupMode::Delta,
        },
        Layer {
            root: base.path().join("FULL").join("database"),
            compression: None,
            incremental: false,
            backup_mode: BackupMode::Full,
        },
    ];

    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;

    let data = overlay.read_range(rel, 0, BLCKSZ * 2)?.expect("file data");

    assert_eq!(data.len(), BLCKSZ * 2);
    assert_eq!(&data[..BLCKSZ], &block0[..]);
    assert_eq!(&data[BLCKSZ..], &vec![b'B'; BLCKSZ][..]);

    // Non-materializing read should not leave a diff copy behind.
    let diff_path = diff.path().join("data").join(rel);
    assert!(!diff_path.exists());

    Ok(())
}

#[test]
fn page_mode_incremental_respects_full_size_metadata() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let inc = tempdir()?;
    let diff = tempdir()?;

    let rel = Path::new("base/16389/16402");

    // Base FULL content: three pages in pg_probackup page-stream format.
    let base_path = base.path().join("FULL").join("database").join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    let mut base_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&base_path)?;
    write_incremental_entry(&mut base_file, 0, &vec![b'A'; BLCKSZ])?;
    write_incremental_entry(&mut base_file, 1, &vec![b'B'; BLCKSZ])?;
    let block2 = vec![b'C'; BLCKSZ];
    write_incremental_entry(&mut base_file, 2, &block2)?;

    // Incremental DELTA file with only block 0 changed in the page stream.
    let inc_root = inc.path().join("INC");
    let inc_path = inc_root.join("database").join(rel);
    fs::create_dir_all(inc_path.parent().unwrap())?;
    let mut inc_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&inc_path)?;
    let block0 = vec![b'X'; BLCKSZ];
    write_incremental_entry(&mut inc_file, 0, &block0)?;

    // pg_probackup writes backup_content.control next to database/ for the backup root.
    let bc_path = inc_root.join("backup_content.control");
    fs::write(
        &bc_path,
        format!(
            "{{\"path\":\"{}\", \"size\":\"{}\", \"mode\":\"33152\", \"is_datafile\":\"1\", \"is_cfs\":\"0\", \"crc\":\"0\", \"compress_alg\":\"none\", \"external_dir_num\":\"0\", \"dbOid\":\"16389\", \"full_size\":\"{}\", \"n_blocks\":\"3\"}}\n",
            rel.display(),
            inc_file.metadata()?.len(),
            BLCKSZ * 3
        ),
    )?;

    let layers = vec![
        Layer {
            root: inc_root.join("database"),
            compression: None,
            incremental: true,
            backup_mode: BackupMode::Delta,
        },
        Layer {
            root: base.path().join("FULL").join("database"),
            compression: None,
            incremental: false,
            backup_mode: BackupMode::Full,
        },
    ];

    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;

    // Logical length should use metadata (3 blocks) even though the page stream has only block 0.
    let logical = overlay.logical_len_for(rel)?.expect("logical length");
    assert_eq!(logical, (BLCKSZ * 3) as u64);

    // Reading block 2 must succeed (falling back to base layer) and return base contents, not EOF.
    let data = overlay
        .read_range(rel, (BLCKSZ * 2) as u64, BLCKSZ)?
        .expect("data for block 2");
    assert_eq!(data.len(), BLCKSZ);
    assert_eq!(&data[..], &block2[..]);

    // Non-materializing read should not require a diff copy; if one appears,
    // it must match the data we just read.
    let diff_path = diff.path().join("data").join(rel);
    if diff_path.exists() {
        let diff_bytes = fs::read(&diff_path)?;
        assert_eq!(diff_bytes.len(), BLCKSZ * 3);
        assert_eq!(&diff_bytes[BLCKSZ * 2..], &block2[..]);
    }

    Ok(())
}

#[test]
fn compressed_incremental_pages_are_applied() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let inc = tempdir()?;
    let diff = tempdir()?;

    let rel = Path::new("base/2/rel");

    // Base FULL content: two pages.
    let base_path = base.path().join("FULL").join("database").join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    let mut base_bytes = Vec::new();
    base_bytes.extend(vec![b'A'; BLCKSZ]);
    base_bytes.extend(vec![b'B'; BLCKSZ]);
    fs::write(&base_path, &base_bytes)?;

    // Incremental PAGE backup with block 1 compressed.
    let inc_path = inc.path().join("INC").join("database").join(rel);
    fs::create_dir_all(inc_path.parent().unwrap())?;
    let mut inc_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&inc_path)?;
    let block1 = vec![b'Z'; BLCKSZ];
    let compressed = compress_zlib_block(&block1)?;
    write_incremental_entry(&mut inc_file, 1, &compressed)?;

    fs::write(inc_path.with_extension("pagemap"), [0b00000010])?;

    let layers = vec![
        Layer {
            root: inc.path().join("INC").join("database"),
            compression: Some(CompressionAlgorithm::Zlib),
            incremental: true,
            backup_mode: BackupMode::Page,
        },
        Layer {
            root: base.path().join("FULL").join("database"),
            compression: None,
            incremental: false,
            backup_mode: BackupMode::Full,
        },
    ];

    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;
    let result = overlay.read(rel)?.expect("materialized file");

    assert_eq!(&result[..BLCKSZ], &vec![b'A'; BLCKSZ][..]);
    assert_eq!(&result[BLCKSZ..BLCKSZ * 2], &block1[..]);

    Ok(())
}

#[test]
fn pagemap_mismatch_surfaces_error() {
    let base = tempdir().unwrap();
    let inc = tempdir().unwrap();
    let diff = tempdir().unwrap();

    let rel = Path::new("base/3/rel");

    // Base with two pages.
    let base_path = base.path().join("FULL").join("database").join(rel);
    fs::create_dir_all(base_path.parent().unwrap()).unwrap();
    let mut base_bytes = Vec::new();
    base_bytes.extend(vec![b'A'; BLCKSZ * 2]);
    fs::write(&base_path, &base_bytes).unwrap();

    // Incremental only contains block 1, but pagemap expects blocks 0 and 1.
    let inc_path = inc.path().join("INC").join("database").join(rel);
    fs::create_dir_all(inc_path.parent().unwrap()).unwrap();
    let mut inc_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&inc_path)
        .unwrap();
    write_incremental_entry(&mut inc_file, 1, &vec![b'Z'; BLCKSZ]).unwrap();
    fs::write(inc_path.with_extension("pagemap"), [0b00000011]).unwrap();

    let layers = vec![
        Layer {
            root: inc.path().join("INC").join("database"),
            compression: None,
            incremental: true,
            backup_mode: BackupMode::Page,
        },
        Layer {
            root: base.path().join("FULL").join("database"),
            compression: None,
            incremental: false,
            backup_mode: BackupMode::Full,
        },
    ];

    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers).unwrap();
    let err = overlay.read(rel).expect_err("expected failure");

    let has_incomplete = err
        .downcast_ref::<pbkfs::Error>()
        .map(|e| matches!(e, pbkfs::Error::IncompleteIncremental { .. }))
        .unwrap_or(false);
    assert!(has_incomplete, "expected IncompleteIncremental error");
}

#[cfg(target_os = "linux")]
#[test]
fn write_surfaces_enospc_from_diff_device() {
    let base = tempdir().unwrap();
    let diff = tempdir().unwrap();
    let overlay = Overlay::new(base.path(), diff.path()).unwrap();

    // Route the target to /dev/full so writes yield ENOSPC.
    let rel = Path::new("dev_full.bin");
    let target = diff.path().join("data").join(rel);
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    std::os::unix::fs::symlink("/dev/full", &target).unwrap();

    let err = overlay
        .write(rel, b"payload")
        .expect_err("should propagate ENOSPC");
    let io_err = err
        .downcast_ref::<std::io::Error>()
        .expect("must be io error");
    let code = io_err.raw_os_error();
    assert!(code == Some(28) || code == Some(libc::EACCES));
}

#[cfg(target_family = "unix")]
#[test]
fn write_surfaces_eacces_when_diff_not_writable() {
    let base = tempdir().unwrap();
    let diff = tempdir().unwrap();
    let overlay = Overlay::new(base.path(), diff.path()).unwrap();

    // Make diff/data read-only to force EACCES on write.
    let diff_data = diff.path().join("data");
    fs::set_permissions(&diff_data, fs::Permissions::from_mode(0o555)).unwrap();

    let rel = Path::new("blocked/file.txt");
    let err = overlay
        .write(rel, b"cannot-write")
        .expect_err("should propagate EACCES");
    let io_err = err
        .downcast_ref::<std::io::Error>()
        .expect("must be io error");
    assert_eq!(io_err.kind(), std::io::ErrorKind::PermissionDenied);
}

#[test]
fn mixed_compression_chain_prefers_newest_layer() -> pbkfs::Result<()> {
    let compressed = tempdir()?;
    let base = tempdir()?;
    let diff = tempdir()?;

    // Newest layer: compressed zstd file
    let top_root = compressed.path().join("INC1").join("database");
    fs::create_dir_all(&top_root)?;
    let original = b"mixed-compression";
    let compressed_bytes = zstd::stream::encode_all(&original[..], 3)?;
    fs::write(top_root.join("tbl"), compressed_bytes)?;

    // Older layer: uncompressed base file with different contents
    let base_root = base.path().join("FULL1").join("database");
    fs::create_dir_all(&base_root)?;
    fs::write(base_root.join("tbl"), b"older-version")?;

    let layers = vec![
        Layer {
            root: top_root,
            compression: Some(CompressionAlgorithm::Zstd),
            incremental: false,
            backup_mode: BackupMode::Full,
        },
        Layer {
            root: base_root,
            compression: None,
            incremental: false,
            backup_mode: BackupMode::Full,
        },
    ];

    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;
    let data = overlay
        .read(Path::new("tbl"))?
        .expect("should materialize from compressed top layer");

    assert_eq!(data.as_slice(), original);

    // Diff should now have decompressed bytes.
    let diff_path = overlay.diff_root().join("tbl");
    assert!(diff_path.exists());
    assert_eq!(fs::read(diff_path)?, original);

    Ok(())
}

#[test]
fn block_read_materializes_only_requested_blocks() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;

    let rel = Path::new("tbl");

    // Three pages of distinct bytes so we can spot accidental full copy-up.
    let mut base_bytes = Vec::new();
    base_bytes.extend(vec![b'A'; BLCKSZ]);
    base_bytes.extend(vec![b'B'; BLCKSZ]);
    base_bytes.extend(vec![b'C'; BLCKSZ]);
    fs::write(base.path().join(rel), &base_bytes)?;

    let overlay = Overlay::new(base.path(), diff.path())?;

    // Read only the middle block.
    let middle = overlay
        .read_range(rel, BLCKSZ as u64, BLCKSZ)?
        .expect("block read");
    assert_eq!(vec![b'B'; BLCKSZ], middle);

    // Diff file should exist with block 1 written but block 0 still a hole.
    let diff_path = overlay.diff_root().join(rel);
    let meta = fs::metadata(&diff_path)?;
    assert!(meta.len() >= (BLCKSZ * 2) as u64);
    // Sparse indicator: allocated bytes smaller than logical length.
    assert!(meta.blocks() * 512 < meta.len());

    // First block in diff should remain zeroed (not copied from base).
    let mut first_block = vec![0u8; BLCKSZ];
    fs::File::open(&diff_path)?.read_exact(&mut first_block)?;
    assert_eq!(vec![0u8; BLCKSZ], first_block);

    Ok(())
}

#[test]
fn block_reads_respect_pagemap_and_remain_sparse() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let inc = tempdir()?;
    let diff = tempdir()?;

    let rel = Path::new("delta/tbl");

    // Base with two blocks.
    let mut base_bytes = Vec::new();
    base_bytes.extend(vec![b'A'; BLCKSZ]);
    base_bytes.extend(vec![b'B'; BLCKSZ]);
    fs::create_dir_all(
        base.path()
            .join("FULL/database")
            .join(rel.parent().unwrap()),
    )?;
    fs::write(base.path().join("FULL/database").join(rel), &base_bytes)?;

    // Incremental changes only block 1.
    let inc_path = inc.path().join("INC/database").join(rel);
    fs::create_dir_all(inc_path.parent().unwrap())?;
    let mut inc_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&inc_path)?;
    write_incremental_entry(&mut inc_file, 1, &vec![b'Z'; BLCKSZ])?;
    fs::write(inc_path.with_extension("pagemap"), [0b00000010])?;

    let layers = vec![
        Layer {
            root: inc.path().join("INC").join("database"),
            compression: None,
            incremental: true,
            backup_mode: BackupMode::Delta,
        },
        Layer {
            root: base.path().join("FULL").join("database"),
            compression: None,
            incremental: false,
            backup_mode: BackupMode::Full,
        },
    ];

    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;

    // Read block 0 only; should come from base, block 1 stays untouched.
    let blk0 = overlay.read_range(rel, 0, BLCKSZ)?.expect("block0");
    assert_eq!(vec![b'A'; BLCKSZ], blk0);

    // Block 1 should not be materialized yet (hole, zeros).
    let diff_path = overlay.diff_root().join(rel);
    let mut blk1 = vec![0u8; BLCKSZ];
    let read = fs::File::open(&diff_path)?.read_at(&mut blk1, BLCKSZ as u64)?;
    let slice = &blk1[..read];
    assert!(slice.iter().all(|b| *b == 0));

    // Now read block 1; should apply incremental page.
    let blk1_read = overlay
        .read_range(rel, BLCKSZ as u64, BLCKSZ)?
        .expect("block1");
    assert_eq!(vec![b'Z'; BLCKSZ], blk1_read);

    Ok(())
}

#[test]
fn datafile_reads_do_not_materialize_when_policy_disabled() -> pbkfs::Result<()> {

    let base = tempdir()?;
    let diff = tempdir()?;

    let rel = Path::new("base/1/12345");
    let base_root = base.path().join("FULL/database");
    fs::create_dir_all(base_root.join(rel.parent().unwrap()))?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(base_root.join(rel))?;
    write_incremental_entry(&mut file, 0, &vec![b'A'; BLCKSZ])?;
    write_incremental_entry(&mut file, 1, &vec![b'B'; BLCKSZ])?;

    let layers = vec![Layer {
        root: base_root,
        compression: None,
        incremental: false,
        backup_mode: BackupMode::Full,
    }];

    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;

    assert!(
        !overlay.diff_root().join(rel).exists(),
        "diff should start empty for datafile"
    );

    let read = overlay.read_range(rel, 0, BLCKSZ * 2)?.expect("datafile");
    assert_eq!(read.len(), BLCKSZ * 2);

    let diff_path = overlay.diff_root().join(rel);
    if diff_path.exists() {
        let meta = fs::metadata(&diff_path)?;
        assert_eq!(meta.len(), 0, "materialize_on_read=0 must not copy blocks");
    }

    let metrics = overlay.metrics();
    assert_eq!(0, metrics.blocks_copied, "read should avoid copy-up");

    Ok(())
}

#[test]
fn write_at_updates_only_requested_range() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;

    let rel = PathBuf::from("tbl");
    let base_path = base.path().join(&rel);
    fs::write(&base_path, vec![b'A'; BLCKSZ * 2])?;

    let overlay = Overlay::new(base.path(), diff.path())?;

    // Patch the second block without rewriting the first.
    overlay.write_at(&rel, BLCKSZ as u64, b"XYZ")?;

    let data = overlay.read(&rel)?.expect("patched file");
    assert_eq!(&data[..BLCKSZ], vec![b'A'; BLCKSZ]);
    assert_eq!(&data[BLCKSZ..BLCKSZ + 3], b"XYZ");
    assert!(data[BLCKSZ + 3..BLCKSZ + 8].iter().all(|b| *b == b'A'));

    Ok(())
}

#[test]
fn invalidate_cache_removes_cached_entries() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;

    let rel = PathBuf::from("cache/me");
    fs::create_dir_all(
        base.path()
            .join("FULL/database")
            .join(rel.parent().unwrap()),
    )?;
    fs::write(
        base.path().join("FULL/database").join(&rel),
        vec![b'Q'; BLCKSZ],
    )?;

    let layers = vec![Layer {
        root: base.path().join("FULL/database"),
        compression: None,
        incremental: false,
        backup_mode: BackupMode::Full,
    }];
    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;

    // Populate cache via a read.
    let _ = overlay.read_range(&rel, 0, BLCKSZ)?;
    assert!(overlay.debug_cache_keys().iter().any(|p| p == &rel));

    overlay.invalidate_cache(&rel);
    assert!(
        !overlay.debug_cache_keys().iter().any(|p| p == &rel),
        "cache entry should be dropped after invalidation"
    );

    Ok(())
}

#[test]
fn mixed_compression_block_decode() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let inc = tempdir()?;
    let diff = tempdir()?;

    let rel = Path::new("comp/tbl");

    // Base block content.
    fs::create_dir_all(
        base.path()
            .join("FULL/database")
            .join(rel.parent().unwrap()),
    )?;
    fs::write(
        base.path().join("FULL/database").join(rel),
        vec![b'A'; BLCKSZ],
    )?;

    // Incremental top layer with zlib-compressed block 0.
    fs::create_dir_all(inc.path().join("INC/database").join(rel.parent().unwrap()))?;
    let inc_path = inc.path().join("INC/database").join(rel);
    let compressed = compress_zlib_block(&vec![b'Z'; BLCKSZ])?;
    let mut inc_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&inc_path)?;
    write_incremental_entry(&mut inc_file, 0, &compressed)?;
    fs::write(inc_path.with_extension("pagemap"), [0b00000001])?;

    let layers = vec![
        Layer {
            root: inc.path().join("INC").join("database"),
            compression: Some(CompressionAlgorithm::Zlib),
            incremental: true,
            backup_mode: BackupMode::Page,
        },
        Layer {
            root: base.path().join("FULL").join("database"),
            compression: None,
            incremental: false,
            backup_mode: BackupMode::Full,
        },
    ];

    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;
    let blk = overlay.read_range(rel, 0, BLCKSZ)?.expect("block read");
    assert_eq!(vec![b'Z'; BLCKSZ], blk);

    Ok(())
}

#[test]
fn non_default_block_size_supported() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;

    let rel = Path::new("custom/blksize.tbl");
    let block = 4096usize;

    // Two 4KiB pages.
    let mut base_bytes = Vec::new();
    base_bytes.extend(vec![b'X'; block]);
    base_bytes.extend(vec![b'Y'; block]);
    fs::create_dir_all(
        base.path()
            .join("FULL/database")
            .join(rel.parent().unwrap()),
    )?;
    fs::write(base.path().join("FULL/database").join(rel), &base_bytes)?;

    let overlay =
        Overlay::new_with_block_size(base.path().join("FULL/database"), diff.path(), block)?;
    let blk1 = overlay
        .read_range(rel, block as u64, block)?
        .expect("block1");
    assert_eq!(vec![b'Y'; block], blk1);

    Ok(())
}

#[test]
fn pg_datafile_range_read_does_not_extend_past_logical_len() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;

    // Simulate a pg_probackup-style FULL datafile for relation "base/1/2662"
    // stored as a stream of BackupPageHeader + BLCKSZ page records.
    let rel = Path::new("base/1/2662");
    let data_root = base.path().join("FULL").join("database");
    let full_path = data_root.join(rel);
    fs::create_dir_all(full_path.parent().unwrap())?;

    // Build four pages with distinct byte patterns.
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&full_path)?;
    for block in 0u32..4 {
        let payload = vec![b'A' + (block as u8); BLCKSZ];
        write_incremental_entry(&mut file, block, &payload)?;
    }

    // pg_probackup records relation existence and datafile status in
    // backup_content.control. Create a minimal mock alongside the FULL root
    // so that Overlay::top_layer_has_datafile() and logical_len() behave as
    // they do for a real backup.
    let content_path = base.path().join("FULL").join("backup_content.control");
    fs::write(
        &content_path,
        concat!(
            r#"{"path":"base/1/2662","size":"-1","mode":"33152","is_datafile":"1","is_cfs":"0","crc":"0","compress_alg":"none","external_dir_num":"0","dbOid":"1","segno":"0","n_blocks":"4"}"#,
            "\n"
        ),
    )?;

    // Single FULL layer providing the per-page formatted datafile.
    let layers = vec![Layer {
        root: data_root.clone(),
        compression: None,
        incremental: false,
        backup_mode: BackupMode::Full,
    }];
    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;

    // First, read the existing 4 * BLCKSZ bytes and ensure the payload matches.
    let first = overlay
        .read_range(rel, 0, BLCKSZ * 4)?
        .expect("existing range");
    assert_eq!(first.len(), BLCKSZ * 4);
    let diff_path = diff.path().join("data").join(rel);

    // Then, read a range starting exactly at logical EOF; we now serve zeros
    // (to avoid EIO during WAL replay) but should not extend the diff file.
    let beyond = overlay
        .read_range(rel, (BLCKSZ * 4) as u64, BLCKSZ)?
        .expect("beyond logical len");
    assert_eq!(beyond.len(), BLCKSZ);
    assert!(beyond.iter().all(|b| *b == 0));
    if diff_path.exists() {
        let meta_after = fs::metadata(&diff_path)?;
        assert_eq!(meta_after.len(), (BLCKSZ * 4) as u64);
    }

    Ok(())
}

#[test]
fn pg_datafile_delta_extends_logical_len() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;

    // Build a single FULL datafile with one block.
    let rel = Path::new("base/1/2662");
    let data_root = base.path().join("FULL").join("database");
    fs::create_dir_all(data_root.join(rel.parent().unwrap()))?;
    let mut full_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(data_root.join(rel))?;
    write_incremental_entry(&mut full_file, 0, &vec![b'A'; BLCKSZ])?;

    // Minimal backup_content.control entry marking the relation as a datafile
    // with one block in the restored backup.
    let content_path = base.path().join("FULL").join("backup_content.control");
    fs::write(
        &content_path,
        concat!(
            r#"{"path":"base/1/2662","size":"-1","mode":"33152","is_datafile":"1","is_cfs":"0","crc":"0","compress_alg":"none","external_dir_num":"0","dbOid":"1","segno":"0","n_blocks":"1"}"#,
            "\n"
        ),
    )?;

    let layers = vec![Layer {
        root: data_root.clone(),
        compression: None,
        incremental: false,
        backup_mode: BackupMode::Full,
    }];
    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;

    // Write a new block far beyond the base length via delta storage.
    let block_idx: u64 = 5;
    overlay.write_at(rel, block_idx * BLCKSZ as u64, &vec![b'Z'; BLCKSZ])?;

    // Logical length must include the newly written block (6 blocks total).
    let logical = overlay.logical_len_for(rel)?.expect("logical len");
    assert_eq!(logical, (block_idx + 1) * BLCKSZ as u64);

    Ok(())
}

#[test]
fn pg_datafile_zero_page_write_relies_on_cache() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;

    // Base FULL datafile with a single block.
    let rel = Path::new("base/1/2662");
    let data_root = base.path().join("FULL").join("database");
    fs::create_dir_all(data_root.join(rel.parent().unwrap()))?;
    let mut full_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(data_root.join(rel))?;
    write_incremental_entry(&mut full_file, 0, &vec![b'A'; BLCKSZ])?;

    // Mark relation as a datafile with one block in backup metadata.
    let content_path = base.path().join("FULL").join("backup_content.control");
    fs::write(
        &content_path,
        concat!(
            r#"{"path":"base/1/2662","size":"-1","mode":"33152","is_datafile":"1","is_cfs":"0","crc":"0","compress_alg":"none","external_dir_num":"0","dbOid":"1","segno":"0","n_blocks":"1"}"#,
            "\n"
        ),
    )?;

    let layers = vec![Layer {
        root: data_root.clone(),
        compression: None,
        incremental: false,
        backup_mode: BackupMode::Full,
    }];
    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;

    // Writing a zero-filled block far beyond the base length produces
    // DeltaDiff::Empty, so only cache-tracked max_written_end can extend EOF.
    let block_idx: u64 = 5;
    overlay.write_at(rel, block_idx * BLCKSZ as u64, &vec![0u8; BLCKSZ])?;

    let logical_with_cache = overlay.logical_len_for(rel)?.expect("logical len");
    assert_eq!(logical_with_cache, (block_idx + 1) * BLCKSZ as u64);

    // Dropping the cache simulates the FUSE invalidation that used to happen
    // before writes; logical length collapses back to metadata-only value.
    overlay.invalidate_cache(rel);
    let logical_after_invalidate = overlay.logical_len_for(rel)?.expect("logical len after invalidate");
    assert_eq!(
        logical_after_invalidate,
        BLCKSZ as u64,
        "cache invalidation must not run for datafiles"
    );

    Ok(())
}

#[test]
fn compressed_incremental_non_data_prefers_newest_layer() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let inc_old = tempdir()?;
    let inc_new = tempdir()?;
    let diff = tempdir()?;

    let rel = Path::new("global/pg_control");

    // Old incremental layer with uncompressed contents.
    let old_root = inc_old.path().join("INC_OLD");
    fs::create_dir_all(old_root.join("global"))?;
    fs::write(old_root.join(rel), b"OLDER_CTRL")?;

    // Newest incremental layer: pg_control is stored as a plain control file
    // even when backup-level compression is enabled. We still mark the layer
    // as compressed in metadata to ensure the overlay special-cases this path.
    let new_root = inc_new.path().join("INC_NEW");
    fs::create_dir_all(new_root.join("global"))?;
    fs::write(new_root.join(rel), b"NEW_CTRL")?;

    // In real pg_probackup backups, pg_control is explicitly marked as
    // uncompressed (compress_alg = "none") in backup_content.control, even
    // when backup-level compression is enabled. Provide a minimal mock to
    // exercise the same behavior.
    let new_content = inc_new.path().join("backup_content.control");
    fs::write(
        &new_content,
        concat!(
            r#"{"path":"global/pg_control","size":"8192","mode":"33152","is_datafile":"0","is_cfs":"0","crc":"0","compress_alg":"none","external_dir_num":"0","dbOid":"0"}"#,
            "\n"
        ),
    )?;

    // Base FULL layer with different contents.
    let base_root = base.path().join("FULL");
    fs::create_dir_all(base_root.join("global"))?;
    fs::write(base_root.join(rel), b"BASE_CTRL")?;

    let layers = vec![
        Layer {
            root: new_root.clone(),
            compression: Some(CompressionAlgorithm::Zlib),
            incremental: true,
            backup_mode: BackupMode::Delta,
        },
        Layer {
            root: old_root.clone(),
            compression: None,
            incremental: true,
            backup_mode: BackupMode::Delta,
        },
        Layer {
            root: base_root.clone(),
            compression: None,
            incremental: false,
            backup_mode: BackupMode::Full,
        },
    ];

    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;

    // Range read must materialize from the newest compressed incremental layer.
    let page = overlay
        .read_range(rel, 0, BLCKSZ)?
        .expect("pg_control contents");
    assert!(page.starts_with(b"NEW_CTRL"));

    let diff_path = diff.path().join("data").join(rel);
    let diff_bytes = fs::read(diff_path)?;
    assert!(diff_bytes.starts_with(b"NEW_CTRL"));

    Ok(())
}

fn write_incremental_entry(file: &mut fs::File, block: u32, payload: &[u8]) -> pbkfs::Result<()> {
    file.write_all(&block.to_le_bytes())?;
    let size = payload.len() as i32;
    file.write_all(&size.to_le_bytes())?;
    file.write_all(payload)?;
    Ok(())
}

fn compress_zlib_block(data: &[u8]) -> pbkfs::Result<Vec<u8>> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(data)?;
    Ok(encoder.finish()?)
}

/// Regression test for sparse diff file reads.
///
/// When PostgreSQL writes to some blocks of a datafile, the diff file becomes
/// sparse (only written blocks contain data). Subsequent reads of blocks that
/// were NOT written to diff must still return data from the backup layer,
/// not zeros or short reads.
///
/// This test simulates the scenario that caused "read only 0 of 8192 bytes"
/// errors: a datafile with 10 blocks in backup, then a sparse diff file is
/// created with only blocks 0-1 containing data, then reading block 5 should
/// return data from backup, not zeros.
#[test]
fn sparse_diff_reads_unmaterialized_blocks_from_backup() -> pbkfs::Result<()> {

    let base = tempdir()?;
    let diff = tempdir()?;

    // Simulate a pg_probackup-style FULL datafile with 10 blocks.
    let rel = Path::new("base/16389/16402");
    let data_root = base.path().join("FULL").join("database");
    let full_path = data_root.join(rel);
    fs::create_dir_all(full_path.parent().unwrap())?;

    // Build 10 pages with distinct byte patterns (block N has byte 'A'+N).
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&full_path)?;
    for block in 0u32..10 {
        let payload = vec![b'A' + (block as u8); BLCKSZ];
        write_incremental_entry(&mut file, block, &payload)?;
    }

    // Create backup_content.control with n_blocks=10.
    let content_path = base.path().join("FULL").join("backup_content.control");
    fs::write(
        &content_path,
        concat!(
            r#"{"path":"base/16389/16402","size":"-1","mode":"33152","is_datafile":"1","is_cfs":"0","crc":"0","compress_alg":"none","external_dir_num":"0","dbOid":"16389","segno":"0","n_blocks":"10"}"#,
            "\n"
        ),
    )?;

    let layers = vec![Layer {
        root: data_root.clone(),
        compression: None,
        incremental: false,
        backup_mode: BackupMode::Full,
    }];
    let overlay = Overlay::new_with_layers(base.path(), diff.path(), layers)?;

    // Step 1: Create a sparse diff file with extended size but only some data.
    // This simulates what happens after PostgreSQL writes to a few blocks:
    // the diff file has the logical size but holes (zeros) in unmaterialized regions.
    let diff_path = diff.path().join("data").join(rel);
    fs::create_dir_all(diff_path.parent().unwrap())?;
    {
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&diff_path)?;
        // Write data only to block 0.
        file.write_at(&vec![b'X'; BLCKSZ], 0)?;
        // Extend file to logical size - blocks 1-9 are "holes" (contain zeros).
        file.set_len((BLCKSZ * 10) as u64)?;
    }

    // Step 2: Read block 5 which is a "hole" in the sparse diff file.
    // The bug was that when FUSE used a file handle on the sparse diff file,
    // read_at() would return zeros instead of data from backup.
    // With the fix, read_range() correctly reads from backup for unmaterialized blocks.
    let block5 = overlay
        .read_range(rel, (BLCKSZ * 5) as u64, BLCKSZ)?
        .expect("block 5");

    // Block 5 should have byte pattern 'F' (= 'A' + 5) from backup, not zeros.
    assert_eq!(block5.len(), BLCKSZ, "block 5 should be full BLCKSZ");
    assert_eq!(
        block5[0], b'F',
        "block 5 first byte should be 'F' from backup, not zero"
    );
    assert!(
        block5.iter().all(|&b| b == b'F'),
        "block 5 should be all 'F's from backup, got zeros or wrong data"
    );

    // Verify that other unmaterialized blocks also read correctly from backup.
    let block9 = overlay
        .read_range(rel, (BLCKSZ * 9) as u64, BLCKSZ)?
        .expect("block 9");
    assert_eq!(
        block9[0], b'J',
        "block 9 first byte should be 'J' from backup"
    );

    Ok(())
}

#[test]
fn delta_read_without_deltas_uses_base() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/16384/16402");

    let mut page = vec![1u8; BLCKSZ];
    page[0] = 7;
    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, &page)?;

    let overlay = Overlay::new(base.path(), diff.path())?;
    let read = overlay.read_range(rel, 0, BLCKSZ)?.expect("page present");
    assert_eq!(read, page);
    Ok(())
}

#[test]
fn delta_read_applies_patch_slot() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/16384/16403");

    let mut base_page = vec![3u8; BLCKSZ];
    base_page[100] = 4;
    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, &base_page)?;

    // Create .patch with a small delta.
    let (patch_path, _) = {
        let overlay = Overlay::new(base.path(), diff.path())?;
        overlay.delta_paths(rel)
    };
    create_patch(&patch_path)?;
    let mut new_page = [0u8; BLCKSZ];
    new_page.copy_from_slice(&base_page);
    new_page[200] = 9;
    let delta = pbkfs::fs::delta::compute_delta(&base_page.try_into().unwrap(), &new_page);
    let payload = delta.to_patch_bytes().unwrap();
    let patch_file = open_patch(&patch_path)?;
    write_patch_slot(&patch_file, 0, &payload)?;

    let overlay = Overlay::new(base.path(), diff.path())?;
    let read = overlay.read_range(rel, 0, BLCKSZ)?.expect("patched page");
    assert_eq!(read[200], 9);
    assert_eq!(read[100], 4);
    assert_eq!(read[0], 3);
    Ok(())
}

#[test]
fn delta_read_full_ref() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/16384/16404");

    let base_page = vec![5u8; BLCKSZ];
    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, &base_page)?;

    let (_, full_path) = {
        let overlay = Overlay::new(base.path(), diff.path())?;
        overlay.delta_paths(rel)
    };
    create_full(&full_path)?;
    let full_file = open_full(&full_path)?;
    let mut new_page = [0u8; BLCKSZ];
    new_page.fill(8);
    write_full_page(&full_file, 0, &new_page)?;
    let patch_path = full_path.with_extension("patch");
    create_patch(&patch_path)?;
    let patch_file = open_patch(&patch_path)?;
    write_full_ref_slot(&patch_file, 0)?;
    let bm = pbkfs::fs::delta::load_bitmap_from_patch(&patch_path)?;
    assert_eq!(bm.get(0), 0b01);

    let overlay = Overlay::new(base.path(), diff.path())?;
    let read = overlay.read_range(rel, 0, BLCKSZ)?.expect("full-ref page");
    assert_eq!(read, new_page);
    Ok(())
}

#[test]
fn full_without_patch_is_error() {
    let base = tempdir().unwrap();
    let diff = tempdir().unwrap();
    let rel = Path::new("base/16384/16405");

    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap()).unwrap();
    fs::write(&base_path, vec![0u8; BLCKSZ]).unwrap();

    let (_, full_path) = {
        let overlay = Overlay::new(base.path(), diff.path()).unwrap();
        overlay.delta_paths(rel)
    };
    create_full(&full_path).unwrap();
    assert!(full_path.exists());
    let err = pbkfs::fs::delta::load_bitmap_from_patch(&full_path.with_extension("patch"))
        .expect_err("missing patch should error when full exists");
    let _ = err.downcast::<pbkfs::Error>().expect("invalid patch error");

    let overlay = Overlay::new(base.path(), diff.path()).unwrap();
    let err = overlay
        .read_range(rel, 0, BLCKSZ)
        .expect_err("should fail without patch");
    let err = err.downcast::<pbkfs::Error>().unwrap();
    match err {
        pbkfs::Error::InvalidPatchFile { .. } => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn truncate_zero_removes_delta_artifacts() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/16384/16406");
    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, vec![1u8; BLCKSZ])?;

    let overlay = Overlay::new(base.path(), diff.path())?;
    // Write a delta to create .patch and .full.
    overlay.write_at(rel, 0, &[2u8; BLCKSZ])?;

    let (patch_path, full_path) = overlay.delta_paths(rel);
    assert!(patch_path.exists());
    assert!(full_path.exists());

    overlay.truncate_pg_datafile(rel, 0)?;
    assert!(!patch_path.exists());
    assert!(!full_path.exists());

    // After truncation to zero, relation should read as zeros (empty).
    let reread = overlay.read_range(rel, 0, BLCKSZ)?.expect("page");
    assert!(reread.iter().all(|b| *b == 0));
    Ok(())
}

#[test]
fn extend_without_write_returns_zero() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/16384/16407");
    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, vec![0u8; BLCKSZ])?;

    let overlay = Overlay::new(base.path(), diff.path())?;
    overlay.truncate_pg_datafile(rel, (BLCKSZ * 2) as u64)?;

    let data = overlay
        .read_range(rel, BLCKSZ as u64, BLCKSZ)?
        .expect("extended block");
    assert_eq!(data, vec![0u8; BLCKSZ]);
    Ok(())
}

#[test]
fn is_pg_datafile_patterns() {
    let base = tempdir().unwrap();
    let diff = tempdir().unwrap();
    let overlay = Overlay::new(base.path(), diff.path()).unwrap();

    assert!(overlay.is_pg_datafile(Path::new("base/16389/16402")));
    assert!(overlay.is_pg_datafile(Path::new("base/16389/16402.1")));
    assert!(overlay.is_pg_datafile(Path::new("pg_tblspc/12345/PG_14_202107181/16384/16402_fsm")));
    assert!(!overlay.is_pg_datafile(Path::new("base/16389/16402_ptrack")));
    assert!(!overlay.is_pg_datafile(Path::new("config/postgresql.conf")));
}
