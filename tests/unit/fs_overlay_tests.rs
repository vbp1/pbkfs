#[cfg(target_family = "unix")]
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
use std::{fs, io::Read, io::Write, path::Path};

use flate2::{write::ZlibEncoder, Compression};
use pbkfs::backup::metadata::{BackupMode, CompressionAlgorithm};
use pbkfs::fs::overlay::{Layer, Overlay};
use tempfile::tempdir;

const BLCKSZ: usize = 8192;

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

    // First, read the existing 4 * BLCKSZ bytes and ensure diff length matches.
    let first = overlay
        .read_range(rel, 0, BLCKSZ * 4)?
        .expect("existing range");
    assert_eq!(first.len(), BLCKSZ * 4);
    let diff_path = diff.path().join("data").join(rel);
    let meta = fs::metadata(&diff_path)?;
    assert_eq!(meta.len(), (BLCKSZ * 4) as u64);

    // Then, read a range starting exactly at logical EOF; this must not
    // materialize a new zero page or extend the diff-backed file.
    let beyond = overlay.read_range(rel, (BLCKSZ * 4) as u64, BLCKSZ)?;
    assert!(matches!(beyond, Some(buf) if buf.is_empty()));
    let meta_after = fs::metadata(&diff_path)?;
    assert_eq!(meta_after.len(), (BLCKSZ * 4) as u64);

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
