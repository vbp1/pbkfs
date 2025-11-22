use std::{fs, io::Write, path::Path};

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
