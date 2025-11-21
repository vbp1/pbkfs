use std::{fs, path::Path};

use pbkfs::backup::metadata::CompressionAlgorithm;
use pbkfs::fs::overlay::Overlay;
use tempfile::tempdir;

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
