//! Perf-adjacent smoke test for mount/overlay flow. The goal is not to
//! benchmark realistically but to ensure the hot-path helpers respond within a
//! small, predictable window for small fixtures.

use std::{fs, time::Instant};

use pbkfs::fs::overlay::Overlay;
use tempfile::tempdir;

#[test]
fn overlay_read_write_is_fast_enough() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;

    // Prepare a ~128 KiB file as a lightweight stand-in for backup pages.
    let payload = vec![1u8; 128 * 1024];
    fs::create_dir_all(base.path().join("data"))?;
    fs::write(base.path().join("data/chunk.bin"), &payload)?;

    let overlay = Overlay::new(base.path(), diff.path())?;

    let start = Instant::now();
    let read = overlay
        .read(std::path::Path::new("data/chunk.bin"))?
        .expect("data should be readable");
    overlay.write(std::path::Path::new("data/chunk.bin"), b"diff")?;
    let elapsed = start.elapsed();

    println!(
        "overlay_read_write elapsed_ms={} payload_bytes={}",
        elapsed.as_millis(),
        payload.len()
    );

    assert_eq!(payload.len(), read.len());
    // The exact budget is generous; we're only guarding against accidental
    // regressions from obvious pathologies (e.g., repeated fs::metadata calls).
    assert!(
        elapsed.as_millis() < 50,
        "overlay operations too slow: {:?}",
        elapsed
    );

    Ok(())
}

#[test]
fn lazy_block_reads_leave_diff_sparse_and_track_cache() -> pbkfs::Result<()> {
    use std::os::unix::fs::FileExt;

    let base = tempdir()?;
    let diff = tempdir()?;

    // Create four 8KiB blocks with identifiable bytes.
    let mut bytes = Vec::new();
    bytes.extend(vec![b'A'; 8192]);
    bytes.extend(vec![b'B'; 8192]);
    bytes.extend(vec![b'C'; 8192]);
    bytes.extend(vec![b'D'; 8192]);
    fs::write(base.path().join("data.bin"), &bytes)?;

    let overlay = Overlay::new(base.path(), diff.path())?;

    // Touch two blocks, then re-read one to hit the cache.
    let _ = overlay.read_range(std::path::Path::new("data.bin"), 0, 8192)?;
    let _ = overlay.read_range(std::path::Path::new("data.bin"), 8192 * 2, 4096)?;
    let _ = overlay.read_range(std::path::Path::new("data.bin"), 0, 4096)?;

    let diff_path = overlay.diff_root().join("data.bin");
    // Unread block #1 should remain zeroed (hole is acceptable, short read OK).
    let mut hole = vec![0u8; 8192];
    let read = fs::File::open(&diff_path)?.read_at(&mut hole, 8192)?;
    let slice = &hole[..read];
    assert!(slice.iter().all(|b| *b == 0));

    let metrics = overlay.metrics();
    assert!(metrics.cache_hits >= 1);
    assert!(metrics.cache_misses >= 1);
    assert_eq!(0, metrics.fallback_used);

    Ok(())
}
