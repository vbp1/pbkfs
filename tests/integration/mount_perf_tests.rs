//! Perf-adjacent smoke test for mount/overlay flow. The goal is not to
//! benchmark realistically but to ensure the hot-path helpers respond within a
//! small, predictable window for small fixtures.

use std::{fs, path::Path, sync::Arc, thread, time::Instant};

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

/// Stress-test concurrent range reads against the overlay to ensure that
/// blockwise reconstruction remains correct and efficient under load.
#[test]
fn concurrent_range_reads_are_consistent() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;

    // Create a moderately sized file composed of repeated 8KiB blocks so that
    // each block has deterministic contents.
    let block_size: usize = 8192;
    let blocks: usize = 64;
    let mut bytes = Vec::with_capacity(block_size * blocks);
    for i in 0..blocks {
        let fill = (i as u8).wrapping_add(1);
        bytes.extend(std::iter::repeat(fill).take(block_size));
    }
    fs::write(base.path().join("data.bin"), &bytes)?;

    let overlay = Arc::new(Overlay::new(base.path(), diff.path())?);
    let start = Instant::now();

    let threads: Vec<_> = (0..8)
        .map(|_| {
            let overlay = overlay.clone();
            thread::spawn(move || -> pbkfs::Result<()> {
                for block in 0..blocks {
                    let offset = (block * block_size) as u64;
                    let data = overlay
                        .read_range(Path::new("data.bin"), offset, block_size)?
                        .expect("range should be readable");
                    assert_eq!(data.len(), block_size);
                    let expected = (block as u8).wrapping_add(1);
                    assert!(data.iter().all(|b| *b == expected));
                }
                Ok(())
            })
        })
        .collect();

    for t in threads {
        t.join().expect("thread panicked")?;
    }

    let elapsed = start.elapsed();
    println!(
        "concurrent_range_reads threads=8 blocks={} elapsed_ms={}",
        blocks,
        elapsed.as_millis()
    );

    // Keep the budget generous; we mainly want to guard against severe
    // regressions where worker-pool or overlay changes cause obvious stalls.
    assert!(
        elapsed.as_secs() < 2,
        "concurrent overlay reads too slow: {:?}",
        elapsed
    );

    Ok(())
}

/// Exercise interleaved reads and writes on distinct files to validate that the
/// overlay remains free of truncation or corruption under concurrent access.
#[test]
fn concurrent_reads_and_writes_remain_correct() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;

    fs::create_dir_all(base.path().join("data"))?;
    fs::write(base.path().join("data/read.bin"), vec![7u8; 64 * 1024])?;
    fs::write(base.path().join("data/write.bin"), vec![0u8; 64 * 1024])?;

    let overlay = Arc::new(Overlay::new(base.path(), diff.path())?);

    let reader = {
        let overlay = overlay.clone();
        thread::spawn(move || -> pbkfs::Result<()> {
            for _ in 0..64 {
                let buf = overlay
                    .read(Path::new("data/read.bin"))?
                    .expect("read.bin should exist");
                assert!(buf.iter().all(|b| *b == 7));
            }
            Ok(())
        })
    };

    let writer = {
        let overlay = overlay.clone();
        thread::spawn(move || -> pbkfs::Result<()> {
            for i in 0..64u8 {
                let pattern = vec![i; 1024];
                let mut existing = overlay
                    .read(Path::new("data/write.bin"))?
                    .unwrap_or_default();
                if existing.len() < pattern.len() {
                    existing.resize(pattern.len(), 0);
                }
                existing[..pattern.len()].copy_from_slice(&pattern);
                overlay.write(Path::new("data/write.bin"), &existing)?;
            }
            Ok(())
        })
    };

    reader.join().expect("reader thread panicked")?;
    writer.join().expect("writer thread panicked")?;

    let final_bytes = overlay
        .read(Path::new("data/write.bin"))?
        .expect("write.bin should exist");
    assert_eq!(final_bytes.len(), 64 * 1024);

    Ok(())
}
