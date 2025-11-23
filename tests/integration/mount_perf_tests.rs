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
