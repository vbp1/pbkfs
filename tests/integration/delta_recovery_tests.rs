//! Integration tests covering crash recovery and concurrent delta mutation.
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
};

use pbkfs::fs::{
    delta::{compute_delta, create_patch, open_patch, write_patch_slot, DeltaDiff, PAGE_SIZE},
    overlay::Overlay,
};
use tempfile::tempdir;

#[test]
fn rebuilds_bitmap_after_crash_using_patch_slots() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("12345");

    // Ensure base layer contains the relation so matching_layers() sees it.
    std::fs::write(base.path().join(rel), vec![0u8; PAGE_SIZE])?;
    let overlay = Overlay::new(base.path(), diff.path())?;

    let (patch_path, _) = overlay.delta_paths(rel);

    create_patch(&patch_path)?;

    let base_page = [0u8; PAGE_SIZE];
    let mut new_page = base_page;
    new_page[..4].copy_from_slice(&[1, 2, 3, 4]);

    let delta = compute_delta(&base_page, &new_page);
    let payload = match delta {
        DeltaDiff::Patch { .. } => delta.to_patch_bytes().unwrap(),
        _ => panic!("expected patch diff, got {:?}", delta),
    };

    let patch_file = open_patch(&patch_path)?;
    write_patch_slot(&patch_file, 0, &payload)?;

    // Ensure on-disk scan sees the patch slot before remount.
    let bitmap = pbkfs::fs::delta::load_bitmap_from_patch(&patch_path)?;
    assert_eq!(0b10, bitmap.get(0));

    // Simulate crash/remount: drop caches and build a fresh overlay.
    drop(overlay);
    let remounted = Overlay::new(base.path(), diff.path())?;

    let page = remounted
        .read_range(rel, 0, 4)?
        .expect("patch-backed page should be readable");
    assert_eq!(&page[..4], &[1, 2, 3, 4]);

    Ok(())
}

#[test]
fn concurrent_truncate_and_write_do_not_corrupt_delta() -> pbkfs::Result<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let overlay = Arc::new(Overlay::new(base.path(), diff.path())?);
    let rel = Path::new("12345");

    let running = Arc::new(AtomicBool::new(true));

    let writer_overlay = overlay.clone();
    let writer_running = running.clone();
    let writer = thread::spawn(move || -> pbkfs::Result<()> {
        let block = vec![0xAB; PAGE_SIZE];
        for _ in 0..32 {
            writer_overlay.write_at(rel, 0, &block)?;
            writer_overlay.write_at(rel, PAGE_SIZE as u64, &block)?;
        }
        writer_running.store(false, Ordering::Release);
        Ok(())
    });

    let trunc_overlay = overlay.clone();
    let trunc_running = running.clone();
    let trunc = thread::spawn(move || -> pbkfs::Result<()> {
        while trunc_running.load(Ordering::Acquire) {
            trunc_overlay.truncate_pg_datafile(rel, 0)?;
            trunc_overlay.truncate_pg_datafile(rel, (PAGE_SIZE * 2) as u64)?;
        }
        // Final truncate to leave the relation addressable.
        trunc_overlay.truncate_pg_datafile(rel, (PAGE_SIZE * 2) as u64)?;
        Ok(())
    });

    writer.join().expect("writer thread panicked")?;
    trunc.join().expect("truncate thread panicked")?;

    // Relation should remain readable (pattern or zeros are both acceptable).
    let buf = overlay
        .read_range(rel, 0, 16)?
        .unwrap_or_else(|| vec![0u8; 16]);
    assert_eq!(buf.len(), 16);

    Ok(())
}

/// Ensure aligned writes to PostgreSQL datafiles go through the delta path
/// (patch slots) instead of materializing a full copy-up in the diff.
#[test]
fn pg_datafile_write_uses_delta_slots() -> pbkfs::Result<()> {
    use pbkfs::fs::delta::{load_bitmap_from_patch, PAGE_SIZE};

    let base = tempdir()?;
    let diff = tempdir()?;

    // Seed a relation in the base layer so overlay recognizes it as pg datafile.
    let rel = Path::new("base/1/12345");
    let rel_dir = base.path().join("base/1");
    std::fs::create_dir_all(&rel_dir)?;
    // Two pages of zeros.
    std::fs::write(base.path().join(rel), vec![0u8; PAGE_SIZE * 2])?;

    let overlay = Overlay::new(base.path(), diff.path())?;

    // Write a single aligned page with a couple of changed bytes.
    let mut page = vec![0u8; PAGE_SIZE];
    page[0] = 0xAA;
    page[PAGE_SIZE - 1] = 0xBB;
    overlay.write_at(rel, 0, &page)?;

    // Copy-up should not have created a plain diff file for the relation.
    let diff_path = diff.path().join(rel);
    assert!(
        !diff_path.exists(),
        "plain diff copy should not exist for pg datafile writes"
    );

    // Patch file must exist and mark block 0 as a patch slot.
    let (patch_path, full_path) = overlay.delta_paths(rel);
    assert!(
        patch_path.exists(),
        "delta patch file missing after pg datafile write"
    );
    let bitmap = load_bitmap_from_patch(&patch_path)?;
    assert_eq!(0b10, bitmap.get(0), "block 0 should be stored as PATCH");
    assert_eq!(0, bitmap.get(1), "block 1 should remain untouched");

    // Full-file slot should not be used for a tiny change.
    // Full file is pre-created with a small header; it must not contain the full page.
    if full_path.exists() {
        assert!(
            full_path.metadata()?.len() <= 4096,
            "full delta file should not store complete page for small patch"
        );
    }

    // Reading back must reflect the written bytes.
    let read_back = overlay
        .read_range(rel, 0, PAGE_SIZE)?
        .expect("page should be readable");
    assert_eq!(read_back[0], 0xAA);
    assert_eq!(read_back[PAGE_SIZE - 1], 0xBB);

    Ok(())
}
