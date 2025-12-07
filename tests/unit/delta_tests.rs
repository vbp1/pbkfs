use std::fs;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

use std::os::unix::fs::FileExt;

use parking_lot::ReentrantMutexGuard;
use pbkfs::fs::delta::{
    apply_patch, compute_delta, create_full, create_patch, open_full, open_patch, punch_full_hole,
    read_full_page, read_patch_slot, write_empty_slot, write_full_page, write_full_ref_slot,
    write_patch_slot, BlockBitmap, DeltaDiff, DeltaIndex, SlotKind, PAGE_SIZE, PATCH_PAYLOAD_MAX,
    PATCH_SLOT_SIZE,
};
use pbkfs::fs::overlay::Overlay;
use pbkfs::Error;
use pbkfs::Result as PbResult;

const BLCKSZ: usize = 8192;

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
    // Hold exclusive env_lock to serialize env overrides between parallel tests.
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
        if let Some(prev) = self.prev.take() {
            std::env::set_var(self.key, prev);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

fn filled_page(value: u8) -> [u8; PAGE_SIZE] {
    let mut page = [0u8; PAGE_SIZE];
    page.fill(value);
    page
}

#[test]
fn delta_empty_for_identical_pages() {
    let base = filled_page(0);
    let diff = compute_delta(&base, &base);

    assert!(matches!(diff, DeltaDiff::Empty));
    assert_eq!(diff.serialized_len(), 0);
}

#[test]
fn small_patch_round_trip() {
    let base = filled_page(0);
    let mut modified = base;
    modified[100] = 3;
    modified[500] = 1;

    let diff = compute_delta(&base, &modified);
    let bytes = match diff {
        DeltaDiff::Patch { .. } => diff.to_patch_bytes().unwrap(),
        other => panic!("expected patch delta, got {:?}", other),
    };

    assert_eq!(bytes.len(), 10); // two segments: (4 + 1) * 2

    let rebuilt = apply_patch(&base, &bytes).expect("patch should apply");
    assert_eq!(rebuilt, modified);
}

#[test]
fn large_delta_falls_back_to_full() {
    let base = filled_page(0);
    let modified = filled_page(1);

    let diff = compute_delta(&base, &modified);
    assert!(matches!(diff, DeltaDiff::Full(_)));
    assert_eq!(diff.serialized_len(), PAGE_SIZE);
}

#[test]
fn patch_rejects_out_of_bounds_segment() {
    let base = filled_page(0);
    // offset 8191 + len 2 overruns a single 8KiB page.
    let mut payload = Vec::new();
    payload.extend_from_slice(&(8191u16).to_le_bytes());
    payload.extend_from_slice(&(2u16).to_le_bytes());
    payload.extend_from_slice(&[1u8, 2u8]);

    let err = apply_patch(&base, &payload).expect_err("should detect overflow");
    match err.downcast::<Error>() {
        Ok(Error::CorruptedPatch { .. }) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn patch_rejects_empty_payload() {
    let base = filled_page(0);
    let err = apply_patch(&base, &[]).expect_err("empty payload must fail");
    match err.downcast::<Error>() {
        Ok(Error::CorruptedPatch { .. }) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn patch_file_round_trip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("file.patch");
    create_patch(&path).unwrap();
    let f = open_patch(&path).unwrap();

    let payload = vec![1u8; 20];
    write_patch_slot(&f, 3, &payload).unwrap();
    let (kind, data) = read_patch_slot(&f, 3).unwrap();
    assert_eq!(kind, SlotKind::Patch);
    assert_eq!(data, payload);

    // Unwritten slot should be EMPTY.
    let (kind, data) = read_patch_slot(&f, 10).unwrap();
    assert_eq!(kind, SlotKind::Empty);
    assert!(data.is_empty());
}

#[test]
fn full_file_round_trip_and_punch() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("file.full");
    create_full(&path).unwrap();
    let f = open_full(&path).unwrap();

    let page = filled_page(9);
    write_full_page(&f, 5, &page).unwrap();
    let read_back = read_full_page(&f, 5).unwrap();
    assert_eq!(read_back, page);

    // Punch hole should zero page on read (if supported), otherwise leave data.
    let _ = punch_full_hole(&f, 5);
    let reread = read_full_page(&f, 5).unwrap();
    // Accept either all zeros (hole) or original data (if punch not supported).
    assert!(reread == page || reread == [0u8; PAGE_SIZE]);
}

#[test]
fn payload_larger_than_slot_errors() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("file.patch");
    create_patch(&path).unwrap();
    let f = open_patch(&path).unwrap();

    let big = vec![0u8; PATCH_PAYLOAD_MAX + 1];
    let err = write_patch_slot(&f, 0, &big).expect_err("should fail for oversize payload");
    match err.downcast::<Error>() {
        Ok(Error::PatchTooLarge { .. }) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn bitmap_set_and_get() {
    let bm = BlockBitmap::default();
    assert_eq!(bm.get(10), 0);

    bm.set(10, 0b10);
    assert_eq!(bm.get(10), 0b10);

    bm.set(11, 0b01);
    assert_eq!(bm.get(10), 0b10);
    assert_eq!(bm.get(11), 0b01);

    // Ensure other entries unaffected.
    assert_eq!(bm.get(12), 0);
    assert!(bm.len_bytes() >= 3);
}

#[test]
fn patch_magic_validation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("bad.patch");
    fs::write(&path, b"not-a-patch").unwrap();
    let err = open_patch(&path).expect_err("bad magic should fail");
    match err.downcast::<Error>() {
        Ok(Error::InvalidPatchFile { .. }) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn patch_rejects_unknown_slot_kind() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("file.patch");
    create_patch(&path).unwrap();
    let f = open_patch(&path).unwrap();
    // Write garbage kind directly into slot header.
    let mut buf = [0u8; 512];
    buf[0] = 9;
    f.write_all_at(&buf, 512).unwrap();

    let err = read_patch_slot(&f, 0).expect_err("unknown kind should fail");
    match err.downcast::<Error>() {
        Ok(Error::InvalidPatchFile { .. }) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn patch_rejects_zero_length_payload() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("file.patch");
    create_patch(&path).unwrap();
    let f = open_patch(&path).unwrap();

    // Manually craft zero-length payload with kind=PATCH
    let mut buf = [0u8; 512];
    buf[0] = SlotKind::Patch as u8;
    buf[2..4].copy_from_slice(&0u16.to_le_bytes());
    f.write_all_at(&buf, 512).unwrap();

    let err = read_patch_slot(&f, 0).expect_err("zero-length patch must fail");
    match err.downcast::<Error>() {
        Ok(Error::InvalidPatchFile { .. }) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn patch_rejects_payload_length_over_slot() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("file.patch");
    create_patch(&path).unwrap();
    let f = open_patch(&path).unwrap();

    let mut buf = [0u8; 512];
    buf[0] = SlotKind::Patch as u8;
    buf[2..4].copy_from_slice(&(PATCH_SLOT_SIZE as u16).to_le_bytes()); // deliberately too large
    f.write_all_at(&buf, 512).unwrap();

    let err = read_patch_slot(&f, 0).expect_err("oversize payload must fail");
    match err.downcast::<Error>() {
        Ok(Error::InvalidPatchFile { .. }) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn bitmap_errors_when_full_missing() {
    let dir = tempdir().unwrap();
    let patch_path = dir.path().join("rel.patch");
    create_patch(&patch_path).unwrap();
    let f = open_patch(&patch_path).unwrap();
    write_full_ref_slot(&f, 0).unwrap();

    let err =
        pbkfs::fs::delta::load_bitmap_from_patch(&patch_path).expect_err("missing full must error");
    match err.downcast::<Error>() {
        Ok(Error::InvalidPatchFile { .. }) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn patch_rejects_truncated_header() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("short.patch");
    fs::write(&path, b"short").unwrap();
    let err = open_patch(&path).expect_err("short header should fail");
    match err.downcast::<Error>() {
        Ok(Error::InvalidPatchFile { .. }) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn full_magic_validation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("bad.full");
    fs::write(&path, b"bad-full").unwrap();
    let err = open_full(&path).expect_err("bad magic should fail");
    match err.downcast::<Error>() {
        Ok(Error::InvalidFullFile { .. }) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn perf_unsafe_marker_placeholder() {
    // Perf-unsafe mode should touch a `.pbkfs-dirty` marker on init and
    // remove it on flush.
    let base = tempdir().unwrap();
    let diff = tempdir().unwrap();

    let _env = EnvGuard::new("PBKFS_PERF_UNSAFE", Some("1"));
    let overlay = Overlay::new(base.path(), diff.path()).expect("overlay init");

    let marker = diff.path().join(pbkfs::fs::overlay::DIRTY_MARKER);
    assert!(marker.exists(), "perf-unsafe mount must create marker");
    assert!(overlay.perf_unsafe(), "flag should propagate to overlay");

    overlay.flush_dirty(false).expect("flush_dirty");
    assert!(
        !marker.exists(),
        "flush_dirty should remove perf-unsafe marker after sync"
    );
}

#[test]
fn load_bitmap_from_patch_sets_bits() {
    let dir = tempdir().unwrap();
    let patch_path = dir.path().join("rel.patch");
    let full_path = dir.path().join("rel.full");
    create_patch(&patch_path).unwrap();
    create_full(&full_path).unwrap();
    let f = open_patch(&patch_path).unwrap();

    // block 0 -> PATCH, block 2 -> FULL
    write_patch_slot(&f, 0, &[1, 2, 3]).unwrap();
    write_full_ref_slot(&f, 2).unwrap();

    let bm = pbkfs::fs::delta::load_bitmap_from_patch(&patch_path).unwrap();
    assert_eq!(bm.get(0), 0b10);
    assert_eq!(bm.get(1), 0b00);
    assert_eq!(bm.get(2), 0b01);
}

#[test]
fn load_bitmap_defaults_empty_when_patch_absent() {
    let dir = tempdir().unwrap();
    let patch_path = dir.path().join("missing.patch");

    let bm = pbkfs::fs::delta::load_bitmap_from_patch(&patch_path).unwrap();
    assert_eq!(bm.get(0), 0);
    assert_eq!(bm.get(10), 0);
    assert!(!patch_path.exists(), "should not create patch on read");
}

#[test]
fn full_without_patch_is_error() {
    let dir = tempdir().unwrap();
    let patch_path = dir.path().join("rel.patch");
    let full_path = dir.path().join("rel.full");
    create_full(&full_path).unwrap();

    let err = pbkfs::fs::delta::load_bitmap_from_patch(&patch_path)
        .expect_err("full without patch must fail");
    match err.downcast::<Error>() {
        Ok(Error::InvalidPatchFile { .. }) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn delta_index_caches_bitmap() {
    let dir = tempdir().unwrap();
    let patch_path = dir.path().join("rel.patch");
    create_patch(&patch_path).unwrap();
    let f = open_patch(&patch_path).unwrap();
    write_patch_slot(&f, 1, &[9]).unwrap();

    let index = DeltaIndex::new();
    let first = index.get_or_load_bitmap(&patch_path).unwrap();
    assert_eq!(first.get(1), 0b10);

    // Overwrite slot to FULL but cache should return old bitmap.
    write_full_ref_slot(&f, 1).unwrap();
    let second = index.get_or_load_bitmap(&patch_path).unwrap();
    assert_eq!(Arc::as_ptr(&first), Arc::as_ptr(&second));
    assert_eq!(second.get(1), 0b10);
}

#[test]
fn bitmap_load_rejects_unknown_kind() {
    let dir = tempdir().unwrap();
    let patch_path = dir.path().join("bad.patch");
    create_patch(&patch_path).unwrap();
    let f = open_patch(&patch_path).unwrap();
    let mut buf = [0u8; 512];
    buf[0] = 9;
    f.write_all_at(&buf, 512).unwrap();

    let err = pbkfs::fs::delta::load_bitmap_from_patch(&patch_path).expect_err("bad kind");
    match err.downcast::<Error>() {
        Ok(Error::InvalidPatchFile { .. }) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn delta_index_is_thread_safe() {
    let dir = tempdir().unwrap();
    let patch_path = dir.path().join("rel.patch");
    create_patch(&patch_path).unwrap();
    let f = open_patch(&patch_path).unwrap();
    write_patch_slot(&f, 3, &[1]).unwrap();

    let index = Arc::new(DeltaIndex::new());
    let mut handles = Vec::new();
    for _ in 0..8 {
        let idx = Arc::clone(&index);
        let path = patch_path.clone();
        handles.push(thread::spawn(move || {
            let bm = idx.get_or_load_bitmap(&path).unwrap();
            assert_eq!(bm.get(3), 0b10);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn identical_page_clears_existing_patch_slot() -> PbResult<()> {
    let dir = tempdir()?;
    let patch_path = dir.path().join("rel.patch");
    let full_path = dir.path().join("rel.full");
    create_patch(&patch_path)?;
    create_full(&full_path)?;

    let patch_file = open_patch(&patch_path)?;
    let full_file = open_full(&full_path)?;

    write_patch_slot(&patch_file, 0, &[1, 2])?;
    write_full_page(&full_file, 0, &filled_page(9))?;

    write_empty_slot(&patch_file, 0)?;
    let _ = punch_full_hole(&full_file, 0);

    let (cleared_kind, cleared_payload) = read_patch_slot(&patch_file, 0)?;
    assert_eq!(cleared_kind, SlotKind::Empty);
    assert!(cleared_payload.is_empty());

    let index = DeltaIndex::new();
    let bm = index.get_or_load_bitmap(&patch_path)?;
    assert_eq!(bm.get(0), 0);
    Ok(())
}

#[test]
fn concurrent_read_and_read_succeeds() {
    let dir = tempdir().unwrap();
    let patch_path = dir.path().join("rel.patch");
    create_patch(&patch_path).unwrap();
    let f = open_patch(&patch_path).unwrap();
    write_patch_slot(&f, 1, &[1, 2, 3]).unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let path = patch_path.clone();
        let b = barrier.clone();
        handles.push(thread::spawn(move || {
            b.wait();
            let f = open_patch(&path).unwrap();
            let (kind, payload) = read_patch_slot(&f, 1).unwrap();
            assert_eq!(kind, SlotKind::Patch);
            assert_eq!(payload, vec![1, 2, 3]);
        }));
    }
    barrier.wait();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn concurrent_read_and_write_observes_consistent_payload() {
    let dir = tempdir().unwrap();
    let patch_path = dir.path().join("rel.patch");
    create_patch(&patch_path).unwrap();
    let f = open_patch(&patch_path).unwrap();
    write_patch_slot(&f, 2, &[7]).unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let path_clone = patch_path.clone();
    let reader = {
        let b = barrier.clone();
        thread::spawn(move || {
            b.wait();
            let f = open_patch(&path_clone).unwrap();
            let (kind, payload) = read_patch_slot(&f, 2).unwrap();
            assert_eq!(kind, SlotKind::Patch);
            assert!(payload == vec![7] || payload == vec![9]);
        })
    };

    let writer = {
        let b = barrier.clone();
        let path = patch_path.clone();
        thread::spawn(move || {
            b.wait();
            thread::sleep(Duration::from_millis(10));
            let f = open_patch(&path).unwrap();
            write_patch_slot(&f, 2, &[9]).unwrap();
        })
    };

    barrier.wait();
    reader.join().unwrap();
    writer.join().unwrap();
}

#[test]
fn concurrent_write_last_writer_wins() {
    let dir = tempdir().unwrap();
    let patch_path = dir.path().join("rel.patch");
    create_patch(&patch_path).unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let writer1 = {
        let b = barrier.clone();
        let path = patch_path.clone();
        thread::spawn(move || {
            b.wait();
            write_patch_slot(&open_patch(&path).unwrap(), 4, &[1]).unwrap();
        })
    };
    let writer2 = {
        let b = barrier.clone();
        let path = patch_path.clone();
        thread::spawn(move || {
            b.wait();
            thread::sleep(Duration::from_millis(15));
            write_patch_slot(&open_patch(&path).unwrap(), 4, &[2, 3]).unwrap();
        })
    };

    barrier.wait();
    writer1.join().unwrap();
    writer2.join().unwrap();

    let f = open_patch(&patch_path).unwrap();
    let (kind, payload) = read_patch_slot(&f, 4).unwrap();
    assert_eq!(kind, SlotKind::Patch);
    assert_eq!(payload, vec![2, 3]);
}

#[test]
fn patch_slot_write_does_not_touch_neighbors() {
    let dir = tempdir().unwrap();
    let patch_path = dir.path().join("rel.patch");
    create_patch(&patch_path).unwrap();
    let f = open_patch(&patch_path).unwrap();

    write_patch_slot(&f, 1, &[9, 8, 7]).unwrap();

    let (k0, p0) = read_patch_slot(&f, 0).unwrap();
    assert_eq!(k0, SlotKind::Empty);
    assert!(p0.is_empty());

    let (k1, p1) = read_patch_slot(&f, 1).unwrap();
    assert_eq!(k1, SlotKind::Patch);
    assert_eq!(p1, vec![9, 8, 7]);

    let (k2, p2) = read_patch_slot(&f, 2).unwrap();
    assert_eq!(k2, SlotKind::Empty);
    assert!(p2.is_empty());
}

#[test]
fn full_without_full_ref_slot_is_rejected() {
    let dir = tempdir().unwrap();
    let patch_path = dir.path().join("rel.patch");
    let full_path = dir.path().join("rel.full");
    create_patch(&patch_path).unwrap();
    create_full(&full_path).unwrap();

    let full = open_full(&full_path).unwrap();
    write_full_page(&full, 0, &filled_page(1)).unwrap();

    let err = pbkfs::fs::delta::load_bitmap_from_patch(&patch_path)
        .expect_err("bitmap load should fail when full page lacks FULL_REF slot");
    match err.downcast::<Error>() {
        Ok(Error::InvalidPatchFile { .. }) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn full_with_matching_full_ref_loads_bitmap() {
    let dir = tempdir().unwrap();
    let patch_path = dir.path().join("rel.patch");
    let full_path = dir.path().join("rel.full");
    create_patch(&patch_path).unwrap();
    create_full(&full_path).unwrap();

    let patch = open_patch(&patch_path).unwrap();
    let full = open_full(&full_path).unwrap();
    write_full_page(&full, 0, &filled_page(2)).unwrap();
    write_full_ref_slot(&patch, 0).unwrap();

    let bm = pbkfs::fs::delta::load_bitmap_from_patch(&patch_path).unwrap();
    assert_eq!(bm.get(0), 0b01);
}

#[test]
fn punch_hole_failure_keeps_full_and_counts_metric() -> PbResult<()> {
    let _env = EnvGuard::new("PBKFS_TEST_PUNCH_FAIL", Some("1"));
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/1/16402");

    // base page of zeroes
    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, vec![0u8; BLCKSZ])?;

    let overlay = Overlay::new(base.path(), diff.path())?;

    // First write a FULL change (all 1s)
    overlay.write_at(rel, 0, &vec![1u8; BLCKSZ])?;

    // Then write a tiny patch so FULL -> PATCH triggers punch_hole
    let mut small = vec![0u8; BLCKSZ];
    small[0] = 7;
    overlay.write_at(rel, 0, &small)?;

    let metrics = overlay.metrics();
    assert_eq!(metrics.delta_punch_hole_failures, 1);

    let (_patch_path, full_path) = overlay.delta_paths(rel);
    let full = open_full(&full_path)?;
    let page = read_full_page(&full, 0)?;
    // Hole punch failed, old FULL remains.
    assert_eq!(page[0], 1);
    Ok(())
}

#[test]
fn datafile_rename_and_truncate_clean_up_delta_artifacts() -> PbResult<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/1/16402");
    let rel2 = Path::new("base/1/16403");

    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, vec![0u8; BLCKSZ * 2])?;

    let overlay = Overlay::new(base.path(), diff.path())?;

    // Write two pages: first FULL, second PATCH-sized change.
    overlay.write_at(rel, 0, &vec![2u8; BLCKSZ])?;
    let mut second = vec![0u8; BLCKSZ];
    second[10] = 9;
    overlay.write_at(rel, BLCKSZ as u64, &second)?;

    // Rename to a new relation name.
    overlay.rename_pg_datafile(rel, rel2)?;
    let (patch2, full2) = overlay.delta_paths(rel2);
    assert!(patch2.exists());
    assert!(full2.exists());

    // Truncate to zero should remove delta artifacts and bitmap entries.
    overlay.truncate_pg_datafile(rel2, 0)?;
    assert!(!patch2.exists());
    assert!(!full2.exists());

    // Subsequent read should fall back to base (zeroed by truncate) without errors.
    let data = overlay
        .read_range(rel2, 0, BLCKSZ)?
        .expect("data after truncate");
    assert_eq!(data.len(), BLCKSZ);
    assert!(data.iter().all(|b| *b == 0));

    Ok(())
}

#[test]
fn datafile_unlink_removes_delta_files() -> PbResult<()> {
    let base = tempdir()?;
    let diff = tempdir()?;
    let rel = Path::new("base/1/20000");

    let base_path = base.path().join(rel);
    fs::create_dir_all(base_path.parent().unwrap())?;
    fs::write(&base_path, vec![1u8; BLCKSZ])?;

    let overlay = Overlay::new(base.path(), diff.path())?;
    overlay.write_at(rel, 0, &vec![2u8; BLCKSZ])?;

    let (patch_path, full_path) = overlay.delta_paths(rel);
    assert!(patch_path.exists());
    assert!(full_path.exists());

    overlay.unlink_pg_datafile(rel)?;

    assert!(!patch_path.exists());
    assert!(!full_path.exists());

    Ok(())
}
