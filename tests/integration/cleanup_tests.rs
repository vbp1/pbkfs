//! Integration tests for cleanup behavior.

use pbkfs::binding::{BindingRecord, DiffDir, BINDING_FILE, LOCK_FILE};
use pbkfs::cli::cleanup::CleanupArgs;
use std::os::unix::fs::symlink;
use std::os::unix::fs::PermissionsExt;
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn cleanup_clears_diff_dir_when_idle() -> pbkfs::Result<()> {
    let diff = tempdir()?;
    let store = tempdir()?;
    let target = tempdir()?;

    let diff_dir = DiffDir::new(diff.path())?;
    let mut binding = BindingRecord::new(
        "main",
        "FULL1",
        store.path(),
        target.path(),
        0,
        "host",
        "0.1.0",
    );
    binding.mark_released();
    binding.write_to_diff(&diff_dir)?;

    let diff_file = diff
        .path()
        .join("data/base/pg_wal/000000010000000000000001");
    std::fs::create_dir_all(diff_file.parent().unwrap())?;
    std::fs::write(&diff_file, b"wal-bytes")?;

    pbkfs::cli::cleanup::execute(CleanupArgs {
        diff_dir: Some(diff.path().to_path_buf()),
        force: false,
    })?;

    assert!(!diff.path().join(BINDING_FILE).exists());
    assert!(!diff.path().join(LOCK_FILE).exists());
    assert!(!diff_file.exists());

    Ok(())
}

#[test]
fn cleanup_blocks_active_and_allows_force_override() -> pbkfs::Result<()> {
    let diff = tempdir()?;
    let store = tempdir()?;
    let target = tempdir()?;

    let diff_dir = DiffDir::new(diff.path())?;
    let pid = std::process::id() as i32;
    let binding = BindingRecord::new(
        "main",
        "FULL1",
        store.path(),
        target.path(),
        pid,
        "host",
        "0.1.0",
    );
    binding.write_to_diff(&diff_dir)?;
    diff_dir.write_lock(Uuid::new_v4())?;

    let err = pbkfs::cli::cleanup::execute(CleanupArgs {
        diff_dir: Some(diff.path().to_path_buf()),
        force: false,
    })
    .expect_err("active mount should be blocked");

    let actual = err
        .downcast_ref::<pbkfs::Error>()
        .expect("should downcast to pbkfs::Error");
    assert!(matches!(actual, pbkfs::Error::BindingInUse(p) if *p == pid));

    pbkfs::cli::cleanup::execute(CleanupArgs {
        diff_dir: Some(diff.path().to_path_buf()),
        force: true,
    })?;

    assert!(!diff.path().join(BINDING_FILE).exists());
    assert!(!diff.path().join(LOCK_FILE).exists());

    Ok(())
}

#[test]
fn cleanup_removes_symlink_without_following() -> pbkfs::Result<()> {
    let diff = tempdir()?;
    let outside = tempdir()?;

    // Create a real file outside the diff dir
    let outside_file = outside.path().join("keep.txt");
    std::fs::write(&outside_file, b"important")?;

    // Symlink inside diff pointing to outside
    let link_path = diff.path().join("data/link.txt");
    std::fs::create_dir_all(link_path.parent().unwrap())?;
    symlink(&outside_file, &link_path)?;

    pbkfs::cli::cleanup::execute(CleanupArgs {
        diff_dir: Some(diff.path().to_path_buf()),
        force: true,
    })?;

    assert!(!link_path.exists());
    assert!(outside_file.exists());

    Ok(())
}

#[test]
fn cleanup_fails_when_diff_not_writable_and_preserves_binding() {
    let diff = tempdir().unwrap();
    let store = tempdir().unwrap();
    let target = tempdir().unwrap();

    let diff_dir = DiffDir::new(diff.path()).unwrap();
    let binding = BindingRecord::new(
        "main",
        "FULL1",
        store.path(),
        target.path(),
        0,
        "host",
        "0.1.0",
    );
    binding.write_to_diff(&diff_dir).unwrap();

    // Make root diff dir read-only so ensure_writable fails.
    let mut perms = std::fs::metadata(diff.path()).unwrap().permissions();
    perms.set_mode(0o500);
    std::fs::set_permissions(diff.path(), perms).unwrap();

    let err = pbkfs::cli::cleanup::execute(CleanupArgs {
        diff_dir: Some(diff.path().to_path_buf()),
        force: true,
    })
    .expect_err("non-writable diff should cause error");

    // Restore perms for tempdir cleanup
    let mut perms = std::fs::metadata(diff.path()).unwrap().permissions();
    perms.set_mode(0o700);
    let _ = std::fs::set_permissions(diff.path(), perms);

    assert!(matches!(
        err.downcast_ref::<pbkfs::Error>(),
        Some(pbkfs::Error::DiffDirNotWritable(_))
    ));
    // Binding file should remain because cleanup aborted
    assert!(diff.path().join(BINDING_FILE).exists());
}
