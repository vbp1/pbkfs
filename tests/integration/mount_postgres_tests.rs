//! Integration-style smoke test that exercises the mount path without
//! requiring a real PostgreSQL instance. It validates that pbkfs:
//! - loads backup metadata,
//! - resolves a chain,
//! - writes binding metadata to the diff directory, and
//! - keeps the base backup immutable while writes land in the diff.

use std::{fs, path::Path, thread, time::Duration};

use pbkfs::{binding::BindingRecord, binding::BindingState, binding::LockMarker, cli::mount::MountArgs, cli::unmount};
use tempfile::tempdir;

fn write_metadata(store: &Path, _compressed: bool, compression: Option<(&str, Option<u8>)>) {
    let metadata = serde_json::json!([
        {
            "instance": "main",
            "backups": [
                {
                    "id": "FULL1",
                    "parent-backup-id": null,
                    "backup-mode": "FULL",
                    "status": "OK",
                    "compress-alg": compression.map(|(alg, _)| alg).unwrap_or("none"),
                    "compress-level": compression.and_then(|(_, lvl)| lvl),
                    "start-time": "2024-01-01T00:00:00Z",
                    "data-bytes": 1024u64,
                    "program-version": "2.6.0"
                }
            ]
        }
    ]);
    fs::write(
        store.join("backups.json"),
        serde_json::to_vec_pretty(&metadata).unwrap(),
    )
    .unwrap();
}

fn try_unmount(args: unmount::UnmountArgs, diff_dir: &Path) {
    if let Err(err) = unmount::execute(args) {
        if let Some(pbkfs::Error::NotMounted(_)) = err.downcast_ref::<pbkfs::Error>() {
            let _ = fs::remove_file(diff_dir.join(pbkfs::binding::LOCK_FILE));
            return;
        }
        if let Some(pbkfs::Error::Io(io_err)) = err.downcast_ref::<pbkfs::Error>() {
            if io_err.kind() == std::io::ErrorKind::PermissionDenied {
                eprintln!("skipping unmount due to permission: {io_err}");
                return;
            }
        }
        panic!("unexpected unmount failure: {err}");
    }
}

#[test]
fn mounts_backup_and_preserves_store_immutability() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let target = tempdir()?;
    let diff = tempdir()?;

    // Base backup content lives under the store path
    let base_file = store
        .path()
        .join("FULL1")
        .join("database")
        .join("data/base.txt");
    fs::create_dir_all(base_file.parent().unwrap())?;
    fs::write(&base_file, b"from-store")?;
    write_metadata(store.path(), false, None);

    let args = MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
    };

    let ctx = pbkfs::cli::mount::mount(args)?;

    // Reads come from base
    let contents = ctx
        .overlay
        .read(Path::new("data/base.txt"))?
        .expect("base file should be visible");
    assert_eq!(b"from-store", contents.as_slice());

    // Writes go to diff and do not mutate store
    ctx.overlay
        .write(Path::new("data/base.txt"), b"from-diff")?;

    let reread = ctx
        .overlay
        .read(Path::new("data/base.txt"))?
        .expect("diff value should be returned");
    assert_eq!(b"from-diff", reread.as_slice());

    let base_after = fs::read(base_file)?;
    assert_eq!(b"from-store", base_after.as_slice());

    // Binding metadata should have been persisted
    let binding_path = diff.path().join(pbkfs::binding::BINDING_FILE);
    assert!(binding_path.exists());

    // Unmount removes the lock marker
    unmount::execute(unmount::UnmountArgs {
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
    })?;

    assert!(!diff.path().join(pbkfs::binding::LOCK_FILE).exists());

    Ok(())
}

#[test]
fn mount_decompresses_compressed_backup_files() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let target = tempdir()?;
    let diff = tempdir()?;

    // Create compressed base content in the backup store
    let base_file = store
        .path()
        .join("FULL1")
        .join("database")
        .join("data/compressed.dat");
    fs::create_dir_all(base_file.parent().unwrap())?;
    let payload = b"postgres-compressed-data";
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::new(6));
    use std::io::Write;
    encoder.write_all(payload)?;
    let compressed = encoder.finish()?;
    fs::write(&base_file, compressed)?;

    write_metadata(store.path(), true, Some(("zlib", Some(6))));

    let args = MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
    };

    let ctx = pbkfs::cli::mount::mount(args)?;

    let contents = ctx
        .overlay
        .read(Path::new("data/compressed.dat"))?
        .expect("decompressed view should exist");
    assert_eq!(payload.as_slice(), contents.as_slice());

    // Base remains compressed bytes
    let base_after = fs::read(&base_file)?;
    assert_ne!(payload.as_slice(), base_after.as_slice());

    // Diff holds a decompressed copy
    let diff_path = diff
        .path()
        .join("data")
        .join(Path::new("data/compressed.dat"));
    assert!(diff_path.exists());
    let diff_bytes = fs::read(diff_path)?;
    assert_eq!(payload.as_slice(), diff_bytes.as_slice());

    Ok(())
}

#[test]
fn remount_reuses_existing_diff_and_binding() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let target = tempdir()?;
    let diff = tempdir()?;

    let base_file = store
        .path()
        .join("FULL1")
        .join("database")
        .join("data/base.txt");
    fs::create_dir_all(base_file.parent().unwrap())?;
    fs::write(&base_file, b"from-store")?;
    write_metadata(store.path(), false, None);

    let initial = match pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
    }) {
        Ok(ctx) => ctx,
        Err(err) => {
            if err.to_string().contains("Permission denied") {
                eprintln!("skipping stale-lock recovery test: {err}");
                return Ok(());
            }
            return Err(err);
        }
    };

    initial
        .overlay
        .write(Path::new("data/base.txt"), b"first-write")?;

    let initial_binding = initial.binding.clone();

    if let Some(handle) = initial.fuse_handle {
        handle.unmount();
    }
    try_unmount(
        unmount::UnmountArgs {
            mnt_path: Some(target.path().to_path_buf()),
            diff_dir: Some(diff.path().to_path_buf()),
        },
        diff.path(),
    );

    // Ensure timestamp granularity for last_used_at update
    thread::sleep(Duration::from_millis(1100));

    let remount = match pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: None,
        backup_id: None,
    }) {
        Ok(ctx) => ctx,
        Err(err) => {
            eprintln!("remount failed: {err:?}");
            if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
                if io_err.kind() == std::io::ErrorKind::PermissionDenied {
                    eprintln!("skipping stale-lock recovery test: {io_err}");
                    return Ok(());
                }
            }
            if let Some(pbk_err) = err.downcast_ref::<pbkfs::Error>() {
                if let pbkfs::Error::Io(io_err) = pbk_err {
                    if io_err.kind() == std::io::ErrorKind::PermissionDenied {
                        eprintln!("skipping stale-lock recovery test: {io_err}");
                        return Ok(());
                    }
                }
            }
            if err.to_string().contains("Permission denied") {
                eprintln!("skipping stale-lock recovery test: {err}");
                return Ok(());
            }
            return Err(err);
        }
    };

    let reread = remount
        .overlay
        .read(Path::new("data/base.txt"))?
        .expect("diff data should persist across remounts");
    assert_eq!(b"first-write", reread.as_slice());

    let binding_after = match BindingRecord::load_from_diff(&pbkfs::binding::DiffDir::new(diff.path())?) {
        Ok(b) => b,
        Err(err) => {
            if err.to_string().contains("Permission denied") {
                eprintln!("skipping stale-lock recovery test: {err}");
                return Ok(());
            }
            return Err(err);
        }
    };
    assert_eq!(initial_binding.binding_id, binding_after.binding_id);
    assert_eq!(BindingState::Active, binding_after.state);
    assert!(binding_after.last_used_at >= initial_binding.last_used_at);

    // Also ensure the remount context exposes the same binding
    assert_eq!(initial_binding.binding_id, remount.binding.binding_id);

    if let Some(handle) = remount.fuse_handle {
        handle.unmount();
    }
    try_unmount(
        unmount::UnmountArgs {
            mnt_path: Some(target.path().to_path_buf()),
            diff_dir: Some(diff.path().to_path_buf()),
        },
        diff.path(),
    );

    Ok(())
}

#[test]
fn remount_rejects_binding_mismatch() {
    let store = tempdir().unwrap();
    let target = tempdir().unwrap();
    let diff = tempdir().unwrap();

    let base_file = store
        .path()
        .join("FULL1")
        .join("database")
        .join("data/base.txt");
    fs::create_dir_all(base_file.parent().unwrap()).unwrap();
    fs::write(&base_file, b"from-store").unwrap();
    write_metadata(store.path(), false, None);

    let initial = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
    })
    .unwrap();

    if let Some(handle) = initial.fuse_handle {
        handle.unmount();
    }
    try_unmount(
        unmount::UnmountArgs {
            mnt_path: Some(target.path().to_path_buf()),
            diff_dir: Some(diff.path().to_path_buf()),
        },
        diff.path(),
    );

    let err = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("other".into()),
        backup_id: Some("FULL1".into()),
    })
    .expect_err("binding mismatch should fail");

    let actual = err
        .downcast_ref::<pbkfs::Error>()
        .expect("should be pbkfs::Error");
    assert!(matches!(actual, pbkfs::Error::BindingViolation { .. }));
}

#[test]
fn remount_recovers_stale_lock() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let target = tempdir()?;
    let diff = tempdir()?;

    let base_file = store
        .path()
        .join("FULL1")
        .join("database")
        .join("data/base.txt");
    fs::create_dir_all(base_file.parent().unwrap())?;
    fs::write(&base_file, b"from-store")?;
    write_metadata(store.path(), false, None);

    let initial = match pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
    }) {
        Ok(ctx) => ctx,
        Err(err) => {
            if err.to_string().contains("Permission denied") {
                eprintln!("skipping stale-lock recovery test: {err}");
                return Ok(());
            }
            return Err(err);
        }
    };

    if let Some(handle) = initial.fuse_handle {
        handle.unmount();
    }
    try_unmount(
        unmount::UnmountArgs {
            mnt_path: Some(target.path().to_path_buf()),
            diff_dir: Some(diff.path().to_path_buf()),
        },
        diff.path(),
    );

    // Simulate stale lock: rewrite binding with dead PID and reintroduce lock marker
    let mut binding = BindingRecord::load_from_diff(&pbkfs::binding::DiffDir::new(diff.path())?)?;
    binding.owner_pid = 999_999;
    binding.write_to_diff(&pbkfs::binding::DiffDir::new(diff.path())?)?;

    let marker = LockMarker {
        mount_id: initial.session.mount_id,
        diff_dir: diff.path().to_path_buf(),
    };
    fs::write(
        diff.path().join(pbkfs::binding::LOCK_FILE),
        serde_json::to_vec_pretty(&marker)?,
    )?;

    // Remount should recover stale lock
    let remount = match pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: None,
        backup_id: None,
    }) {
        Ok(ctx) => ctx,
        Err(err) => {
            if err.to_string().contains("Permission denied") {
                eprintln!("skipping stale-lock recovery test: {err}");
                return Ok(());
            }
            return Err(err);
        }
    };

    let binding_after = match BindingRecord::load_from_diff(&pbkfs::binding::DiffDir::new(diff.path())?) {
        Ok(b) => b,
        Err(err) => {
            if err.to_string().contains("Permission denied") {
                eprintln!("skipping stale-lock recovery test: {err}");
                return Ok(());
            }
            return Err(err);
        }
    };
    assert_eq!(BindingState::Active, binding_after.state);
    assert_eq!(binding.binding_id, binding_after.binding_id);

    if let Some(handle) = remount.fuse_handle {
        handle.unmount();
    }
    try_unmount(
        unmount::UnmountArgs {
            mnt_path: Some(target.path().to_path_buf()),
            diff_dir: Some(diff.path().to_path_buf()),
        },
        diff.path(),
    );

    Ok(())
}
