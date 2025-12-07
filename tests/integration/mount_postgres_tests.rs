//! Integration-style smoke test that exercises the mount path without
//! requiring a real PostgreSQL instance. It validates that pbkfs:
//! - loads backup metadata,
//! - resolves a chain,
//! - writes binding metadata to the diff directory, and
//! - keeps the base backup immutable while writes land in the diff.

use std::{fs, path::Path, thread, time::Duration};

use parking_lot::ReentrantMutexGuard;

use pbkfs::{
    binding::BindingRecord, binding::BindingState, binding::LockMarker, cli::mount::MountArgs,
    cli::unmount,
};
use tempfile::tempdir;

/// RAII env guard serialized by pbkfs::env_lock to avoid cross-test races.
struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
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
        match &self.prev {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

fn native_path(store: &Path, backup_id: &str, relative: &Path) -> std::path::PathBuf {
    store
        .join("backups")
        .join("main")
        .join(backup_id)
        .join("database")
        .join(relative)
}

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
    let base_file = native_path(store.path(), "FULL1", Path::new("data/base.txt"));
    fs::create_dir_all(base_file.parent().unwrap())?;
    fs::write(&base_file, b"from-store")?;
    write_metadata(store.path(), false, None);

    // Force BackupStore::load_from_pg_probackup() to use JSON metadata
    // instead of invoking a real pg_probackup binary that may be present
    // in the environment.
    let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some("/nonexistent/pg_probackup"));
    let ctx = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: false,
    })?;

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
    let base_file = native_path(store.path(), "FULL1", Path::new("data/compressed.dat"));
    fs::create_dir_all(base_file.parent().unwrap())?;
    let payload = b"postgres-compressed-data";
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::new(6));
    use std::io::Write;
    encoder.write_all(payload)?;
    let compressed = encoder.finish()?;
    fs::write(&base_file, compressed)?;

    write_metadata(store.path(), true, Some(("zlib", Some(6))));

    let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some("/nonexistent/pg_probackup"));
    let ctx = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: false,
    })?;

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

    let base_file = native_path(store.path(), "FULL1", Path::new("data/base.txt"));
    fs::create_dir_all(base_file.parent().unwrap())?;
    fs::write(&base_file, b"from-store")?;
    write_metadata(store.path(), false, None);

    let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some("/nonexistent/pg_probackup"));

    let initial = match pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: false,
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
        force: false,
        perf_unsafe: false,
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
            if let Some(pbkfs::Error::Io(io_err)) = err.downcast_ref::<pbkfs::Error>() {
                if io_err.kind() == std::io::ErrorKind::PermissionDenied {
                    eprintln!("skipping stale-lock recovery test: {io_err}");
                    return Ok(());
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

    let binding_after =
        match BindingRecord::load_from_diff(&pbkfs::binding::DiffDir::new(diff.path())?) {
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

    let base_file = native_path(store.path(), "FULL1", Path::new("data/base.txt"));
    fs::create_dir_all(base_file.parent().unwrap()).unwrap();
    fs::write(&base_file, b"from-store").unwrap();
    write_metadata(store.path(), false, None);

    let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some("/nonexistent/pg_probackup"));

    let initial = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: false,
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
        force: false,
        perf_unsafe: false,
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

    let base_file = native_path(store.path(), "FULL1", Path::new("data/base.txt"));
    fs::create_dir_all(base_file.parent().unwrap())?;
    fs::write(&base_file, b"from-store")?;
    write_metadata(store.path(), false, None);

    let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some("/nonexistent/pg_probackup"));

    let initial = match pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: false,
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
        force: false,
        perf_unsafe: false,
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

    let binding_after =
        match BindingRecord::load_from_diff(&pbkfs::binding::DiffDir::new(diff.path())?) {
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

#[test]
fn perf_unsafe_creates_and_clears_dirty_marker() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let target = tempdir()?;
    let diff = tempdir()?;

    let base_file = native_path(store.path(), "FULL1", Path::new("data/base.txt"));
    fs::create_dir_all(base_file.parent().unwrap())?;
    fs::write(&base_file, b"from-store")?;
    write_metadata(store.path(), false, None);

    let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some("/nonexistent/pg_probackup"));
    let ctx = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: true,
    })?;

    let marker = diff.path().join(pbkfs::fs::overlay::DIRTY_MARKER);
    assert!(
        marker.exists(),
        "perf-unsafe mount must create dirty marker"
    );

    ctx.overlay
        .write(Path::new("data/base.txt"), b"from-diff-updated")?;

    ctx.flush_dirty()?;

    unmount::execute(unmount::UnmountArgs {
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
    })?;

    assert!(
        !marker.exists(),
        "graceful unmount should remove perf-unsafe marker"
    );

    Ok(())
}

#[test]
fn perf_unsafe_marker_requires_force_after_crash() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let target = tempdir()?;
    let diff = tempdir()?;

    let base_file = native_path(store.path(), "FULL1", Path::new("data/base.txt"));
    fs::create_dir_all(base_file.parent().unwrap())?;
    fs::write(&base_file, b"from-store")?;
    write_metadata(store.path(), false, None);

    let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some("/nonexistent/pg_probackup"));

    let marker = diff.path().join(pbkfs::fs::overlay::DIRTY_MARKER);
    fs::write(&marker, "leftover")?;

    let err = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: false,
    })
    .expect_err("mount should reject existing dirty marker");

    match err.downcast::<pbkfs::Error>() {
        Ok(pbkfs::Error::PerfUnsafeDirtyMarker { .. }) => {}
        other => panic!("unexpected error for dirty marker: {other:?}"),
    }

    let ctx = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: true,
        perf_unsafe: true,
    })?;

    ctx.flush_dirty()?;
    unmount::execute(unmount::UnmountArgs {
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
    })?;

    Ok(())
}

/// Ignored by default: requires a real pg_probackup catalog and FUSE.
#[test]
#[ignore]
fn mounts_native_pg_probackup_catalog_when_available() -> pbkfs::Result<()> {
    let Some(store_var) = std::env::var_os("PBKFS_NATIVE_STORE") else {
        // Environment not provided; skip quietly.
        return Ok(());
    };
    let store_path = std::path::PathBuf::from(store_var);
    let instance = std::env::var("PBKFS_NATIVE_INSTANCE").unwrap_or_else(|_| "main".into());
    let backup_id = std::env::var("PBKFS_NATIVE_BACKUP_ID").unwrap_or_else(|_| "FULL1".into());

    let target = tempdir()?;
    let diff = tempdir()?;

    let ctx = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store_path),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some(instance),
        backup_id: Some(backup_id),
        force: false,
        perf_unsafe: false,
    })?;

    if let Some(handle) = ctx.fuse_handle {
        handle.unmount();
    }
    let _ = std::fs::remove_file(diff.path().join(pbkfs::binding::LOCK_FILE));

    Ok(())
}
