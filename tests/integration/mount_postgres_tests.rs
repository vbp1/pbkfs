//! Integration-style smoke test that exercises the mount path without
//! requiring a real PostgreSQL instance. It validates that pbkfs:
//! - loads backup metadata,
//! - resolves a chain,
//! - writes binding metadata to the diff directory, and
//! - keeps the base backup immutable while writes land in the diff.

use std::{
    fs,
    path::Path,
    thread,
    time::Duration,
};

#[cfg(unix)]
use std::process::{Command, Stdio};

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
        no_wal: false,
        ..Default::default()
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
        ..Default::default()
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
        no_wal: false,
        ..Default::default()
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
        no_wal: false,
        ..Default::default()
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
            ..Default::default()
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
        no_wal: false,
        ..Default::default()
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
            ..Default::default()
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
        no_wal: false,
        ..Default::default()
    })
    .unwrap();

    if let Some(handle) = initial.fuse_handle {
        handle.unmount();
    }
    try_unmount(
        unmount::UnmountArgs {
            mnt_path: Some(target.path().to_path_buf()),
            diff_dir: Some(diff.path().to_path_buf()),
            ..Default::default()
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
        no_wal: false,
        ..Default::default()
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
        no_wal: false,
        ..Default::default()
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
            ..Default::default()
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
        no_wal: false,
        ..Default::default()
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
            ..Default::default()
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
        no_wal: false,
        ..Default::default()
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
        ..Default::default()
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
        no_wal: false,
        ..Default::default()
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
        no_wal: false,
        ..Default::default()
    })?;

    ctx.flush_dirty()?;
    unmount::execute(unmount::UnmountArgs {
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        ..Default::default()
    })?;

    Ok(())
}

#[test]
fn mount_no_wal_virtualizes_wal_writes() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let target = tempdir()?;
    let diff = tempdir()?;

    let wal_rel = Path::new("pg_wal/000000010000000000000001");
    let wal_base = native_path(store.path(), "FULL1", wal_rel);
    fs::create_dir_all(wal_base.parent().unwrap())?;
    fs::write(&wal_base, b"base-wal")?;
    write_metadata(store.path(), false, None);

    let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some("/nonexistent/pg_probackup"));
    let ctx = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: false,
        no_wal: true,
        ..Default::default()
    })?;

    // If FUSE is unavailable in the environment, skip to avoid spurious failures.
    if ctx.fuse_handle.is_none() {
        eprintln!("skipping no_wal_virtualization test: /dev/fuse unavailable");
        return Ok(());
    }

    let wal_path = target.path().join(wal_rel);
    std::fs::write(&wal_path, b"new-wal-bytes")?;

    let read_back = std::fs::read(&wal_path)?;
    assert!(
        read_back.iter().all(|b| *b == 0),
        "changed WAL must read back as zeros under --no-wal"
    );
    // Logical size should reflect the larger of base size and written length.
    assert_eq!(
        read_back.len(),
        b"new-wal-bytes".len().max(b"base-wal".len())
    );
    let meta = fs::metadata(&wal_path)?;
    assert_eq!(meta.len() as usize, read_back.len());

    let diff_wal = diff.path().join("data").join(wal_rel);
    assert!(
        !diff_wal.exists(),
        "WAL writes under --no-wal must not create diff files"
    );

    if let Some(handle) = ctx.fuse_handle {
        handle.unmount();
    }
    try_unmount(
        unmount::UnmountArgs {
            mnt_path: Some(target.path().to_path_buf()),
            diff_dir: Some(diff.path().to_path_buf()),
            ..Default::default()
        },
        diff.path(),
    );

    Ok(())
}

#[test]
fn mount_no_wal_reads_unchanged_wal_from_backup_chain() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let target = tempdir()?;
    let diff = tempdir()?;

    let wal_rel = Path::new("pg_wal/000000010000000000000002");
    let wal_base = native_path(store.path(), "FULL1", wal_rel);
    fs::create_dir_all(wal_base.parent().unwrap())?;
    let payload = b"wal-from-backup";
    fs::write(&wal_base, payload)?;
    write_metadata(store.path(), false, None);

    let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some("/nonexistent/pg_probackup"));
    let ctx = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: false,
        no_wal: true,
        ..Default::default()
    })?;

    // If FUSE is unavailable in the environment, skip to avoid spurious failures.
    if ctx.fuse_handle.is_none() {
        eprintln!("skipping unchanged WAL read test: /dev/fuse unavailable");
        return Ok(());
    }

    let wal_path = target.path().join(wal_rel);
    let read_back = std::fs::read(&wal_path)?;
    assert_eq!(payload.as_slice(), read_back.as_slice());

    let diff_wal = diff.path().join("data").join(wal_rel);
    assert!(
        !diff_wal.exists(),
        "unchanged WAL read under --no-wal must not materialize diff copy"
    );

    if let Some(handle) = ctx.fuse_handle {
        handle.unmount();
    }
    try_unmount(
        unmount::UnmountArgs {
            mnt_path: Some(target.path().to_path_buf()),
            diff_dir: Some(diff.path().to_path_buf()),
            ..Default::default()
        },
        diff.path(),
    );

    Ok(())
}

/// Requires a real pg_probackup catalog and FUSE; skips when env is not provided.
#[test]
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
        no_wal: false,
        ..Default::default()
    })?;

    if let Some(handle) = ctx.fuse_handle {
        handle.unmount();
    }
    let _ = std::fs::remove_file(diff.path().join(pbkfs::binding::LOCK_FILE));

    Ok(())
}

#[cfg(unix)]
fn pbkfs_bin() -> &'static str {
    env!("CARGO_BIN_EXE_pbkfs")
}

#[cfg(unix)]
fn diff_pid_path(diff: &Path) -> std::path::PathBuf {
    diff.join("data")
        .join(pbkfs::cli::pid::DEFAULT_PID_FILE_REL)
}

#[cfg(unix)]
fn wait_for_path(path: &Path, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    path.exists()
}

#[cfg(unix)]
fn kill_pid(pid: i32, sig: i32) {
    unsafe {
        libc::kill(pid, sig);
    }
}

#[cfg(unix)]
fn mount_cmd(store: &Path, mnt: &Path, diff: &Path) -> Command {
    let mut cmd = Command::new(pbkfs_bin());
    cmd.arg("mount")
        .arg("-B")
        .arg(store)
        .arg("-D")
        .arg(mnt)
        .arg("-d")
        .arg(diff)
        .arg("-I")
        .arg("main")
        .arg("-i")
        .arg("FULL1")
        // Avoid invoking a real pg_probackup binary even if present.
        .env("PG_PROBACKUP_BIN", "/nonexistent/pg_probackup")
        // Keep child quiet and deterministic unless the test needs output.
        .stdin(Stdio::null());
    cmd
}

/// Phase 8: background-by-default launcher returns after phase-1 "started"
/// and leaves a detached worker running.
#[cfg(unix)]
#[test]
fn daemon_background_returns_after_started_and_leaves_worker_running() {
    let store = tempdir().unwrap();
    let mnt = tempdir().unwrap();
    let diff = tempdir().unwrap();

    let pid_path = diff_pid_path(diff.path());

    let out = mount_cmd(store.path(), mnt.path(), diff.path())
        // Keep worker alive so we can observe it.
        .env("PBKFS_TEST_DAEMON_DELAY_MS", "10000")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("launcher should run");

    assert!(
        out.status.success(),
        "launcher must exit successfully; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    assert!(
        wait_for_path(&pid_path, Duration::from_secs(2)),
        "worker pid file should appear in diff layer"
    );
    let record = pbkfs::cli::pid::read_pid_record(&pid_path).expect("pid record should be valid");
    assert!(
        pbkfs::cli::pid::pid_alive(record.pid),
        "worker pid should be alive"
    );

    // Cleanup: stop the detached worker and remove pid record.
    kill_pid(record.pid, libc::SIGKILL);
    thread::sleep(Duration::from_millis(50));
    let _ = fs::remove_file(&pid_path);
}

/// Phase 8: Ctrl+C during `--wait` cancels waiting only and does not kill worker.
#[cfg(unix)]
#[test]
fn daemon_wait_ctrlc_cancels_without_killing_worker() {
    let store = tempdir().unwrap();
    let mnt = tempdir().unwrap();
    let diff = tempdir().unwrap();

    let pid_path = diff_pid_path(diff.path());

    let mut child = mount_cmd(store.path(), mnt.path(), diff.path())
        .arg("--wait")
        .env("PBKFS_TEST_DAEMON_DELAY_MS", "10000")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("launcher should spawn");

    assert!(
        wait_for_path(&pid_path, Duration::from_secs(2)),
        "worker pid file should appear"
    );

    // Give the launcher a moment to transition into phase-2 waiting.
    thread::sleep(Duration::from_millis(100));
    kill_pid(child.id() as i32, libc::SIGINT);

    let status = child.wait().expect("launcher should exit after SIGINT");
    assert!(
        status.success(),
        "launcher should treat cancellation as success"
    );

    let record = pbkfs::cli::pid::read_pid_record(&pid_path).expect("pid record should be valid");
    assert!(
        pbkfs::cli::pid::pid_alive(record.pid),
        "worker must still be running after launcher Ctrl+C"
    );

    kill_pid(record.pid, libc::SIGKILL);
    thread::sleep(Duration::from_millis(50));
    let _ = fs::remove_file(&pid_path);
}

/// Phase 8: `--timeout` exits but leaves worker running.
#[cfg(unix)]
#[test]
fn daemon_timeout_leaves_worker_running() {
    let store = tempdir().unwrap();
    let mnt = tempdir().unwrap();
    let diff = tempdir().unwrap();

    let pid_path = diff_pid_path(diff.path());

    let out = mount_cmd(store.path(), mnt.path(), diff.path())
        .arg("--timeout")
        .arg("1")
        .env("PBKFS_TEST_DAEMON_DELAY_MS", "10000")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("launcher should run");

    assert!(
        out.status.success(),
        "timeout should not be fatal; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    assert!(
        wait_for_path(&pid_path, Duration::from_secs(2)),
        "worker pid file should appear"
    );
    let record = pbkfs::cli::pid::read_pid_record(&pid_path).expect("pid record should be valid");
    assert!(pbkfs::cli::pid::pid_alive(record.pid));

    kill_pid(record.pid, libc::SIGKILL);
    thread::sleep(Duration::from_millis(50));
    let _ = fs::remove_file(&pid_path);
}

/// Phase 8: if worker dies after phase-1 "started" but before phase-2, launcher reports error.
#[cfg(unix)]
#[test]
fn daemon_wait_reports_worker_death_between_phases() {
    let store = tempdir().unwrap();
    let mnt = tempdir().unwrap();
    let diff = tempdir().unwrap();

    let pid_path = diff_pid_path(diff.path());

    let out = mount_cmd(store.path(), mnt.path(), diff.path())
        .arg("--wait")
        .env("PBKFS_TEST_DAEMON_CRASH_AFTER_STARTED", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("launcher should run");

    assert!(
        !out.status.success(),
        "launcher must fail when worker dies mid-handshake"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("worker exited before reporting mount completion")
            || stderr.contains("mount failed")
            || stderr.contains("pbkfs error"),
        "stderr should mention handshake failure; stderr={stderr}"
    );

    // Cleanup pid record left behind by the intentional crash.
    let _ = fs::remove_file(&pid_path);
}

/// Phase 8: end-to-end `--wait` success path (requires working FUSE).
#[cfg(unix)]
#[test]
fn daemon_wait_succeeds_when_fuse_available() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let mnt = tempdir()?;
    let diff = tempdir()?;

    let base_file = native_path(store.path(), "FULL1", Path::new("data/base.txt"));
    fs::create_dir_all(base_file.parent().unwrap())?;
    fs::write(&base_file, b"from-store")?;
    write_metadata(store.path(), false, None);

    let out = mount_cmd(store.path(), mnt.path(), diff.path())
        .arg("--wait")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("launcher should run");

    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("Permission denied") || stderr.contains("/dev/fuse") {
            eprintln!("skipping daemon wait success test: {stderr}");
            return Ok(());
        }
        anyhow::bail!("unexpected mount failure: {stderr}");
    }

    // PID/stat/log files should be visible via FUSE.
    assert!(mnt.path().join(".pbkfs/worker.pid").exists());
    assert!(mnt.path().join(".pbkfs/pbkfs.log").exists());

    // Cleanup via CLI unmount (signal-based).
    let out = Command::new(pbkfs_bin())
        .arg("unmount")
        .arg("--mnt-path")
        .arg(mnt.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("unmount should run");
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("Permission denied") || stderr.contains("/dev/fuse") {
            eprintln!("skipping daemon unmount in success test: {stderr}");
            return Ok(());
        }
        anyhow::bail!("unexpected unmount failure: {stderr}");
    }

    Ok(())
}

/// Phase 8: `pbkfs stat` integration (requires working FUSE).
#[cfg(unix)]
#[test]
fn stat_command_dumps_and_resets_counters_when_fuse_available() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let mnt = tempdir()?;
    let diff = tempdir()?;

    let base_file = native_path(store.path(), "FULL1", Path::new("data/base.txt"));
    fs::create_dir_all(base_file.parent().unwrap())?;
    fs::write(&base_file, b"from-store")?;
    write_metadata(store.path(), false, None);

    // Use the daemonized worker so PID/stat files exist under `<mnt>/.pbkfs/`.
    let out = mount_cmd(store.path(), mnt.path(), diff.path())
        .arg("--wait")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("launcher should run");
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("Permission denied") || stderr.contains("/dev/fuse") {
            eprintln!("skipping stat integration test: {stderr}");
            return Ok(());
        }
        anyhow::bail!("unexpected mount failure: {stderr}");
    }

    // Generate some traffic to bump counters.
    let base_path = mnt.path().join("data/base.txt");
    let _ = fs::read(&base_path)?;
    fs::write(&base_path, b"from-diff")?;
    assert_eq!(b"from-diff".as_slice(), fs::read(&base_path)?.as_slice());

    let out = Command::new(pbkfs_bin())
        .arg("stat")
        .arg("--mnt-path")
        .arg(mnt.path())
        .arg("--format")
        .arg("json")
        .arg("--counters-reset")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("stat should run");
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("Permission denied") || stderr.contains("/dev/fuse") {
            eprintln!("skipping stat integration test: {stderr}");
            return Ok(());
        }
        anyhow::bail!("unexpected stat failure: {stderr}");
    }

    let v: serde_json::Value = serde_json::from_slice(&out.stdout)?;
    assert!(v.get("overlay").is_some());
    assert!(v.get("fuse").is_some());

    // After reset, immediate second stat should show zeroed "copy-up" counters.
    let out2 = Command::new(pbkfs_bin())
        .arg("stat")
        .arg("--mnt-path")
        .arg(mnt.path())
        .arg("--format")
        .arg("json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("stat should run");
    if !out2.status.success() {
        anyhow::bail!(
            "unexpected stat failure after reset: {}",
            String::from_utf8_lossy(&out2.stderr)
        );
    }

    let v2: serde_json::Value = serde_json::from_slice(&out2.stdout)?;
    let bytes_copied = v2["overlay"]["bytes_copied"].as_u64().unwrap_or(0);
    let blocks_copied = v2["overlay"]["blocks_copied"].as_u64().unwrap_or(0);
    assert_eq!(0, bytes_copied);
    assert_eq!(0, blocks_copied);

    // Cleanup via CLI unmount.
    let out = Command::new(pbkfs_bin())
        .arg("unmount")
        .arg("--mnt-path")
        .arg(mnt.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("unmount should run");
    if !out.status.success() {
        anyhow::bail!(
            "unexpected unmount failure in stat test: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    Ok(())
}

/// Phase 8: force-unmount a busy mount (requires working FUSE).
#[cfg(unix)]
#[test]
fn unmount_force_detaches_busy_mount_when_fuse_available() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let mnt = tempdir()?;
    let diff = tempdir()?;

    let base_file = native_path(store.path(), "FULL1", Path::new("data/base.txt"));
    fs::create_dir_all(base_file.parent().unwrap())?;
    fs::write(&base_file, b"from-store")?;
    write_metadata(store.path(), false, None);

    // Ensure tests don't depend on a real pg_probackup binary even if one is available in PATH.
    let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some("/nonexistent/pg_probackup"));

    let mut ctx = match pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(mnt.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: false,
        no_wal: false,
        ..Default::default()
    }) {
        Ok(ctx) => ctx,
        Err(err) => {
            if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
                if io_err.kind() == std::io::ErrorKind::PermissionDenied {
                    eprintln!("skipping force-unmount test: {io_err}");
                    return Ok(());
                }
            }
            if err.to_string().contains("Permission denied") || err.to_string().contains("/dev/fuse")
            {
                eprintln!("skipping force-unmount test: {err}");
                return Ok(());
            }
            return Err(err);
        }
    };

    // Make the mount definitely "busy" for a regular unmount by keeping a
    // separate process working directory inside the mountpoint.
    let mut busy_proc = Command::new("sleep")
        .arg("60")
        .current_dir(mnt.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    let out = Command::new(pbkfs_bin())
        .arg("unmount")
        .arg("--mnt-path")
        .arg(mnt.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("unmount should run");
    assert!(
        !out.status.success(),
        "regular unmount should fail while mount is busy"
    );
    let stderr = String::from_utf8_lossy(&out.stderr).to_lowercase();
    assert!(
        stderr.contains("busy"),
        "expected busy error, got: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let out = Command::new(pbkfs_bin())
        .arg("unmount")
        .arg("--mnt-path")
        .arg(mnt.path())
        .arg("--force")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("force unmount should run");
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("Permission denied") || stderr.contains("/dev/fuse") {
            eprintln!("skipping force-unmount test: {stderr}");
            return Ok(());
        }
        anyhow::bail!("unexpected force-unmount failure: {stderr}");
    }

    // Terminate the helper process holding CWD inside the mountpoint before
    // joining the background FUSE session.
    let _ = busy_proc.kill();
    let _ = busy_proc.wait();

    // Join the background session thread now that the mount is gone.
    if let Some(handle) = ctx.fuse_handle.take() {
        handle.unmount();
    }

    Ok(())
}

/// Phase 8: stale PID + broken endpoint fallback (requires working FUSE).
#[cfg(unix)]
#[test]
fn unmount_falls_back_when_pid_is_stale_when_fuse_available() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let mnt = tempdir()?;
    let diff = tempdir()?;

    let base_file = native_path(store.path(), "FULL1", Path::new("data/base.txt"));
    fs::create_dir_all(base_file.parent().unwrap())?;
    fs::write(&base_file, b"from-store")?;
    write_metadata(store.path(), false, None);

    let out = mount_cmd(store.path(), mnt.path(), diff.path())
        .arg("--wait")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("launcher should run");
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("Permission denied") || stderr.contains("/dev/fuse") {
            eprintln!("skipping stale-pid unmount test: {stderr}");
            return Ok(());
        }
        anyhow::bail!("unexpected mount failure: {stderr}");
    }

    let pid_path = mnt.path().join(".pbkfs/worker.pid");
    let record = pbkfs::cli::pid::read_pid_record(&pid_path)?;

    // Kill the worker abruptly; pid file becomes stale.
    kill_pid(record.pid, libc::SIGKILL);
    thread::sleep(Duration::from_millis(100));
    assert!(!pbkfs::cli::pid::pid_alive(record.pid));

    let out = Command::new(pbkfs_bin())
        .arg("unmount")
        .arg("--mnt-path")
        .arg(mnt.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("unmount should run");
    if !out.status.success() {
        // Busy/broken endpoints may require --force; validate it works.
        let out2 = Command::new(pbkfs_bin())
            .arg("unmount")
            .arg("--mnt-path")
            .arg(mnt.path())
            .arg("--force")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .expect("force unmount should run");
        if !out2.status.success() {
            anyhow::bail!(
                "unmount failed even with --force: {}",
                String::from_utf8_lossy(&out2.stderr)
            );
        }
    }

    Ok(())
}
