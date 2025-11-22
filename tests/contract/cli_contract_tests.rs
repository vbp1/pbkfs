//! CLI contract tests for pbkfs argument validation.

use pbkfs::{binding::DiffDir, cli::mount::MountArgs, binding::BindingRecord, Error};
use tempfile::tempdir;
use uuid::Uuid;

fn expect_error(args: &[&str], expected: Error) {
    let err = pbkfs::run(args.iter().copied()).expect_err("command should fail");
    let actual = err
        .downcast_ref::<Error>()
        .unwrap_or_else(|| panic!("unexpected error type: {err:?}"));
    match expected {
        Error::Cli(ref expected_msg) => {
            assert!(matches!(actual, Error::Cli(msg) if msg == expected_msg));
        }
        _ => {
            assert_eq!(
                std::mem::discriminant(actual),
                std::mem::discriminant(&expected)
            );
        }
    }
}

#[test]
fn mount_requires_target_and_store_paths() {
    // Missing all required paths
    expect_error(
        &["pbkfs", "mount"],
        Error::Cli("pbk_store is required".into()),
    );

    // Non-empty target directory should fail fast
    let store = tempdir().unwrap();
    let target = tempdir().unwrap();
    let diff = tempdir().unwrap();
    std::fs::write(target.path().join("keep.txt"), b"occupied").unwrap();

    let err = pbkfs::run([
        "pbkfs",
        "mount",
        "-B",
        store.path().to_str().unwrap(),
        "--mnt-path",
        target.path().to_str().unwrap(),
        "--diff-dir",
        diff.path().to_str().unwrap(),
        "--instance",
        "main",
        "-i",
        "FULL1",
    ])
    .expect_err("non-empty target must fail");

    let actual = err
        .downcast_ref::<Error>()
        .expect("should downcast to pbkfs::Error");
    assert!(matches!(actual, Error::TargetNotEmpty(_)));
}

#[test]
fn unmount_requires_mnt_path() {
    expect_error(
        &["pbkfs", "unmount"],
        Error::Cli("mnt_path is required".into()),
    );

    // Non-existent mount path should also error
    let err = pbkfs::run(["pbkfs", "unmount", "--mnt-path", "/no/such/path"])
        .expect_err("invalid path should fail");
    let actual = err
        .downcast_ref::<Error>()
        .expect("should downcast to pbkfs::Error");
    assert!(matches!(actual, Error::InvalidTargetDir(_)));
}

fn write_metadata(store: &std::path::Path) {
    let metadata = serde_json::json!([
        {
            "instance": "main",
            "backups": [
                {
                    "id": "FULL1",
                    "parent-backup-id": null,
                    "backup-mode": "FULL",
                    "status": "OK",
                    "compress-alg": "none",
                    "compress-level": null,
                    "start-time": "2024-01-01T00:00:00Z",
                    "data-bytes": 512u64,
                    "program-version": "2.6.0"
                }
            ]
        }
    ]);

    std::fs::write(
        store.join("backups.json"),
        serde_json::to_vec_pretty(&metadata).unwrap(),
    )
    .unwrap();
}

#[test]
fn mount_reuses_binding_with_diff_only_arguments() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let target = tempdir()?;
    let diff = tempdir()?;

    // Base backup content and metadata
    let base_file = store
        .path()
        .join("FULL1")
        .join("database")
        .join("data/base.txt");
    std::fs::create_dir_all(base_file.parent().unwrap())?;
    std::fs::write(&base_file, b"from-store")?;
    write_metadata(store.path());

    // Existing binding record ties diff to instance/backup.
    let diff_dir = DiffDir::new(diff.path())?;
    let binding = BindingRecord::new(
        "main",
        "FULL1",
        store.path(),
        target.path(),
        42,
        "host",
        "0.1.0",
    );
    binding.write_to_diff(&diff_dir)?;

    // Prior diff content should be visible after remount.
    let diff_data = diff.path().join("data").join("data/base.txt");
    std::fs::create_dir_all(diff_data.parent().unwrap())?;
    std::fs::write(&diff_data, b"from-diff")?;

    let ctx = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: None,
        backup_id: None,
    })?;

    assert_eq!(binding.binding_id, ctx.binding.binding_id);
    assert_eq!("main", ctx.binding.instance_name);
    assert_eq!("FULL1", ctx.chain.target_backup_id);

    let contents = ctx
        .overlay
        .read(std::path::Path::new("data/base.txt"))?
        .expect("diff content should be visible");
    assert_eq!(b"from-diff", contents.as_slice());

    if let Some(handle) = ctx.fuse_handle {
        handle.unmount();
    }
    let _ = std::fs::remove_file(diff.path().join(pbkfs::binding::LOCK_FILE));

    Ok(())
}

#[test]
fn mount_rejects_binding_mismatch_when_instance_differs() {
    let store = tempdir().unwrap();
    let target = tempdir().unwrap();
    let diff = tempdir().unwrap();

    write_metadata(store.path());
    let diff_dir = DiffDir::new(diff.path()).unwrap();
    let binding = BindingRecord::new(
        "main",
        "FULL1",
        store.path(),
        target.path(),
        7,
        "host",
        "0.1.0",
    );
    binding.write_to_diff(&diff_dir).unwrap();

    let err = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("other".into()),
        backup_id: Some("FULL1".into()),
    })
    .expect_err("mismatched instance should fail");

    let actual = err
        .downcast_ref::<Error>()
        .expect("should downcast to pbkfs::Error");
    assert!(matches!(actual, Error::BindingViolation { .. }));
}

#[test]
fn mount_rejects_binding_mismatch_when_backup_differs() {
    let store = tempdir().unwrap();
    let target = tempdir().unwrap();
    let diff = tempdir().unwrap();

    write_metadata(store.path());
    let diff_dir = DiffDir::new(diff.path()).unwrap();
    let binding = BindingRecord::new(
        "main",
        "FULL1",
        store.path(),
        target.path(),
        7,
        "host",
        "0.1.0",
    );
    binding.write_to_diff(&diff_dir).unwrap();

    let err = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("INC1".into()),
    })
    .expect_err("mismatched backup should fail");

    let actual = err
        .downcast_ref::<Error>()
        .expect("should downcast to pbkfs::Error");
    assert!(matches!(actual, Error::BindingViolation { .. }));
}

#[test]
fn mount_rejects_binding_mismatch_when_store_differs() {
    let store_expected = tempdir().unwrap();
    let store_actual = tempdir().unwrap();
    let target = tempdir().unwrap();
    let diff = tempdir().unwrap();

    write_metadata(store_expected.path());
    write_metadata(store_actual.path());

    let diff_dir = DiffDir::new(diff.path()).unwrap();
    let binding = BindingRecord::new(
        "main",
        "FULL1",
        store_expected.path(),
        target.path(),
        9,
        "host",
        "0.1.0",
    );
    binding.write_to_diff(&diff_dir).unwrap();

    let err = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store_actual.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: None,
        backup_id: None,
    })
    .expect_err("store path mismatch should fail");

    let actual = err
        .downcast_ref::<Error>()
        .expect("should downcast to pbkfs::Error");
    assert!(matches!(actual, Error::BindingViolation { .. }));
}

#[test]
fn cleanup_requires_diff_dir() {
    expect_error(
        &["pbkfs", "cleanup"],
        Error::Cli("diff_dir is required".into()),
    );
}

#[test]
fn cleanup_removes_binding_and_diff_data_when_idle() -> pbkfs::Result<()> {
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

    let diff_file = diff.path().join("data/db/heap");
    std::fs::create_dir_all(diff_file.parent().unwrap())?;
    std::fs::write(&diff_file, b"dirty-bytes")?;

    pbkfs::run([
        "pbkfs",
        "cleanup",
        "--diff-dir",
        diff.path().to_str().unwrap(),
    ])?;

    assert!(!diff_dir.binding_path().exists());
    assert!(!diff_dir.lock_path().exists());
    assert!(!diff_file.exists());

    Ok(())
}

#[test]
fn cleanup_rejects_active_mount_without_force() {
    let diff = tempdir().unwrap();
    let store = tempdir().unwrap();
    let target = tempdir().unwrap();

    let diff_dir = DiffDir::new(diff.path()).unwrap();
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
    binding.write_to_diff(&diff_dir).unwrap();
    diff_dir.write_lock(Uuid::new_v4()).unwrap();

    let err = pbkfs::run([
        "pbkfs",
        "cleanup",
        "--diff-dir",
        diff.path().to_str().unwrap(),
    ])
    .expect_err("active mount should be rejected");

    let actual = err
        .downcast_ref::<Error>()
        .expect("should downcast to pbkfs::Error");
    assert!(matches!(actual, Error::BindingInUse(p) if *p == pid));
}

#[test]
fn cleanup_force_overrides_active_lock_and_cleans() -> pbkfs::Result<()> {
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

    let diff_file = diff.path().join("data/db/heap");
    std::fs::create_dir_all(diff_file.parent().unwrap())?;
    std::fs::write(&diff_file, b"dirty")?;

    pbkfs::run([
        "pbkfs",
        "cleanup",
        "--diff-dir",
        diff.path().to_str().unwrap(),
        "--force",
    ])?;

    assert!(!diff_dir.binding_path().exists());
    assert!(!diff_dir.lock_path().exists());
    assert!(!diff_file.exists());

    Ok(())
}
