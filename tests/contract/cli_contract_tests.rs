//! CLI contract tests for pbkfs argument validation.

use pbkfs::{binding::BindingRecord, binding::DiffDir, cli::mount::MountArgs, Error};
use tempfile::tempdir;
use uuid::Uuid;

use parking_lot::ReentrantMutexGuard;
/// RAII env guard serialized with the global pbkfs env_lock to avoid
/// cross-test races when modifying environment variables (e.g. PG_PROBACKUP_BIN).
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

fn mount_guarded(args: MountArgs) -> pbkfs::Result<pbkfs::cli::mount::MountContext> {
    let _guard = pbkfs::env_lock().lock();
    pbkfs::cli::mount::mount(args)
}

fn native_path(
    store: &std::path::Path,
    backup_id: &str,
    rel: &std::path::Path,
) -> std::path::PathBuf {
    store
        .join("backups")
        .join("main")
        .join(backup_id)
        .join("database")
        .join(rel)
}

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
fn mount_daemon_flags_parse_and_defaults() -> pbkfs::Result<()> {
    use pbkfs::cli::{mount::LogSink, mount::MountMode, mount::WaitMode, Command};

    let args = pbkfs::cli::parse_args([
        "pbkfs", "mount", "-B", "/store", "-D", "/mnt", "-d", "/diff", "-I", "main", "-i", "FULL1",
    ])?;

    let Command::Mount(mount) = args.command else {
        panic!("expected mount command");
    };

    assert_eq!(mount.mode(), MountMode::Background);
    assert_eq!(mount.wait_mode(), WaitMode::StartOnly);
    assert_eq!(mount.log_sink.unwrap_or_default(), LogSink::File);
    assert!(!mount.debug);

    let defaults = mount.daemon_defaults()?;
    assert_eq!(
        defaults.pid_visible,
        std::path::PathBuf::from("/mnt/.pbkfs/worker.pid")
    );
    assert_eq!(
        defaults.stat_visible,
        std::path::PathBuf::from("/mnt/.pbkfs/stat")
    );
    assert_eq!(
        defaults.log_visible,
        std::path::PathBuf::from("/mnt/.pbkfs/pbkfs.log")
    );

    Ok(())
}

#[test]
fn mount_parses_logging_and_pid_overrides() -> pbkfs::Result<()> {
    use pbkfs::cli::{mount::LogFormatArg, mount::LogSink, Command};

    let args = pbkfs::cli::parse_args([
        "pbkfs",
        "mount",
        "-B",
        "/store",
        "--mnt-path",
        "/mnt",
        "--diff-dir",
        "/diff",
        "--instance",
        "main",
        "--backup-id",
        "FULL1",
        "--log-format",
        "json",
        "--log-sink",
        "journald",
        "--log-file",
        "/var/log/pbkfs.log",
        "--pid-file",
        ".pbkfs/custom.pid",
        "--debug",
    ])?;

    let Command::Mount(mount) = args.command else {
        panic!("expected mount command");
    };

    assert_eq!(mount.log_format, LogFormatArg::Json);
    assert_eq!(mount.log_sink, Some(LogSink::Journald));
    assert_eq!(
        mount.log_file,
        Some(std::path::PathBuf::from("/var/log/pbkfs.log"))
    );
    assert_eq!(
        mount.pid_file,
        Some(std::path::PathBuf::from(".pbkfs/custom.pid"))
    );
    assert!(mount.debug);

    let defaults = mount.daemon_defaults()?;
    assert_eq!(
        defaults.pid_visible,
        std::path::PathBuf::from("/mnt/.pbkfs/custom.pid")
    );
    Ok(())
}

#[test]
fn mount_wait_timeout_priority_rules() -> pbkfs::Result<()> {
    use pbkfs::cli::{mount::WaitMode, Command};

    let args = pbkfs::cli::parse_args([
        "pbkfs",
        "mount",
        "-B",
        "/store",
        "--mnt-path",
        "/mnt",
        "--diff-dir",
        "/diff",
        "--instance",
        "main",
        "--backup-id",
        "FULL1",
        "--wait",
        "--timeout",
        "5",
    ])?;

    let Command::Mount(mount) = args.command else {
        panic!("expected mount command");
    };

    assert_eq!(
        mount.wait_mode(),
        WaitMode::Timeout(std::time::Duration::from_secs(5))
    );
    Ok(())
}

#[test]
fn mount_console_ignores_wait_semantics() -> pbkfs::Result<()> {
    use pbkfs::cli::{mount::MountMode, mount::WaitMode, Command};

    let args = pbkfs::cli::parse_args([
        "pbkfs",
        "mount",
        "-B",
        "/store",
        "--mnt-path",
        "/mnt",
        "--diff-dir",
        "/diff",
        "--instance",
        "main",
        "--backup-id",
        "FULL1",
        "--console",
        "--wait",
        "--timeout",
        "1",
    ])?;

    let Command::Mount(mount) = args.command else {
        panic!("expected mount command");
    };

    assert_eq!(mount.mode(), MountMode::Console);
    assert_eq!(mount.wait_mode(), WaitMode::Foreground);
    Ok(())
}

#[test]
fn unmount_force_flag_and_busy_classification() -> pbkfs::Result<()> {
    use pbkfs::cli::{unmount::UnmountFailureKind, Command};

    let args = pbkfs::cli::parse_args(["pbkfs", "unmount", "--mnt-path", "/mnt", "--force"])?;
    let Command::Unmount(unmount) = args.command else {
        panic!("expected unmount command");
    };
    assert!(unmount.force);

    assert_eq!(
        pbkfs::cli::unmount::classify_unmount_stderr("umount: /mnt: target is busy\n"),
        UnmountFailureKind::Busy
    );
    assert_eq!(
        pbkfs::cli::unmount::classify_unmount_stderr("umount: /mnt: not mounted.\n"),
        UnmountFailureKind::NotMounted
    );
    assert_eq!(
        pbkfs::cli::unmount::classify_unmount_stderr("some other error\n"),
        UnmountFailureKind::Other
    );
    Ok(())
}

#[test]
fn unmount_help_mentions_force_flag() -> pbkfs::Result<()> {
    let mut cmd = pbkfs::cli::clap_command();
    let unmount = cmd
        .find_subcommand_mut("unmount")
        .expect("unmount subcommand exists");
    let help = unmount.render_long_help().to_string();
    assert!(help.contains("--force"));
    Ok(())
}

#[test]
fn stat_command_parses_and_flags_work() -> pbkfs::Result<()> {
    use pbkfs::cli::{stat::StatFormat, Command};

    let args = pbkfs::cli::parse_args(["pbkfs", "stat", "--mnt-path", "/mnt"])?;
    let Command::Stat(stat) = args.command else {
        panic!("expected stat command");
    };
    assert_eq!(stat.format, StatFormat::Text);
    assert!(!stat.counters_reset);

    let args = pbkfs::cli::parse_args([
        "pbkfs",
        "stat",
        "--mnt-path",
        "/mnt",
        "--format",
        "json",
        "--counters-reset",
    ])?;
    let Command::Stat(stat) = args.command else {
        panic!("expected stat command");
    };
    assert_eq!(stat.format, StatFormat::Json);
    assert!(stat.counters_reset);

    Ok(())
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
fn mount_errors_for_unknown_store_layout() {
    let store = tempdir().unwrap();
    let target = tempdir().unwrap();
    let diff = tempdir().unwrap();

    // No backups/ directory and no backups.json file.
    let err = mount_guarded(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: false,
        no_wal: false,
        console: false,
        wait: false,
        timeout: None,
        log_file: None,
        log_format: pbkfs::cli::mount::LogFormatArg::Text,
        log_sink: None,
        pid_file: None,
        debug: false,
    })
    .expect_err("non-pg_probackup layout should fail");

    let actual = err.downcast_ref::<Error>().expect("should be pbkfs::Error");
    assert!(matches!(actual, Error::InvalidStoreLayout(_)));
}

#[test]
fn mount_sets_and_enforces_no_wal_binding_flag() {
    let store = tempdir().unwrap();
    let target = tempdir().unwrap();
    let diff = tempdir().unwrap();

    // Minimal backup contents.
    let base_file = native_path(store.path(), "FULL1", std::path::Path::new("data/base.txt"));
    std::fs::create_dir_all(base_file.parent().unwrap()).unwrap();
    std::fs::write(&base_file, b"base").unwrap();
    write_metadata(store.path());

    let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some("/nonexistent/pg_probackup"));
    let first = mount_guarded(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: false,
        no_wal: true,
        console: false,
        wait: false,
        timeout: None,
        log_file: None,
        log_format: pbkfs::cli::mount::LogFormatArg::Text,
        log_sink: None,
        pid_file: None,
        debug: false,
    })
    .expect("first mount with --no-wal should succeed");

    let binding =
        BindingRecord::load_from_diff(&DiffDir::new(diff.path()).unwrap()).expect("binding exists");
    assert!(
        binding.no_wal_used,
        "binding must record prior --no-wal usage"
    );

    if let Some(handle) = first.fuse_handle {
        handle.unmount();
    }

    let err = mount_guarded(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: false,
        no_wal: false,
        console: false,
        wait: false,
        timeout: None,
        log_file: None,
        log_format: pbkfs::cli::mount::LogFormatArg::Text,
        log_sink: None,
        pid_file: None,
        debug: false,
    })
    .expect_err("reusing diff after --no-wal must fail");

    let actual = err.downcast_ref::<Error>().expect("should be pbkfs::Error");
    assert!(matches!(actual, Error::Cli(msg) if msg.contains("previously mounted with --no-wal")));
}

#[test]
fn mount_prefers_native_layout_over_json_when_both_exist() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let target = tempdir()?;
    let diff = tempdir()?;

    // Native layout data
    let base_file = native_path(store.path(), "FULL1", std::path::Path::new("data/base.txt"));
    std::fs::create_dir_all(base_file.parent().unwrap())?;
    std::fs::write(&base_file, b"native")?;

    // Intentionally write invalid backups.json that would fail if used.
    std::fs::write(store.path().join("backups.json"), b"not json")?;

    // Provide a stub pg_probackup binary via env override.
    let bin_dir = tempdir()?;
    let script_path = bin_dir.path().join("pg_probackup");
    std::fs::write(
        &script_path,
        r#"#!/bin/bash
cat <<'JSON'
[
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
        "start-time": "2024-01-01 00:00:00+00",
        "data-bytes": 512,
        "program-version": "2.6.0"
      }
    ]
  }
]
JSON
"#,
    )?;
    let mut perms = std::fs::metadata(&script_path)?.permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o755);
        std::fs::set_permissions(&script_path, perms)?;
    }
    {
        let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some(script_path.to_str().unwrap()));

        let ctx = pbkfs::cli::mount::mount(MountArgs {
            pbk_store: Some(store.path().to_path_buf()),
            mnt_path: Some(target.path().to_path_buf()),
            diff_dir: Some(diff.path().to_path_buf()),
            instance: Some("main".into()),
            backup_id: Some("FULL1".into()),
            force: false,
            perf_unsafe: false,
            no_wal: false,
            console: false,
            wait: false,
            timeout: None,
            log_file: None,
            log_format: pbkfs::cli::mount::LogFormatArg::Text,
            log_sink: None,
            pid_file: None,
            debug: false,
        })?;

        let contents = ctx
            .overlay
            .read(std::path::Path::new("data/base.txt"))?
            .expect("native data should be readable");
        assert_eq!(b"native", contents.as_slice());

        if let Some(handle) = ctx.fuse_handle {
            handle.unmount();
        }
        let _ = std::fs::remove_file(diff.path().join(pbkfs::binding::LOCK_FILE));
    }

    Ok(())
}

#[cfg(unix)]
#[test]
fn chmod_on_mounted_path_inval_caches() -> pbkfs::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let store = tempdir()?;
    let target = tempdir()?;
    let diff = tempdir()?;

    // Native layout data: single file in backup FULL1.
    let base_file = native_path(store.path(), "FULL1", std::path::Path::new("data/base.txt"));
    std::fs::create_dir_all(base_file.parent().unwrap())?;
    std::fs::write(&base_file, b"native")?;

    // Provide a stub pg_probackup binary via env override to satisfy metadata lookup.
    let bin_dir = tempdir()?;
    let script_path = bin_dir.path().join("pg_probackup");
    std::fs::write(
        &script_path,
        r#"#!/bin/bash
cat <<'JSON'
[
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
        "start-time": "2024-01-01 00:00:00+00",
        "data-bytes": 512,
        "program-version": "2.6.0"
      }
    ]
  }
]
JSON
"#,
    )?;
    let mut perms = std::fs::metadata(&script_path)?.permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms)?;

    let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some(script_path.to_str().unwrap()));

    let ctx = pbkfs::cli::mount::mount(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: false,
        no_wal: false,
        console: false,
        wait: false,
        timeout: None,
        log_file: None,
        log_format: pbkfs::cli::mount::LogFormatArg::Text,
        log_sink: None,
        pid_file: None,
        debug: false,
    })?;

    let mounted_file = target.path().join("data/base.txt");
    let meta_before = std::fs::metadata(&mounted_file)?;
    let before_mode = meta_before.permissions().mode() & 0o777;

    // Change mode through the mounted path; cache should be invalidated.
    let mut new_perms = meta_before.permissions();
    new_perms.set_mode(0o700);
    std::fs::set_permissions(&mounted_file, new_perms)?;

    let meta_after = std::fs::metadata(&mounted_file)?;
    let after_mode = meta_after.permissions().mode() & 0o777;

    assert_ne!(before_mode, after_mode, "mode should change");
    assert_eq!(0o700, after_mode, "mode should reflect chmod");

    if let Some(handle) = ctx.fuse_handle {
        handle.unmount();
    }
    let _ = std::fs::remove_file(diff.path().join(pbkfs::binding::LOCK_FILE));

    Ok(())
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
    let data_root = native_path(store, "FULL1", std::path::Path::new(""));
    std::fs::create_dir_all(&data_root).unwrap();

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

fn write_corrupt_metadata(store: &std::path::Path) {
    let data_root = native_path(store, "FULL1", std::path::Path::new(""));
    std::fs::create_dir_all(&data_root).unwrap();

    let metadata = serde_json::json!([
        {
            "instance": "main",
            "backups": [
                {
                    "id": "FULL1",
                    "parent-backup-id": null,
                    "backup-mode": "FULL",
                    "status": "CORRUPT",
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
    let base_file = native_path(store.path(), "FULL1", std::path::Path::new("data/base.txt"));
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

    let ctx = {
        let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some("/nonexistent/pg_probackup"));

        pbkfs::cli::mount::mount(MountArgs {
            pbk_store: Some(store.path().to_path_buf()),
            mnt_path: Some(target.path().to_path_buf()),
            diff_dir: Some(diff.path().to_path_buf()),
            instance: None,
            backup_id: None,
            force: false,
            perf_unsafe: false,
            no_wal: false,
            console: false,
            wait: false,
            timeout: None,
            log_file: None,
            log_format: pbkfs::cli::mount::LogFormatArg::Text,
            log_sink: None,
            pid_file: None,
            debug: false,
        })?
    };

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

    let err = mount_guarded(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("other".into()),
        backup_id: Some("FULL1".into()),
        force: false,
        perf_unsafe: false,
        no_wal: false,
        console: false,
        wait: false,
        timeout: None,
        log_file: None,
        log_format: pbkfs::cli::mount::LogFormatArg::Text,
        log_sink: None,
        pid_file: None,
        debug: false,
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

    let err = mount_guarded(MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("INC1".into()),
        force: false,
        perf_unsafe: false,
        no_wal: false,
        console: false,
        wait: false,
        timeout: None,
        log_file: None,
        log_format: pbkfs::cli::mount::LogFormatArg::Text,
        log_sink: None,
        pid_file: None,
        debug: false,
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

    let err = mount_guarded(MountArgs {
        pbk_store: Some(store_actual.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: None,
        backup_id: None,
        force: false,
        perf_unsafe: false,
        no_wal: false,
        console: false,
        wait: false,
        timeout: None,
        log_file: None,
        log_format: pbkfs::cli::mount::LogFormatArg::Text,
        log_sink: None,
        pid_file: None,
        debug: false,
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

#[test]
fn mount_rejects_corrupt_chain_without_force() {
    let store = tempdir().unwrap();
    let target = tempdir().unwrap();
    let diff = tempdir().unwrap();

    let base_file = native_path(store.path(), "FULL1", std::path::Path::new("data/base.txt"));
    std::fs::create_dir_all(base_file.parent().unwrap()).unwrap();
    std::fs::write(&base_file, b"data").unwrap();
    write_corrupt_metadata(store.path());

    let err = {
        let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some("/nonexistent/pg_probackup"));

        pbkfs::cli::mount::mount(MountArgs {
            pbk_store: Some(store.path().to_path_buf()),
            mnt_path: Some(target.path().to_path_buf()),
            diff_dir: Some(diff.path().to_path_buf()),
            instance: Some("main".into()),
            backup_id: Some("FULL1".into()),
            force: false,
            perf_unsafe: false,
            no_wal: false,
            console: false,
            wait: false,
            timeout: None,
            log_file: None,
            log_format: pbkfs::cli::mount::LogFormatArg::Text,
            log_sink: None,
            pid_file: None,
            debug: false,
        })
        .expect_err("corrupt chain should fail without --force")
    };

    let actual = err
        .downcast_ref::<Error>()
        .expect("should downcast to pbkfs::Error");
    assert!(matches!(actual, Error::Cli(msg) if msg.contains("corrupted")));
}

#[test]
fn mount_allows_corrupt_chain_with_force() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let target = tempdir()?;
    let diff = tempdir()?;

    let base_file = native_path(store.path(), "FULL1", std::path::Path::new("data/base.txt"));
    std::fs::create_dir_all(base_file.parent().unwrap())?;
    std::fs::write(&base_file, b"data")?;
    write_corrupt_metadata(store.path());

    let ctx = {
        let _env = EnvGuard::new("PG_PROBACKUP_BIN", Some("/nonexistent/pg_probackup"));

        pbkfs::cli::mount::mount(MountArgs {
            pbk_store: Some(store.path().to_path_buf()),
            mnt_path: Some(target.path().to_path_buf()),
            diff_dir: Some(diff.path().to_path_buf()),
            instance: Some("main".into()),
            backup_id: Some("FULL1".into()),
            force: true,
            perf_unsafe: false,
            no_wal: false,
            console: false,
            wait: false,
            timeout: None,
            log_file: None,
            log_format: pbkfs::cli::mount::LogFormatArg::Text,
            log_sink: None,
            pid_file: None,
            debug: false,
        })?
    };

    assert_eq!("FULL1", ctx.chain.target_backup_id);

    if let Some(handle) = ctx.fuse_handle {
        handle.unmount();
    }
    let _ = std::fs::remove_file(diff.path().join(pbkfs::binding::LOCK_FILE));

    Ok(())
}
