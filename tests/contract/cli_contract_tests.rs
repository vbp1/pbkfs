//! CLI contract tests for pbkfs argument validation.

use pbkfs::Error;
use tempfile::tempdir;

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
