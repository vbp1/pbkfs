use std::fs;

use pbkfs::backup::metadata::{BackupStore, CompressionAlgorithm};
use tempfile::tempdir;

fn write_backups_json(dir: &std::path::Path, body: serde_json::Value) {
    fs::write(
        dir.join("backups.json"),
        serde_json::to_vec_pretty(&body).unwrap(),
    )
    .unwrap();
}

#[test]
fn loads_compression_algorithm_and_level() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let metadata = serde_json::json!([
        {
            "instance": "main",
            "backups": [
                {
                    "id": "FULL1",
                    "parent-backup-id": null,
                    "backup-mode": "FULL",
                    "status": "OK",
                    "compress-alg": "zstd",
                    "compress-level": 6,
                    "start-time": "2024-01-01T00:00:00Z",
                    "data-bytes": 1024u64,
                    "program-version": "2.6.0"
                }
            ]
        }
    ]);
    let json_path = store.path().join("backups.json");
    write_backups_json(store.path(), metadata);

    let loaded = BackupStore::load_from_json_file(store.path(), &json_path, "main")?;
    let backup = loaded.find_backup("FULL1").expect("backup present");
    assert!(backup.is_compressed());
    assert_eq!(
        Some(CompressionAlgorithm::Zstd),
        backup.compression_algorithm()
    );
    assert_eq!(Some(6), backup.compression_level());

    Ok(())
}

#[test]
fn rejects_unsupported_compression_algorithm() {
    let store = tempdir().unwrap();
    let metadata = serde_json::json!([
        {
            "instance": "main",
            "backups": [
                {
                    "id": "FULL1",
                    "parent-backup-id": null,
                    "backup-mode": "FULL",
                    "status": "OK",
                    "compress-alg": "pglz",
                    "start-time": "2024-01-01T00:00:00Z",
                    "data-bytes": 1024u64,
                    "program-version": "2.6.0"
                }
            ]
        }
    ]);
    let json_path = store.path().join("backups.json");
    write_backups_json(store.path(), metadata);

    let err = BackupStore::load_from_json_file(store.path(), &json_path, "main").unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.to_lowercase().contains("unsupported"));
    assert!(msg.contains("pglz"));
}

#[test]
#[ignore] // Only runs if pg_probackup is available in PATH
fn invokes_pg_probackup_directly() {
    // This test requires pg_probackup to be installed and configured.
    // It verifies that BackupStore::load_from_pg_probackup actually invokes
    // the pg_probackup binary instead of reading a static file.

    let store = tempdir().unwrap();

    // Without backups.json file, the old implementation would fail with file not found.
    // The new implementation should invoke pg_probackup (which will fail in test environment).
    let result = BackupStore::load_from_pg_probackup(store.path(), "main");

    // We expect this to fail because:
    // 1. pg_probackup might not be installed, or
    // 2. The test store is not a valid pg_probackup backup catalog, or
    // 3. There might be permission issues
    // Either way, we should NOT get "backups.json: No such file or directory"
    assert!(result.is_err());
    let err_msg = format!("{:#}", result.unwrap_err());

    // Should NOT be looking for backups.json file
    assert!(
        !err_msg.contains("backups.json"),
        "Should not mention backups.json file, got: {err_msg}"
    );

    // Should mention pg_probackup or be a system error (permission, etc)
    let is_valid_error = err_msg.contains("pg_probackup")
        || err_msg.contains("PATH")
        || err_msg.contains("Permission denied")
        || err_msg.contains("exit code");

    assert!(
        is_valid_error,
        "Expected pg_probackup or system-related error, got: {err_msg}"
    );
}
