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
    let metadata = serde_json::json!({
        "instance_name": "main",
        "version_pg_probackup": "2.6.0",
        "backups": [
            {
                "backup_id": "FULL1",
                "instance_name": "main",
                "backup_type": "Full",
                "parent_id": null,
                "start_time": "2024-01-01T00:00:00Z",
                "status": "Ok",
                "compressed": true,
                "compression": { "algorithm": "zstd", "level": 6 },
                "size_bytes": 1024u64,
                "checksum_state": "Verified"
            }
        ]
    });
    write_backups_json(store.path(), metadata);

    let loaded = BackupStore::load_from_pg_probackup(store.path(), "main")?;
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
    let metadata = serde_json::json!({
        "instance_name": "main",
        "version_pg_probackup": "2.6.0",
        "backups": [
            {
                "backup_id": "FULL1",
                "instance_name": "main",
                "backup_type": "Full",
                "parent_id": null,
                "start_time": "2024-01-01T00:00:00Z",
                "status": "Ok",
                "compressed": true,
                "compression": { "algorithm": "pglz" },
                "size_bytes": 1024u64,
                "checksum_state": "Verified"
            }
        ]
    });
    write_backups_json(store.path(), metadata);

    let err = BackupStore::load_from_pg_probackup(store.path(), "main").unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.to_lowercase().contains("unsupported"));
    assert!(msg.contains("pglz"));
}
