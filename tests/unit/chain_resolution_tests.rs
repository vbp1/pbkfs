use std::fs;

use pbkfs::backup::chain::{BackupChain, ChainIntegrity, CompressionMix};
use pbkfs::backup::metadata::{
    BackupMetadata, BackupStatus, BackupStore, BackupType, ChecksumState, Compression,
    CompressionAlgorithm, StoreLayout,
};
use tempfile::tempdir;

fn meta(
    id: &str,
    parent: Option<&str>,
    backup_type: BackupType,
    compressed: bool,
    compression_algo: Option<CompressionAlgorithm>,
) -> BackupMetadata {
    meta_with_status(
        id,
        parent,
        backup_type,
        compressed,
        compression_algo,
        BackupStatus::Ok,
    )
}

fn meta_with_status(
    id: &str,
    parent: Option<&str>,
    backup_type: BackupType,
    compressed: bool,
    compression_algo: Option<CompressionAlgorithm>,
    status: BackupStatus,
) -> BackupMetadata {
    let compression = compressed.then(|| Compression {
        algorithm: compression_algo.unwrap_or(CompressionAlgorithm::Zstd),
        level: Some(3),
    });
    BackupMetadata {
        backup_id: id.to_string(),
        instance_name: "main".to_string(),
        backup_type,
        backup_mode: if matches!(backup_type, BackupType::Full) {
            pbkfs::backup::BackupMode::Full
        } else {
            pbkfs::backup::BackupMode::Delta
        },
        parent_id: parent.map(|p| p.to_string()),
        start_time: "2024-01-01T00:00:00Z".to_string(),
        status,
        compressed,
        compression,
        size_bytes: 1024,
        checksum_state: ChecksumState::Verified,
    }
}

#[test]
fn constructs_chain_from_incremental() -> pbkfs::Result<()> {
    let backups = vec![
        meta(
            "FULL1",
            None,
            BackupType::Full,
            true,
            Some(CompressionAlgorithm::Zstd),
        ),
        meta(
            "INC1",
            Some("FULL1"),
            BackupType::Incremental,
            true,
            Some(CompressionAlgorithm::Zstd),
        ),
        meta("INC2", Some("INC1"), BackupType::Incremental, false, None),
    ];
    let tmp = tempdir()?;
    fs::create_dir_all(tmp.path().join("backups/main"))?;
    let store = BackupStore::new(tmp.path(), "main", "2.6.0", backups, StoreLayout::Native)?;

    let chain = BackupChain::from_target_backup(&store, "INC2")?;
    assert_eq!(3, chain.elements.len());
    assert_eq!("FULL1", chain.elements.first().unwrap().backup_id);
    assert_eq!("INC2", chain.elements.last().unwrap().backup_id);
    assert_eq!(ChainIntegrity::Valid, chain.integrity_state);
    assert_eq!(CompressionMix::Mixed, chain.compressed_mix);

    Ok(())
}

#[test]
fn marks_chain_incomplete_when_parent_missing() -> pbkfs::Result<()> {
    let backups = vec![meta(
        "INC1",
        Some("MISSING"),
        BackupType::Incremental,
        false,
        None,
    )];
    let tmp = tempdir()?;
    fs::create_dir_all(tmp.path().join("backups/main"))?;
    let store = BackupStore::new(tmp.path(), "main", "2.6.0", backups, StoreLayout::Native)?;

    let chain = BackupChain::from_target_backup(&store, "INC1")?;
    assert_eq!(ChainIntegrity::Incomplete, chain.integrity_state);
    assert_eq!(1, chain.elements.len());

    Ok(())
}

#[test]
fn captures_multiple_compression_algorithms() -> pbkfs::Result<()> {
    let backups = vec![
        meta(
            "FULL1",
            None,
            BackupType::Full,
            true,
            Some(CompressionAlgorithm::Zlib),
        ),
        meta(
            "INC1",
            Some("FULL1"),
            BackupType::Incremental,
            true,
            Some(CompressionAlgorithm::Zstd),
        ),
    ];
    let tmp = tempdir()?;
    fs::create_dir_all(tmp.path().join("backups/main"))?;
    let store = BackupStore::new(tmp.path(), "main", "2.6.0", backups, StoreLayout::Native)?;
    let chain = BackupChain::from_target_backup(&store, "INC1")?;
    assert_eq!(2, chain.compression_algorithms.len());
    assert!(chain
        .compression_algorithms
        .contains(&CompressionAlgorithm::Zlib));
    assert!(chain
        .compression_algorithms
        .contains(&CompressionAlgorithm::Zstd));
    Ok(())
}

#[test]
fn accepts_done_status_as_valid() -> pbkfs::Result<()> {
    let backups = vec![
        meta_with_status(
            "FULL1",
            None,
            BackupType::Full,
            false,
            None,
            BackupStatus::Done,
        ),
        meta_with_status(
            "INC1",
            Some("FULL1"),
            BackupType::Incremental,
            false,
            None,
            BackupStatus::Done,
        ),
    ];
    let tmp = tempdir()?;
    fs::create_dir_all(tmp.path().join("backups/main"))?;
    let store = BackupStore::new(tmp.path(), "main", "2.6.0", backups, StoreLayout::Native)?;
    let chain = BackupChain::from_target_backup(&store, "INC1")?;

    // Both DONE and OK should result in Valid chain
    assert_eq!(ChainIntegrity::Valid, chain.integrity_state);
    assert_eq!(2, chain.elements.len());

    // Verify is_ok() returns true for Done status
    assert!(chain.elements[0].is_ok());
    assert!(chain.elements[1].is_ok());

    Ok(())
}

#[test]
fn marks_chain_corrupt_when_backup_has_error_status() -> pbkfs::Result<()> {
    let backups = vec![
        meta_with_status(
            "FULL1",
            None,
            BackupType::Full,
            false,
            None,
            BackupStatus::Ok,
        ),
        meta_with_status(
            "INC1",
            Some("FULL1"),
            BackupType::Incremental,
            false,
            None,
            BackupStatus::Corrupt,
        ),
    ];
    let tmp = tempdir()?;
    fs::create_dir_all(tmp.path().join("backups/main"))?;
    let store = BackupStore::new(tmp.path(), "main", "2.6.0", backups, StoreLayout::Native)?;
    let chain = BackupChain::from_target_backup(&store, "INC1")?;

    // Chain with corrupt backup should be marked as Corrupt
    assert_eq!(ChainIntegrity::Corrupt, chain.integrity_state);

    Ok(())
}

#[test]
fn mixed_ok_and_done_statuses_are_valid() -> pbkfs::Result<()> {
    let backups = vec![
        meta_with_status(
            "FULL1",
            None,
            BackupType::Full,
            false,
            None,
            BackupStatus::Ok,
        ),
        meta_with_status(
            "INC1",
            Some("FULL1"),
            BackupType::Incremental,
            false,
            None,
            BackupStatus::Done,
        ),
        meta_with_status(
            "INC2",
            Some("INC1"),
            BackupType::Incremental,
            false,
            None,
            BackupStatus::Ok,
        ),
    ];
    let tmp = tempdir()?;
    fs::create_dir_all(tmp.path().join("backups/main"))?;
    let store = BackupStore::new(tmp.path(), "main", "2.6.0", backups, StoreLayout::Native)?;
    let chain = BackupChain::from_target_backup(&store, "INC2")?;

    // Mix of OK and DONE should still be Valid
    assert_eq!(ChainIntegrity::Valid, chain.integrity_state);
    assert!(chain.elements.iter().all(|b| b.is_ok()));

    Ok(())
}
