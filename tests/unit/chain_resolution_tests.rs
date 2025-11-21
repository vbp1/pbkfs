use pbkfs::backup::chain::{BackupChain, ChainIntegrity, CompressionMix};
use pbkfs::backup::metadata::{
    BackupMetadata, BackupStatus, BackupStore, BackupType, ChecksumState, Compression,
    CompressionAlgorithm,
};

fn meta(
    id: &str,
    parent: Option<&str>,
    backup_type: BackupType,
    compressed: bool,
    compression_algo: Option<CompressionAlgorithm>,
) -> BackupMetadata {
    let compression = compressed.then(|| Compression {
        algorithm: compression_algo.unwrap_or(CompressionAlgorithm::Zstd),
        level: Some(3),
    });
    BackupMetadata {
        backup_id: id.to_string(),
        instance_name: "main".to_string(),
        backup_type,
        parent_id: parent.map(|p| p.to_string()),
        start_time: "2024-01-01T00:00:00Z".to_string(),
        status: BackupStatus::Ok,
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
    let store = BackupStore::new("/tmp", "main", "2.6.0", backups)?;

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
    let store = BackupStore::new("/tmp", "main", "2.6.0", backups)?;

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
    let store = BackupStore::new("/tmp", "main", "2.6.0", backups)?;
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
