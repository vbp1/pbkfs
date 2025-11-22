use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::{
    backup::metadata::{BackupStore, CompressionAlgorithm},
    binding::BindingRecord,
    Error, Result,
};

use super::metadata::{BackupMetadata, BackupType};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChainIntegrity {
    Valid,
    Incomplete,
    Corrupt,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CompressionMix {
    AllCompressed,
    AllUncompressed,
    Mixed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupChain {
    pub target_backup_id: String,
    pub elements: Vec<BackupMetadata>,
    pub compressed_mix: CompressionMix,
    pub integrity_state: ChainIntegrity,
    pub compression_algorithms: Vec<CompressionAlgorithm>,
}

impl BackupChain {
    pub fn from_target_backup(store: &BackupStore, target_backup_id: &str) -> Result<Self> {
        store.validate()?;

        let mut lookup: HashMap<String, &BackupMetadata> = HashMap::new();
        for backup in &store.backups {
            lookup.insert(backup.backup_id.clone(), backup);
        }

        let mut seen = HashSet::new();
        let mut current_id = target_backup_id.to_string();
        let mut chain: Vec<BackupMetadata> = Vec::new();
        let mut integrity = ChainIntegrity::Valid;

        while let Some(bk) = lookup.get(&current_id) {
            if !bk.is_ok() {
                integrity = ChainIntegrity::Corrupt;
            }
            if seen.contains(&bk.backup_id) {
                return Err(Error::ChainCycle(bk.backup_id.clone()).into());
            }
            seen.insert(bk.backup_id.clone());

            chain.push((*bk).clone());
            match bk.parent_id.as_deref() {
                None => break,
                Some(parent) => current_id = parent.to_string(),
            }
        }

        chain.reverse();

        // Validate chain starts with a FULL backup.
        if chain.is_empty() || !matches!(chain.first().unwrap().backup_type, BackupType::Full) {
            integrity = ChainIntegrity::Incomplete;
        }

        if !matches!(chain.last().map(|b| b.backup_id.as_str()), Some(id) if id.eq_ignore_ascii_case(target_backup_id))
        {
            return Err(Error::MissingBackup(target_backup_id.to_string()).into());
        }

        let compressed_mix = classify_compression(&chain);
        let compression_algorithms = compression_algorithms(&chain);

        Ok(Self {
            target_backup_id: target_backup_id.to_string(),
            elements: chain,
            compressed_mix,
            integrity_state: integrity,
            compression_algorithms,
        })
    }

    pub fn from_binding(store: &BackupStore, binding: &BindingRecord) -> Result<Self> {
        binding.validate_store_path(&store.path)?;
        store
            .find_backup(&binding.backup_id)
            .ok_or_else(|| Error::MissingBackup(binding.backup_id.clone()))?;
        Self::from_target_backup(store, &binding.backup_id)
    }
}

fn classify_compression(chain: &[BackupMetadata]) -> CompressionMix {
    let mut compressed = 0usize;
    let mut uncompressed = 0usize;
    for bk in chain {
        if bk.is_compressed() {
            compressed += 1;
        } else {
            uncompressed += 1;
        }
    }

    match (compressed, uncompressed) {
        (0, _) => CompressionMix::AllUncompressed,
        (_, 0) => CompressionMix::AllCompressed,
        _ => CompressionMix::Mixed,
    }
}

fn compression_algorithms(chain: &[BackupMetadata]) -> Vec<CompressionAlgorithm> {
    let mut set = HashSet::new();
    for bk in chain {
        if let Some(algo) = bk.compression_algorithm() {
            set.insert(algo);
        }
    }
    let mut list: Vec<_> = set.into_iter().collect();
    list.sort_by_key(|a| match a {
        CompressionAlgorithm::Zlib => 0,
        CompressionAlgorithm::Lz4 => 1,
        CompressionAlgorithm::Zstd => 2,
    });
    list
}
