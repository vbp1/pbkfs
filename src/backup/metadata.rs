use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Result};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BackupType {
    Full,
    Incremental,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BackupStatus {
    Ok,
    Corrupt,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChecksumState {
    Verified,
    NotVerified,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupMetadata {
    pub backup_id: String,
    pub instance_name: String,
    pub backup_type: BackupType,
    pub parent_id: Option<String>,
    pub start_time: String,
    pub status: BackupStatus,
    pub compressed: bool,
    pub size_bytes: u64,
    pub checksum_state: ChecksumState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupStore {
    pub path: PathBuf,
    pub instance_name: String,
    pub backups: Vec<BackupMetadata>,
    pub version_pg_probackup: String,
    pub version_postgres_supported: (u16, u16),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializedStore {
    instance_name: String,
    version_pg_probackup: String,
    backups: Vec<BackupMetadata>,
}

impl BackupStore {
    pub fn new<P: AsRef<Path>>(
        path: P,
        instance_name: impl Into<String>,
        version_pg_probackup: impl Into<String>,
        backups: Vec<BackupMetadata>,
    ) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(Error::InvalidStorePath(path.display().to_string()).into());
        }

        Ok(Self {
            path: path.to_path_buf(),
            instance_name: instance_name.into(),
            backups,
            version_pg_probackup: version_pg_probackup.into(),
            version_postgres_supported: (14, 17),
        })
    }

    /// Minimal validation that the store is usable for mounting.
    pub fn validate(&self) -> Result<()> {
        if !self.path.exists() {
            return Err(Error::InvalidStorePath(self.path.display().to_string()).into());
        }
        if !self.path.is_dir() {
            return Err(Error::InvalidStorePath(self.path.display().to_string()).into());
        }
        if self.version_pg_probackup.trim().is_empty() {
            return Err(
                Error::UnsupportedPgProbackupVersion(self.version_pg_probackup.clone()).into(),
            );
        }
        if !version_supported(&self.version_pg_probackup) {
            return Err(
                Error::UnsupportedPgProbackupVersion(self.version_pg_probackup.clone()).into(),
            );
        }
        Ok(())
    }

    pub fn find_backup(&self, backup_id: &str) -> Option<&BackupMetadata> {
        self.backups
            .iter()
            .find(|b| b.backup_id.eq_ignore_ascii_case(backup_id))
    }

    /// Load backup store metadata from a JSON payload produced by pg_probackup.
    ///
    /// For this codebase we expect a fixture file `<pbk_store>/backups.json`
    /// containing a serialized [`SerializedStore`].
    pub fn load_from_pg_probackup<P: AsRef<Path>>(path: P, instance: &str) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(Error::InvalidStorePath(path.display().to_string()).into());
        }

        let meta_path = path.join("backups.json");
        let contents = std::fs::read(&meta_path)?;
        let parsed: SerializedStore = serde_json::from_slice(&contents)?;

        if parsed.instance_name != instance {
            return Err(Error::BindingViolation {
                expected: instance.to_string(),
                actual: parsed.instance_name,
            }
            .into());
        }

        let store = BackupStore::new(
            path,
            parsed.instance_name,
            parsed.version_pg_probackup.clone(),
            parsed.backups,
        )?;
        store.validate()?;
        Ok(store)
    }
}

impl BackupMetadata {
    pub fn is_ok(&self) -> bool {
        matches!(self.status, BackupStatus::Ok)
    }

    pub fn ensure_instance(&self, instance: &str) -> Result<()> {
        if self.instance_name != instance {
            return Err(Error::BindingViolation {
                expected: instance.to_string(),
                actual: self.instance_name.clone(),
            }
            .into());
        }
        Ok(())
    }
}

/// Convenience helper for binding identifiers.
pub fn new_binding_id() -> Uuid {
    Uuid::new_v4()
}

fn version_supported(version: &str) -> bool {
    let mut parts = version.split('.').filter_map(|p| p.parse::<u32>().ok());
    match (parts.next(), parts.next()) {
        (Some(major), Some(minor)) => (major == 2 && minor >= 5) || major > 2,
        _ => false,
    }
}
