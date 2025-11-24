use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use tracing::debug;
use uuid::Uuid;

use crate::{Error, Result};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum StoreLayout {
    /// Native pg_probackup catalog layout: backups/<instance>/<id>/database
    Native,
    /// JSON-only fallback used for tests or environments without pg_probackup
    JsonFallback,
}

impl std::fmt::Display for StoreLayout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreLayout::Native => write!(f, "native"),
            StoreLayout::JsonFallback => write!(f, "json-fallback"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BackupType {
    Full,
    Incremental,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BackupMode {
    Full,
    Delta,
    Page,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BackupStatus {
    Ok,
    Done,
    Corrupt,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChecksumState {
    Verified,
    NotVerified,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum CompressionAlgorithm {
    Zlib,
    Lz4,
    Zstd,
}

impl CompressionAlgorithm {
    pub fn from_pg_probackup(value: &str) -> Result<Self> {
        match value.to_lowercase().as_str() {
            "zlib" => Ok(Self::Zlib),
            "lz4" => Ok(Self::Lz4),
            "zstd" | "zstandard" => Ok(Self::Zstd),
            other => Err(Error::UnsupportedCompressionAlgorithm(other.to_string()).into()),
        }
    }
}

impl<'de> Deserialize<'de> for CompressionAlgorithm {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        CompressionAlgorithm::from_pg_probackup(&raw).map_err(|_| {
            serde::de::Error::custom(format!("unsupported compression algorithm: {raw}"))
        })
    }
}

impl FromStr for CompressionAlgorithm {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::from_pg_probackup(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Compression {
    pub algorithm: CompressionAlgorithm,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupMetadata {
    pub backup_id: String,
    pub instance_name: String,
    pub backup_type: BackupType,
    pub backup_mode: BackupMode,
    pub parent_id: Option<String>,
    pub start_time: String,
    pub status: BackupStatus,
    pub compressed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,
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
    pub layout: StoreLayout,
}

#[derive(Debug, Clone, Deserialize)]
struct ShowBackupJson {
    id: String,
    #[serde(rename = "parent-backup-id")]
    parent_backup_id: Option<String>,
    #[serde(rename = "backup-mode")]
    backup_mode: String,
    status: String,
    #[serde(rename = "compress-alg")]
    compress_alg: String,
    #[serde(rename = "compress-level")]
    compress_level: Option<u8>,
    #[serde(rename = "start-time")]
    start_time: String,
    #[serde(rename = "data-bytes")]
    data_bytes: Option<u64>,
    #[serde(rename = "uncompressed-bytes")]
    #[allow(dead_code)]
    uncompressed_bytes: Option<u64>,
    #[serde(rename = "wal-bytes")]
    #[allow(dead_code)]
    wal_bytes: Option<u64>,
    #[serde(rename = "program-version")]
    program_version: Option<String>,
    #[serde(rename = "server-version")]
    #[allow(dead_code)]
    server_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ShowInstanceJson {
    instance: String,
    backups: Vec<ShowBackupJson>,
}

impl BackupStore {
    pub fn new<P: AsRef<Path>>(
        path: P,
        instance_name: impl Into<String>,
        version_pg_probackup: impl Into<String>,
        backups: Vec<BackupMetadata>,
        layout: StoreLayout,
    ) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(Error::InvalidStorePath(path.display().to_string()).into());
        }

        let instance_name = instance_name.into();
        match layout {
            StoreLayout::Native => {
                let backups_dir = path.join("backups").join(&instance_name);
                if !backups_dir.is_dir() {
                    return Err(Error::InvalidStoreLayout(format!(
                        "layout=native but {} is missing or not a directory",
                        backups_dir.display()
                    ))
                    .into());
                }
            }
            StoreLayout::JsonFallback => {
                let json_file = path.join("backups.json");
                if !json_file.exists() {
                    return Err(Error::InvalidStoreLayout(format!(
                        "layout=json-fallback but {} is missing",
                        json_file.display()
                    ))
                    .into());
                }
            }
        }

        Ok(Self {
            path: path.to_path_buf(),
            instance_name,
            backups,
            version_pg_probackup: version_pg_probackup.into(),
            version_postgres_supported: (14, 17),
            layout,
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
        if !self.version_pg_probackup.trim().is_empty()
            && !version_supported(&self.version_pg_probackup)
        {
            return Err(
                Error::UnsupportedPgProbackupVersion(self.version_pg_probackup.clone()).into(),
            );
        }
        for backup in &self.backups {
            backup.validate_compression()?;
        }
        Ok(())
    }

    pub fn find_backup(&self, backup_id: &str) -> Option<&BackupMetadata> {
        self.backups
            .iter()
            .find(|b| b.backup_id.eq_ignore_ascii_case(backup_id))
    }

    /// Resolve the expected data root for a given backup id, preferring native layout.
    pub fn backup_data_root(&self, backup: &BackupMetadata) -> Result<PathBuf> {
        let native = self
            .path
            .join("backups")
            .join(&self.instance_name)
            .join(&backup.backup_id)
            .join("database");
        if native.exists() {
            return Ok(native);
        }

        // Legacy fallback (pre-native layout) for compatibility with older fixtures.
        let legacy = self.path.join(&backup.backup_id).join("database");
        if legacy.exists() {
            return Ok(legacy);
        }

        Err(Error::InvalidStoreLayout(format!(
            "backup {} data directory not found (tried native: {}, legacy: {})",
            backup.backup_id,
            native.display(),
            legacy.display()
        ))
        .into())
    }

    /// Load backup store metadata by invoking pg_probackup directly.
    ///
    /// This method executes `pg_probackup show -B <path> --instance <instance> --format=json`
    /// and parses the output.
    ///
    /// **Fallback behavior for testing:**
    /// If pg_probackup is not found or fails with a permission error, and a `backups.json`
    /// file exists in the store path, this method will fall back to reading that file.
    /// This allows tests to work without requiring a working pg_probackup installation.
    ///
    /// For explicit file-based loading, use `load_from_json_file` instead.
    pub fn load_from_pg_probackup<P: AsRef<Path>>(path: P, instance: &str) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(Error::InvalidStorePath(path.display().to_string()).into());
        }

        let has_native_layout = path.join("backups").join(instance).is_dir();
        let json_fallback = path.join("backups.json");
        if !has_native_layout && !json_fallback.exists() {
            return Err(Error::InvalidStoreLayout(format!(
                "{} does not look like a pg_probackup store (missing backups/<instance>/... and backups.json)",
                path.display()
            ))
            .into());
        }

        debug!(
            store_path = %path.display(),
            instance = %instance,
            "invoking pg_probackup to fetch backup metadata"
        );

        let pg_probackup_bin =
            std::env::var("PG_PROBACKUP_BIN").unwrap_or_else(|_| "pg_probackup".into());

        let output = match Command::new(&pg_probackup_bin)
            .args([
                "show",
                "-B",
                path.to_str().ok_or_else(|| {
                    Error::Cli(format!("invalid UTF-8 in path: {}", path.display()))
                })?,
                "--instance",
                instance,
                "--format=json",
            ])
            .output()
        {
            Ok(output) => output,
            Err(e) => {
                // Try fallback to backups.json file for testing scenarios
                let fallback_path = path.join("backups.json");
                if fallback_path.exists()
                    && (e.kind() == std::io::ErrorKind::NotFound
                        || e.kind() == std::io::ErrorKind::PermissionDenied)
                {
                    debug!(
                        "pg_probackup invocation failed ({}), falling back to backups.json",
                        e
                    );
                    return Self::load_from_json_file_with_layout(
                        path,
                        &fallback_path,
                        instance,
                        StoreLayout::JsonFallback,
                    );
                }

                return Err(if e.kind() == std::io::ErrorKind::NotFound {
                    Error::PgProbackupMissingBinary(pg_probackup_bin.clone())
                } else if e.kind() == std::io::ErrorKind::PermissionDenied {
                    Error::PgProbackupNotExecutable(pg_probackup_bin.clone())
                } else {
                    Error::Io(e)
                }
                .into());
            }
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::PgProbackupFailed {
                code: output.status.code(),
                message: stderr.trim().to_string(),
            }
            .into());
        }

        let mut instances: Vec<ShowInstanceJson> = serde_json::from_slice(&output.stdout)
            .map_err(|e| Error::PgProbackupInvalidJson(e.to_string()))?;

        let inst = instances
            .drain(..)
            .find(|i| i.instance == instance)
            .ok_or_else(|| Error::PgProbackupInstanceMissing(instance.to_string()))?;

        let version_pg_probackup = inst
            .backups
            .iter()
            .find_map(|b| b.program_version.clone())
            .unwrap_or_else(|| "unknown".into());

        let backups = inst
            .backups
            .into_iter()
            .map(|b| backup_from_show(instance, b))
            .collect::<Result<Vec<_>>>()?;

        let layout = if has_native_layout {
            StoreLayout::Native
        } else {
            StoreLayout::JsonFallback
        };

        let store = BackupStore::new(path, inst.instance, version_pg_probackup, backups, layout)?;
        store.validate()?;
        Ok(store)
    }

    /// Load backup store metadata from a pre-generated JSON file.
    ///
    /// This is useful for testing or scenarios where you've already run:
    /// `pg_probackup show -B <path> --instance <instance> --format=json > backups.json`
    ///
    /// Expected format: [ { "instance": "name", "backups": [ { .. } ] } ]
    pub fn load_from_json_file<P: AsRef<Path>>(
        store_path: P,
        json_path: P,
        instance: &str,
    ) -> Result<Self> {
        Self::load_from_json_file_with_layout(
            store_path,
            json_path,
            instance,
            StoreLayout::JsonFallback,
        )
    }

    fn load_from_json_file_with_layout<P: AsRef<Path>>(
        store_path: P,
        json_path: P,
        instance: &str,
        layout: StoreLayout,
    ) -> Result<Self> {
        let store_path = store_path.as_ref();
        if !store_path.exists() {
            return Err(Error::InvalidStorePath(store_path.display().to_string()).into());
        }

        let json_path = json_path.as_ref();
        debug!(
            json_file = %json_path.display(),
            instance = %instance,
            "loading backup metadata from JSON file"
        );

        let contents = std::fs::read(json_path)?;
        let mut instances: Vec<ShowInstanceJson> = serde_json::from_slice(&contents)?;

        let inst = instances
            .drain(..)
            .find(|i| i.instance == instance)
            .ok_or_else(|| Error::BindingViolation {
                expected: instance.to_string(),
                actual: "<missing>".into(),
            })?;

        let version_pg_probackup = inst
            .backups
            .iter()
            .find_map(|b| b.program_version.clone())
            .unwrap_or_else(|| "unknown".into());

        let backups = inst
            .backups
            .into_iter()
            .map(|b| backup_from_show(instance, b))
            .collect::<Result<Vec<_>>>()?;

        let store = BackupStore::new(
            store_path,
            inst.instance,
            version_pg_probackup,
            backups,
            layout,
        )?;
        store.validate()?;
        Ok(store)
    }
}

impl BackupMetadata {
    pub fn is_ok(&self) -> bool {
        matches!(self.status, BackupStatus::Ok | BackupStatus::Done)
    }

    pub fn is_compressed(&self) -> bool {
        self.compressed || self.compression.is_some()
    }

    pub fn compression_algorithm(&self) -> Option<CompressionAlgorithm> {
        self.compression.as_ref().map(|c| c.algorithm)
    }

    pub fn compression_level(&self) -> Option<u8> {
        self.compression.as_ref().and_then(|c| c.level)
    }

    pub fn validate_compression(&self) -> Result<()> {
        if !self.is_compressed() {
            return Ok(());
        }

        if self.compression.is_none() {
            return Err(Error::MissingCompressionMetadata(self.backup_id.clone()).into());
        }

        Ok(())
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

fn backup_from_show(instance: &str, b: ShowBackupJson) -> Result<BackupMetadata> {
    let compression = if b.compress_alg.to_lowercase() == "none" {
        None
    } else {
        Some(Compression {
            algorithm: CompressionAlgorithm::from_pg_probackup(&b.compress_alg)?,
            level: b.compress_level,
        })
    };

    let (backup_type, backup_mode) = match b.backup_mode.to_uppercase().as_str() {
        "FULL" => (BackupType::Full, BackupMode::Full),
        "DELTA" => (BackupType::Incremental, BackupMode::Delta),
        "PAGE" => (BackupType::Incremental, BackupMode::Page),
        _ => (BackupType::Incremental, BackupMode::Unknown),
    };

    let status = match b.status.to_uppercase().as_str() {
        "OK" => BackupStatus::Ok,
        "DONE" => BackupStatus::Done,
        "ERROR" | "CORRUPT" => BackupStatus::Corrupt,
        _ => BackupStatus::Unknown,
    };

    Ok(BackupMetadata {
        backup_id: b.id,
        instance_name: instance.to_string(),
        backup_type,
        backup_mode,
        parent_id: b.parent_backup_id,
        start_time: b.start_time,
        status,
        compressed: compression.is_some(),
        compression,
        size_bytes: b.data_bytes.unwrap_or(0),
        checksum_state: ChecksumState::NotVerified,
    })
}

fn version_supported(version: &str) -> bool {
    let mut parts = version.split('.').filter_map(|p| p.parse::<u32>().ok());
    match (parts.next(), parts.next()) {
        (Some(major), Some(minor)) => (major == 2 && minor >= 5) || major > 2,
        _ => false,
    }
}
