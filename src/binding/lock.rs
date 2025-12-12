use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{backup::metadata::new_binding_id, fs::overlay, Error, Result};

pub const BINDING_FILE: &str = ".pbkfs-binding.json";
pub const LOCK_FILE: &str = ".pbkfs-lock";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LockMarker {
    pub mount_id: Uuid,
    pub diff_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BindingRecord {
    pub binding_id: Uuid,
    pub instance_name: String,
    pub backup_id: String,
    pub pbk_store_path: PathBuf,
    pub pbk_target_path: PathBuf,
    pub created_at: u64,
    pub last_used_at: u64,
    pub owner_pid: i32,
    pub owner_host: String,
    pub pbkfs_version: String,
    pub state: BindingState,
    /// Marker that this diff directory has ever been mounted with `--no-wal`.
    /// Defaults to false for backward compatibility with older binding files.
    #[serde(default)]
    pub no_wal_used: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BindingState {
    Active,
    Released,
    Stale,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiffDir {
    pub path: PathBuf,
    pub writable: bool,
}

impl DiffDir {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        let writable = path.is_dir() && is_writable(path);
        Ok(Self {
            path: path.to_path_buf(),
            writable,
        })
    }

    pub fn ensure_writable(&self) -> Result<()> {
        if !self.writable {
            return Err(Error::DiffDirNotWritable(self.path.display().to_string()).into());
        }
        Ok(())
    }

    pub fn ensure_directory(&self) -> Result<()> {
        if !self.path.is_dir() {
            return Err(Error::Cli("diff_dir must be a directory".into()).into());
        }
        Ok(())
    }

    pub fn write_lock(&self, mount_id: Uuid) -> Result<()> {
        let marker = LockMarker {
            mount_id,
            diff_dir: self.path.clone(),
        };
        let data = serde_json::to_vec_pretty(&marker)?;
        std::fs::write(self.path.join(LOCK_FILE), data)?;
        Ok(())
    }

    pub fn lock_path(&self) -> PathBuf {
        self.path.join(LOCK_FILE)
    }

    pub fn binding_path(&self) -> PathBuf {
        self.path.join(BINDING_FILE)
    }

    pub fn load_binding(&self) -> Result<Option<BindingRecord>> {
        let path = self.binding_path();
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read(path)?;
        let record: BindingRecord = serde_json::from_slice(&contents)?;
        Ok(Some(record))
    }
}

/// Cleanup a diff directory, respecting active mount detection unless forced.
///
/// * Validates writability and presence of lock/binding metadata.
/// * Rejects cleanup when an active owner is detected unless `force` is set.
/// * Removes lock/binding files and wipes diff data while keeping root dir.
pub fn cleanup_diff_dir(diff_dir: &DiffDir, force: bool) -> Result<()> {
    if diff_dir.path.parent().is_none() {
        return Err(Error::Cli("refusing to cleanup root path".into()).into());
    }

    diff_dir.ensure_directory()?;
    diff_dir.ensure_writable()?;

    let lock_exists = diff_dir.lock_path().exists();
    let binding = diff_dir.load_binding()?;

    let active_pid = binding.as_ref().and_then(|b| {
        if b.is_owner_alive() {
            Some(b.owner_pid)
        } else {
            None
        }
    });

    if let Some(pid) = active_pid {
        if !force {
            warn!(
                pid,
                diff_dir = %diff_dir.path.display(),
                "cleanup blocked: active mount detected"
            );
            return Err(Error::BindingInUse(pid).into());
        }
        warn!(pid, diff_dir = %diff_dir.path.display(), "forcing cleanup despite active mount");
    }

    if lock_exists && binding.is_none() && !force {
        warn!(diff_dir = %diff_dir.path.display(), "cleanup blocked: lock present without binding");
        return Err(Error::Cli(
            "lock present without binding metadata; unmount first or use --force".into(),
        )
        .into());
    }

    if lock_exists {
        fs::remove_file(diff_dir.lock_path())?;
    }

    overlay::wipe_diff_dir(&diff_dir.path)?;

    if diff_dir.binding_path().exists() {
        fs::remove_file(diff_dir.binding_path())?;
    }

    if binding.is_some() {
        info!(diff_dir = %diff_dir.path.display(), force, "binding cleared during cleanup");
    }
    info!(diff_dir = %diff_dir.path.display(), force, "cleanup completed");
    Ok(())
}

impl BindingRecord {
    pub fn new(
        instance_name: impl Into<String>,
        backup_id: impl Into<String>,
        pbk_store_path: impl Into<PathBuf>,
        pbk_target_path: impl Into<PathBuf>,
        owner_pid: i32,
        owner_host: impl Into<String>,
        pbkfs_version: impl Into<String>,
    ) -> Self {
        let now = now_secs();
        Self {
            binding_id: new_binding_id(),
            instance_name: instance_name.into(),
            backup_id: backup_id.into(),
            pbk_store_path: pbk_store_path.into(),
            pbk_target_path: pbk_target_path.into(),
            created_at: now,
            last_used_at: now,
            owner_pid,
            owner_host: owner_host.into(),
            pbkfs_version: pbkfs_version.into(),
            state: BindingState::Active,
            no_wal_used: false,
        }
    }

    pub fn touch(&mut self) {
        self.last_used_at = now_secs();
    }

    pub fn mark_stale(&mut self) {
        self.state = BindingState::Stale;
    }

    pub fn mark_released(&mut self) {
        self.state = BindingState::Released;
    }

    pub fn write_to_diff(&self, diff_dir: &DiffDir) -> Result<()> {
        diff_dir.ensure_writable()?;
        let binding_path = diff_dir.path.join(BINDING_FILE);
        let data = serde_json::to_vec_pretty(self)?;
        fs::write(binding_path, data)?;
        Ok(())
    }

    pub fn load_from_diff(diff_dir: &DiffDir) -> Result<Self> {
        let path = diff_dir.path.join(BINDING_FILE);
        let contents = fs::read(&path)?;
        let record: BindingRecord = serde_json::from_slice(&contents)?;
        Ok(record)
    }

    pub fn validate_store_path(&self, pbk_store: &Path) -> Result<()> {
        if !paths_match(&self.pbk_store_path, pbk_store) {
            return Err(Error::BindingViolation {
                expected: self.pbk_store_path.display().to_string(),
                actual: pbk_store.display().to_string(),
            }
            .into());
        }
        Ok(())
    }

    pub fn validate_binding_args(
        &self,
        instance: Option<&str>,
        backup: Option<&str>,
    ) -> Result<()> {
        if let Some(inst) = instance {
            if !self.instance_name.eq(inst) {
                return Err(Error::BindingViolation {
                    expected: self.instance_name.clone(),
                    actual: inst.to_string(),
                }
                .into());
            }
        }

        if let Some(backup_id) = backup {
            // pg_probackup backup IDs are case-insensitive (commonly uppercased)
            if !self.backup_id.eq_ignore_ascii_case(backup_id) {
                return Err(Error::BindingViolation {
                    expected: self.backup_id.clone(),
                    actual: backup_id.to_string(),
                }
                .into());
            }
        }

        Ok(())
    }

    pub fn refresh_for_reuse(
        &mut self,
        pbk_target_path: impl Into<PathBuf>,
        owner_pid: i32,
        owner_host: impl Into<String>,
    ) {
        self.pbk_target_path = pbk_target_path.into();
        self.owner_pid = owner_pid;
        self.owner_host = owner_host.into();
        self.state = BindingState::Active;
        self.touch();
    }

    pub fn is_owner_alive(&self) -> bool {
        pid_alive(self.owner_pid)
    }
}

impl LockMarker {
    pub fn read(path: &Path) -> Result<Self> {
        let data = std::fs::read(path)?;
        let marker: LockMarker = serde_json::from_slice(&data)?;
        Ok(marker)
    }

    pub fn from_diff_dir(path: &Path) -> Result<Self> {
        let data = std::fs::read(path.join(LOCK_FILE))?;
        let marker: LockMarker = serde_json::from_slice(&data)?;
        Ok(marker)
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn is_writable(path: &Path) -> bool {
    let test_file = path.join(".pbkfs_write_test");
    match fs::write(&test_file, b"pbkfs") {
        Ok(_) => fs::remove_file(test_file).is_ok(),
        Err(_) => false,
    }
}

fn pid_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    // Linux-only: rely on /proc/<pid> presence to detect liveness
    let path = PathBuf::from("/proc").join(pid.to_string());
    path.exists()
}

fn canonicalize_or(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn paths_match(a: &Path, b: &Path) -> bool {
    canonicalize_or(a) == canonicalize_or(b)
}
