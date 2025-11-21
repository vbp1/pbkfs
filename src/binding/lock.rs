use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{backup::metadata::new_binding_id, Error, Result};

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

    pub fn write_lock(&self, mount_id: Uuid) -> Result<()> {
        let marker = LockMarker {
            mount_id,
            diff_dir: self.path.clone(),
        };
        let data = serde_json::to_vec_pretty(&marker)?;
        std::fs::write(self.path.join(LOCK_FILE), data)?;
        Ok(())
    }
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
}

impl LockMarker {
    pub fn read(path: &Path) -> Result<Self> {
        let data = std::fs::read(path)?;
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
